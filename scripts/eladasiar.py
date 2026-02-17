#!/usr/bin/env python3
"""Post-process #1 report: append SellRef vs current (PM/AH) price deltas for watchlist lines.

Design goals:
 - Keep report_runner.py untouched (or minimally touched in workflow only).
 - Never fail the workflow: if price fetch is blocked, exit 0 and leave report as-is.
 - Read sell reference prices from the same master.csv that runner downloaded.

Usage (GitHub Actions step):
  python3 scripts/eladasiar.py --master reports/master.csv --report reports/summary_report_1.md
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Dict, Iterable, List, Optional, Tuple


UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _now_budapest() -> datetime:
    """Return current time in Europe/Budapest without external deps."""
    try:
        from zoneinfo import ZoneInfo  # py3.9+

        return datetime.now(tz=ZoneInfo("Europe/Budapest"))
    except Exception:
        # Fallback: local time (best-effort)
        return datetime.now()


def _session_label(now: datetime) -> Optional[str]:
    """Return 'PM' or 'AH' if we are inside the report's relevant window; else None."""
    t = now.timetz() if hasattr(now, "timetz") else now.time()

    # Premarket: 10:00–15:30 CET/CEST
    if dtime(10, 0) <= t.replace(tzinfo=None) <= dtime(15, 30):
        return "PM"

    # After-hours: 22:00–02:00 (wrap)
    t_naive = t.replace(tzinfo=None)
    if t_naive >= dtime(22, 0) or t_naive <= dtime(2, 0):
        return "AH"

    return None


def _parse_price(val: str) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # HU decimal comma
    s = s.replace(" ", "").replace("\xa0", "").replace(",", ".")
    # strip currency symbols
    s = re.sub(r"[^0-9.\-]", "", s)
    if not s or s in {"-", "."}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def read_sellref_master_csv(path: str) -> Dict[str, float]:
    """Build ticker -> last non-empty sell reference price (Eladasi ar)."""
    sell: Dict[str, float] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        # Normalize header names (strip)
        field_map = {k.strip(): k for k in (reader.fieldnames or [])}
        tcol = field_map.get("Ticker")
        scol = field_map.get("Eladasi ar")
        if not tcol or not scol:
            return sell
        for row in reader:
            t = (row.get(tcol) or "").strip().upper()
            if not t:
                continue
            p = _parse_price(row.get(scol, ""))
            if p is None:
                continue
            # Rule: last non-empty wins
            sell[t] = p
    return sell


def http_json(url: str, timeout: int = 25) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "close",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode("utf-8", errors="replace")
    return json.loads(raw)


@dataclass
class Quote:
    regular: Optional[float] = None
    pre: Optional[float] = None
    post: Optional[float] = None


def _yahoo_quote_batch(symbols: List[str], chunk: int = 20) -> Dict[str, Quote]:
    """Fetch quotes via Yahoo v7 quote endpoint. Returns partial results on failures."""
    out: Dict[str, Quote] = {s: Quote() for s in symbols}
    if not symbols:
        return out

    base = "https://query1.finance.yahoo.com/v7/finance/quote"
    for i in range(0, len(symbols), chunk):
        batch = symbols[i : i + chunk]
        qs = urllib.parse.urlencode({"symbols": ",".join(batch)})
        url = f"{base}?{qs}"
        try:
            data = http_json(url)
            results = (data.get("quoteResponse") or {}).get("result") or []
            for it in results:
                sym = (it.get("symbol") or "").upper()
                if not sym:
                    continue
                q = out.get(sym) or Quote()
                q.regular = it.get("regularMarketPrice") or q.regular
                q.pre = it.get("preMarketPrice") or q.pre
                q.post = it.get("postMarketPrice") or q.post
                out[sym] = q
        except Exception:
            # swallow (401/blocked/etc.) and move on
            continue
        time.sleep(0.15)
    return out


def _pick_current(q: Quote, session: str) -> Optional[float]:
    if session == "PM":
        return q.pre if q.pre is not None else q.regular
    if session == "AH":
        return q.post if q.post is not None else q.regular
    return None


def _format_delta(now_px: float, sell_px: float) -> str:
    if sell_px == 0:
        return "n/a"
    pct = (now_px / sell_px - 1.0) * 100.0
    return f"{pct:+.2f}%"


WATCHLIST_HEADER_RE = re.compile(r"^##\s+Watchlist\s*$", re.IGNORECASE)
TICK_LINE_RE = re.compile(r"^-\s+([A-Z0-9.\-]+)\s+—\s+AH\s+.*\|\s+PM\s+.*$", re.IGNORECASE)


def patch_report(report_path: str, sell: Dict[str, float], quotes: Dict[str, Quote], session: str) -> Tuple[bool, int]:
    """Patch only the Watchlist bullet lines. Returns (changed, patched_count)."""
    with open(report_path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    in_watchlist = False
    changed = False
    patched = 0
    new_lines: List[str] = []

    for line in lines:
        if WATCHLIST_HEADER_RE.match(line):
            in_watchlist = True
            new_lines.append(line)
            continue
        if in_watchlist and line.startswith("## ") and not WATCHLIST_HEADER_RE.match(line):
            in_watchlist = False

        if in_watchlist:
            m = TICK_LINE_RE.match(line)
            if m:
                t = m.group(1).upper()
                sell_px = sell.get(t)
                if sell_px is not None:
                    now_px = _pick_current(quotes.get(t, Quote()), session)
                    if now_px is not None:
                        # Avoid double-appending if rerun
                        if "SellRef:" not in line:
                            delta = _format_delta(now_px, sell_px)
                            line = (
                                f"{line} | SellRef: ${sell_px:.2f} → Now({session}) ${now_px:.2f} ({delta})"
                            )
                            changed = True
                            patched += 1
        new_lines.append(line)

    if changed:
        with open(report_path, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(new_lines) + "\n")
    return changed, patched


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--report", default="reports/summary_report_1.md")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    now = _now_budapest()
    session = _session_label(now)
    if session is None:
        # Outside PM/AH windows: per your rule, don't patch.
        if not args.quiet:
            print(f"SellRef postprocess: outside PM/AH window ({now.isoformat()}); skip")
        return 0

    try:
        sell = read_sellref_master_csv(args.master)
    except Exception as e:
        if not args.quiet:
            print(f"SellRef postprocess: cannot read master ({args.master}): {e}")
        return 0

    if not sell:
        if not args.quiet:
            print("SellRef postprocess: no sell prices found; skip")
        return 0

    symbols = sorted(sell.keys())
    quotes = _yahoo_quote_batch(symbols)
    changed, patched = patch_report(args.report, sell, quotes, session)
    if not args.quiet:
        print(
            f"SellRef postprocess: session={session} sellrefs={len(sell)} patched={patched} changed={changed}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
