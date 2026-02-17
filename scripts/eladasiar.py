#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""eladasiar.py — SellRef (Eladási ár) delta patcher for #1 report

Purpose
-------
Post-processes the generated `summary_report_1.md` and appends a compact
SellRef delta to each bullet line where MASTER provides an "Eladasi ar" value.

Design goals
------------
- Must NOT break the workflow: on any network/API issue => exit 0 without changes.
- No runner coupling: operates on the already generated markdown.
- Session-aware: uses PM price during Premarket (10:00–15:30 Europe/Budapest)
  and AH price during After-hours (22:00–02:00 Europe/Budapest). Outside these
  windows it skips (per your #1 definition).
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore


TZ = "Europe/Budapest"


PM_START = time(10, 0)
PM_END = time(15, 30)

AH_START = time(22, 0)
AH_END = time(2, 0)


YQ_URL = "https://query1.finance.yahoo.com/v7/finance/quote"


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}


@dataclass(frozen=True)
class SessionPick:
    label: str  # "PM" or "AH"
    field: str  # Yahoo quote field to prefer


def now_local() -> datetime:
    if ZoneInfo is None:
        return datetime.now()
    return datetime.now(ZoneInfo(TZ))


def in_premarket(dt: datetime) -> bool:
    t = dt.timetz().replace(tzinfo=None)
    return PM_START <= t <= PM_END


def in_afterhours(dt: datetime) -> bool:
    t = dt.timetz().replace(tzinfo=None)
    # window crosses midnight
    return t >= AH_START or t <= AH_END


def pick_session(dt: datetime) -> Optional[SessionPick]:
    if in_premarket(dt):
        return SessionPick(label="PM", field="preMarketPrice")
    if in_afterhours(dt):
        return SessionPick(label="AH", field="postMarketPrice")
    return None


def parse_number(val: str) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # HU decimal comma -> dot
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def load_sell_prices(master_csv: Path) -> Dict[str, float]:
    """Return ticker->sell_price using 'last non-empty wins' rule."""
    with master_csv.open("r", encoding="utf-8", errors="replace", newline="") as f:
        rows = list(csv.DictReader(f))

    out: Dict[str, float] = {}
    # last non-empty wins => scan top->bottom overwriting only when value present
    for r in rows:
        t = (r.get("Ticker") or "").strip().upper()
        if not t:
            continue
        sp = parse_number(r.get("Eladasi ar") or "")
        if sp is None:
            continue
        out[t] = sp
    return out


def chunked(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def yahoo_quote_batch(symbols: List[str]) -> Dict[str, dict]:
    """Best-effort Yahoo quote fetch. Raises on hard failure."""
    if requests is None:
        raise RuntimeError("requests not available")

    m: Dict[str, dict] = {}
    for batch in chunked(symbols, 50):
        params = {"symbols": ",".join(batch)}
        r = requests.get(YQ_URL, params=params, headers=HEADERS, timeout=25)
        if r.status_code != 200:
            raise RuntimeError(f"Yahoo quote HTTP {r.status_code}")
        j = r.json()
        results = (((j or {}).get("quoteResponse") or {}).get("result")) or []
        for q in results:
            sym = (q.get("symbol") or "").upper()
            if sym:
                m[sym] = q
    return m


def extract_price(q: dict, preferred_field: str) -> Optional[float]:
    for k in (preferred_field, "regularMarketPrice"):
        v = q.get(k)
        if isinstance(v, (int, float)):
            return float(v)
    return None


def fmt_money(x: float) -> str:
    return f"${x:.2f}"


def fmt_pct(x: float) -> str:
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.2f}%"


WATCHLIST_SECTION_RE = re.compile(r"^## Watchlist\s*$", re.M)
BULLET_RE = re.compile(r"^-\s+([A-Z0-9\.\-]+)\s+—\s+.*$", re.M)


def patch_report(report_md: Path, sell_prices: Dict[str, float], sess: SessionPick) -> Tuple[bool, int]:
    """Returns (changed, patched_count)."""
    text = report_md.read_text(encoding="utf-8", errors="replace")
    if "SellRef:" in text:
        # already patched earlier; avoid duplicates
        return False, 0

    # find Watchlist section start
    m = WATCHLIST_SECTION_RE.search(text)
    if not m:
        return False, 0

    start = m.end()
    # watchlist section ends at next header or EOF
    next_h = re.search(r"\n##\s+", text[start:])
    end = start + next_h.start() if next_h else len(text)
    watch = text[start:end]

    tickers_in_watch = []
    for bm in BULLET_RE.finditer(watch):
        t = bm.group(1).strip().upper()
        if t in sell_prices:
            tickers_in_watch.append(t)

    tickers_in_watch = sorted(set(tickers_in_watch))
    if not tickers_in_watch:
        return False, 0

    # fetch quotes
    quote_map = yahoo_quote_batch(tickers_in_watch)

    def repl(line: str) -> str:
        mm = re.match(r"^-\s+([A-Z0-9\.\-]+)\s+—\s+(.*)$", line)
        if not mm:
            return line
        t = mm.group(1).strip().upper()
        if t not in sell_prices:
            return line
        q = quote_map.get(t)
        if not q:
            return line
        now_p = extract_price(q, sess.field)
        sell_p = sell_prices.get(t)
        if now_p is None or sell_p is None or sell_p == 0:
            return line
        delta = (now_p / sell_p - 1.0) * 100.0
        suffix = f" | SellRef: {fmt_money(sell_p)} → Now({sess.label}) {fmt_money(now_p)} ({fmt_pct(delta)})"
        return line + suffix

    patched = 0
    new_lines: List[str] = []
    for ln in watch.splitlines():
        if ln.startswith("- ") and "—" in ln and "SellRef:" not in ln:
            new_ln = repl(ln)
            if new_ln != ln:
                patched += 1
            new_lines.append(new_ln)
        else:
            new_lines.append(ln)

    if patched == 0:
        return False, 0

    new_watch = "\n".join(new_lines)
    new_text = text[:start] + new_watch + text[end:]
    report_md.write_text(new_text, encoding="utf-8")
    return True, patched


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="Path to reports/master.csv")
    ap.add_argument("--report", required=True, help="Path to reports/summary_report_1.md")
    args = ap.parse_args(argv)

    master = Path(args.master)
    report = Path(args.report)

    # hard requirements
    if not master.exists() or not report.exists():
        return 0

    sess = pick_session(now_local())
    if sess is None:
        # per #1 spec: only PM/AH
        return 0

    try:
        sell_prices = load_sell_prices(master)
        if not sell_prices:
            return 0
        changed, patched = patch_report(report, sell_prices, sess)
        # optional small stdout for logs
        if changed:
            print(f"SellRef patched: {patched}")
        return 0
    except Exception as e:
        # best-effort: never break the pipeline
        print(f"SellRef patch skipped: {e}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
