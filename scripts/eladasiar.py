#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eladasiar.py — v1.0.3 (2026-02-17)

Post-process #1 markdown report: append SellRef delta vs current (PM/AH) price
for tickers that have "Eladasi ar" in MASTER (reports/master.csv).

Design goals:
- NEVER break the workflow: return 0 on all errors (caller may still use "|| true")
- Always print a single-line status summary to STDOUT so GH Actions logs show activity.
- Robust CSV header handling: trims whitespace (handles "Eladasi ar ").
- Duplicated tickers: last non-empty sell price wins (file order).
"""

from __future__ import annotations
import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

# --- Session detection (CEST/CET) ---
# We follow your #1 definition: only AH (22:00–02:00 CEST) and PM (10:00–15:30 CEST).
# If outside these windows, we do nothing (but still log).
try:
    from zoneinfo import ZoneInfo
    BUDAPEST = ZoneInfo("Europe/Budapest")
except Exception:  # pragma: no cover
    BUDAPEST = None

def now_budapest() -> datetime:
    if BUDAPEST is None:
        return datetime.now()
    return datetime.now(BUDAPEST)

def detect_session(dt: datetime) -> Optional[str]:
    """Return 'PM' or 'AH' or None."""
    h = dt.hour
    m = dt.minute
    # PM: 10:00–15:30
    if (h > 10 or (h == 10 and m >= 0)) and (h < 15 or (h == 15 and m <= 30)):
        return "PM"
    # AH: 22:00–23:59 or 00:00–02:00
    if (h > 22 or (h == 22 and m >= 0)) or (h < 2 or (h == 2 and m == 0)):
        return "AH"
    return None

# --- Parsing utilities ---
def parse_float_any(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    # handle HU decimal comma
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def load_sell_prices(master_csv: str) -> Tuple[Dict[str, float], str]:
    """
    Returns:
      sell_price_by_ticker, chosen_header_name
    """
    sell: Dict[str, float] = {}
    with open(master_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        header_norm = [h.strip() for h in header]

        # find ticker col
        try:
            t_idx = header_norm.index("Ticker")
        except ValueError:
            # fallback: first col
            t_idx = 0

        # find sell price column among variants
        candidates = {"Eladasi ar", "Eladási ár", "Eladasi_ar", "Sell", "SellPrice"}
        sell_idx = None
        chosen = ""
        for i, h in enumerate(header_norm):
            if h in candidates:
                sell_idx = i
                chosen = header[i]  # original
                break
        if sell_idx is None:
            # heuristic: contains 'Eladasi' substring
            for i, h in enumerate(header_norm):
                if "eladas" in h.lower():
                    sell_idx = i
                    chosen = header[i]
                    break

        if sell_idx is None:
            return {}, ""

        for row in reader:
            if not row or len(row) <= max(t_idx, sell_idx):
                continue
            ticker = (row[t_idx] or "").strip().upper()
            if not ticker:
                continue
            sp = parse_float_any(row[sell_idx])
            if sp is None:
                continue
            # last non-empty wins
            sell[ticker] = sp

    return sell, chosen

# --- Yahoo quote (best-effort) ---
import urllib.request

def http_json(url: str, timeout: int = 25) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "close",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8", errors="replace"))

def yahoo_quote_batch(tickers: List[str]) -> Dict[str, dict]:
    """
    Returns map ticker -> quote dict.
    Uses query1 quote endpoint (more stable on GH runners).
    """
    out: Dict[str, dict] = {}
    if not tickers:
        return out
    # Chunk to keep URL sane
    CHUNK = 40
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i+CHUNK]
        symbols = ",".join(chunk)
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols}"
        data = http_json(url)
        results = (data.get("quoteResponse") or {}).get("result") or []
        for q in results:
            sym = (q.get("symbol") or "").upper()
            if sym:
                out[sym] = q
    return out

def pick_current_price(q: dict, session: str) -> Optional[float]:
    """
    For PM: prefer preMarketPrice, fallback regularMarketPrice
    For AH: prefer postMarketPrice, fallback regularMarketPrice
    """
    if not q:
        return None
    if session == "PM":
        return q.get("preMarketPrice") or q.get("regularMarketPrice") or None
    if session == "AH":
        return q.get("postMarketPrice") or q.get("regularMarketPrice") or None
    return None

# --- Markdown patching ---
TICKER_LINE_RE = re.compile(r"^\s*-\s*([A-Z0-9\.\-]+)\s+—\s+(.*)$")

def patch_report(report_md: str, sell_prices: Dict[str, float], session: str) -> Tuple[int, int]:
    """
    Returns (patched_lines, eligible_lines_seen)
    We patch only within the "## Watchlist" section (until next "## " header).
    """
    with open(report_md, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    in_watch = False
    patched = 0
    eligible = 0

    # We only quote tickers that actually appear in Watchlist bullet lines and have sell price.
    tickers_to_quote: List[str] = []
    line_tickers: List[Tuple[int, str]] = []

    for idx, line in enumerate(lines):
        if line.strip().startswith("## "):
            in_watch = (line.strip() == "## Watchlist")
            continue
        if not in_watch:
            continue
        if line.strip().startswith("## "):
            break
        m = TICKER_LINE_RE.match(line)
        if not m:
            continue
        t = m.group(1).upper()
        if t in sell_prices:
            eligible += 1
            line_tickers.append((idx, t))
            if t not in tickers_to_quote:
                tickers_to_quote.append(t)

    if eligible == 0:
        return 0, 0

    quote_map: Dict[str, dict] = {}
    try:
        quote_map = yahoo_quote_batch(tickers_to_quote)
    except Exception as e:
        # best-effort: no patching
        return 0, eligible

    for idx, t in line_tickers:
        sp = sell_prices.get(t)
        q = quote_map.get(t)
        cur = pick_current_price(q, session) if q else None
        if sp is None or cur is None:
            continue
        delta = (cur / sp - 1.0) * 100.0
        # avoid duplicate patching if already present
        if "SellRef:" in lines[idx]:
            continue
        lines[idx] = f"{lines[idx]} | SellRef: ${sp:.2f} → Now({session}) ${cur:.2f} ({delta:+.2f}%)"
        patched += 1

    if patched > 0:
        with open(report_md, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    return patched, eligible

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--report", required=True)
    args = ap.parse_args()

    ts = now_budapest()
    session = detect_session(ts)

    if not os.path.isfile(args.report):
        print(f"SellRef: SKIP (report not found: {args.report})")
        return 0
    if not os.path.isfile(args.master):
        print(f"SellRef: SKIP (master not found: {args.master})")
        return 0
    if session is None:
        print(f"SellRef: SKIP (outside PM/AH window; now={ts.strftime('%Y-%m-%d %H:%M %Z')})")
        return 0

    sell_prices, header_name = load_sell_prices(args.master)
    if not sell_prices:
        h = header_name or "n/a"
        print(f"SellRef: NOOP (no sell prices found; header={h!r})")
        return 0

    patched, eligible = patch_report(args.report, sell_prices, session)
    print(f"SellRef: patched={patched} eligible={eligible} session={session} sell_prices={len(sell_prices)} header={header_name!r}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
