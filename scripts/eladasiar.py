#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eladasiar.py — v1.0.4 (2026-02-17)

Post-process #1 markdown report: append SellRef delta vs current (PM/AH) price
for tickers that have "Eladasi ar" in MASTER (reports/master.csv).

Key points
- NEVER break the workflow: return 0 on all errors (caller may still use "|| true")
- Always print a single-line status summary to STDOUT so GH Actions logs show activity.
- Robust CSV header handling: trims whitespace (handles "Eladasi ar ").
- Duplicated tickers: last non-empty sell price wins (file order).
- Price sourcing (best-effort):
  1) Yahoo quote batch (query1 v7/finance/quote)
  2) Fallback: Yahoo chart v8 per-ticker (includePrePost=true) and pick latest non-null close
     (this often works when quote gets 401/blocked on GH runners)
"""

from __future__ import annotations
import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime
from typing import Dict, Optional, Tuple, List

# --- Session detection (CEST/CET) ---
try:
    from zoneinfo import ZoneInfo
    BUDAPEST = ZoneInfo("Europe/Budapest")
except Exception:  # pragma: no cover
    BUDAPEST = None

def now_budapest() -> datetime:
    return datetime.now(BUDAPEST) if BUDAPEST else datetime.now()

def detect_session(dt: datetime) -> Optional[str]:
    """Return 'PM' or 'AH' or None, per your #1 definition."""
    h, m = dt.hour, dt.minute
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
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def load_sell_prices(master_csv: str) -> Tuple[Dict[str, float], str]:
    """Return (sell_price_by_ticker, chosen_header_name)."""
    sell: Dict[str, float] = {}
    with open(master_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        header_norm = [h.strip() for h in header]

        # ticker col
        try:
            t_idx = header_norm.index("Ticker")
        except ValueError:
            t_idx = 0

        # sell col
        candidates = {"Eladasi ar", "Eladási ár", "Eladasi_ar", "Sell", "SellPrice"}
        sell_idx = None
        chosen = ""
        for i, h in enumerate(header_norm):
            if h in candidates:
                sell_idx = i
                chosen = header[i]  # original header
                break
        if sell_idx is None:
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
            sell[ticker] = sp  # last non-empty wins

    return sell, chosen

# --- HTTP helpers (best-effort Yahoo) ---
import urllib.request
import urllib.error

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

def http_json(url: str, timeout: int = 25) -> dict:
    req = urllib.request.Request(url, headers=UA_HEADERS, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8", errors="replace"))

def yahoo_quote_batch(tickers: List[str]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    if not tickers:
        return out
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

def pick_current_price_from_quote(q: dict, session: str) -> Optional[float]:
    if not q:
        return None
    if session == "PM":
        return q.get("preMarketPrice") or q.get("regularMarketPrice") or None
    if session == "AH":
        return q.get("postMarketPrice") or q.get("regularMarketPrice") or None
    return None

def yahoo_chart_last_close(ticker: str) -> Optional[float]:
    """Fallback: Yahoo chart v8, pick latest non-null close (includePrePost=true)."""
    t = ticker.strip().upper()
    url = (
        f"https://query2.finance.yahoo.com/v8/finance/chart/{t}"
        f"?interval=1m&range=1d&includePrePost=true"
    )
    data = http_json(url)
    chart = (data.get("chart") or {})
    res = (chart.get("result") or [None])[0]
    if not res:
        return None
    ind = (res.get("indicators") or {}).get("quote") or []
    if not ind:
        return None
    closes = ind[0].get("close") or []
    # last non-null
    for v in reversed(closes):
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            continue
    # meta fallback
    meta = res.get("meta") or {}
    for k in ("regularMarketPrice", "chartPreviousClose"):
        v = meta.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    return None

# --- Markdown patching ---
TICKER_LINE_RE = re.compile(r"^\s*-\s*([A-Z0-9\.\-]+)\s+—\s+(.*)$")

def patch_report(report_md: str, sell_prices: Dict[str, float], session: str) -> Tuple[int, int, int, str]:
    """
    Returns (patched_lines, eligible_lines_seen, price_ok_count, price_source)
    We patch only within the '## Watchlist' section (until next '## ' header).
    """
    with open(report_md, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    in_watch = False
    patched = 0
    eligible = 0

    tickers_to_price: List[str] = []
    line_tickers: List[Tuple[int, str]] = []

    for idx, line in enumerate(lines):
        if line.strip().startswith("## "):
            in_watch = (line.strip() == "## Watchlist")
            continue
        if not in_watch:
            continue
        # Stop at next section header (single # or ##)
        if line.strip().startswith("#"):
            break
        m = TICKER_LINE_RE.match(line)
        if not m:
            continue
        t = m.group(1).upper()
        if t in sell_prices:
            eligible += 1
            line_tickers.append((idx, t))
            if t not in tickers_to_price:
                tickers_to_price.append(t)

    if eligible == 0:
        return 0, 0, 0, "n/a"

    # 1) Try quote batch
    prices: Dict[str, float] = {}
    price_source = "quote"
    try:
        quote_map = yahoo_quote_batch(tickers_to_price)
        for t in tickers_to_price:
            cur = pick_current_price_from_quote(quote_map.get(t), session)
            if cur is not None:
                prices[t] = float(cur)
    except Exception:
        prices = {}

    # 2) Fallback to chart if quote gave nothing (common on GH runners)
    if not prices:
        price_source = "chart"
        for t in tickers_to_price:
            cur = None
            try:
                cur = yahoo_chart_last_close(t)
            except Exception:
                cur = None
            if cur is not None:
                prices[t] = float(cur)

    price_ok = 0
    for idx, t in line_tickers:
        sp = sell_prices.get(t)
        cur = prices.get(t)
        if sp is None or cur is None:
            continue
        price_ok += 1
        delta = (cur / sp - 1.0) * 100.0
        if "SellRef:" in lines[idx]:
            continue
        lines[idx] = f"{lines[idx]} | SellRef: ${sp:.2f} → Now({session}) ${cur:.2f} ({delta:+.2f}%)"
        patched += 1

    if patched > 0:
        with open(report_md, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    return patched, eligible, price_ok, price_source

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
        print(f"SellRef: NOOP (no sell prices found; header={header_name or 'n/a'!r})")
        return 0

    patched, eligible, price_ok, source = patch_report(args.report, sell_prices, session)
    print(
        f"SellRef: patched={patched} eligible={eligible} session={session} "
        f"sell_prices={len(sell_prices)} header={header_name!r} price_ok={price_ok} source={source}"
    )
    return 0

if __name__ == "__main__":
    sys.exit(main())
