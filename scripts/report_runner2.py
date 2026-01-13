#!/usr/bin/env python3
"""
report_runner2.py — v1.0.0-biblia-positions-lock-2026-01-13
#2 jelentés: előző kereskedési nap OPEN → CLOSE (tickerenként), WEBBIBLIA-kompatibilis sorrenddel:
- Pozíciók (darabszámos tickerek): MIND bent (nincs 3% küszöb)
- Watchlist: csak abs(% Open→Close) >= 3.00 vagy ha külön engedélyezve
Megjegyzés: PKN.WA alapból kihagyva (csak ha INCLUDE_PKN_WA=1).

Használat:
  python scripts/report_runner2.py --master <csv_path_or_url> --out reports/summary_report_2.md

Környezeti változók (opcionális):
  INCLUDE_PKN_WA=1            -> PKN.WA is bekerül
  WATCHLIST_THRESHOLD=3.0     -> watchlist küszöb (alap 3.0)
  INCLUDE_WATCHLIST_ALL=1     -> watchlistből mindet kilistázza (debug)
  HTTP_TIMEOUT=20             -> Yahoo request timeout (sec)
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


YAHOO_CHART_V8 = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
WATCHLIST_THRESHOLD = float(os.getenv("WATCHLIST_THRESHOLD", "3.0"))
INCLUDE_WATCHLIST_ALL = os.getenv("INCLUDE_WATCHLIST_ALL", "0") == "1"
INCLUDE_PKN_WA = os.getenv("INCLUDE_PKN_WA", "0") == "1"


@dataclass(frozen=True)
class TickerRow:
    ticker: str
    shares: float  # 0 or NaN => watchlist


@dataclass(frozen=True)
class OhlcDay:
    date_utc: datetime
    open_: float
    close: float


def now_budapest_str() -> str:
    if ZoneInfo is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return datetime.now(ZoneInfo("Europe/Budapest")).strftime("%Y-%m-%d %H:%M %Z")


def read_master_csv(master: str) -> List[TickerRow]:
    """
    Master can be a local file path or an http(s) URL.
    We accept flexible column names:
      ticker: Ticker / ticker / Symbol / symbol
      shares: Darabszam / Darabszám / Shares / shares / Quantity / qty
    If shares missing/empty -> 0.
    """
    if master.startswith("http://") or master.startswith("https://"):
        resp = requests.get(master, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        raw = resp.content.decode("utf-8", errors="replace")
    else:
        raw = open(master, "r", encoding="utf-8", errors="replace").read()

    # Sniff delimiter (comma/semicolon/tab)
    sample = raw[:2048]
    dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
    reader = csv.DictReader(io.StringIO(raw), dialect=dialect)

    # Normalize headers
    def norm(s: str) -> str:
        return (s or "").strip().lower().replace(" ", "").replace("_", "")

    rows: List[TickerRow] = []
    for r in reader:
        keys = {norm(k): k for k in r.keys() if k is not None}

        ticker_key = None
        for cand in ["ticker", "symbol"]:
            if cand in keys:
                ticker_key = keys[cand]
                break
        if ticker_key is None:
            # fallback: first column
            ticker_key = list(r.keys())[0]

        shares_key = None
        for cand in ["darabszam", "darabszám", "shares", "quantity", "qty", "darab"]:
            if norm(cand) in keys:
                shares_key = keys[norm(cand)]
                break

        ticker = (r.get(ticker_key) or "").strip().upper()
        if not ticker:
            continue

        if (ticker == "PKN.WA") and (not INCLUDE_PKN_WA):
            continue

        shares_val = 0.0
        if shares_key:
            raw_sh = (r.get(shares_key) or "").strip().replace(",", ".")
            if raw_sh:
                try:
                    shares_val = float(raw_sh)
                except ValueError:
                    shares_val = 0.0

        rows.append(TickerRow(ticker=ticker, shares=shares_val))

    # De-dup by ticker (keep max shares)
    by: Dict[str, float] = {}
    for tr in rows:
        by[tr.ticker] = max(by.get(tr.ticker, 0.0), tr.shares)
    return [TickerRow(ticker=t, shares=s) for t, s in sorted(by.items())]


def fetch_prev_day_ohlc(ticker: str) -> Optional[OhlcDay]:
    """
    Fetch last completed daily candle (open/close) from Yahoo chart v8.
    Uses range=10d to survive holidays/weekends.
    """
    url = YAHOO_CHART_V8.format(ticker=ticker)
    params = {
        "range": "10d",
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }
    r = requests.get(url, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except json.JSONDecodeError:
        return None

    try:
        result = data["chart"]["result"][0]
        ts = result["timestamp"]
        quote = result["indicators"]["quote"][0]
        opens = quote.get("open", [])
        closes = quote.get("close", [])
    except Exception:
        return None

    # Build list of candles where both open and close are valid numbers
    candles: List[Tuple[int, float, float]] = []
    for i in range(min(len(ts), len(opens), len(closes))):
        o = opens[i]
        c = closes[i]
        if o is None or c is None:
            continue
        if isinstance(o, (int, float)) and isinstance(c, (int, float)) and math.isfinite(o) and math.isfinite(c):
            candles.append((ts[i], float(o), float(c)))

    if not candles:
        return None

    # "Previous trading day" = last completed candle in the series
    tsec, o, c = candles[-1]
    dt = datetime.fromtimestamp(tsec, tz=timezone.utc)
    return OhlcDay(date_utc=dt, open_=o, close=c)


def pct_change(open_: float, close: float) -> float:
    if open_ == 0:
        return float("nan")
    return (close / open_ - 1.0) * 100.0


def fmt_price(x: float) -> str:
    return f"{x:.2f}"


def fmt_pct(x: float) -> str:
    if not math.isfinite(x):
        return "n/a"
    return f"{x:+.2f}%"


def build_report(rows: List[TickerRow]) -> Tuple[str, List[str]]:
    # Split positions vs watchlist
    positions = [r for r in rows if r.shares and r.shares > 0]
    watchlist = [r for r in rows if not (r.shares and r.shares > 0)]

    missing: List[str] = []
    rec: Dict[str, OhlcDay] = {}

    for r in rows:
        ohlc = fetch_prev_day_ohlc(r.ticker)
        if ohlc is None:
            missing.append(r.ticker)
        else:
            rec[r.ticker] = ohlc

    # Determine reference day (most common date among fetched tickers)
    day_str = "n/a"
    if rec:
        # pick max date (latest)
        latest_dt = max(v.date_utc for v in rec.values())
        if ZoneInfo is None:
            day_str = latest_dt.strftime("%Y-%m-%d")
        else:
            day_str = latest_dt.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    lines: List[str] = []
    lines.append(f"# #2 — Előző kereskedési nap OPEN → CLOSE")
    lines.append(f"**Időbélyeg:** {now_budapest_str()}")
    lines.append(f"**Referencia nap (US market):** {day_str}")
    lines.append("")
    if missing:
        lines.append(f"## Lefedettség-ellenőrzés")
        lines.append(f"- **Lefedettség: HIÁNYOS** — nem elérhető ticker(ek): {', '.join(missing)}")
    else:
        lines.append(f"## Lefedettség-ellenőrzés")
        lines.append(f"- **Lefedettség: TELJES**")

    lines.append("")
    lines.append("## Pozíciók (darabszámos tickerek)")
    if not positions:
        lines.append("- (nincs darabszámos ticker)")
    else:
        lines.append("| Ticker | Prev Open | Prev Close | Open→Close |")
        lines.append("|---|---:|---:|---:|")
        for r in sorted(positions, key=lambda x: x.ticker):
            ohlc = rec.get(r.ticker)
            if not ohlc:
                lines.append(f"| {r.ticker} | n/a | n/a | n/a |")
                continue
            p = pct_change(ohlc.open_, ohlc.close)
            lines.append(f"| {r.ticker} | {fmt_price(ohlc.open_)} | {fmt_price(ohlc.close)} | {fmt_pct(p)} |")

    lines.append("")
    lines.append("## Watchlist (csak trigger / ≥ |3.00%|)")
    wl_rows = []
    for r in sorted(watchlist, key=lambda x: x.ticker):
        ohlc = rec.get(r.ticker)
        if not ohlc:
            continue
        p = pct_change(ohlc.open_, ohlc.close)
        if INCLUDE_WATCHLIST_ALL or (math.isfinite(p) and abs(p) >= WATCHLIST_THRESHOLD):
            wl_rows.append((r.ticker, ohlc, p))

    if not wl_rows:
        lines.append(f"- Nincs ≥ |{WATCHLIST_THRESHOLD:.2f}%| Open→Close mozgó watchlist ticker.")
    else:
        lines.append("| Ticker | Prev Open | Prev Close | Open→Close |")
        lines.append("|---|---:|---:|---:|")
        for t, ohlc, p in wl_rows:
            lines.append(f"| {t} | {fmt_price(ohlc.open_)} | {fmt_price(ohlc.close)} | {fmt_pct(p)} |")

    lines.append("")
    lines.append("---")
    lines.append("Megjegyzés: #2 jelentés csak az előző napi OPEN és CLOSE értékeket adja vissza (nincs intraday max/min, nincs today-open).")

    return "\n".join(lines) + "\n", missing


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="MASTER CSV path vagy URL")
    ap.add_argument("--out", required=True, help="Kimeneti markdown (pl. reports/summary_report_2.md)")
    args = ap.parse_args()

    rows = read_master_csv(args.master)
    report_md, _missing = build_report(rows)

    out_path = args.out
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report_md)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
