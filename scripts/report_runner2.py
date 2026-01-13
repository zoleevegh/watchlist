#!/usr/bin/env python3
"""
report_runner2.py — v1.0.2-biblia-splitfix-watchlist-all-2026-01-13

#2 jelentés: előző kereskedési nap OPEN → CLOSE (tickerenként)

FONTOS: mindig KÉT blokkot ír ki:
- Pozíciók (darabszámos): MINDEN ticker, ahol a darabszám > 0
- Watchlist: MINDEN ticker, ahol a darabszám üres/0/nem szám

Alapértelmezésben a watchlistet is TELJESEN kilistázza (nem csak ≥3%).
Ha mégis trigger-mód kell:
  WATCHLIST_MODE=trigger  (ekkor abs(Open→Close) >= WATCHLIST_THRESHOLD, alap 3.0)

Megjegyzés: PKN.WA alapból kihagyva (csak INCLUDE_PKN_WA=1).

Használat:
  python scripts/report_runner2.py --master <csv_path_or_url> --out reports/summary_report_2.md
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
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


YAHOO_CHART_V8 = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
WATCHLIST_THRESHOLD = float(os.getenv("WATCHLIST_THRESHOLD", "3.0"))
WATCHLIST_MODE = os.getenv("WATCHLIST_MODE", "all").strip().lower()  # all | trigger
INCLUDE_PKN_WA = os.getenv("INCLUDE_PKN_WA", "0") == "1"


@dataclass(frozen=True)
class TickerRow:
    ticker: str
    shares: float  # >0 => pozíció, 0 => watchlist


@dataclass(frozen=True)
class OhlcDay:
    date_utc: datetime
    open_: float
    close: float


def http_get_text(url: str, timeout: float = DEFAULT_TIMEOUT) -> str:
    req = Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    with urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return data.decode("utf-8", errors="replace")


def http_get_json(url: str, timeout: float = DEFAULT_TIMEOUT) -> Optional[dict]:
    req = Request(url, headers={"User-Agent": UA, "Accept": "application/json,text/plain,*/*"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        return json.loads(data.decode("utf-8", errors="replace"))
    except Exception:
        return None


def now_budapest_str() -> str:
    if ZoneInfo is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return datetime.now(ZoneInfo("Europe/Budapest")).strftime("%Y-%m-%d %H:%M %Z")


def read_master_csv(master: str) -> List[TickerRow]:
    """
    MASTER can be local file path or http(s) URL.
    Flexible headers:
      ticker: Ticker / ticker / Symbol / symbol
      shares: Darabszam / Darabszám / Shares / shares / Quantity / qty
    If shares missing/empty/non-numeric -> 0 (watchlist).
    """
    raw = http_get_text(master) if master.startswith(("http://", "https://")) else \
          open(master, "r", encoding="utf-8", errors="replace").read()

    sample = raw[:2048]
    dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
    reader = csv.DictReader(io.StringIO(raw), dialect=dialect)

    def norm(s: str) -> str:
        return (s or "").strip().lower().replace(" ", "").replace("_", "")

    rows: List[TickerRow] = []
    for r in reader:
        keys = {norm(k): k for k in r.keys() if k is not None}

        ticker_key = next((keys[k] for k in ["ticker", "symbol"] if k in keys), None) or list(r.keys())[0]

        shares_key = None
        for cand in ["darabszam", "darabszám", "shares", "quantity", "qty", "darab"]:
            nc = norm(cand)
            if nc in keys:
                shares_key = keys[nc]
                break

        ticker = (r.get(ticker_key) or "").strip().upper()
        if not ticker:
            continue
        if ticker == "PKN.WA" and not INCLUDE_PKN_WA:
            continue

        shares_val = 0.0
        if shares_key:
            raw_sh = (r.get(shares_key) or "").strip().replace(",", ".")
            if raw_sh:
                try:
                    shares_val = float(raw_sh)
                except ValueError:
                    shares_val = 0.0

        # Treat tiny/negative as watchlist
        if not (shares_val and shares_val > 0):
            shares_val = 0.0

        rows.append(TickerRow(ticker=ticker, shares=shares_val))

    # De-dup by ticker (keep max shares)
    by: Dict[str, float] = {}
    for tr in rows:
        by[tr.ticker] = max(by.get(tr.ticker, 0.0), tr.shares)
    return [TickerRow(ticker=t, shares=s) for t, s in sorted(by.items())]


def fetch_prev_day_ohlc(ticker: str) -> Optional[OhlcDay]:
    url = YAHOO_CHART_V8.format(ticker=ticker) + "?" + urlencode({
        "range": "10d",
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    })
    data = http_get_json(url)
    if not data:
        return None
    try:
        result = data["chart"]["result"][0]
        ts = result["timestamp"]
        quote = result["indicators"]["quote"][0]
        opens = quote.get("open", [])
        closes = quote.get("close", [])
    except Exception:
        return None

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

    tsec, o, c = candles[-1]
    return OhlcDay(date_utc=datetime.fromtimestamp(tsec, tz=timezone.utc), open_=o, close=c)


def pct_change(open_: float, close: float) -> float:
    return float("nan") if open_ == 0 else (close / open_ - 1.0) * 100.0


def fmt_price(x: float) -> str:
    return f"{x:.2f}"


def fmt_pct(x: float) -> str:
    return "n/a" if not math.isfinite(x) else f"{x:+.2f}%"


def build_report(rows: List[TickerRow]) -> str:
    positions = [r for r in rows if r.shares > 0]
    watchlist = [r for r in rows if r.shares == 0]

    missing: List[str] = []
    rec: Dict[str, OhlcDay] = {}
    for r in rows:
        ohlc = fetch_prev_day_ohlc(r.ticker)
        if ohlc is None:
            missing.append(r.ticker)
        else:
            rec[r.ticker] = ohlc

    day_str = "n/a"
    if rec:
        latest_dt = max(v.date_utc for v in rec.values())
        if ZoneInfo is not None:
            day_str = latest_dt.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
        else:
            day_str = latest_dt.strftime("%Y-%m-%d")

    lines: List[str] = []
    lines.append("# #2 — Előző kereskedési nap OPEN → CLOSE")
    lines.append(f"**Időbélyeg:** {now_budapest_str()}")
    lines.append(f"**Referencia nap (US market):** {day_str}")
    lines.append("")
    lines.append("## Lefedettség-ellenőrzés")
    lines.append("- **Lefedettség: TELJES**" if not missing else f"- **Lefedettség: HIÁNYOS** — nem elérhető: {', '.join(missing)}")
    lines.append("")

    # Positions
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

    # Watchlist
    mode_txt = "MINDEN" if WATCHLIST_MODE == "all" else f"csak ≥ |{WATCHLIST_THRESHOLD:.2f}%|"
    lines.append(f"## Watchlist ({mode_txt})")
    wl_rows = []
    for r in sorted(watchlist, key=lambda x: x.ticker):
        ohlc = rec.get(r.ticker)
        if not ohlc:
            continue
        p = pct_change(ohlc.open_, ohlc.close)
        if WATCHLIST_MODE == "all" or (math.isfinite(p) and abs(p) >= WATCHLIST_THRESHOLD):
            wl_rows.append((r.ticker, ohlc, p))

    if not wl_rows:
        lines.append("- (nincs listázandó watchlist ticker a jelen beállítással)")
    else:
        lines.append("| Ticker | Prev Open | Prev Close | Open→Close |")
        lines.append("|---|---:|---:|---:|")
        for t, ohlc, p in wl_rows:
            lines.append(f"| {t} | {fmt_price(ohlc.open_)} | {fmt_price(ohlc.close)} | {fmt_pct(p)} |")

    lines.append("")
    lines.append("---")
    lines.append("Megjegyzés: #2 jelentés csak az előző napi OPEN és CLOSE értékeket adja vissza.")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="MASTER CSV path vagy URL")
    ap.add_argument("--out", required=True, help="Kimeneti markdown (pl. reports/summary_report_2.md)")
    args = ap.parse_args()

    rows = read_master_csv(args.master)
    report_md = build_report(rows)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(report_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
