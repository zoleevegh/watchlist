#!/usr/bin/env python3
# earnings.py — v0.2.0-earnings-next7d-investing-nasdaq-fallback-2026-01-10
#
# CÉL:
# - API key nélkül, stabilan listázni a MASTER tickerek közül azokat,
#   amelyeknek a következő 7 napon belül várható earnings dátuma van.
# - Elsődleges próbálkozás: Nasdaq public calendar endpoint (ha elérhető).
# - Másodlagos: Investing.com Earnings Calendar (HTML parse).
# - Ha egyik sem ad adatot: explicit "unavailable".
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
import datetime as dt
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import urllib.request
import urllib.error

from bs4 import BeautifulSoup


OUT_DIR = "reports"
MASTER_LOCAL = os.path.join(OUT_DIR, "master.csv")
OUT_JSON = os.path.join(OUT_DIR, "earnings_audit.json")
OUT_MD = os.path.join(OUT_DIR, "earnings_next7d.md")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
)

DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


@dataclass
class EarningsItem:
    ticker: str
    earnings_date: Optional[str] = None  # YYYY-MM-DD
    earnings_time: Optional[str] = None  # Before Open / After Close / Time Not Supplied
    eps_forecast: Optional[str] = None
    revenue_forecast: Optional[str] = None
    source: str = "unavailable"


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def http_get(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "close",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        charset = r.headers.get_content_charset() or "utf-8"
        return r.read().decode(charset, errors="replace")


def load_master_tickers() -> List[str]:
    """
    MASTER CSV expected to be already downloaded into reports/master.csv by workflow.
    We read the first column containing tickers; tolerate headers.
    """
    if not os.path.isfile(MASTER_LOCAL):
        raise FileNotFoundError(f"MASTER not found at {MASTER_LOCAL}")

    tickers: List[str] = []
    with open(MASTER_LOCAL, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            cell = (row[0] or "").strip()
            if not cell:
                continue
            # skip common headers
            if cell.lower() in {"ticker", "symbol"}:
                continue
            # normalize
            t = cell.upper()
            # allow dots/dashes (e.g. BRK.B, PKN.WA)
            if re.fullmatch(r"[A-Z0-9][A-Z0-9\.\-]{0,15}", t):
                tickers.append(t)
    # de-dup preserve order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def next_7d_window_utc(today_utc: dt.date | None = None) -> Tuple[dt.date, dt.date]:
    if today_utc is None:
        today_utc = dt.datetime.utcnow().date()
    return today_utc, today_utc + dt.timedelta(days=7)


# --- Nasdaq (best-effort) -----------------------------------------------------

def nasdaq_calendar_by_date(date_yyyy_mm_dd: str) -> List[Dict]:
    """
    Best-effort Nasdaq public endpoint.
    NOTE: Nasdaq may throttle/403; we treat any failure as "no data".
    """
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={date_yyyy_mm_dd}"
    try:
        raw = http_get(url, timeout=25)
        j = json.loads(raw)
        rows = (j.get("data") or {}).get("rows") or []
        return rows
    except Exception:
        return []


def build_from_nasdaq(tickers_set: set[str], start: dt.date, end: dt.date) -> Dict[str, EarningsItem]:
    out: Dict[str, EarningsItem] = {}
    cur = start
    while cur <= end:
        rows = nasdaq_calendar_by_date(cur.isoformat())
        for r in rows:
            sym = (r.get("symbol") or "").strip().upper()
            if not sym or sym not in tickers_set:
                continue
            item = out.get(sym) or EarningsItem(ticker=sym)
            item.earnings_date = cur.isoformat()
            # Nasdaq fields vary
            item.earnings_time = (r.get("time") or r.get("timeOfDay") or item.earnings_time)
            item.eps_forecast = (r.get("epsForecast") or r.get("eps") or item.eps_forecast)
            item.revenue_forecast = (r.get("revenueForecast") or r.get("revenue") or item.revenue_forecast)
            item.source = "nasdaq"
            out[sym] = item
        cur += dt.timedelta(days=1)
        # be polite
        time.sleep(0.25)
    return out


# --- Investing.com (stable fallback) ------------------------------------------

DATE_LINE_RE = re.compile(
    r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})$"
)
TICKER_RE = re.compile(r"\(([A-Z0-9\.\-]{1,16})\)")

def parse_investing_calendar(html: str) -> List[EarningsItem]:
    """
    Parse Investing.com earnings calendar HTML into items.
    This is a resilient parser that works even if the table layout changes,
    because it primarily uses the rendered text.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    items: List[EarningsItem] = []
    cur_date: Optional[dt.date] = None

    i = 0
    while i < len(lines):
        ln = lines[i]
        m = DATE_LINE_RE.match(ln)
        if m:
            # "Monday, January 12, 2026"
            month_name = m.group(2)
            day = int(m.group(3))
            year = int(m.group(4))
            try:
                month = dt.datetime.strptime(month_name, "%B").month
                cur_date = dt.date(year, month, day)
            except Exception:
                cur_date = None
            i += 1
            continue

        # rows usually include "... Company Name (TICKER) ..." and nearby "Before Open/After Close"
        tm = TICKER_RE.search(ln)
        if tm and cur_date:
            ticker = tm.group(1).upper()

            # Heuristics: look ahead a few lines for time + EPS/Rev forecasts
            look = " | ".join(lines[i:i+8])
            time_of_day = None
            if "Before Open" in look:
                time_of_day = "Before Open"
            elif "After Close" in look:
                time_of_day = "After Close"
            elif "Time Not Supplied" in look:
                time_of_day = "Time Not Supplied"

            # EPS / Revenue forecasts: try common patterns (numbers near 'EPS'/'Revenue')
            eps = None
            rev = None
            # Example snippets on Investing: "EPS Forecast 1.23" "Revenue Forecast 12.3B"
            eps_m = re.search(r"EPS Forecast\s*([-+]?\d+(\.\d+)?)", look)
            if eps_m:
                eps = eps_m.group(1)
            rev_m = re.search(r"Revenue Forecast\s*([-\d\.\,]+[MBT]?)", look)
            if rev_m:
                rev = rev_m.group(1).replace(",", "")

            items.append(EarningsItem(
                ticker=ticker,
                earnings_date=cur_date.isoformat(),
                earnings_time=time_of_day,
                eps_forecast=eps,
                revenue_forecast=rev,
                source="investing",
            ))
        i += 1

    # De-dup per ticker (keep earliest)
    best: Dict[str, EarningsItem] = {}
    for it in items:
        if not it.ticker or not it.earnings_date:
            continue
        prev = best.get(it.ticker)
        if prev is None or it.earnings_date < prev.earnings_date:
            best[it.ticker] = it
    return list(best.values())


def build_from_investing(tickers_set: set[str]) -> Dict[str, EarningsItem]:
    """
    Fetch Investing earnings calendar and return only tickers found on the page.
    We will later filter to next 7 days.
    """
    url = "https://www.investing.com/earnings-calendar/"
    try:
        html = http_get(url, timeout=35)
        items = parse_investing_calendar(html)
        out: Dict[str, EarningsItem] = {}
        for it in items:
            if it.ticker in tickers_set:
                out[it.ticker] = it
        return out
    except Exception:
        return {}


# --- Output -------------------------------------------------------------------

def write_outputs(
    tickers: List[str],
    items_map: Dict[str, EarningsItem],
    start: dt.date,
    end: dt.date,
    meta: Dict,
) -> None:
    # JSON (full mapping for traceability)
    results: Dict[str, Dict] = {}
    found = 0
    for t in tickers:
        it = items_map.get(t)
        if it and it.earnings_date:
            results[t] = asdict(it)
            found += 1
        else:
            results[t] = asdict(EarningsItem(ticker=t))

    payload = {
        "meta": meta,
        "window": {"from": start.isoformat(), "to": end.isoformat()},
        "found": found,
        "results": results,
    }
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # Markdown (only tickers with earnings in the window)
    # Sort: earliest date, then time
    def sort_key(it: EarningsItem):
        return (it.earnings_date or "9999-12-31", it.earnings_time or "", it.ticker)

    in_window: List[EarningsItem] = []
    for it in items_map.values():
        if not it.earnings_date:
            continue
        try:
            d = dt.date.fromisoformat(it.earnings_date)
        except Exception:
            continue
        if start <= d <= end:
            in_window.append(it)

    in_window.sort(key=sort_key)

    lines: List[str] = []
    lines.append(f"# Earnings – következő 7 nap (MASTER szűrés)")
    lines.append("")
    lines.append(f"Verzió: {meta.get('version')} | Futás: {meta.get('generated_hu')} (CET/CEST)")
    lines.append(f"Időablak: {start.isoformat()} → {end.isoformat()} (UTC dátumablak)")
    lines.append(f"Találat: {len(in_window)} / {len(tickers)} ticker")
    lines.append("")
    if not in_window:
        lines.append("Nincs találat a következő 7 napban a MASTER tickerek között (vagy a forrás nem adott adatot).")
    else:
        lines.append("| Dátum | Idő | Ticker | EPS forecast | Revenue forecast | Forrás |")
        lines.append("|---|---|---|---:|---:|---|")
        for it in in_window:
            lines.append(
                f"| {it.earnings_date} | {it.earnings_time or ''} | {it.ticker} | {it.eps_forecast or ''} | {it.revenue_forecast or ''} | {it.source} |"
            )
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")


def main() -> int:
    ensure_out_dir()

    tickers = load_master_tickers()
    tickers_set = set(tickers)

    start, end = next_7d_window_utc()

    generated_epoch = int(time.time())
    generated_hu = dt.datetime.now().strftime("%Y-%m-%d %H:%M")

    meta = {
        "version": "v0.2.0-earnings-next7d-investing-nasdaq-fallback-2026-01-10",
        "generated_epoch": generated_epoch,
        "generated_hu": generated_hu,
        "tickers_total": len(tickers),
        "sources": ["nasdaq", "investing", "unavailable"],
        "notes": "Nasdaq endpoint best-effort; Investing HTML fallback; window is based on UTC dates.",
    }

    # 1) Nasdaq best-effort for next 7 days
    items = build_from_nasdaq(tickers_set, start, end)

    # 2) Investing fallback: fill missing
    missing = {t for t in tickers if t not in items}
    if missing:
        inv = build_from_investing(missing)
        items.update(inv)

    write_outputs(tickers, items, start, end, meta)

    # non-zero if EVERYTHING missing (helps workflow visibility but not hard-fail)
    if all(v.get("earnings_date") is None for v in json.load(open(OUT_JSON, "r", encoding="utf-8"))["results"].values()):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
