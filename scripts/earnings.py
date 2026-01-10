#!/usr/bin/env python3
# scripts/earnings.py
# v0.2.4-earnings-next7d-nasdaq-calendar-primary-headersfix-2026-01-10
#
# CÉL (külön futó modul, AH/PM runner-től függetlenül):
# - Csak a következő 7 NAPTÁRI nap (MASTER szűrés)
# - API key nélkül
# - Kimenet: reports/earnings_next7d.md (CSAK EZ)
#
# Stabil fallback-lánc (API key nélkül):
# 1) Nasdaq earnings calendar endpoint (primary)  -> https://api.nasdaq.com/api/calendar/earnings?date=YYYY-MM-DD
# 2) Investing.com earnings calendar (secondary)  -> https://www.investing.com/earnings-calendar/
# 3) Earnings unavailable (explicit jelzés)
#
# FONTOS:
# - A "7 nap" ablak *lokális naptári nap* (Europe/Budapest), nem UTC.
# - A jelentés tartalmaz egy lefedettség / hibastatisztika blokkot (hogy lásd: blokkolás vs tényleg 0 találat).
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

from __future__ import annotations

import csv
import datetime as dt
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import urllib.error
import urllib.request

from bs4 import BeautifulSoup

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

OUT_DIR = "reports"
MASTER_LOCAL = os.path.join(OUT_DIR, "master.csv")
OUT_MD = os.path.join(OUT_DIR, "earnings_next7d.md")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
)

NASDAQ_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
    "Referer": "https://www.nasdaq.com/",
    "Origin": "https://www.nasdaq.com",
}

INVESTING_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
DATE_LINE_RE = re.compile(
    r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})$"
)
TICKER_RE = re.compile(r"\(([A-Z0-9\.\-]{1,16})\)")


@dataclass
class EarningsItem:
    ticker: str
    earnings_date: str  # YYYY-MM-DD
    earnings_time: Optional[str] = None
    eps_forecast: Optional[str] = None
    revenue_forecast: Optional[str] = None
    source: str = "nasdaq"  # nasdaq | investing


def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def now_local() -> dt.datetime:
    if ZoneInfo is not None:
        try:
            return dt.datetime.now(ZoneInfo("Europe/Budapest"))
        except Exception:
            pass
    return dt.datetime.now()


def next_7d_window_local(today: Optional[dt.date] = None) -> Tuple[dt.date, dt.date]:
    if today is None:
        today = now_local().date()
    return today, today + dt.timedelta(days=7)


def http_get(url: str, headers: Dict[str, str], timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        charset = r.headers.get_content_charset() or "utf-8"
        return r.read().decode(charset, errors="replace")


def load_master_tickers() -> List[str]:
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
            if cell.lower() in {"ticker", "symbol"}:
                continue
            t = cell.upper()
            if re.fullmatch(r"[A-Z0-9][A-Z0-9\.\-]{0,15}", t):
                tickers.append(t)

    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# --- Nasdaq calendar (primary) -----------------------------------------------

def nasdaq_calendar_by_date(date_yyyy_mm_dd: str) -> Tuple[List[dict], Optional[int], Optional[str]]:
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={date_yyyy_mm_dd}"
    try:
        raw = http_get(url, headers=NASDAQ_HEADERS, timeout=25)
        j = json.loads(raw)
        rows = (j.get("data") or {}).get("rows") or []
        return rows, None, None
    except urllib.error.HTTPError as e:
        return [], int(getattr(e, "code", 0) or 0), f"HTTP {getattr(e, 'code', '')}".strip()
    except Exception as e:
        return [], None, (str(e)[:140] if e else "unknown_error")


def build_from_nasdaq(tickers_set: set[str], start: dt.date, end: dt.date, cov: dict) -> Dict[str, EarningsItem]:
    out: Dict[str, EarningsItem] = {}
    cur = start
    while cur <= end:
        rows, http_code, note = nasdaq_calendar_by_date(cur.isoformat())
        if http_code:
            cov["nasdaq_http_errors"][str(http_code)] = cov["nasdaq_http_errors"].get(str(http_code), 0) + 1
            cov["nasdaq_days_failed"] += 1
        if note and not http_code:
            cov["nasdaq_other_errors"] += 1

        cov["nasdaq_days_total"] += 1
        cov["nasdaq_rows_total"] += len(rows)

        for r in rows:
            sym = (r.get("symbol") or "").strip().upper()
            if not sym or sym not in tickers_set:
                continue
            if sym in out:
                continue
            out[sym] = EarningsItem(
                ticker=sym,
                earnings_date=cur.isoformat(),
                earnings_time=(r.get("time") or r.get("timeOfDay") or None),
                eps_forecast=(r.get("epsForecast") or r.get("eps") or None),
                revenue_forecast=(r.get("revenueForecast") or r.get("revenue") or None),
                source="nasdaq",
            )

        cur += dt.timedelta(days=1)
        time.sleep(0.20)

    return out


# --- Investing calendar (secondary) ------------------------------------------

def parse_investing_calendar(html: str) -> List[EarningsItem]:
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

        tm = TICKER_RE.search(ln)
        if tm and cur_date:
            ticker = tm.group(1).upper()
            look = " | ".join(lines[i:i+10])

            time_of_day = None
            if "Before Open" in look:
                time_of_day = "Before Open"
            elif "After Close" in look:
                time_of_day = "After Close"
            elif "Time Not Supplied" in look:
                time_of_day = "Time Not Supplied"

            eps = None
            rev = None
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

    best: Dict[str, EarningsItem] = {}
    for it in items:
        prev = best.get(it.ticker)
        if prev is None or it.earnings_date < prev.earnings_date:
            best[it.ticker] = it
    return list(best.values())


def build_from_investing(tickers_set: set[str], cov: dict) -> Dict[str, EarningsItem]:
    url = "https://www.investing.com/earnings-calendar/"
    try:
        html = http_get(url, headers=INVESTING_HEADERS, timeout=35)
        items = parse_investing_calendar(html)
        out: Dict[str, EarningsItem] = {}
        for it in items:
            if it.ticker in tickers_set:
                out[it.ticker] = it
        cov["investing_ok"] = True
        cov["investing_items_parsed"] = len(items)
        cov["investing_items_matched_master"] = len(out)
        return out
    except urllib.error.HTTPError as e:
        cov["investing_ok"] = False
        cov["investing_http_error"] = int(getattr(e, "code", 0) or 0)
        return {}
    except Exception as e:
        cov["investing_ok"] = False
        cov["investing_other_error"] = (str(e)[:140] if e else "unknown_error")
        return {}


# --- Output ------------------------------------------------------------------

def write_md(
    tickers_total: int,
    in_window: List[EarningsItem],
    start: dt.date,
    end: dt.date,
    cov: dict,
    version: str,
) -> None:
    run_stamp = now_local().strftime("%Y-%m-%d %H:%M (CET/CEST)")
    lines: List[str] = []
    lines.append("# Earnings – következő 7 nap (MASTER szűrés)")
    lines.append("")
    lines.append(f"Verzió: {version} | Futás: {run_stamp}")
    lines.append(f"Időablak: {start.isoformat()} → {end.isoformat()} (lokális naptári nap, nem UTC)")
    lines.append(f"Találat: {len(in_window)} / {tickers_total} ticker")
    lines.append("")
    lines.append("## Lefedettség / hibastatisztika")
    lines.append(f"- Nasdaq napok: {cov['nasdaq_days_total']} (sikertelen napok: {cov['nasdaq_days_failed']})")
    lines.append(f"- Nasdaq összes sor: {cov['nasdaq_rows_total']}")
    if cov["nasdaq_http_errors"]:
        lines.append(f"- Nasdaq HTTP hibák: {json.dumps(cov['nasdaq_http_errors'], ensure_ascii=False)}")
    if cov["nasdaq_other_errors"]:
        lines.append(f"- Nasdaq egyéb hibák: {cov['nasdaq_other_errors']}")
    lines.append(f"- Investing ok: {cov.get('investing_ok', False)}")
    if cov.get("investing_ok"):
        lines.append(f"  - parsed: {cov.get('investing_items_parsed', 0)}, matched MASTER: {cov.get('investing_items_matched_master', 0)}")
    else:
        if "investing_http_error" in cov:
            lines.append(f"  - HTTP: {cov.get('investing_http_error')}")
        if "investing_other_error" in cov:
            lines.append(f"  - error: {cov.get('investing_other_error')}")
    lines.append("")
    if not in_window:
        lines.append("Nincs találat a következő 7 napban a MASTER tickerek között **VAGY** a források blokkoltak / nem adtak adatot.")
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

    version = "v0.2.4-earnings-next7d-nasdaq-calendar-primary-headersfix-2026-01-10"

    tickers = load_master_tickers()
    tickers_set = set(tickers)

    start, end = next_7d_window_local()

    cov = {
        "nasdaq_days_total": 0,
        "nasdaq_days_failed": 0,
        "nasdaq_rows_total": 0,
        "nasdaq_http_errors": {},
        "nasdaq_other_errors": 0,
        "investing_ok": False,
    }

    items = build_from_nasdaq(tickers_set, start, end, cov)

    missing = {t for t in tickers if t not in items}
    if missing:
        inv = build_from_investing(missing, cov)
        items.update(inv)

    in_window: List[EarningsItem] = []
    for it in items.values():
        try:
            d = dt.date.fromisoformat(it.earnings_date)
        except Exception:
            continue
        if start <= d <= end:
            in_window.append(it)

    in_window.sort(key=lambda x: (x.earnings_date, x.ticker))

    write_md(len(tickers), in_window, start, end, cov, version)

    print(f"OK: wrote {OUT_MD}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
