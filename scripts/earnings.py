#!/usr/bin/env python3
# scripts/earnings.py
# v0.2.2-earnings-next7d-localdatefix-2026-01-10
#
# PURPOSE (TEMP, isolated):
#   - Earnings: only the NEXT 7 CALENDAR DAYS (MASTER filter)
#   - NO Yahoo, NO API keys
#   - Output ONLY: reports/earnings_next7d.md
#
# Sources (fallback chain, API key nélkül):
#   1) Nasdaq earnings page (primary)
#   2) Investing.com equities earnings page (secondary)
#   3) unavailable (explicit)
#
# NOTE:
#   - Date filtering is done on *calendar date* (no UTC shifting).
#   - This fixes the previous bug where UTC date window could exclude valid "PM" earnings dates.

from __future__ import annotations

import csv
import re
import time as _time
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from html import unescape
from pathlib import Path
from typing import Optional, List, Tuple

import urllib.request

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

REPORTS_DIR = Path("reports")
MASTER_CSV = REPORTS_DIR / "master.csv"
OUT_MD = REPORTS_DIR / "earnings_next7d.md"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _now_local_dt() -> datetime:
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo("Europe/Budapest"))
        except Exception:
            pass
    return datetime.now()


def _today_local_date() -> date:
    return _now_local_dt().date()


def http_get(url: str, timeout: int = 25) -> str:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="ignore")


def load_master_tickers() -> List[str]:
    if not MASTER_CSV.exists():
        raise FileNotFoundError(f"MASTER CSV not found: {MASTER_CSV}")
    tickers: List[str] = []
    with MASTER_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = (row.get("Ticker") or row.get("ticker") or "").strip().upper()
            if t:
                tickers.append(t)
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def parse_date_any(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None
    m = re.match(r"^([A-Za-z]{3,9})\s+(\d{1,2}),\s*(\d{4})$", s)
    if m:
        mon = MONTHS.get(m.group(1).lower())
        if not mon:
            return None
        try:
            return date(int(m.group(3)), mon, int(m.group(2)))
        except Exception:
            return None
    return None


@dataclass
class EarningsInfo:
    date_str: Optional[str]
    date_obj: Optional[date]
    time_hint: Optional[str]  # 'AM'/'PM'/None
    eps_est: Optional[str]
    source: str               # 'nasdaq' | 'investing' | 'unavailable'


def nasdaq_fetch(ticker: str) -> EarningsInfo:
    url = f"https://www.nasdaq.com/market-activity/stocks/{ticker.lower()}/earnings"
    try:
        html = http_get(url)
        m = re.search(r"Earnings Date[^0-9A-Za-z]*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", html)
        date_str = unescape(m.group(1)).strip() if m else None

        time_hint = None
        mh = re.search(r"(Before Market Open|After Market Close|BMO|AMC)", html, re.IGNORECASE)
        if mh:
            v = mh.group(1).lower()
            if "after" in v or "amc" in v:
                time_hint = "PM"
            elif "before" in v or "bmo" in v:
                time_hint = "AM"

        eps_est = None
        meps = re.search(r"EPS Forecast[^$0-9\-]*([$]?\-?\d+(\.\d+)?)", html)
        if meps:
            eps_est = meps.group(1)
            if not eps_est.startswith("$") and re.match(r"^\-?\d", eps_est):
                eps_est = f"${eps_est}"

        return EarningsInfo(date_str, parse_date_any(date_str) if date_str else None, time_hint, eps_est, "nasdaq")
    except Exception:
        return EarningsInfo(None, None, None, None, "nasdaq")


def investing_fetch(ticker: str) -> EarningsInfo:
    url = f"https://www.investing.com/equities/{ticker.lower()}-earnings"
    try:
        html = http_get(url)
        m = re.search(r"Earnings Date[^0-9A-Za-z]*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", html)
        date_str = unescape(m.group(1)).strip() if m else None

        eps_est = None
        meps = re.search(r"EPS\s*Forecast[^$0-9\-]*([$]?\-?\d+(\.\d+)?)", html, re.IGNORECASE)
        if meps:
            eps_est = meps.group(1)
            if not eps_est.startswith("$") and re.match(r"^\-?\d", eps_est):
                eps_est = f"${eps_est}"

        return EarningsInfo(date_str, parse_date_any(date_str) if date_str else None, None, eps_est, "investing")
    except Exception:
        return EarningsInfo(None, None, None, None, "investing")


def pick_best(a: EarningsInfo, b: EarningsInfo) -> EarningsInfo:
    if a.date_obj:
        return a
    if b.date_obj:
        return b
    return EarningsInfo(None, None, None, None, "unavailable")


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    tickers = load_master_tickers()

    today = _today_local_date()
    end = today + timedelta(days=7)

    run_dt = _now_local_dt()
    run_stamp = run_dt.strftime("%Y-%m-%d %H:%M (CET/CEST)")

    hits: List[Tuple[str, EarningsInfo]] = []

    for t in tickers:
        n = nasdaq_fetch(t)
        _time.sleep(0.15)
        inv = EarningsInfo(None, None, None, None, "investing")
        if not n.date_obj:
            inv = investing_fetch(t)
            _time.sleep(0.15)
        best = pick_best(n, inv)

        if best.date_obj and today <= best.date_obj <= end:
            hits.append((t, best))

    hits.sort(key=lambda x: (x[1].date_obj or date.max, x[0]))

    lines: List[str] = []
    lines.append("# Earnings – következő 7 nap (MASTER szűrés)")
    lines.append("")
    lines.append("Verzió: v0.2.2-earnings-next7d-localdatefix-2026-01-10 | Futás: " + run_stamp)
    lines.append(f"Időablak: {today.isoformat()} → {end.isoformat()} (lokális naptári nap, nem UTC)")
    lines.append(f"Találat: {len(hits)} / {len(tickers)} ticker")
    lines.append("")

    if not hits:
        lines.append("Nincs találat a következő 7 napban a MASTER tickerek között (vagy a forrás nem adott adatot).")
    else:
        lines.append("| Ticker | Dátum | Idő (AM/PM) | EPS (ha van) | Forrás |")
        lines.append("|---|---:|:---:|---:|:---|")
        for t, info in hits:
            d = info.date_obj.isoformat() if info.date_obj else "n/a"
            tm = info.time_hint or "n/a"
            eps = info.eps_est or "n/a"
            lines.append(f"| {t} | {d} | {tm} | {eps} | {info.source} |")

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"OK: wrote {OUT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
