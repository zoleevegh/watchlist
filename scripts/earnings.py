#!/usr/bin/env python3
# scripts/earnings.py
# v0.2.3-earnings-next7d-nasdaq-api-coverage-2026-01-10
#
# PURPOSE (TEMP, isolated):
#   - Earnings: only the NEXT 7 CALENDAR DAYS (MASTER filter)
#   - NO Yahoo, NO API keys
#   - Output ONLY: reports/earnings_next7d.md
#
# Sources (fallback chain, API key nélkül):
#   1) Nasdaq JSON API (primary): api.nasdaq.com/api/quote/{ticker}/earnings?assetclass=stocks
#   2) Nasdaq HTML (secondary): nasdaq.com/.../earnings
#   3) Investing.com HTML (tertiary): investing.com/equities/{ticker}-earnings
#   4) unavailable (explicit)
#
# IMPORTANT:
#   - Date filtering is on *local calendar date* (Europe/Budapest). No UTC shifting.
#   - This build also writes a coverage summary into the markdown so you can see if
#     the issue is "no earnings in 7d" vs "sources blocked / parsing failed".

from __future__ import annotations

import csv
import json
import re
import time as _time
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from html import unescape
from pathlib import Path
from typing import Optional, List, Tuple, Dict

import urllib.request
import urllib.error

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

REPORTS_DIR = Path("reports")
MASTER_CSV = REPORTS_DIR / "master.csv"
OUT_MD = REPORTS_DIR / "earnings_next7d.md"

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

NASDAQ_API_HEADERS = {
    **BASE_HEADERS,
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nasdaq.com/",
    "Origin": "https://www.nasdaq.com",
}

HTML_HEADERS = {
    **BASE_HEADERS,
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


def http_get(url: str, headers: Dict[str, str], timeout: int = 25) -> str:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="ignore")


def http_get_json(url: str, headers: Dict[str, str], timeout: int = 25) -> dict:
    txt = http_get(url, headers=headers, timeout=timeout)
    return json.loads(txt)


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
    source: str               # 'nasdaq_api' | 'nasdaq_html' | 'investing' | 'unavailable'
    note: Optional[str] = None


def nasdaq_api_fetch(ticker: str) -> EarningsInfo:
    url = f"https://api.nasdaq.com/api/quote/{ticker.lower()}/earnings?assetclass=stocks"
    try:
        j = http_get_json(url, headers=NASDAQ_API_HEADERS)
        data = (j or {}).get("data") or {}
        earnings = data.get("earnings") or {}

        cand = None
        for path in [
            ("earningsForecast", "earningsDate"),
            ("earningsForecast", "earningsDateText"),
            ("earningsForecast", "earningsDateTime"),
            ("earningsForecast", "earningsDateEst"),
        ]:
            obj = earnings
            for k in path:
                if isinstance(obj, dict) and k in obj:
                    obj = obj.get(k)
                else:
                    obj = None
                    break
            if isinstance(obj, str) and obj.strip():
                cand = obj.strip()
                break

        date_str = None
        d_obj = None
        if cand:
            m = re.search(r"(\d{4}-\d{2}-\d{2})", cand)
            if m:
                date_str = m.group(1)
                d_obj = parse_date_any(date_str)
            else:
                m2 = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", cand)
                if m2:
                    date_str = m2.group(1)
                    d_obj = parse_date_any(date_str)

        time_hint = None
        et = None
        ef = earnings.get("earningsForecast")
        if isinstance(ef, dict):
            et = ef.get("earningsTime") or ef.get("time") or ef.get("earningsTimeText")
        if isinstance(et, str):
            v = et.lower()
            if "after" in v or "amc" in v:
                time_hint = "PM"
            elif "before" in v or "bmo" in v:
                time_hint = "AM"

        eps_est = None
        if isinstance(ef, dict):
            eps_est = ef.get("epsForecast") or ef.get("epsEstimate") or ef.get("eps")
        if isinstance(eps_est, str):
            eps_est = eps_est.strip() or None

        return EarningsInfo(date_str, d_obj, time_hint, eps_est, "nasdaq_api", None if d_obj else "no_date_in_api")
    except urllib.error.HTTPError as e:
        return EarningsInfo(None, None, None, None, "nasdaq_api", note=f"HTTP {e.code}")
    except Exception as e:
        return EarningsInfo(None, None, None, None, "nasdaq_api", note=str(e)[:120])


def nasdaq_html_fetch(ticker: str) -> EarningsInfo:
    url = f"https://www.nasdaq.com/market-activity/stocks/{ticker.lower()}/earnings"
    try:
        html = http_get(url, headers=HTML_HEADERS)
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

        d_obj = parse_date_any(date_str) if date_str else None
        return EarningsInfo(date_str, d_obj, time_hint, eps_est, "nasdaq_html", None if d_obj else "no_date_in_html")
    except urllib.error.HTTPError as e:
        return EarningsInfo(None, None, None, None, "nasdaq_html", note=f"HTTP {e.code}")
    except Exception as e:
        return EarningsInfo(None, None, None, None, "nasdaq_html", note=str(e)[:120])


def investing_fetch(ticker: str) -> EarningsInfo:
    url = f"https://www.investing.com/equities/{ticker.lower()}-earnings"
    try:
        html = http_get(url, headers=HTML_HEADERS)
        m = re.search(r"Earnings Date[^0-9A-Za-z]*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", html)
        date_str = unescape(m.group(1)).strip() if m else None
        d_obj = parse_date_any(date_str) if date_str else None

        eps_est = None
        meps = re.search(r"EPS\s*Forecast[^$0-9\-]*([$]?\-?\d+(\.\d+)?)", html, re.IGNORECASE)
        if meps:
            eps_est = meps.group(1)
            if not eps_est.startswith("$") and re.match(r"^\-?\d", eps_est):
                eps_est = f"${eps_est}"

        return EarningsInfo(date_str, d_obj, None, eps_est, "investing", None if d_obj else "no_date_in_investing")
    except urllib.error.HTTPError as e:
        return EarningsInfo(None, None, None, None, "investing", note=f"HTTP {e.code}")
    except Exception as e:
        return EarningsInfo(None, None, None, None, "investing", note=str(e)[:120])


def pick_best(a: EarningsInfo, b: EarningsInfo, c: EarningsInfo) -> EarningsInfo:
    for info in (a, b, c):
        if info.date_obj:
            return info
    note = a.note or b.note or c.note
    return EarningsInfo(None, None, None, None, "unavailable", note=note)


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    tickers = load_master_tickers()

    today = _today_local_date()
    end = today + timedelta(days=7)

    run_dt = _now_local_dt()
    run_stamp = run_dt.strftime("%Y-%m-%d %H:%M (CET/CEST)")

    hits: List[Tuple[str, EarningsInfo]] = []

    cov = {
        "nasdaq_api_ok": 0,
        "nasdaq_api_blocked": 0,
        "nasdaq_html_ok": 0,
        "nasdaq_html_blocked": 0,
        "investing_ok": 0,
        "investing_blocked": 0,
        "date_missing_all": 0,
    }

    for t in tickers:
        a = nasdaq_api_fetch(t)
        if a.date_obj:
            cov["nasdaq_api_ok"] += 1
        elif (a.note or "").startswith("HTTP"):
            cov["nasdaq_api_blocked"] += 1

        _time.sleep(0.12)

        b = EarningsInfo(None, None, None, None, "nasdaq_html")
        if not a.date_obj:
            b = nasdaq_html_fetch(t)
            if b.date_obj:
                cov["nasdaq_html_ok"] += 1
            elif (b.note or "").startswith("HTTP"):
                cov["nasdaq_html_blocked"] += 1
            _time.sleep(0.12)

        c = EarningsInfo(None, None, None, None, "investing")
        if not a.date_obj and not b.date_obj:
            c = investing_fetch(t)
            if c.date_obj:
                cov["investing_ok"] += 1
            elif (c.note or "").startswith("HTTP"):
                cov["investing_blocked"] += 1
            _time.sleep(0.12)

        best = pick_best(a, b, c)
        if not best.date_obj:
            cov["date_missing_all"] += 1
            continue

        if today <= best.date_obj <= end:
            hits.append((t, best))

    hits.sort(key=lambda x: (x[1].date_obj or date.max, x[0]))

    lines: List[str] = []
    lines.append("# Earnings – következő 7 nap (MASTER szűrés)")
    lines.append("")
    lines.append("Verzió: v0.2.3-earnings-next7d-nasdaq-api-coverage-2026-01-10 | Futás: " + run_stamp)
    lines.append(f"Időablak: {today.isoformat()} → {end.isoformat()} (lokális naptári nap, nem UTC)")
    lines.append(f"Találat: {len(hits)} / {len(tickers)} ticker")
    lines.append("")
    lines.append("## Lefedettség (források)")
    lines.append(f"- Nasdaq API dátum találat: {cov['nasdaq_api_ok']}")
    lines.append(f"- Nasdaq API HTTP blokkolt: {cov['nasdaq_api_blocked']}")
    lines.append(f"- Nasdaq HTML dátum találat: {cov['nasdaq_html_ok']}")
    lines.append(f"- Nasdaq HTML HTTP blokkolt: {cov['nasdaq_html_blocked']}")
    lines.append(f"- Investing dátum találat: {cov['investing_ok']}")
    lines.append(f"- Investing HTTP blokkolt: {cov['investing_blocked']}")
    lines.append(f"- Dátum nem elérhető egyik forrásból sem: {cov['date_missing_all']}")
    lines.append("")
    if not hits:
        lines.append("Nincs találat a következő 7 napban a MASTER tickerek között **VAGY** a források blokkoltak / nem adtak dátumot.")
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
