#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
earnings_fetcher.py – v4.0.0 (A: MarketBeat earnings)

Kimenet:
  - reports/earnings_{report}.json

Bemenet (automatikus):
  - reports/latest_{report}.json  (ha létezik)  -> tickerek
  - különben reports/master.csv  (Ticker oszlop / első oszlop)

Forrás:
  - MarketBeat earnings oldal (exchange fallback: NASDAQ -> NYSE -> AMEX -> OTCMKTS -> TSX -> TSXV)

Megjegyzés:
  - Ez a modul *nem* a jelentés AH/PM ármozgás-időablakát kezeli, hanem a közelgő eseményeket jelzi
    (pl. \"MU holnap jelent\") – ezt a postprocess fűzi be.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
from urllib.request import Request, urlopen

VERSION = "4.0.0"

EXCHANGES = ["NASDAQ", "NYSE", "AMEX", "OTCMKTS", "TSX", "TSXV"]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

DATE_PATTERNS = [
    # MarketBeat: "Jan. 29 After Market Closes Estimated"
    re.compile(r"(?P<mon>[A-Z][a-z]{2})\.?\s+(?P<day>\d{1,2})(?:,\s*(?P<year>\d{4}))?", re.I),
    # "January 29, 2026"
    re.compile(r"(?P<mon>[A-Z][a-z]+)\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})", re.I),
]

MON_MAP = {
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


def _http_get(url: str, timeout: int = 20) -> str:
    req = Request(url, headers={"User-Agent": UA, "Accept": "text/html,*/*"})
    with urlopen(req, timeout=timeout) as r:
        raw = r.read()
    # best-effort decode
    for enc in ("utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="replace")


def _load_universe(report: str) -> List[str]:
    latest = Path("reports") / f"latest_{report}.json"
    if latest.exists():
        try:
            data = json.loads(latest.read_text(encoding="utf-8"))
            # support both {"tickers":[...]} and list of objects with "ticker"
            if isinstance(data, dict) and isinstance(data.get("tickers"), list):
                return [str(x).strip().upper() for x in data["tickers"] if str(x).strip()]
            if isinstance(data, list):
                out = []
                for row in data:
                    if isinstance(row, dict):
                        t = row.get("ticker") or row.get("Ticker") or row.get("symbol")
                        if t:
                            out.append(str(t).strip().upper())
                    elif isinstance(row, str):
                        out.append(row.strip().upper())
                return [t for t in out if t]
        except Exception:
            pass

    master = Path("reports") / "master.csv"
    if master.exists():
        out = []
        with master.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
        if not rows:
            return []
        header = [h.strip().lower() for h in rows[0]]
        ticker_idx = 0
        for i, h in enumerate(header):
            if h in ("ticker", "symbol"):
                ticker_idx = i
                break
        for r in rows[1:]:
            if len(r) > ticker_idx:
                t = r[ticker_idx].strip().upper()
                if t:
                    out.append(t)
        return out

    return []


def _parse_date(text: str, now_utc: datetime) -> Optional[datetime]:
    text = (text or "").strip()
    if not text:
        return None

    for pat in DATE_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        mon_raw = (m.group("mon") or "").strip().lower().rstrip(".")
        day = int(m.group("day"))
        year = m.groupdict().get("year")
        if year:
            year_i = int(year)
        else:
            # default: pick nearest future date (within ~9 months) relative to now
            year_i = now_utc.year
            mon_i = MON_MAP.get(mon_raw, None)
            if not mon_i:
                continue
            cand = datetime(year_i, mon_i, day, tzinfo=timezone.utc)
            if cand < now_utc - timedelta(days=30):
                cand = datetime(year_i + 1, mon_i, day, tzinfo=timezone.utc)
            return cand
        mon_i = MON_MAP.get(mon_raw, None)
        if not mon_i:
            continue
        return datetime(year_i, mon_i, day, tzinfo=timezone.utc)

    return None


def _marketbeat_earnings_url(ticker: str, exch: str) -> str:
    return f"https://www.marketbeat.com/stocks/{exch}/{quote(ticker)}/earnings/"


def _extract_upcoming_earnings(html: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns: (raw_date_text, raw_time_hint)
    """
    # Try common MarketBeat pattern: "Upcoming ... Earnings Date" then a nearby text cell
    # We'll just grab the first match of 'Upcoming' + 'Earnings Date' and capture next <td>...</td>
    m = re.search(
        r"Upcoming\\s+Q\\d+.*?Earnings\\s+Date\\s*</[^>]+>\\s*<[^>]+>(?P<td>.*?)</",
        html,
        flags=re.I | re.S,
    )
    if m:
        td = re.sub(r"<[^>]+>", " ", m.group("td"))
        td = re.sub(r"\\s+", " ", td).strip()
        # Example: "Jan. 29 After Market Closes Estimated"
        return td, None

    # Fallback: generic "Earnings Date" label in a table
    m2 = re.search(
        r"Earnings\\s+Date\\s*</[^>]+>\\s*<[^>]+>(?P<td>.*?)</",
        html,
        flags=re.I | re.S,
    )
    if m2:
        td = re.sub(r"<[^>]+>", " ", m2.group("td"))
        td = re.sub(r"\\s+", " ", td).strip()
        return td, None

    return None, None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="1")
    ap.add_argument("--max-days-ahead", type=int, default=10)
    ap.add_argument("--sleep", type=float, default=0.6)
    args = ap.parse_args()

    report = str(args.report)
    now_utc = datetime.now(timezone.utc)
    tickers = _load_universe(report)

    out_items: List[Dict] = []
    errors: List[str] = []
    fetched = 0
    ok_count = 0

    for t in tickers:
        # Skip non-US formats that MarketBeat won't handle well here
        if "." in t or "/" in t:
            continue

        found = False
        last_err = None
        for exch in EXCHANGES:
            url = _marketbeat_earnings_url(t, exch)
            try:
                html = _http_get(url)
                fetched += 1
                raw_dt, _ = _extract_upcoming_earnings(html)
                if not raw_dt:
                    last_err = f"{t}: no-earnings-date-found ({exch})"
                    continue
                dt = _parse_date(raw_dt, now_utc)
                if not dt:
                    last_err = f"{t}: date-parse-failed: {raw_dt}"
                    continue
                days_ahead = (dt - now_utc).days
                if days_ahead < 0 or days_ahead > int(args.max_days_ahead):
                    found = True  # found but outside window
                    break
                out_items.append(
                    {
                        "ticker": t,
                        "source": "marketbeat",
                        "exchange": exch,
                        "earningsDateUtc": dt.strftime("%Y-%m-%d"),
                        "raw": raw_dt,
                        "daysAhead": days_ahead,
                    }
                )
                ok_count += 1
                found = True
                break
            except Exception as e:
                last_err = f"{t}: fetch-error ({exch}): {type(e).__name__}: {e}"
                continue
            finally:
                if args.sleep:
                    time.sleep(float(args.sleep))

        if not found and last_err:
            errors.append(last_err)

    payload = {
        "ok": True,
        "type": "earnings",
        "version": VERSION,
        "report": report,
        "generatedAt": now_utc.isoformat(),
        "window": {"maxDaysAhead": int(args.max_days_ahead)},
        "count": len(out_items),
        "items": sorted(out_items, key=lambda x: (x.get("daysAhead", 9999), x.get("ticker", ""))),
        "sources": {
            "marketbeat_earnings": {
                "ok": ok_count > 0,
                "count": ok_count,
                "fetchedPages": fetched,
                "errors": errors[:200],
            }
        },
    }

    out_path = Path("reports") / f"earnings_{report}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
