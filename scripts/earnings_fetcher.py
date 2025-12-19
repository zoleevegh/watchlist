#!/usr/bin/env python3
"""
earnings_fetcher.py – v1.0.0-nasdaq-calendar

Cél:
- Nasdaq earnings calendar alapján "közelgő jelentések" lista előállítása.
- Kimenet: reports/earnings_{report}.json (alapértelmezés: report=1 → reports/earnings_1.json)
- Opcionális szűrés: MASTER/watchlist CSV alapján (alapértelmezés: reports/master.csv)

VERZIÓZÁS SZABÁLY: bármely fájl módosításakor a verziószámot folytatólagosan növelni kell, kihagyás nélkül.

# IMÁDSÁG (hibajavítás után)
# Bocsáss meg uram, mert balfék voltam, és rossz argumentumot vártam.
# Add uram, hogy ez a módosítás most hibátlanul fusson.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
from typing import Dict, List, Optional, Set, Tuple

import requests

__version__ = "v1.0.0-nasdaq-calendar"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nasdaq.com/",
        "Origin": "https://www.nasdaq.com",
        "Connection": "keep-alive",
    }
)

TICKER_COL_CANDIDATES = ["ticker", "symbol", "szimbólum", "Ticker", "SYMBOL", "Symbol"]


def _debug(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def _find_col(headers: List[str], candidates: List[str]) -> Optional[str]:
    lower = {h.strip().lower(): h for h in headers if h}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def load_master_tickers(path: str) -> Set[str]:
    """Load tickers from a MASTER/watchlist CSV. If file missing, return empty set (means no filter)."""
    if not path or not os.path.exists(path):
        _debug(f"[EARN] master not found (no filter): {path}")
        return set()

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return set()
        headers = [h.strip() for h in reader.fieldnames]
        col = _find_col(headers, TICKER_COL_CANDIDATES)
        if not col:
            _debug("[EARN] master has no ticker column (no filter).")
            return set()

        out: Set[str] = set()
        for row in reader:
            sym = (row.get(col) or "").strip().upper()
            if sym:
                out.add(sym)
        return out


def _map_session(nasdaq_time: str) -> str:
    """Map Nasdaq 'time' field to short session label."""
    t = (nasdaq_time or "").strip().lower()
    if not t:
        return "TNS"
    if "after" in t and "close" in t:
        return "AMC"
    if ("before" in t and "open" in t) or ("pre-market" in t) or ("premarket" in t):
        return "BMO"
    if "time not supplied" in t:
        return "TNS"
    # fallback
    return nasdaq_time.strip()


def fetch_nasdaq_earnings_for_date(date_yyyy_mm_dd: str) -> List[dict]:
    url = "https://api.nasdaq.com/api/calendar/earnings"
    params = {"date": date_yyyy_mm_dd}
    try:
        r = SESSION.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        _debug(f"[EARN] Nasdaq fetch failed for {date_yyyy_mm_dd}: {e}")
        return []

    rows = (((data or {}).get("data") or {}).get("rows")) or []
    if not isinstance(rows, list):
        return []

    out: List[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        sym = (row.get("symbol") or "").strip().upper()
        name = (row.get("companyName") or row.get("company") or "").strip()
        time_str = (row.get("time") or "").strip()
        session = _map_session(time_str)
        if not sym:
            continue
        out.append(
            {
                "ticker": sym,
                "title": name,
                "date": date_yyyy_mm_dd,
                "session": session,
                "time_raw": time_str,
            }
        )
    return out


def build_upcoming(days: int, master_tickers: Set[str]) -> List[dict]:
    today = dt.date.today()
    items: List[dict] = []
    seen: Set[Tuple[str, str]] = set()

    for i in range(max(1, days)):
        d = today + dt.timedelta(days=i)
        d_str = d.strftime("%Y-%m-%d")
        rows = fetch_nasdaq_earnings_for_date(d_str)
        for it in rows:
            t = it.get("ticker", "").upper()
            if master_tickers and t not in master_tickers:
                continue
            key = (t, it.get("date", ""))
            if key in seen:
                continue
            seen.add(key)
            items.append(it)

    # Sort by date then ticker
    items.sort(key=lambda x: (x.get("date", ""), x.get("ticker", "")))
    return items


def main() -> None:
    p = argparse.ArgumentParser(description="Nasdaq earnings calendar fetcher → reports/earnings_{report}.json")
    p.add_argument("--report", type=int, choices=[1, 2, 3], default=1, help="Report index (1/2/3). Default: 1.")
    p.add_argument("--days", type=int, default=7, help="How many days ahead to scan (default: 7).")
    p.add_argument("--master", default="reports/master.csv", help="MASTER/watchlist CSV (default: reports/master.csv).")
    p.add_argument("--out", default=None, help="Override output path (default: reports/earnings_{report}.json).")

    args = p.parse_args()

    out_path = args.out or f"reports/earnings_{args.report}.json"

    master_tickers = load_master_tickers(args.master)
    items = build_upcoming(days=args.days, master_tickers=master_tickers)

    payload = {
        "source": "nasdaq_calendar",
        "script_version": __version__,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "report": args.report,
        "days_ahead": args.days,
        "master_filter_enabled": bool(master_tickers),
        "count": len(items),
        "items": items,
    }

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[EARN] wrote {out_path} (count={len(items)})")


if __name__ == "__main__":
    main()
