#!/usr/bin/env python3
"""earnings_fetcher_v3.py – v3.0.0-nasdaq-calendar

Cél:
  - Nasdaq (public) earnings calendar lekérése több napra előre.
  - Kiszűrni a MASTER (reports/master.csv) tickereit.
  - Kimenet: reports/earnings_1.json

Miért Nasdaq API:
  - GitHub Actions környezetben jellemzően stabilabb, mint MarketBeat (403/CF).
  - Egyszerű, napi bontású feed.

Megjegyzés:
  - A Nasdaq endpoint nem hivatalosan dokumentált; a script védetten (retry + header) hívja.
  - Ha a Nasdaq API elérhetetlen, a JSON-ben ok:false és error mező kerül kiírásra.

Verziószabály:
  - Bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests


NASDAQ_EARNINGS_URL = "https://api.nasdaq.com/api/calendar/earnings"


def _ua_headers() -> Dict[str, str]:
    # Nasdaq gyakran kér normális böngésző headert.
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nasdaq.com/market-activity/earnings",
        "Connection": "keep-alive",
    }


def _read_master_symbols(master_csv: Path) -> Set[str]:
    # MASTER CSV: első oszlopban (vagy 'Ticker' nevű oszlopban) a ticker.
    if not master_csv.exists():
        raise FileNotFoundError(f"MASTER CSV nem található: {master_csv}")

    symbols: Set[str] = set()
    with master_csv.open("r", encoding="utf-8", newline="") as f:
        sample = f.read(8192)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample) if sample.strip() else csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        if not reader.fieldnames:
            return symbols

        # Ticker mező keresése
        fn = [x.strip() for x in reader.fieldnames if x]
        ticker_key = None
        for cand in ("Ticker", "ticker", "Symbol", "symbol"):
            if cand in fn:
                ticker_key = cand
                break
        if ticker_key is None:
            # fallback: első oszlop
            ticker_key = fn[0]

        for row in reader:
            t = (row.get(ticker_key) or "").strip().upper()
            if t and t != "TICKER" and t != "SYMBOL":
                symbols.add(t)

    # PKN.WA-t alapból kihagyjuk (a biblia szerint)
    symbols.discard("PKN.WA")
    return symbols


def _http_get_json(url: str, params: Dict[str, str], retries: int = 3, timeout: int = 20) -> Tuple[bool, Optional[dict], Optional[str]]:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            # jitter + enyhe rate-limit
            if attempt > 1:
                time.sleep(0.7 + random.random() * 0.8)

            r = requests.get(url, params=params, headers=_ua_headers(), timeout=timeout)
            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}"
                continue
            return True, r.json(), None
        except Exception as e:
            last_err = str(e)
    return False, None, last_err


def _parse_rows(payload: dict) -> List[dict]:
    # Nasdaq API tipikusan: {"data":{"rows":[...], ...}, "status": {...}}
    data = payload.get("data") if isinstance(payload, dict) else None
    rows = (data or {}).get("rows")
    if isinstance(rows, list):
        return rows
    return []


def fetch_earnings_for_date(d: date) -> Tuple[bool, List[dict], Optional[str]]:
    ok, payload, err = _http_get_json(NASDAQ_EARNINGS_URL, params={"date": d.isoformat()})
    if not ok or not payload:
        return False, [], err or "unknown"
    rows = _parse_rows(payload)
    return True, rows, None


def _normalize_session(row: dict) -> str:
    # Nasdaq általában "time" / "time" jellegű mezőkkel adja: "After Market Close", "Before Market Open", stb.
    raw = (row.get("time") or row.get("marketTime") or row.get("market_time") or "").strip().lower()
    if not raw:
        return ""
    if "after" in raw or "close" in raw or "pm" in raw:
        return "AMC"
    if "before" in raw or "open" in raw or "am" in raw:
        return "BMO"
    return raw.upper()[:12]


def build_earnings_items(master_symbols: Set[str], days_ahead: int) -> Tuple[bool, List[dict], List[str]]:
    items: List[dict] = []
    errors: List[str] = []
    today = date.today()

    for i in range(0, days_ahead + 1):
        d = today + timedelta(days=i)
        ok, rows, err = fetch_earnings_for_date(d)
        if not ok:
            errors.append(f"{d.isoformat()}: {err}")
            continue

        for row in rows:
            sym = (row.get("symbol") or row.get("ticker") or "").strip().upper()
            if not sym or sym not in master_symbols:
                continue
            company = (row.get("name") or row.get("company") or row.get("companyName") or "").strip()
            sess = _normalize_session(row)
            items.append(
                {
                    "ticker": sym,
                    "date": d.isoformat(),
                    "session": sess,
                    "title": company,
                    "source": "nasdaq",
                }
            )

    # dedupe (ticker+date+session)
    seen = set()
    out = []
    for it in items:
        k = (it.get("ticker"), it.get("date"), it.get("session"))
        if k in seen:
            continue
        seen.add(k)
        out.append(it)

    # sort
    out.sort(key=lambda x: (x.get("date",""), x.get("session",""), x.get("ticker","")))
    return (len(errors) == 0), out, errors


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="reports/master.csv", help="MASTER CSV útvonal (default: reports/master.csv)")
    ap.add_argument("--out", default="reports/earnings_1.json", help="Kimeneti JSON (default: reports/earnings_1.json)")
    ap.add_argument("--days", type=int, default=14, help="Hány napra előre nézzük (default: 14)")
    args = ap.parse_args()

    master_path = Path(args.master)
    out_path = Path(args.out)

    try:
        symbols = _read_master_symbols(master_path)
    except Exception as e:
        payload = {
            "ok": False,
            "source": "nasdaq",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "count": 0,
            "items": [],
            "error": f"MASTER read error: {e}",
        }
        write_json(out_path, payload)
        return 0

    ok, items, errors = build_earnings_items(symbols, days_ahead=max(1, args.days))

    payload = {
        "ok": ok,
        "source": "nasdaq",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "count": len(items),
        "days_ahead": args.days,
        "items": items,
    }
    if errors:
        payload["errors"] = errors[:200]

    write_json(out_path, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
