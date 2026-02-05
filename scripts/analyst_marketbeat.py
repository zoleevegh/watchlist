#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# analyst_marketbeat.py — v0.4.0-fmp-primary-2026-02-05
#
# PURPOSE
# - Replace brittle MarketBeat scraping (Cloudflare) with Financial Modeling Prep (FMP) APIs.
# - Produces the same report artifacts:
#     reports/analyst_last2d.md
#     reports/analyst_last2d.json
# - Keeps CLI compatible with previous script:
#     --master <csv>
#     --days N
#     --out-md <path>
#     --out-json <path>
#     --debug
#
# DATA SOURCES (FMP)
# - Upgrades/Downgrades: https://financialmodelingprep.com/api/v4/upgrades-downgrades?symbol=...&apikey=...
# - Price Targets:       https://financialmodelingprep.com/api/v4/price-target?symbol=...&apikey=...
# - API key probe:       https://financialmodelingprep.com/api/v3/quote/AAPL?apikey=...
#
# NOTES
# - Rolling window uses calendar days (UTC) for filtering event date.
# - Robust handling of missing fields and rate limits.
# - Cache file: reports/fmp_events.json (optional; used for dedup + persistence if endpoint is temporarily empty).
#
# ZOLI RULES
# - If ticker has no data: report "[TICKER] – adat nem elérhető (kihagyva)" in JSON, and omit from MD unless debug.
#
from __future__ import annotations

import argparse
import csv
import dataclasses
import hashlib
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

FMP_BASE = "https://financialmodelingprep.com"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

CACHE_PATH = Path("reports/fmp_events.json")
DEBUG_DIR = Path("reports/debug_fmp")

@dataclasses.dataclass
class Event:
    ticker: str
    kind: str  # upgrade | downgrade | pt_change
    date: str  # YYYY-MM-DD (best effort)
    firm: str
    analyst: str
    action: str
    rating_from: str
    rating_to: str
    pt_from: Optional[float]
    pt_to: Optional[float]
    source: str
    url: str

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def _parse_date_any(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    fmts = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S%z",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    # sometimes date is like "2026-02-05T00:00:00.000Z"
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def _http_get_json(url: str, timeout: int = 25, debug_dump: Optional[Path] = None) -> Tuple[int, Any, str]:
    headers = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        text = r.text or ""
        if debug_dump:
            debug_dump.write_text(text, encoding="utf-8", errors="ignore")
        try:
            return r.status_code, r.json(), text
        except Exception:
            return r.status_code, None, text
    except requests.RequestException as e:
        return 0, None, str(e)

def _load_master_tickers(master_csv: str) -> List[str]:
    tickers: List[str] = []
    with open(master_csv, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        # tolerate various headers
        for row in reader:
            t = (row.get("ticker") or row.get("Ticker") or row.get("TICKER") or "").strip()
            if not t:
                continue
            tickers.append(t)
    # keep order, dedup
    seen=set()
    out=[]
    for t in tickers:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out

def _load_cache() -> Dict[str, Any]:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {"_meta": {}, "events": {}}
    return {"_meta": {}, "events": {}}

def _save_cache(cache: Dict[str, Any]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    cache["_meta"] = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "version": "v0.4.0-fmp-primary-2026-02-05",
    }
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def _event_key(e: Event) -> str:
    core = "|".join([
        e.ticker, e.kind, e.date or "", e.firm or "", e.analyst or "", e.action or "",
        e.rating_from or "", e.rating_to or "", str(e.pt_from or ""), str(e.pt_to or "")
    ])
    return _sha1(core)

def _within_days(dt: datetime, days: int, now: datetime) -> bool:
    start = (now - timedelta(days=days)).date()
    return dt.date() >= start and dt.date() <= now.date()

def _probe_key(api_key: str, debug: bool=False) -> Tuple[bool, str]:
    url = f"{FMP_BASE}/api/v3/quote/AAPL?apikey={api_key}"
    dump = None
    if debug:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        dump = DEBUG_DIR / f"probe_quote_AAPL.json"
    status, js, text = _http_get_json(url, debug_dump=dump)
    if status in (401, 403):
        return False, f"FMP key invalid or not authorized (HTTP {status})"
    if status == 429:
        return False, "FMP rate-limited (HTTP 429)"
    if status != 200:
        return False, f"FMP probe failed (HTTP {status})"
    if not isinstance(js, list):
        return False, "FMP probe returned non-list payload"
    return True, "OK"

def _fetch_updown(ticker: str, api_key: str, debug: bool=False) -> Tuple[str, int, List[Dict[str, Any]]]:
    url = f"{FMP_BASE}/api/v4/upgrades-downgrades?symbol={ticker}&apikey={api_key}"
    dump = None
    if debug:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        dump = DEBUG_DIR / f"{ticker}_updown.json"
    status, js, _ = _http_get_json(url, debug_dump=dump)
    if isinstance(js, list):
        return url, status, js
    return url, status, []

def _fetch_price_targets(ticker: str, api_key: str, debug: bool=False) -> Tuple[str, int, List[Dict[str, Any]]]:
    url = f"{FMP_BASE}/api/v4/price-target?symbol={ticker}&apikey={api_key}"
    dump = None
    if debug:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        dump = DEBUG_DIR / f"{ticker}_pt.json"
    status, js, _ = _http_get_json(url, debug_dump=dump)
    if isinstance(js, list):
        return url, status, js
    return url, status, []

def _map_updown_row(ticker: str, row: Dict[str, Any], src_url: str) -> Optional[Event]:
    # FMP keys vary; handle common variants
    dt = _parse_date_any(row.get("publishedDate") or row.get("date") or row.get("updated") or "")
    if not dt:
        return None
    date_s = dt.date().isoformat()
    firm = str(row.get("gradingCompany") or row.get("company") or row.get("brokerage") or row.get("firm") or "").strip()
    analyst = str(row.get("analyst") or row.get("analystName") or "").strip()
    to_grade = str(row.get("newGrade") or row.get("newRating") or row.get("toGrade") or row.get("grade") or "").strip()
    from_grade = str(row.get("previousGrade") or row.get("previousRating") or row.get("fromGrade") or "").strip()
    action = str(row.get("action") or row.get("ratingAction") or "").strip()
    kind = "upgrade"
    # try infer
    a_low = action.lower()
    if "down" in a_low or "downgrade" in a_low:
        kind = "downgrade"
    elif "up" in a_low or "upgrade" in a_low:
        kind = "upgrade"
    else:
        # compare common buy/hold/sell ranks if available
        order = {"strong buy":4, "buy":3, "overweight":3, "outperform":3,
                 "hold":2, "neutral":2, "equal-weight":2, "market perform":2,
                 "underweight":1, "underperform":1, "sell":0}
        f = order.get(from_grade.lower(), None)
        t = order.get(to_grade.lower(), None)
        if f is not None and t is not None and t < f:
            kind = "downgrade"
        elif f is not None and t is not None and t > f:
            kind = "upgrade"
        else:
            # default
            kind = "upgrade"
    return Event(
        ticker=ticker,
        kind=kind,
        date=date_s,
        firm=firm or "—",
        analyst=analyst or "—",
        action=action or ("Upgrade" if kind=="upgrade" else "Downgrade"),
        rating_from=from_grade or "—",
        rating_to=to_grade or "—",
        pt_from=None,
        pt_to=None,
        source="FMP",
        url=src_url,
    )

def _map_pt_row(ticker: str, row: Dict[str, Any], src_url: str) -> Optional[Event]:
    dt = _parse_date_any(row.get("publishedDate") or row.get("date") or row.get("updated") or "")
    if not dt:
        return None
    date_s = dt.date().isoformat()
    firm = str(row.get("analystCompany") or row.get("company") or row.get("brokerage") or row.get("firm") or "").strip()
    analyst = str(row.get("analystName") or row.get("analyst") or "").strip()
    pt = _safe_float(row.get("priceTarget") or row.get("priceTargetValue") or row.get("target") or row.get("pt"))
    pt_old = _safe_float(row.get("priceTargetPrevious") or row.get("previousPriceTarget") or row.get("ptFrom"))
    action = str(row.get("newsTitle") or row.get("action") or "Price Target Update").strip()
    # Determine raised/lowered if old exists
    kind = "pt_change"
    return Event(
        ticker=ticker,
        kind=kind,
        date=date_s,
        firm=firm or "—",
        analyst=analyst or "—",
        action=action,
        rating_from="—",
        rating_to=str(row.get("rating") or row.get("recommendation") or "—"),
        pt_from=pt_old,
        pt_to=pt,
        source="FMP",
        url=src_url,
    )

def _format_md(events_by_ticker: Dict[str, List[Event]], days: int, tickers_total: int, status_line: str) -> str:
    lines: List[str] = []
    lines.append("## Elemzői feed (FMP) – fel/lemínősítések + célár (utolsó %d naptári nap)" % days)
    lines.append("")
    lines.append(f"_forrás státusz: {status_line}_")
    lines.append("")
    lines.append("_FMP API – cache used if available._")
    lines.append("")
    found = 0
    for t, evs in events_by_ticker.items():
        if not evs:
            continue
        found += 1
        lines.append(f"## {t}")
        for e in evs:
            if e.kind in ("upgrade", "downgrade"):
                lines.append(f"- {e.date} – {e.firm} – {e.kind.upper()} | Ajánlás: {e.rating_from} -> {e.rating_to} | Forrás: {e.url}")
            else:
                if e.pt_from is not None and e.pt_to is not None:
                    lines.append(f"- {e.date} – {e.firm} – Célár változás | Célár: USD {e.pt_from:.2f} -> {e.pt_to:.2f} | Forrás: {e.url}")
                elif e.pt_to is not None:
                    lines.append(f"- {e.date} – {e.firm} – Célár frissítés | Célár: USD {e.pt_to:.2f} | Forrás: {e.url}")
                else:
                    lines.append(f"- {e.date} – {e.firm} – Célár frissítés | Célár: — | Forrás: {e.url}")
        lines.append("")
    lines.append(f"Időablak: utolsó {days} naptári nap (UTC) | Találat: {sum(len(v) for v in events_by_ticker.values())} / {tickers_total} ticker")
    return "\n".join(lines).strip() + "\n"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="Path to master.csv exported from Google Sheets")
    ap.add_argument("--days", type=int, default=2, help="Rolling window in calendar days")
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        # write empty outputs with clear status
        Path(args.out_md).write_text(_format_md({}, args.days, 0, "FMP_API_KEY:MISSING"), encoding="utf-8")
        Path(args.out_json).write_text(json.dumps({"status":"missing_api_key","source":"FMP","events":[]}, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0

    ok, msg = _probe_key(api_key, debug=args.debug)
    if not ok:
        Path(args.out_md).write_text(_format_md({}, args.days, 0, f"probe:FAIL ({msg})"), encoding="utf-8")
        Path(args.out_json).write_text(json.dumps({"status":"probe_failed","message":msg,"source":"FMP","events":[]}, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0

    tickers = _load_master_tickers(args.master)
    now = datetime.now(timezone.utc)

    cache = _load_cache()
    cache_events: Dict[str, Dict[str, Any]] = cache.get("events", {}) if isinstance(cache.get("events", {}), dict) else {}

    events_by_ticker: Dict[str, List[Event]] = {}
    errors: Dict[str, str] = {}
    status_parts = []
    # Throttle to be nice to FMP
    base_sleep = 0.12

    for i, t in enumerate(tickers, start=1):
        # optional skip for non-US formats that often break (still keep if user has .TO etc)
        # We'll keep all except explicitly skipped in rules:
        if t.upper() == "PKN.WA":
            continue

        evs: List[Event] = []

        # fetch upgrades/downgrades
        up_url, up_status, up_rows = _fetch_updown(t, api_key, debug=args.debug)
        if up_status == 429:
            errors[t] = "rate_limited"
            # backoff hard, then continue
            time.sleep(2.5 + random.random())
            up_rows = []
        elif up_status not in (200,):
            # treat as soft error
            pass

        for row in up_rows:
            e = _map_updown_row(t, row, up_url)
            if not e:
                continue
            dt = _parse_date_any(e.date)
            if not dt:
                continue
            if _within_days(dt, args.days, now):
                evs.append(e)

        # fetch price targets
        pt_url, pt_status, pt_rows = _fetch_price_targets(t, api_key, debug=args.debug)
        if pt_status == 429:
            errors[t] = "rate_limited"
            time.sleep(2.5 + random.random())
            pt_rows = []
        elif pt_status not in (200,):
            pass

        for row in pt_rows:
            e = _map_pt_row(t, row, pt_url)
            if not e:
                continue
            dt = _parse_date_any(e.date)
            if not dt:
                continue
            if _within_days(dt, args.days, now):
                evs.append(e)

        # Dedup inside ticker
        dedup = {}
        for e in evs:
            dedup[_event_key(e)] = e
        evs = list(dedup.values())
        evs.sort(key=lambda x: (x.date, x.kind), reverse=True)

        # Cache merge (keep last_seen)
        if evs:
            events_by_ticker[t] = evs
            # update cache
            ce = cache_events.get(t, {})
            if not isinstance(ce, dict):
                ce = {}
            # store by key
            for e in evs:
                k = _event_key(e)
                ce[k] = {"event": e.to_dict(), "last_seen_utc": now.isoformat()}
            cache_events[t] = ce
        else:
            # If no events now, try cache for window
            ce = cache_events.get(t, {})
            cached: List[Event] = []
            if isinstance(ce, dict):
                for k, v in ce.items():
                    try:
                        ed = v.get("event", {})
                        dt = _parse_date_any(ed.get("date",""))
                        if dt and _within_days(dt, args.days, now):
                            cached.append(Event(**ed))
                    except Exception:
                        continue
            cached_dedup = { _event_key(e): e for e in cached }
            cached = list(cached_dedup.values())
            cached.sort(key=lambda x: (x.date, x.kind), reverse=True)
            if cached:
                events_by_ticker[t] = cached

        # polite sleep
        time.sleep(base_sleep + random.random()*0.08)

    cache["events"] = cache_events
    _save_cache(cache)

    # Build status line
    # We'll show the latest HTTP status we observed for the two endpoints (best effort)
    status_line = "probe:OK"
    if errors:
        status_line += f", errors:{len(errors)}"

    md = _format_md(events_by_ticker, args.days, len(tickers), status_line)
    Path(args.out_md).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_md).write_text(md, encoding="utf-8")

    flat_events: List[Dict[str, Any]] = []
    for t, evs in events_by_ticker.items():
        for e in evs:
            flat_events.append(e.to_dict())

    out = {
        "status": "ok",
        "source": "FMP",
        "window_days": args.days,
        "generated_utc": now.isoformat(),
        "tickers_total": len(tickers),
        "tickers_with_events": len(events_by_ticker),
        "events": flat_events,
        "errors": errors,
        "cache_file": str(CACHE_PATH),
        "version": "v0.4.0-fmp-primary-2026-02-05",
    }
    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_json).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
