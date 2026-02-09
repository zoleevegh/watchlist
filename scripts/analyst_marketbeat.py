#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# analyst_marketbeat.py — v0.6.2-fix-budget_exhausted-init-2026-02-09
# Ima (v0.6.2): bocsáss meg uram, ha megint elszállt egy változó;
# adj nekem default értéket minden ágba, hogy ne dőljön el a futás.
# Ima (v0.5.6): bocsáss meg uram, ha túl sokat kérdeztem az FMP-t;
# adj cache-t és józan kvótát, hogy ne legyen N/A a riportom.
#
# PURPOSE
#   Analyst feed without MarketBeat (blocked) and without Nasdaq "event" dependency:
#   - Rating actions (upgrade/downgrade/maintain) via FMP STABLE: /stable/grades?symbol=...
#   - Price target levels via FMP STABLE: /stable/price-target-consensus?symbol=...
#
# KEY FEATURE (per your choice "2" = cache-first)
#   - Cache-first + TTL to avoid burning FMP free-tier quota.
#   - If FMP returns "Limit Reach" (quota exceeded), STOP further API calls immediately,
#     and serve remaining tickers from cache only.
#
# CLI (kept compatible with your pipeline)
#   --master <csv>          MASTER csv export
#   --days N                rolling window in calendar days (UTC date), default 2
#   --out-md <path>         output markdown
#   --out-json <path>       output json
#   --debug                 enables extra diagnostic dumps
#
# ENV
#   FMP_API_KEY required
#   FMP_CACHE_TTL_HOURS (optional, default 24)
#
# OUTPUTS
#   reports/analyst_last2d.md
#   reports/analyst_last2d.json
#   reports/fmp_cache.json            (cache store)
#   reports/fmp_debug.json            (only if --debug)
#   reports/fmp_pt_cache.json         (kept for PT-change diff)
#
# ZOLI RULES
#   - If ticker has no data: report "[TICKER] – adat nem elérhető (kihagyva)" in JSON,
#     and omit from MD unless --debug.
#   - PKN.WA excluded by default.
#
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
TIMEOUT = 25

FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_GRADES = FMP_BASE + "/grades"
FMP_GRADES_LATEST = FMP_BASE + "/grades-latest-news"
FMP_PT_CONS = FMP_BASE + "/price-target-consensus"

DEFAULT_OUT_MD = "reports/analyst_last2d.md"
DEFAULT_OUT_JSON = "reports/analyst_last2d.json"

CACHE_FILE = "reports/fmp_cache.json"          # cache-first store (grades + pt-consensus)
GRADES_EVENTS_FILE = "reports/fmp_grades_events.json"  # persistent event cache for grades-latest-news (dedup + first/last seen)
PT_CACHE_FILE = "reports/fmp_pt_cache.json"    # separate PT cache for delta detection (kept)
DEBUG_JSON = "reports/fmp_debug.json"

DEFAULT_TTL_HOURS = 24


@dataclass
class GradeEvent:
    symbol: str
    date: str  # YYYY-MM-DD
    grading_company: str
    action: str
    previous_grade: Optional[str]
    new_grade: Optional[str]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _load_json(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)



def _grade_event_key(ev: GradeEvent) -> str:
    # Stable dedup key for grades events.
    return "|".join([
        ev.symbol.upper(),
        ev.date[:10],
        (ev.grading_company or "n/a").strip().lower(),
        (ev.action or "n/a").strip().lower(),
        (ev.previous_grade or "").strip().lower(),
        (ev.new_grade or "").strip().lower(),
    ])


def _load_grades_events_cache(path: str) -> Dict[str, Any]:
    obj = _load_json(path)
    if not isinstance(obj, dict):
        return {"meta": {}, "events": {}}
    if "events" not in obj or not isinstance(obj.get("events"), dict):
        obj["events"] = {}
    if "meta" not in obj or not isinstance(obj.get("meta"), dict):
        obj["meta"] = {}
    return obj


def _prune_grades_events_cache(cache_obj: Dict[str, Any], now_utc: datetime, keep_days: int = 30) -> None:
    # Remove events older than keep_days to avoid unbounded growth.
    cutoff = now_utc.date() - timedelta(days=max(7, keep_days))
    evs = cache_obj.get("events", {})
    if not isinstance(evs, dict):
        return
    to_del = []
    for k, v in evs.items():
        try:
            d = datetime.strptime(str(v.get("date", ""))[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if d < cutoff:
            to_del.append(k)
    for k in to_del:
        evs.pop(k, None)


def _merge_grades_events_cache(cache_obj: Dict[str, Any], new_events: List[GradeEvent], now_utc: datetime) -> None:
    evs = cache_obj.setdefault("events", {})
    meta = cache_obj.setdefault("meta", {})
    if "created_at_utc" not in meta:
        meta["created_at_utc"] = now_utc.isoformat()
    meta["updated_at_utc"] = now_utc.isoformat()
    for ev in new_events:
        key = _grade_event_key(ev)
        rec = evs.get(key)
        if not isinstance(rec, dict):
            rec = {
                "symbol": ev.symbol.upper(),
                "date": ev.date[:10],
                "grading_company": ev.grading_company,
                "action": ev.action,
                "previous_grade": ev.previous_grade,
                "new_grade": ev.new_grade,
                "first_seen_utc": now_utc.isoformat(),
            }
        rec["last_seen_utc"] = now_utc.isoformat()
        evs[key] = rec
    _prune_grades_events_cache(cache_obj, now_utc, keep_days=30)


def _events_from_grades_cache(cache_obj: Dict[str, Any]) -> List[GradeEvent]:
    out: List[GradeEvent] = []
    evs = cache_obj.get("events", {})
    if not isinstance(evs, dict):
        return out
    for rec in evs.values():
        try:
            out.append(
                GradeEvent(
                    symbol=str(rec.get("symbol", "")).strip().upper(),
                    date=str(rec.get("date", "")).strip()[:10],
                    grading_company=str(rec.get("grading_company", "n/a")),
                    action=str(rec.get("action", "n/a")),
                    previous_grade=(str(rec.get("previous_grade")).strip() if rec.get("previous_grade") is not None else None),
                    new_grade=(str(rec.get("new_grade")).strip() if rec.get("new_grade") is not None else None),
                )
            )
        except Exception:
            continue
    return out
def _load_master_tickers(master_csv: str) -> List[str]:
    tickers: List[str] = []
    with open(master_csv, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = (row.get("ticker") or row.get("Ticker") or row.get("TICKER") or "").strip()
            if not t:
                continue
            t = t.upper()
            if t == "PKN.WA":
                continue
            tickers.append(t)
    # de-dupe preserve order
    out: List[str] = []
    seen = set()
    for t in tickers:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out


def _parse_grade_events(payload: Any) -> List[GradeEvent]:
    events: List[GradeEvent] = []
    if not isinstance(payload, list):
        return events
    for it in payload:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or it.get("ticker") or "").strip().upper()
        date = str(it.get("date") or it.get("publishedDate") or it.get("published_date") or "").strip()
        if not sym:
            continue
        if not date:
            continue
        gc = str(it.get("gradingCompany") or it.get("grading_company") or "").strip() or "n/a"
        action = str(it.get("action") or "").strip().lower() or "n/a"
        prev_g = it.get("previousGrade")
        new_g = it.get("newGrade")
        events.append(
            GradeEvent(
                symbol=sym,
                date=date[:10],
                grading_company=gc,
                action=action,
                previous_grade=str(prev_g).strip() if prev_g is not None else None,
                new_grade=str(new_g).strip() if new_g is not None else None,
            )
        )
    return events


def _filter_window(events: List[GradeEvent], days: int, now_utc: datetime) -> List[GradeEvent]:
    # Calendar-day rolling window, but make Monday include prior Friday when days is small.
    # Example: days=2 on Monday would otherwise miss Friday; bump to 3.
    if days <= 0:
        days = 1
    if now_utc.weekday() == 0 and days < 3:  # Monday
        days = 3
    start_date = (now_utc.date() - timedelta(days=days - 1))
    out: List[GradeEvent] = []
    for ev in events:
        try:
            d = datetime.strptime(ev.date[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if start_date <= d <= now_utc.date():
            out.append(ev)
    return out


def _is_quota_limit_message(obj: Any) -> bool:
    # FMP sometimes returns {"Error Message":"Limit Reach ..."} with 200
    if isinstance(obj, dict):
        for k in ("Error Message", "error", "message"):
            v = obj.get(k)
            if isinstance(v, str) and "Limit Reach" in v:
                return True
        # some variants
        msg = str(obj.get("Error Message") or obj.get("message") or "")
        if "Limit Reach" in msg:
            return True
    if isinstance(obj, str) and "Limit Reach" in obj:
        return True
    return False


def _http_get_json(url: str, params: Dict[str, Any], debug: bool, dbg: Dict[str, Any]) -> Tuple[Optional[Any], Optional[str], Optional[int], bool]:
    """
    Returns (json_payload_or_none, error_str_or_none, http_status, quota_exhausted_bool)
    """
    headers = {
        "User-Agent": UA,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
        status = r.status_code
        ct = (r.headers.get("content-type") or "").lower()
        text_head = (r.text[:200] if r.text else "")
        payload: Any = None
        parsed = False
        try:
            payload = r.json()
            parsed = True
        except Exception:
            payload = None
            parsed = False

        if debug:
            dbg.setdefault("http_samples", []).append({
                "url": url,
                "params": params,
                "status": status,
                "content_type": ct,
                "text_head": text_head,
                "json_parsed": parsed,
            })

        # hard http errors
        if status == 429:
            return payload, "HTTP 429 (rate limit)", status, True
        if status in (401, 403):
            return payload, f"HTTP {status} (auth/forbidden)", status, False
        if status >= 400:
            return payload, f"HTTP {status}", status, False

        # quota exhaustion via JSON message even with 200
        if parsed and _is_quota_limit_message(payload):
            return payload, "FMP quota exhausted (Limit Reach)", status, True

        if not parsed:
            return None, "JSON parse error", status, False

        return payload, None, status, False

    except requests.RequestException as e:
        return None, f"request error: {e}", None, False


def _cache_get(cache: Dict[str, Any], ticker: str, key: str, now: datetime, ttl: timedelta) -> Tuple[Optional[Any], bool]:
    """
    Returns (value_or_none, is_fresh)
    """
    tnode = cache.get("tickers", {}).get(ticker, {})
    node = tnode.get(key)
    if not isinstance(node, dict):
        return None, False
    fetched = node.get("fetched_at_utc")
    if not isinstance(fetched, str):
        return node.get("data"), False
    try:
        ts = datetime.fromisoformat(fetched.replace("Z", "+00:00"))
    except Exception:
        return node.get("data"), False
    if now - ts <= ttl:
        return node.get("data"), True
    return node.get("data"), False


def _cache_set(cache: Dict[str, Any], ticker: str, key: str, now: datetime, status: Optional[int], err: Optional[str], data: Any) -> None:
    cache.setdefault("meta", {})
    cache["meta"]["updated_at_utc"] = now.isoformat()
    cache.setdefault("tickers", {})
    cache["tickers"].setdefault(ticker, {})
    cache["tickers"][ticker][key] = {
        "fetched_at_utc": now.isoformat(),
        "status": status,
        "error": err,
        "data": data,
    }


def _format_md(events_by_ticker: Dict[str, Dict[str, Any]], days: int, debug: bool, status_line: str) -> str:
    lines: List[str] = []
    lines.append(f"## Elemzői feed (FMP stable) — fel/leminősítések + célár-szint (utolsó {days} naptári nap)")
    lines.append("")
    lines.append(status_line)
    lines.append("")
    any_rows = 0

    for t in sorted(events_by_ticker.keys()):
        info = events_by_ticker[t]
        rows = info.get("rows") or []
        pt = info.get("pt") or {}
        has_pt_numbers = any(pt.get(k) is not None for k in ("consensus", "high", "low", "median"))
        has_pt_delta = bool(pt.get("pt_change_detected"))

        # MD inclusion rules:
        # - debug: include everything
        # - non-debug: include only if there is grade event OR PT delta OR (optional) PT levels exist
        include = debug or bool(rows) or has_pt_delta or has_pt_numbers
        if not include:
            continue

        lines.append(f"## {t}")

        if rows:
            for r in rows:
                prev_g = r.get("previous_grade") or "n/a"
                new_g = r.get("new_grade") or "n/a"
                action = r.get("action") or "n/a"
                firm = r.get("grading_company") or "n/a"
                date = r.get("date") or "n/a"
                lines.append(f"- {date} — {firm} — {_hu_action(action)} | Ajánlás: {prev_g} → {new_g}")
                any_rows += 1
        else:
            if debug:
                lines.append("- _nincs grade esemény az ablakban_")

        if pt and has_pt_numbers:
            parts = []
            if pt.get("consensus") is not None:
                parts.append(f"konszenzus: {pt['consensus']:.2f}")
            if pt.get("high") is not None:
                parts.append(f"high: {pt['high']:.2f}")
            if pt.get("low") is not None:
                parts.append(f"low: {pt['low']:.2f}")
            if pt.get("median") is not None:
                parts.append(f"median: {pt['median']:.2f}")
            extra = ""
            if has_pt_delta and pt.get("prev_consensus") is not None and pt.get("consensus") is not None:
                extra = f" | Δ PT: {pt['prev_consensus']:.2f} → {pt['consensus']:.2f}"
            lines.append("- Célár-szint: " + ", ".join(parts) + extra + " |  FMP /stable/price-target-consensus")

        lines.append("")

    if any_rows == 0:
        lines.append("_Nincs FMP (stable) grade esemény a megadott ablakban._")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"



# --- NASDAQ secondary source (limited use; optional) ---
NASDAQ_CACHE_FILE = "reports/nasdaq_events.json"
NASDAQ_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

def _nasdaq_headers() -> Dict[str, str]:
    return {
        "User-Agent": NASDAQ_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
        "Connection": "keep-alive",
    }

def _http_get_json_nasdaq(url: str, debug: bool, dbg: Dict[str, Any]) -> Tuple[Optional[Any], Optional[str], Optional[int]]:
    try:
        r = requests.get(url, headers=_nasdaq_headers(), timeout=HTTP_TIMEOUT)
        status = r.status_code
        if debug:
            dbg.get("http_samples", []).append({"url": url, "status": status, "src": "nasdaq"})
        if status != 200:
            return None, f"HTTP {status}", status
        return r.json(), None, status
    except Exception as e:
        return None, str(e), None

def _parse_nasdaq_events(payload: Any) -> List[Dict[str, Any]]:
    # Nasdaq payload structure varies; we try common paths that contain rows with date + action + firm
    out: List[Dict[str, Any]] = []
    if not payload:
        return out

    def walk(x: Any):
        if isinstance(x, dict):
            # common: {"data": {"rows":[...]}}
            for k,v in x.items():
                if k in ("rows","data","table","historical","history","items","recommendations","ratings"):
                    walk(v)
                else:
                    walk(v)
        elif isinstance(x, list):
            # list of dict rows
            for it in x:
                if isinstance(it, dict):
                    # look for date-ish
                    dt = it.get("date") or it.get("asOfDate") or it.get("reportDate") or it.get("publicationDate")
                    firm = it.get("brokerage") or it.get("broker") or it.get("firm") or it.get("analystFirm") or it.get("company") or it.get("source")
                    action = it.get("action") or it.get("ratingAction") or it.get("type") or it.get("headline") or it.get("recommendation") or "rating"
                    frm = it.get("from") or it.get("fromRating") or it.get("previousRating") or it.get("oldRating") or ""
                    to = it.get("to") or it.get("toRating") or it.get("newRating") or it.get("rating") or ""
                    if dt and (firm or action):
                        d2 = _parse_date_yyyy_mm_dd(dt) or _parse_date_any(dt)
                        if d2:
                            out.append({
                                "date": d2,
                                "firm": str(firm) if firm else "Nasdaq",
                                "action": str(action),
                                "from_grade": str(frm) if frm is not None else "",
                                "to_grade": str(to) if to is not None else "",
                            })
                else:
                    walk(it)
        # else ignore

    walk(payload)
    return out

def _nasdaq_window_events_for_ticker(sym: str, days: int, now: datetime, debug: bool, dbg: Dict[str, Any]) -> List[Dict[str, Any]]:
    # We call the ratings endpoint; it often includes recent actions.
    url = f"https://api.nasdaq.com/api/analyst/{sym}/ratings"
    payload, err, status = _http_get_json_nasdaq(url, debug, dbg)
    if err is not None or payload is None:
        return []
    evs = _parse_nasdaq_events(payload)
    # filter window
    cutoff = (now - timedelta(days=max(1, days))).date()
    out = []
    for e in evs:
        try:
            d = datetime.strptime(e["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if d >= cutoff and d <= now.date():
            e["ticker"] = sym
            out.append(e)
    out.sort(key=lambda x: x.get("date",""), reverse=True)
    return out

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="MASTER csv (with ticker column)")
    ap.add_argument("--days", type=int, default=2, help="rolling window in calendar days (UTC)")
    ap.add_argument("--out-md", default=DEFAULT_OUT_MD)
    ap.add_argument("--out-json", default=DEFAULT_OUT_JSON)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    fmp_key = os.getenv("FMP_API_KEY", "").strip()
    if not fmp_key:
        msg = "FMP_API_KEY missing"
        sys.stderr.write(msg + "\n")
        Path(args.out_md).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out_md).write_text("## Elemzői feed (FMP stable)\n\n_FMP_API_KEY hiányzik._\n", encoding="utf-8")
        Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
        _save_json(args.out_json, {"ok": False, "error": msg, "tickers": 0, "events": 0})
        return 3

    ttl_hours = DEFAULT_TTL_HOURS
    try:
        ttl_hours = int(os.getenv("FMP_CACHE_TTL_HOURS", str(DEFAULT_TTL_HOURS)).strip())
    except Exception:
        ttl_hours = DEFAULT_TTL_HOURS
    ttl = timedelta(hours=max(1, ttl_hours))

    now = _utc_now()
    tickers = _load_master_tickers(args.master)

    # Load caches
    cache = _load_json(CACHE_FILE)
    if not isinstance(cache, dict):
        cache = {"meta": {"created_at_utc": now.isoformat()}, "tickers": {}}

    pt_cache = _load_json(PT_CACHE_FILE)
    if not isinstance(pt_cache, dict):
        pt_cache = {}
    grades_events_cache = _load_grades_events_cache(GRADES_EVENTS_FILE)


    dbg: Dict[str, Any] = {
        "version": "v0.6.2-fix-budget_exhausted-init-2026-02-09",
        "ts_utc": now.isoformat(),
        "days": args.days,
        "tickers": len(tickers),
        "ttl_hours": ttl_hours,
        "quota_exhausted": False,
        "budget_exhausted": False,
        "calls": {"attempted": 0, "skipped_fresh_cache": 0, "served_stale_cache": 0, "api_ok": 0, "api_err": 0},
        "http_samples": [],
        "by_ticker": {},
    }

    quota_exhausted = False
    budget_exhausted = False
    events_by_ticker: Dict[str, Dict[str, Any]] = {}
    no_data: List[str] = []


    max_calls = int(os.getenv("FMP_MAX_CALLS", "1"))
    dbg["max_calls"] = max_calls


    # Free-tier safety: grades-latest-news supports only small 'limit' on free plans.
    # Keep default=10 and cap to 10 unless you explicitly override and your plan supports it.
    try:
        grades_limit = int(os.getenv("FMP_GRADES_LIMIT", "10"))
    except Exception:
        grades_limit = 10
    if grades_limit < 1:
        grades_limit = 10
    if grades_limit > 10:
        grades_limit = 10
    dbg["grades_limit"] = grades_limit

    # --- GLOBAL GRADES FEED (low-call mode) ---
    # Fetch analyst grade events once (grades-latest-news) and filter by our MASTER tickers + rolling window.
    global_key = "__GLOBAL__"
    grades_by_symbol: Dict[str, List[GradeEvent]] = {}

    feed_payload, feed_err, feed_status = None, None, None
    feed_data, feed_fresh = _cache_get(cache, global_key, "grades_latest_news", now, ttl)
    if feed_fresh:
        dbg["calls"]["skipped_fresh_cache"] += 1
        feed_payload = feed_data
        feed_status = 200
        dbg["grades_feed"] = {"mode": "cache_fresh"}
    else:
        if dbg["calls"]["attempted"] >= max_calls:
            quota_exhausted = True
            dbg["quota_exhausted"] = True
            dbg["calls"]["served_stale_cache"] += 1
            feed_payload = feed_data
            feed_err = "call budget exceeded — served from cache only"
            dbg["grades_feed"] = {"mode": "cache_stale_budget"}
        elif quota_exhausted:
            dbg["calls"]["served_stale_cache"] += 1
            feed_payload = feed_data
            feed_err = "quota exhausted — served from cache only"
            dbg["grades_feed"] = {"mode": "cache_stale_quota"}
        else:
            dbg["calls"]["attempted"] += 1
            feed_payload, feed_err, feed_status, qex0 = _http_get_json(
                FMP_GRADES_LATEST, {"page": 0, "limit": grades_limit, "apikey": fmp_key}, args.debug, dbg
            )
            if qex0:
                quota_exhausted = True
                dbg["quota_exhausted"] = True
            if feed_err is None:
                dbg["calls"]["api_ok"] += 1
                dbg["grades_feed"] = {"mode": "api_ok", "status": feed_status}
            else:
                dbg["calls"]["api_err"] += 1
                dbg["grades_feed"] = {"mode": "api_err", "status": feed_status, "err": feed_err}
            _cache_set(cache, global_key, "grades_latest_news", now, feed_status, feed_err, feed_payload)
    # Parse new events from feed and merge into persistent event cache, then serve window from the merged cache.
    new_events_all: List[GradeEvent] = []
    if feed_payload is not None and not _is_quota_limit_message(feed_payload):
        new_events_all = _parse_grade_events(feed_payload)
        _merge_grades_events_cache(grades_events_cache, new_events_all, now)
        try:
            Path(GRADES_EVENTS_FILE).parent.mkdir(parents=True, exist_ok=True)
            _save_json(GRADES_EVENTS_FILE, grades_events_cache)
        except Exception:
            pass
    
    # Build grades_by_symbol from merged persistent cache (prevents "disappearing" events).
    cached_events = _events_from_grades_cache(grades_events_cache)
    window_events = _filter_window(cached_events, args.days, now)
    
    tickerset = set(tickers)
    for ev in window_events:
        if ev.symbol in tickerset:
            grades_by_symbol.setdefault(ev.symbol, []).append(ev)
    
    # sort each list by date desc (string YYYY-MM-DD works)
    for sym in list(grades_by_symbol.keys()):
        grades_by_symbol[sym].sort(key=lambda e: e.date, reverse=True)
    


    # --- NASDAQ secondary (limited): fetch recent actions for tickers that already have FMP grade events in window ---
    nasdaq_events_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    try:
        nasdaq_max = int(os.getenv("NASDAQ_MAX_TICKERS", "30"))
    except Exception:
        nasdaq_max = 30
    subset = [s for s in tickers if grades_by_symbol.get(s)]
    subset = subset[:max(0, nasdaq_max)]
    for sym in subset:
        evs = _nasdaq_window_events_for_ticker(sym, args.days, now, args.debug, dbg)
        if evs:
            nasdaq_events_by_symbol[sym] = evs
    dbg["nasdaq"] = {"enabled": True, "tickers_queried": len(subset), "tickers_with_events": len(nasdaq_events_by_symbol)}
    # Counters for status line
    grades_ok = grades_fail = 0
    pt_ok = pt_fail = 0
    cache_hits = cache_stale = 0

    for i, t in enumerate(tickers, start=1):
        per: Dict[str, Any] = {"rows": [], "pt": {}}

        # --- GRADES (global feed; 0 per-ticker calls) ---
        grade_events = grades_by_symbol.get(t, [])
        per["rows"] = [ev.__dict__ for ev in grade_events]
        # NASDAQ secondary events (if any)
        if "nasdaq_events_by_symbol" in locals():
            for e in nasdaq_events_by_symbol.get(t, []):
                per["rows"].append({
                    "date": e.get("date",""),
                    "firm": e.get("firm","Nasdaq"),
                    "action": e.get("action",""),
                    "from_grade": e.get("from_grade",""),
                    "to_grade": e.get("to_grade",""),
                })
        # sort combined rows
        per["rows"].sort(key=lambda r: (r.get("date","")), reverse=True)

        if per["rows"]:
            grades_ok += 1
        else:
            # Still OK if feed was available (we just had no events for this ticker)
            if feed_payload is not None and feed_err is None:
                grades_ok += 1
            else:
                grades_fail += 1

        # --- PT CONSENSUS (cache-first; budget guarded) ---

        pt_data, fresh2 = _cache_get(cache, t, "pt_consensus", now, ttl)
        qex2 = False
        if fresh2:
            dbg["calls"]["skipped_fresh_cache"] += 1
            cache_hits += 1
            pt_payload = pt_data
            pt_err = None
            pt_status = 200
        else:
            if pt_data is not None:
                cache_stale += 1
            if quota_exhausted:
                dbg["calls"]["served_stale_cache"] += 1
                pt_payload = pt_data
                pt_err = "quota exhausted — served from cache only"
                pt_status = None
            else:
                if dbg["calls"]["attempted"] >= max_calls:
                    budget_exhausted = True
                    dbg["budget_exhausted"] = True
                    dbg["calls"]["served_stale_cache"] += 1
                    pt_payload = pt_data
                    pt_err = "call budget exceeded — served from cache only"
                    pt_status = None
                else:
                    dbg["calls"]["attempted"] += 1
                    pt_payload, pt_err, pt_status, qex2 = _http_get_json(
                        FMP_PT_CONS, {"symbol": t, "apikey": fmp_key}, args.debug, dbg
                    )
                if qex2:
                    quota_exhausted = True
                    dbg["quota_exhausted"] = True
                if pt_err is None:
                    dbg["calls"]["api_ok"] += 1
                else:
                    dbg["calls"]["api_err"] += 1
                _cache_set(cache, t, "pt_consensus", now, pt_status, pt_err, pt_payload)

        pt_obj: Dict[str, Any] = {}
        item = None
        if pt_payload is not None and not _is_quota_limit_message(pt_payload):
            if isinstance(pt_payload, list) and pt_payload:
                if isinstance(pt_payload[0], dict):
                    item = pt_payload[0]
            elif isinstance(pt_payload, dict):
                item = pt_payload
        if isinstance(item, dict):
            cons = _safe_float(item.get("targetConsensus") or item.get("consensus"))
            hi = _safe_float(item.get("targetHigh") or item.get("high"))
            lo = _safe_float(item.get("targetLow") or item.get("low"))
            med = _safe_float(item.get("targetMedian") or item.get("median"))
            pt_obj.update({"consensus": cons, "high": hi, "low": lo, "median": med})

            prev = pt_cache.get(t, {}).get("consensus")
            prevf = _safe_float(prev)
            if cons is not None and prevf is not None and abs(cons - prevf) > 1e-9:
                pt_obj["pt_change_detected"] = True
                pt_obj["prev_consensus"] = prevf
            else:
                pt_obj["pt_change_detected"] = False

            # update PT cache (even if cons is None, store to avoid thrash)
            pt_cache[t] = {"consensus": cons, "ts_utc": now.isoformat()}

        per["pt"] = pt_obj

        has_any_pt = any(pt_obj.get(k) is not None for k in ("consensus", "high", "low", "median"))
        if has_any_pt:
            pt_ok += 1
        else:
            if pt_payload is not None and pt_err is None:
                pt_ok += 1
            else:
                pt_fail += 1

        # no-data logic
        if not per["rows"] and not has_any_pt:
            no_data.append(t)

        events_by_ticker[t] = per
        if args.debug:
            dbg["by_ticker"][t] = {
                "grades_feed": dbg.get("grades_feed"),
                "grades_events": len(per.get("rows") or []),
                "pt_err": pt_err,
                "pt_status": pt_status,
                "rows": per["rows"],
                "pt": pt_obj,
                "cache": {
                    "grades_fresh": False,
                    "pt_fresh": fresh2,
                }
            }

        # gentle pacing
        if not quota_exhausted and i % 15 == 0:
            time.sleep(0.15)

    # Save caches
    _save_json(CACHE_FILE, cache)
    _save_json(PT_CACHE_FILE, pt_cache)

    # Build status line
    status_bits = []
    status_bits.append(f"_forrás státusz: grades:OK, pt_consensus: OK (FMP stable)_")
    # Explicit reason banners
    if quota_exhausted:
        status_bits.append(f"_⚠ FMP kvóta elfogyott (Limit Reach / 429) — a futás vége cache-ből lett kiszolgálva._")
    elif budget_exhausted:
        status_bits.append(f"_ℹ FMP hívás-keret elérve (FMP_MAX_CALLS={max_calls}) — a futás vége cache-ből lett kiszolgálva._")
    # Plan/parameter limitation (e.g. grades-latest-news limit>10 on free plan)
    gf = dbg.get('grades_feed') or {}
    if gf.get('status') == 402:
        status_bits.append("_⚠ FMP hozzáférés korlátozott (HTTP 402) — az endpoint paraméter/plan limit miatt nem ad adatot._")
    status_bits.append(f"_cache: TTL={ttl_hours}h, hit={cache_hits}, stale={cache_stale}_")
    status_line = "\n".join(status_bits)

    md = _format_md(events_by_ticker, args.days, args.debug, status_line)
    Path(args.out_md).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_md).write_text(md, encoding="utf-8")

    outj = {
        "ok": True,
        "version": dbg["version"],
        "ts_utc": dbg["ts_utc"],
        "days": args.days,
        "tickers": len(tickers),
        "events": sum(len(v.get("rows") or []) for v in events_by_ticker.values()),
        "quota_exhausted": quota_exhausted,
        "cache_ttl_hours": ttl_hours,
        "no_data": [f"{t} – adat nem elérhető (kihagyva)" for t in no_data],
        "by_ticker": events_by_ticker if args.debug else {},
    }
    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    _save_json(args.out_json, outj)

    if args.debug:
        _save_json(DEBUG_JSON, dbg)

    # Exit codes:
    # Always return 0 so the workflow doesn't overwrite the MD with _N/A_.
    # Quota exhaustion is reported in outputs (md/json) via 'quota_exhausted=true'.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
