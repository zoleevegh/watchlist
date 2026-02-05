#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# analyst_marketbeat.py — v0.4.1-fmp-diagnostics-2026-02-05
#
# PURPOSE
# - MarketBeat HTML scraping is blocked on GitHub-hosted runners (Cloudflare).
# - This script replaces MarketBeat scraping with Financial Modeling Prep (FMP) APIs.
# - Adds HARD diagnostics + explicit exit codes so failures are visible in workflow logs.
#
# OUTPUTS
# - reports/analyst_last2d.md
# - reports/analyst_last2d.json
# - reports/analyst_probe.json (always)
# - reports/analyst_runtime.json (always)
# - reports/debug_fmp/* (when --debug)
#
# CLI (compatible)
#   --master <csv>
#   --days N
#   --out-md <path>
#   --out-json <path>
#   --debug
#
# EXIT CODES
#   0 = success (API access OK; events may be 0)
#   2 = configuration error (missing key)
#   3 = auth error (401/403)
#   4 = rate limited (429)
#   5 = unexpected API/parse error (non-JSON, schema mismatch, etc.)
#
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

FMP_BASE = "https://financialmodelingprep.com"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)

# Endpoints (FMP)
EP_QUOTE_V3 = "/api/v3/quote/{symbol}"
EP_UPDOWN_V4 = "/api/v4/upgrades-downgrades"
EP_PT_V4 = "/api/v4/price-target"

DEFAULT_TIMEOUT = 25


@dataclass
class FetchResult:
    ok: bool
    status: int
    url: str
    error: Optional[str] = None
    json_ok: bool = False
    items: int = 0


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Any) -> None:
    _ensure_parent(path)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_master_tickers(master_csv: str) -> List[str]:
    tickers: List[str] = []
    with open(master_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = (row.get("Ticker") or row.get("ticker") or "").strip()
            if t:
                tickers.append(t)

    # de-dup preserve order
    seen = set()
    out: List[str] = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _classify_http(status: int) -> Tuple[bool, Optional[int], str]:
    '''
    Returns: (terminal_error, exit_code_if_terminal, label)
    '''
    if status in (401, 403):
        return True, 3, "AUTH"
    if status == 429:
        return True, 4, "RATE_LIMIT"
    if status >= 500:
        return False, None, "SERVER"
    if status >= 400:
        return False, 5, "HTTP_ERROR"
    return False, None, "OK"


def _is_fmp_error_payload(obj: Any) -> Optional[str]:
    # FMP sometimes returns {"Error Message": "..."} or {"error": "..."} etc.
    if isinstance(obj, dict):
        for k in ("Error Message", "error", "message", "Error"):
            if k in obj and isinstance(obj[k], str):
                return obj[k]
    return None


def _fetch_json(session: requests.Session, url: str, debug_path: Optional[Path] = None) -> Tuple[FetchResult, Any]:
    headers = {"User-Agent": UA}
    try:
        r = session.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    except Exception as e:
        fr = FetchResult(ok=False, status=0, url=url, error=f"request_exception:{e}", json_ok=False)
        if debug_path:
            _ensure_parent(debug_path)
            debug_path.write_text(str(e), encoding="utf-8")
        return fr, None

    status = r.status_code
    text = r.text or ""

    if debug_path:
        _ensure_parent(debug_path)
        debug_path.write_text(text, encoding="utf-8", errors="ignore")

    # Try JSON parse
    try:
        obj = r.json()
        json_ok = True
    except Exception:
        obj = None
        json_ok = False

    terminal, exit_code, label = _classify_http(status)

    if status >= 400:
        msg = None
        if json_ok and obj is not None:
            msg = _is_fmp_error_payload(obj)
        if not msg:
            msg = (text.strip()[:200] if text else None)
        fr = FetchResult(ok=False, status=status, url=url, error=f"{label}:{msg}", json_ok=json_ok)
        return fr, obj

    # status < 400
    if not json_ok:
        fr = FetchResult(ok=False, status=status, url=url, error="PARSE:non_json_response", json_ok=False)
        return fr, None

    err_msg = _is_fmp_error_payload(obj)
    if err_msg:
        fr = FetchResult(ok=False, status=status, url=url, error=f"PAYLOAD_ERROR:{err_msg}", json_ok=True)
        return fr, obj

    items = len(obj) if isinstance(obj, list) else (len(obj.keys()) if isinstance(obj, dict) else 0)
    fr = FetchResult(ok=True, status=status, url=url, json_ok=True, items=items)
    return fr, obj


def _probe_key(session: requests.Session, api_key: str, debug: bool) -> Dict[str, Any]:
    symbol = "AAPL"
    url = f"{FMP_BASE}{EP_QUOTE_V3.format(symbol=symbol)}?apikey={api_key}"
    dbg = Path("reports/debug_fmp/probe_quote_AAPL.txt") if debug else None
    fr, obj = _fetch_json(session, url, dbg)

    diag: Dict[str, Any] = {
        "ts_utc": _now_utc().isoformat(),
        "endpoint": "quote_v3",
        "symbol": symbol,
        "url": url.replace(api_key, "****"),
        "http_status": fr.status,
        "ok": fr.ok,
        "error": fr.error,
        "json_ok": fr.json_ok,
        "items": fr.items,
    }

    if fr.ok:
        # quote endpoint should return list[dict]
        if not isinstance(obj, list) or not obj or not isinstance(obj[0], dict):
            diag["ok"] = False
            diag["error"] = "SCHEMA:quote_v3_expected_list_of_dict"
        else:
            diag["sample_keys"] = sorted(list(obj[0].keys()))[:25]

    _write_json(Path("reports/analyst_probe.json"), diag)
    return diag


def _parse_date_any(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _within_days(dt: datetime, days: int) -> bool:
    start = _now_utc() - timedelta(days=days)
    return dt >= start


def _event_key(e: Dict[str, Any]) -> str:
    parts = [
        str(e.get("ticker", "")),
        str(e.get("date", "")),
        str(e.get("firm", "")),
        str(e.get("type", "")),
        str(e.get("rating_to", "")),
        str(e.get("pt_to", "")),
    ]
    return "|".join(parts)


def _load_cache(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_cache(path: Path, events: List[Dict[str, Any]]) -> None:
    _write_json(path, events)


def _fetch_updown_for(session: requests.Session, api_key: str, ticker: str, debug: bool) -> Tuple[FetchResult, Any]:
    url = f"{FMP_BASE}{EP_UPDOWN_V4}?symbol={ticker}&apikey={api_key}"
    dbg = Path(f"reports/debug_fmp/{ticker}_updown.txt") if debug else None
    return _fetch_json(session, url, dbg)


def _fetch_pt_for(session: requests.Session, api_key: str, ticker: str, debug: bool) -> Tuple[FetchResult, Any]:
    url = f"{FMP_BASE}{EP_PT_V4}?symbol={ticker}&apikey={api_key}"
    dbg = Path(f"reports/debug_fmp/{ticker}_pt.txt") if debug else None
    return _fetch_json(session, url, dbg)


def _extract_events_updown(ticker: str, obj: Any, days: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(obj, list):
        return out
    for it in obj:
        if not isinstance(it, dict):
            continue
        dt = _parse_date_any(it.get("publishedDate") or it.get("date") or it.get("time"))
        if not dt or not _within_days(dt, days):
            continue
        action = (it.get("action") or it.get("type") or "").strip()
        firm = (it.get("gradingCompany") or it.get("firm") or it.get("company") or "").strip()
        r_from = (it.get("previousGrade") or it.get("fromGrade") or it.get("ratingFrom") or "").strip() or None
        r_to = (it.get("newGrade") or it.get("toGrade") or it.get("ratingTo") or "").strip() or None
        kind = "upgrade" if action.lower() == "upgrade" else ("downgrade" if action.lower() == "downgrade" else "rating")
        out.append({
            "source": "FMP",
            "type": kind,
            "ticker": ticker,
            "date": dt.date().isoformat(),
            "ts_utc": dt.isoformat(),
            "firm": firm,
            "action": action or kind,
            "rating_from": r_from,
            "rating_to": r_to,
            "pt_from": None,
            "pt_to": None,
        })
    return out


def _extract_events_pt(ticker: str, obj: Any, days: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(obj, list):
        return out
    for it in obj:
        if not isinstance(it, dict):
            continue
        dt = _parse_date_any(it.get("publishedDate") or it.get("date") or it.get("time"))
        if not dt or not _within_days(dt, days):
            continue
        firm = (it.get("analystCompany") or it.get("gradingCompany") or it.get("firm") or "").strip()
        rating = (it.get("rating") or it.get("analystRating") or "").strip() or None
        pt_from = it.get("oldPriceTarget") if "oldPriceTarget" in it else it.get("ptFrom")
        pt_to = it.get("newPriceTarget") if "newPriceTarget" in it else (it.get("priceTarget") or it.get("ptTo"))
        out.append({
            "source": "FMP",
            "type": "pt_change",
            "ticker": ticker,
            "date": dt.date().isoformat(),
            "ts_utc": dt.isoformat(),
            "firm": firm,
            "action": "price_target",
            "rating_from": None,
            "rating_to": rating,
            "pt_from": pt_from,
            "pt_to": pt_to,
        })
    return out


def _render_md(events: List[Dict[str, Any]], days: int, status_line: str) -> str:
    lines: List[str] = []
    lines.append(f"## Elemzői feed (FMP) – fel/lemínősítések + célár (utolsó {days} naptári nap)")
    lines.append("")
    lines.append(status_line)
    lines.append("")
    if not events:
        lines.append("_Nincs találat az ablakban._")
        return "\n".join(lines) + "\n"

    by_t: Dict[str, List[Dict[str, Any]]] = {}
    for e in events:
        by_t.setdefault(e["ticker"], []).append(e)

    for t in sorted(by_t.keys()):
        lines.append(f"## {t}")
        for e in sorted(by_t[t], key=lambda x: x.get("ts_utc", ""), reverse=True):
            firm = e.get("firm") or "?"
            date = e.get("date") or "?"
            typ = e.get("type")
            if typ == "pt_change":
                lines.append(
                    f"- {date} – {firm} – célár változás | Ajánlás: {e.get('rating_to') or '-'} | "
                    f"Célár: {e.get('pt_from') or '-'} → {e.get('pt_to') or '-'} | Forrás: FMP"
                )
            else:
                lines.append(
                    f"- {date} – {firm} – {typ} | Ajánlás: {e.get('rating_from') or '-'} → {e.get('rating_to') or '-'} | Forrás: FMP"
                )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    api_key = (os.environ.get("FMP_API_KEY") or os.environ.get("FMP_KEY") or "").strip()

    runtime = {
        "ts_utc": _now_utc().isoformat(),
        "version": "v0.4.1-fmp-diagnostics-2026-02-05",
        "master": args.master,
        "days": args.days,
        "debug": bool(args.debug),
        "api_key_present": bool(api_key),
    }
    _write_json(Path("reports/analyst_runtime.json"), runtime)

    if not api_key:
        status_line = "_forrás státusz: probe:FAIL (missing FMP_API_KEY secret)._"
        Path(args.out_md).write_text(_render_md([], args.days, status_line), encoding="utf-8")
        _write_json(Path(args.out_json), {"events": [], "probe": {"ok": False, "reason": "missing_key"}, "version": runtime["version"]})
        print("FMP_PROBE:FAIL missing_key", file=sys.stderr)
        sys.exit(2)

    session = requests.Session()
    probe = _probe_key(session, api_key, args.debug)

    if not probe.get("ok"):
        st = int(probe.get("http_status") or 0)
        _, exit_code, _ = _classify_http(st)
        status_line = f"_forrás státusz: probe:FAIL (HTTP {st}) | {probe.get('error') or ''}_"
        Path(args.out_md).write_text(_render_md([], args.days, status_line), encoding="utf-8")
        _write_json(Path(args.out_json), {"events": [], "probe": probe, "version": runtime["version"]})
        print(f"FMP_PROBE:FAIL status={st} error={probe.get('error')}", file=sys.stderr)
        sys.exit(exit_code or 5)

    tickers = _read_master_tickers(args.master)

    cache_path = Path("reports/fmp_events.json")
    cache = _load_cache(cache_path)

    all_events: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    ok_calls = 0
    auth_err = 0
    rate_err = 0
    other_err = 0

    for i, t in enumerate(tickers, start=1):
        if i % 25 == 0:
            time.sleep(0.8)

        fr_ud, obj_ud = _fetch_updown_for(session, api_key, t, args.debug)
        fr_pt, obj_pt = _fetch_pt_for(session, api_key, t, args.debug)

        for fr in (fr_ud, fr_pt):
            if fr.ok:
                ok_calls += 1
            else:
                st = fr.status
                _, exit_code, label = _classify_http(st)
                if label == "AUTH":
                    auth_err += 1
                elif label == "RATE_LIMIT":
                    rate_err += 1
                else:
                    other_err += 1

        if fr_ud.ok:
            all_events.extend(_extract_events_updown(t, obj_ud, args.days))
        else:
            errors.append({"ticker": t, "kind": "updown", "status": fr_ud.status, "error": fr_ud.error})

        if fr_pt.ok:
            all_events.extend(_extract_events_pt(t, obj_pt, args.days))
        else:
            errors.append({"ticker": t, "kind": "pt", "status": fr_pt.status, "error": fr_pt.error})

    # Merge with cache (keep last 30 days)
    keep_from = (_now_utc() - timedelta(days=30)).date().isoformat()
    merged: Dict[str, Dict[str, Any]] = {}
    for e in (cache + all_events):
        if (e.get("date") or "") < keep_from:
            continue
        merged[_event_key(e)] = e
    merged_events = list(merged.values())
    _save_cache(cache_path, merged_events)

    # Filter output window
    out_events: List[Dict[str, Any]] = []
    for e in merged_events:
        dt = _parse_date_any(e.get("ts_utc") or e.get("date"))
        if dt and _within_days(dt, args.days):
            out_events.append(e)

    status_line = f"_forrás státusz: probe:OK(200) | calls_ok={ok_calls} auth_err={auth_err} rate_err={rate_err} other_err={other_err}_"
    out_obj = {
        "version": runtime["version"],
        "probe": probe,
        "events": out_events,
        "stats": {
            "tickers": len(tickers),
            "events_window": len(out_events),
            "calls_ok": ok_calls,
            "auth_err": auth_err,
            "rate_err": rate_err,
            "other_err": other_err,
        },
        "errors_sample": errors[:80],
    }
    _write_json(Path(args.out_json), out_obj)
    Path(args.out_md).write_text(_render_md(out_events, args.days, status_line), encoding="utf-8")

    # Escalate if everything failed after a good probe (rare, but makes issues visible)
    if ok_calls == 0 and rate_err > 0:
        print("FMP_CALLS:FAIL rate_limited", file=sys.stderr)
        sys.exit(4)
    if ok_calls == 0 and auth_err > 0:
        print("FMP_CALLS:FAIL auth", file=sys.stderr)
        sys.exit(3)

    sys.exit(0)


if __name__ == "__main__":
    main()
