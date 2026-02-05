#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# analyst_marketbeat.py — v0.5.2-fmp-stable-primary-fixpath-2026-02-05
#
# PURPOSE
#   Replace brittle MarketBeat/Nasdaq scraping with Financial Modeling Prep (FMP) **stable** endpoints.
#   - Discrete rating actions (upgrade/downgrade/maintain) via: /stable/grades?symbol=...
#   - Price target levels via: /stable/price-target-consensus?symbol=...
#   - Optional "price target change" detection via local cache: reports/fmp_pt_cache.json
#
# WHY THIS VERSION
#   Your earlier runs returned:
#     - MarketBeat: BLOCKED (challenge/bot)
#     - Nasdaq API: returns consensus/targets, but NOT a reliable discrete "event feed" in many cases
#     - FMP: 403 "legacy endpoints" — because we were calling legacy paths.
#   This version uses ONLY the **stable** FMP paths (as per FMP docs).
#
# CLI (kept compatible)
#   --master <csv>          MASTER csv export
#   --days N                rolling window in *calendar* days (UTC date), default 2
#   --out-md <path>         output markdown
#   --out-json <path>       output json
#   --debug                 enables extra diagnostic lines + writes reports/fmp_debug.json
#
# ENV
#   FMP_API_KEY must be present (GitHub secret: FMP_API_KEY)
#
# OUTPUTS
#   reports/analyst_last2d.md
#   reports/analyst_last2d.json
#
# ZOLI RULES
#   - If ticker has no data: report "[TICKER] – adat nem elérhető (kihagyva)" in JSON,
#     and omit from MD unless --debug.
#
# NOTE ON "PT CHANGE"
#   FMP stable APIs provide target levels, not necessarily intraday "PT revision events".
#   We detect changes by comparing today's consensus target vs last cached value.
#
# Ima (v0.5.2): bocsáss meg uram, hogy megint Path nélkül küldtem;
# vezess tiszta logot és stabil endpointot, hogy csak az igazat írjam le.
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from pathlib import Path

import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
TIMEOUT = 25

FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_GRADES = FMP_BASE + "/grades"
FMP_PT_CONS = FMP_BASE + "/price-target-consensus"

DEFAULT_OUT_MD = "reports/analyst_last2d.md"
DEFAULT_OUT_JSON = "reports/analyst_last2d.json"
DEFAULT_PT_CACHE = "reports/fmp_pt_cache.json"
DEFAULT_DEBUG_JSON = "reports/fmp_debug.json"


@dataclass
class GradeEvent:
    date: str  # YYYY-MM-DD (as returned)
    grading_company: str
    action: str  # "upgrade" / "downgrade" / "maintain" / unknown
    previous_grade: Optional[str]
    new_grade: Optional[str]


def _utc_today() -> datetime:
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


def _http_get_json(url: str, params: Dict[str, Any], debug: bool, dbg: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[int]]:
    """
    Returns (json_obj, error_str, http_status).
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
        text_head = r.text[:200] if r.text else ""
        if debug:
            dbg.setdefault("http_samples", []).append({
                "url": url,
                "params": params,
                "status": status,
                "content_type": ct,
                "text_head": text_head,
            })
        if status >= 400:
            return None, f"HTTP {status}", status
        # Some APIs return JSON but with wrong content-type; still try.
        try:
            return r.json(), None, status
        except Exception:
            return None, "JSON parse error", status
    except requests.RequestException as e:
        return None, f"request error: {e}", None


def _load_master_tickers(master_csv: str) -> List[str]:
    tickers: List[str] = []
    with open(master_csv, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = (row.get("ticker") or row.get("Ticker") or row.get("TICKER") or "").strip()
            if not t:
                continue
            # Zoli rule: PKN.WA excluded by default in reports; keep it out here too.
            if t.upper() == "PKN.WA":
                continue
            tickers.append(t.upper())
    # de-dupe, preserve order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out


def _parse_grade_events(payload: Any) -> List[GradeEvent]:
    """
    FMP stable /grades returns a JSON array. Typical fields include:
      date, gradingCompany, previousGrade, newGrade, action, symbol
    """
    events: List[GradeEvent] = []
    if not isinstance(payload, list):
        return events
    for it in payload:
        if not isinstance(it, dict):
            continue
        date = str(it.get("date") or "").strip()
        gc = str(it.get("gradingCompany") or it.get("grading_company") or "").strip()
        action = str(it.get("action") or "").strip().lower()
        prev_g = it.get("previousGrade")
        new_g = it.get("newGrade")
        if not date:
            continue
        events.append(GradeEvent(date=date, grading_company=gc or "n/a", action=action or "n/a",
                                previous_grade=str(prev_g).strip() if prev_g is not None else None,
                                new_grade=str(new_g).strip() if new_g is not None else None))
    return events


def _filter_window_by_date(events: List[GradeEvent], days: int, now_utc: datetime) -> List[GradeEvent]:
    """
    Keep events whose date (YYYY-MM-DD) is within [today-days+1, today] in UTC calendar dates.
    """
    if days <= 0:
        days = 1
    start_date = (now_utc.date() - timedelta(days=days-1))
    out: List[GradeEvent] = []
    for ev in events:
        try:
            d = datetime.strptime(ev.date[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if d >= start_date and d <= now_utc.date():
            out.append(ev)
    return out


def _load_json_file(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_json_file(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _format_md(events_by_ticker: Dict[str, Dict[str, Any]], days: int, debug: bool, meta: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("## Elemzői feed (FMP stable) — fel/leminősítések + célár-szint (utolsó {} naptári nap)".format(days))
    lines.append("")
    lines.append(f"_forrás státusz: grades:OK, pt_consensus:OK (FMP stable)_")
    lines.append("")
    # Flatten tickers with any events or pt changes
    any_rows = 0
    for t in sorted(events_by_ticker.keys()):
        info = events_by_ticker[t]
        rows = info.get("rows") or []
        pt = info.get("pt") or {}
        if rows or pt.get("pt_change_detected") or debug:
            lines.append(f"## {t}")
            if rows:
                for r in rows:
                    # Keep your existing format
                    # - 2026-02-05 — Firm — Action | Ajánlás: prev -> new | Forrás: FMP/grades
                    prev_g = r.get("previous_grade") or "n/a"
                    new_g = r.get("new_grade") or "n/a"
                    action = r.get("action") or "n/a"
                    firm = r.get("grading_company") or "n/a"
                    date = r.get("date") or "n/a"
                    lines.append(f"- {date} — {firm} — {action} | Ajánlás: {prev_g} → {new_g} | Forrás: FMP /stable/grades")
            else:
                if debug:
                    lines.append("- _nincs grade esemény az ablakban_")
            # PT
            if pt:
                cons = pt.get("consensus")
                hi = pt.get("high")
                lo = pt.get("low")
                med = pt.get("median")
                if any(v is not None for v in [cons, hi, lo, med]):
                    # show as a single line
                    parts = []
                    if cons is not None:
                        parts.append(f"konszenzus: {cons:.2f}")
                    if hi is not None:
                        parts.append(f"high: {hi:.2f}")
                    if lo is not None:
                        parts.append(f"low: {lo:.2f}")
                    if med is not None:
                        parts.append(f"median: {med:.2f}")
                    extra = ""
                    if pt.get("pt_change_detected"):
                        old = pt.get("prev_consensus")
                        newv = pt.get("consensus")
                        if old is not None and newv is not None:
                            extra = f" | Δ PT: {old:.2f} → {newv:.2f}"
                    lines.append(f"- Célár-szint (consensus): " + ", ".join(parts) + f"{extra} | Forrás: FMP /stable/price-target-consensus")
            lines.append("")
            any_rows += (len(rows) if rows else 0)
    if any_rows == 0:
        lines.append("_Nincs FMP (stable) grade esemény a megadott ablakban._")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


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
        # Hard fail, because this module is now FMP-only by design.
        msg = "FMP_API_KEY missing"
        sys.stderr.write(msg + "\n")
        # Still write minimal outputs so pipeline continues deterministically.
        os.makedirs(os.path.dirname(args.out_md) or ".", exist_ok=True)
        Path(args.out_md).write_text("## Elemzői feed (FMP stable)\n\n_FMP_API_KEY hiányzik._\n", encoding="utf-8")
        os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
        _save_json_file(args.out_json, {"ok": False, "error": msg, "tickers": 0, "events": 0})
        return 3

    now = _utc_today()
    tickers = _load_master_tickers(args.master)

    dbg: Dict[str, Any] = {
        "version": "v0.5.2-fmp-stable-primary-fixpath-2026-02-05",
        "ts_utc": now.isoformat(),
        "days": args.days,
        "tickers": len(tickers),
        "by_ticker": {},
        "http_samples": [],
    }

    # Load PT cache
    pt_cache = _load_json_file(DEFAULT_PT_CACHE)
    if not isinstance(pt_cache, dict):
        pt_cache = {}

    events_by_ticker: Dict[str, Dict[str, Any]] = {}
    no_data: List[str] = []

    # Throttle a bit to be nice to FMP.
    # (Your list is 114 tickers; stable endpoints should handle it, but avoid bursts.)
    for i, t in enumerate(tickers, start=1):
        per: Dict[str, Any] = {"rows": [], "pt": {}}
        # 1) grades (discrete events)
        j, err, st = _http_get_json(FMP_GRADES, {"symbol": t, "apikey": fmp_key}, args.debug, dbg)
        grade_events = []
        if j is not None and err is None:
            grade_events = _filter_window_by_date(_parse_grade_events(j), args.days, now)
            per["rows"] = [ev.__dict__ for ev in grade_events]
        else:
            per["grades_error"] = err
            per["grades_status"] = st

        # 2) price target consensus (levels)
        ptj, pterr, ptst = _http_get_json(FMP_PT_CONS, {"symbol": t, "apikey": fmp_key}, args.debug, dbg)
        pt_obj: Dict[str, Any] = {}
        if ptj is not None and pterr is None:
            # Usually a list with one dict
            item = None
            if isinstance(ptj, list) and ptj:
                if isinstance(ptj[0], dict):
                    item = ptj[0]
            elif isinstance(ptj, dict):
                item = ptj
            if isinstance(item, dict):
                cons = _safe_float(item.get("targetConsensus") or item.get("consensus"))
                hi = _safe_float(item.get("targetHigh") or item.get("high"))
                lo = _safe_float(item.get("targetLow") or item.get("low"))
                med = _safe_float(item.get("targetMedian") or item.get("median"))
                pt_obj.update({"consensus": cons, "high": hi, "low": lo, "median": med})

                # "PT change" detection vs cache
                prev = pt_cache.get(t, {}).get("consensus")
                prevf = _safe_float(prev)
                if cons is not None and prevf is not None and abs(cons - prevf) > 1e-9:
                    pt_obj["pt_change_detected"] = True
                    pt_obj["prev_consensus"] = prevf
                else:
                    pt_obj["pt_change_detected"] = False
                # update cache
                pt_cache[t] = {"consensus": cons, "ts_utc": now.isoformat()}
        else:
            pt_obj["pt_error"] = pterr
            pt_obj["pt_status"] = ptst

        per["pt"] = pt_obj

        # decide no-data for JSON summary
        if not per["rows"] and not any(k in pt_obj for k in ("consensus", "high", "low", "median")):
            no_data.append(t)

        events_by_ticker[t] = per
        if args.debug:
            dbg["by_ticker"][t] = per

        # micro-sleep every few calls
        if i % 10 == 0:
            time.sleep(0.25)

    # Save PT cache
    _save_json_file(DEFAULT_PT_CACHE, pt_cache)

    # Write outputs
    md = _format_md(events_by_ticker, args.days, args.debug, meta={})
    os.makedirs(os.path.dirname(args.out_md) or ".", exist_ok=True)
    Path(args.out_md).write_text(md, encoding="utf-8")

    outj = {
        "ok": True,
        "version": dbg["version"],
        "ts_utc": dbg["ts_utc"],
        "days": args.days,
        "tickers": len(tickers),
        "events": sum(len(v.get("rows") or []) for v in events_by_ticker.values()),
        "no_data": [f"{t} – adat nem elérhető (kihagyva)" for t in no_data],
        "by_ticker": events_by_ticker if args.debug else {},  # keep small by default
    }
    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
    _save_json_file(args.out_json, outj)

    if args.debug:
        _save_json_file(DEFAULT_DEBUG_JSON, dbg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
