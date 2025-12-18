#!/usr/bin/env python3
"""
earnings_fetcher.py – v2.0.0

Goal:
  Build reports/earnings_{report}.json with *upcoming* earnings for tickers in the report universe.
  Primary source: Yahoo Finance quoteSummary (calendarEvents module).
  Fallback: Yahoo Finance /quote/<TICKER> HTML embedded JSON (QuoteSummaryStore).

Notes:
  - Designed to run inside GitHub Actions (internet available there).
  - Outputs are deterministic JSON; never raises hard failure unless args invalid.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import urllib.request
import urllib.error

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0 Safari/537.36"
)

DEFAULT_MAX_DAYS_AHEAD = 10  # show next 10 days by default

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _read_json_file(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)

def _http_get(url: str, timeout: int = 20) -> Tuple[Optional[bytes], Optional[int], Optional[str]]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json,text/html,*/*"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read(), resp.getcode(), None
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
        except Exception:
            body = None
        return body, e.code, f"HTTPError {e.code}"
    except Exception as e:
        return None, None, f"{type(e).__name__}: {e}"

def _qs_calendar_events(ticker: str) -> Tuple[Optional[dict], Optional[str]]:
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=calendarEvents&corsDomain=finance.yahoo.com"
    body, status, err = _http_get(url)
    if body is None:
        return None, err or f"no-body (status={status})"
    try:
        return json.loads(body.decode("utf-8", errors="replace")), None
    except Exception as e:
        return None, f"json-parse: {e}"

_QSS_STORE_RE = re.compile(r"root\.App\.main\s*=\s*(\{.*?\});\s*}\(this\)\);", re.DOTALL)

def _html_quote_summary_store(ticker: str) -> Tuple[Optional[dict], Optional[str]]:
    url = f"https://finance.yahoo.com/quote/{ticker}"
    body, status, err = _http_get(url, timeout=25)
    if body is None:
        return None, err or f"no-body (status={status})"
    html = body.decode("utf-8", errors="replace")
    m = _QSS_STORE_RE.search(html)
    if not m:
        return None, "QuoteSummaryStore not found in HTML"
    try:
        app = json.loads(m.group(1))
        store = (app.get("context", {})
                  .get("dispatcher", {})
                  .get("stores", {})
                  .get("QuoteSummaryStore"))
        if not store:
            return None, "QuoteSummaryStore missing/empty"
        return store, None
    except Exception as e:
        return None, f"html-store-parse: {e}"

def _parse_earnings_from_calendar_events(obj: dict) -> Optional[Tuple[int, int]]:
    try:
        result = obj.get("quoteSummary", {}).get("result")
        if isinstance(result, list) and result:
            cal = result[0].get("calendarEvents", {})
            ed = cal.get("earnings", {}).get("earningsDate")
            if isinstance(ed, list) and ed:
                raw_vals = [x.get("raw") for x in ed if isinstance(x, dict) and isinstance(x.get("raw"), (int, float))]
                if raw_vals:
                    return int(min(raw_vals)), int(max(raw_vals))
    except Exception:
        pass
    return None

def _epoch_to_iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

def _load_tickers(report: str) -> List[str]:
    env = os.getenv("TICKERS", "").strip()
    if env:
        return [t.strip().upper() for t in env.split(",") if t.strip()]

    candidates = [f"reports/{report}/latest_{report}.json", f"reports/latest_{report}.json"]
    for p in candidates:
        j = _read_json_file(p)
        if not j:
            continue
        if isinstance(j, list):
            return [str(x).upper() for x in j if str(x).strip()]
        if isinstance(j, dict):
            if isinstance(j.get("tickers"), list):
                return [str(x).upper() for x in j["tickers"] if str(x).strip()]
            out: List[str] = []
            for key in ("positions", "watchlist", "symbols"):
                arr = j.get(key)
                if isinstance(arr, list):
                    for it in arr:
                        if isinstance(it, str):
                            out.append(it.upper())
                        elif isinstance(it, dict) and it.get("ticker"):
                            out.append(str(it["ticker"]).upper())
            if out:
                return sorted(set(out))

    txt_path = f"reports/tickers_{report}.txt"
    try:
        with open(txt_path, "r", encoding="utf-8") as f:
            return sorted({line.strip().upper() for line in f if line.strip()})
    except FileNotFoundError:
        return []

@dataclass
class SourceStatus:
    ok: bool
    count: int
    errors: List[str]

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, help="1|2|3")
    ap.add_argument("--max-days-ahead", type=int, default=DEFAULT_MAX_DAYS_AHEAD)
    ap.add_argument("--out", default=None, help="override output path")
    args = ap.parse_args()

    report = str(args.report).strip()
    max_days = max(1, int(args.max_days_ahead))

    tickers = _load_tickers(report)
    out_path = args.out or f"reports/earnings_{report}.json"
    _ensure_dir(os.path.dirname(out_path) or ".")

    now_epoch = int(time.time())
    cutoff_epoch = now_epoch + max_days * 86400

    items: List[dict] = []
    src_qs = SourceStatus(ok=True, count=0, errors=[])
    src_html = SourceStatus(ok=True, count=0, errors=[])

    if not tickers:
        payload = {
            "ok": True,
            "type": "earnings",
            "report": report,
            "generatedAt": _utc_iso(),
            "window": {"maxDaysAhead": max_days},
            "count": 0,
            "items": [],
            "sources": {
                "yahoo_quoteSummary_calendarEvents": asdict(src_qs),
                "yahoo_html_fallback": asdict(src_html),
            },
            "warning": "No tickers found (set env TICKERS or provide reports/latest_*.json)."
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return 0

    for t in tickers:
        data, err = _qs_calendar_events(t)
        parsed = _parse_earnings_from_calendar_events(data or {})
        if parsed:
            src_qs.count += 1
            start, end = parsed
            if end >= now_epoch and start <= cutoff_epoch:
                items.append({
                    "ticker": t,
                    "source": "YahooFinance.quoteSummary.calendarEvents",
                    "earningsDateUtcStart": _epoch_to_iso(start),
                    "earningsDateUtcEnd": _epoch_to_iso(end),
                    "earningsEpochStart": start,
                    "earningsEpochEnd": end,
                })
            continue
        if err:
            src_qs.ok = False
            src_qs.errors.append(f"{t}: {err}")

        store, err2 = _html_quote_summary_store(t)
        if store and isinstance(store, dict):
            try:
                cal = store.get("calendarEvents", {})
                ed = (cal.get("earnings", {}) or {}).get("earningsDate")
                if isinstance(ed, list) and ed:
                    raw_vals = [x.get("raw") for x in ed if isinstance(x, dict) and isinstance(x.get("raw"), (int, float))]
                    if raw_vals:
                        src_html.count += 1
                        start = int(min(raw_vals))
                        end = int(max(raw_vals))
                        if end >= now_epoch and start <= cutoff_epoch:
                            items.append({
                                "ticker": t,
                                "source": "YahooFinance.quote.HTML.QuoteSummaryStore.calendarEvents",
                                "earningsDateUtcStart": _epoch_to_iso(start),
                                "earningsDateUtcEnd": _epoch_to_iso(end),
                                "earningsEpochStart": start,
                                "earningsEpochEnd": end,
                            })
                        continue
            except Exception as e:
                err2 = f"html-parse: {e}"

        if err2:
            src_html.ok = False
            src_html.errors.append(f"{t}: {err2}")

    # Dedup by ticker keeping earliest date
    by_t: Dict[str, dict] = {}
    for it in items:
        t = it["ticker"]
        if t not in by_t or it["earningsEpochStart"] < by_t[t]["earningsEpochStart"]:
            by_t[t] = it
    items = sorted(by_t.values(), key=lambda x: (x["earningsEpochStart"], x["ticker"]))

    payload = {
        "ok": True,
        "type": "earnings",
        "report": report,
        "generatedAt": _utc_iso(),
        "window": {"maxDaysAhead": max_days},
        "count": len(items),
        "items": items,
        "sources": {
            "yahoo_quoteSummary_calendarEvents": asdict(src_qs),
            "yahoo_html_fallback": asdict(src_html),
        },
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
