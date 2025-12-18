#!/usr/bin/env python3
"""
earnings_fetcher.py – v3.0.0 (Yahoo HTML scrape)

B-option implementation:
  - Primary: scrape https://finance.yahoo.com/calendar/earnings?symbol={TICKER}
    and extract the earnings date from embedded root.App.main JSON (preferred)
    or from visible HTML table (fallback).
  - Secondary: scrape https://finance.yahoo.com/quote/{TICKER} and attempt
    QuoteSummaryStore.calendarEvents earningsDate (if present).

Output:
  reports/earnings_{report}.json
Schema:
  { ok, type, report, generatedAt, window, count, items[], sources{...} }

No external deps.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import urllib.request
import urllib.error

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0 Safari/537.36"
)

DEFAULT_MAX_DAYS_AHEAD = 10


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def _read_json_file(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _http_get(url: str, timeout: int = 25) -> Tuple[Optional[bytes], Optional[int], Optional[str]]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html,application/json,*/*"})
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


def _extract_root_app_main(html: str) -> Tuple[Optional[dict], Optional[str]]:
    """
    Robustly extract the JSON assigned to root.App.main = {...};
    Uses brace balancing from the first '{' after the assignment.
    """
    idx = html.find("root.App.main")
    if idx < 0:
        return None, "root.App.main not found"
    eq = html.find("=", idx)
    if eq < 0:
        return None, "root.App.main '=' not found"
    brace = html.find("{", eq)
    if brace < 0:
        return None, "root.App.main '{' not found"
    depth = 0
    in_str = False
    esc = False
    for i in range(brace, len(html)):
        ch = html[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    blob = html[brace:i+1]
                    try:
                        return json.loads(blob), None
                    except Exception as e:
                        return None, f"root.App.main json parse error: {e}"
    return None, "root.App.main JSON not closed"


def _epoch_to_iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _parse_epoch_from_store(obj: dict) -> List[int]:
    """
    Try multiple likely store paths. Returns list of epoch ints.
    """
    epochs: List[int] = []

    def add_raw(v: Any) -> None:
        if isinstance(v, (int, float)):
            epochs.append(int(v))
        elif isinstance(v, dict) and isinstance(v.get("raw"), (int, float)):
            epochs.append(int(v["raw"]))

    store = (obj.get("context", {})
               .get("dispatcher", {})
               .get("stores", {}))

    # Path 1: QuoteSummaryStore.calendarEvents.earnings.earningsDate
    try:
        qss = store.get("QuoteSummaryStore") or {}
        cal = qss.get("calendarEvents") or {}
        earnings = cal.get("earnings") or {}
        ed = earnings.get("earningsDate")
        if isinstance(ed, list):
            for x in ed:
                add_raw(x)
    except Exception:
        pass

    # Path 2: Calendar-related stores (earnings page)
    for key in ("FinanceCalendarStore", "CalendarStore", "EarningsCalendarStore"):
        try:
            s = store.get(key) or {}
            for k in ("earnings", "events", "items", "rows", "data"):
                v = s.get(k)
                if isinstance(v, list):
                    for it in v:
                        if isinstance(it, dict):
                            for dk in ("earningsDate", "eventDate", "startDate", "date"):
                                add_raw(it.get(dk))
                elif isinstance(v, dict):
                    for dk in ("events", "items", "rows", "result"):
                        vv = v.get(dk)
                        if isinstance(vv, list):
                            for it in vv:
                                if isinstance(it, dict):
                                    for dd in ("earningsDate", "eventDate", "startDate", "date"):
                                        add_raw(it.get(dd))
        except Exception:
            pass

    return epochs


_EARNINGS_DATE_TEXT_RE = re.compile(r"(?i)\bEarnings Date\b")
_DATE_CELL_RE = re.compile(r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b\.?\s+\d{1,2},\s+\d{4})")


def _parse_date_text_from_html(html: str) -> Optional[str]:
    m = _EARNINGS_DATE_TEXT_RE.search(html)
    if not m:
        return None
    snippet = html[m.start(): m.start()+2500]
    d = _DATE_CELL_RE.search(snippet)
    if d:
        return d.group(1)
    return None


def _load_tickers(report: str) -> List[str]:
    env = os.getenv("TICKERS", "").strip()
    if env:
        return [t.strip().upper() for t in env.split(",") if t.strip()]

    for p in (f"reports/{report}/latest_{report}.json", f"reports/latest_{report}.json"):
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

    try:
        with open(f"reports/tickers_{report}.txt", "r", encoding="utf-8") as f:
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
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    report = str(args.report).strip()
    max_days = max(1, int(args.max_days_ahead))
    out_path = args.out or f"reports/earnings_{report}.json"
    _ensure_dir(os.path.dirname(out_path) or ".")

    tickers = _load_tickers(report)
    now_epoch = int(time.time())
    cutoff_epoch = now_epoch + max_days * 86400

    src_cal = SourceStatus(ok=True, count=0, errors=[])
    src_quote = SourceStatus(ok=True, count=0, errors=[])
    src_text = SourceStatus(ok=True, count=0, errors=[])

    items: List[dict] = []

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
                "yahoo_calendar_earnings_html": asdict(src_cal),
                "yahoo_quote_html": asdict(src_quote),
                "yahoo_calendar_text_fallback": asdict(src_text),
            },
            "warning": "No tickers found (set env TICKERS or provide reports/latest_*.json).",
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return 0

    for t in tickers:
        found_epochs: List[int] = []

        # 1) Earnings calendar page
        url = f"https://finance.yahoo.com/calendar/earnings?symbol={t}"
        body, status, err = _http_get(url, timeout=25)
        if body is not None:
            html = body.decode("utf-8", errors="replace")
            app, eapp = _extract_root_app_main(html)
            if app:
                epochs = _parse_epoch_from_store(app)
                found_epochs.extend(epochs)
                if epochs:
                    src_cal.count += 1
            else:
                src_cal.ok = False
                src_cal.errors.append(f"{t}: {eapp or 'root.App.main parse failed'}")
                dtxt = _parse_date_text_from_html(html)
                if dtxt:
                    src_text.count += 1
                    items.append({
                        "ticker": t,
                        "source": "YahooFinance.calendar.earnings.HTML.text",
                        "earningsDateText": dtxt,
                    })
        else:
            src_cal.ok = False
            src_cal.errors.append(f"{t}: {err or f'no-body (status={status})'}")

        # 2) Quote page (sometimes has calendarEvents)
        if not found_epochs:
            qurl = f"https://finance.yahoo.com/quote/{t}"
            body2, status2, err2 = _http_get(qurl, timeout=25)
            if body2 is not None:
                html2 = body2.decode("utf-8", errors="replace")
                app2, eapp2 = _extract_root_app_main(html2)
                if app2:
                    epochs2 = _parse_epoch_from_store(app2)
                    found_epochs.extend(epochs2)
                    if epochs2:
                        src_quote.count += 1
                else:
                    src_quote.ok = False
                    src_quote.errors.append(f"{t}: {eapp2 or 'root.App.main parse failed'}")
            else:
                src_quote.ok = False
                src_quote.errors.append(f"{t}: {err2 or f'no-body (status={status2})'}")

        # finalize epoch-based item
        if found_epochs:
            start = min(found_epochs)
            end = max(found_epochs)
            if end >= now_epoch and start <= cutoff_epoch:
                items.append({
                    "ticker": t,
                    "source": "YahooFinance.HTML.root.App.main",
                    "earningsDateUtcStart": _epoch_to_iso(start),
                    "earningsDateUtcEnd": _epoch_to_iso(end),
                    "earningsEpochStart": int(start),
                    "earningsEpochEnd": int(end),
                })

    # Dedup by ticker: keep earliest epoch item; if only text exists and no epoch, keep text
    best: Dict[str, dict] = {}
    for it in items:
        t = it["ticker"]
        if "earningsEpochStart" in it:
            if t not in best or "earningsEpochStart" not in best[t] or it["earningsEpochStart"] < best[t]["earningsEpochStart"]:
                best[t] = it
        else:
            if t not in best:
                best[t] = it

    def _sort_key(x: dict):
        if "earningsEpochStart" in x:
            return (0, x["earningsEpochStart"], x["ticker"])
        return (1, 10**18, x["ticker"])

    out_items = sorted(best.values(), key=_sort_key)

    payload = {
        "ok": True,
        "type": "earnings",
        "report": report,
        "generatedAt": _utc_iso(),
        "window": {"maxDaysAhead": max_days},
        "count": len(out_items),
        "items": out_items,
        "sources": {
            "yahoo_calendar_earnings_html": asdict(src_cal),
            "yahoo_quote_html": asdict(src_quote),
            "yahoo_calendar_text_fallback": asdict(src_text),
        },
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
