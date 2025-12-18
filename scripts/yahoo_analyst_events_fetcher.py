#!/usr/bin/env python3
"""
yahoo_analyst_events_fetcher.py – v3.0.0 (Yahoo HTML scrape)

B-option implementation:
  - Scrape https://finance.yahoo.com/quote/{TICKER}/analysis?p={TICKER}
    and extract target snapshot + (if present) vendor analyst action items from embedded JSON.
  - Also scrape quote page for QuoteSummaryStore.financialData as backup (targets snapshot).

Output:
  reports/yahoo_analyst_{report}.json
No external deps.
"""
from __future__ import annotations

import argparse
import json
import os
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

DEFAULT_LOOKBACK_DAYS = 14


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
    return []


@dataclass
class SourceStatus:
    ok: bool
    count: int
    errors: List[str]


def _epoch_to_iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _dig(obj: Any, *keys: str) -> Any:
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _raw(x: Any) -> Any:
    if isinstance(x, dict) and "raw" in x:
        return x.get("raw")
    return x


def _extract_targets_from_app(app: dict) -> Optional[dict]:
    stores = _dig(app, "context", "dispatcher", "stores") or {}
    qss = stores.get("QuoteSummaryStore") or {}
    fd = qss.get("financialData") or {}
    snap = {
        "targetMeanPrice": _raw(fd.get("targetMeanPrice")),
        "targetHighPrice": _raw(fd.get("targetHighPrice")),
        "targetLowPrice": _raw(fd.get("targetLowPrice")),
        "recommendationKey": fd.get("recommendationKey"),
        "recommendationMean": _raw(fd.get("recommendationMean")),
        "numberOfAnalystOpinions": _raw(fd.get("numberOfAnalystOpinions")),
    }
    if any(v is not None for v in snap.values()):
        return snap
    return None


def _extract_vendor_events(app: dict, ticker: str, since_epoch: int) -> List[dict]:
    stores = _dig(app, "context", "dispatcher", "stores") or {}
    events: List[dict] = []

    def walk(x: Any):
        if isinstance(x, dict):
            keys = set(x.keys())
            # heuristic: benzinga-like payload
            if {"action_company", "action_pt"} <= keys or {"adjusted_pt_current", "adjusted_pt_prior"} <= keys:
                epoch = None
                for k in ("epoch", "date", "published", "timestamp"):
                    v = x.get(k)
                    if isinstance(v, (int, float)):
                        epoch = int(v)
                        break
                    if isinstance(v, dict) and isinstance(v.get("raw"), (int, float)):
                        epoch = int(v["raw"])
                        break
                if epoch is None:
                    # keep but mark unknown time; use now so it appears (better than dropping)
                    epoch = int(time.time())
                if epoch >= since_epoch:
                    events.append({
                        "ticker": ticker,
                        "source": "YahooFinance.HTML.vendor_feed",
                        "epoch": epoch,
                        "dateUtc": _epoch_to_iso(epoch),
                        "firm": x.get("firm") or x.get("action_firm") or x.get("analyst"),
                        "actionCompany": x.get("action_company") or x.get("actionCompany"),
                        "actionPt": x.get("action_pt") or x.get("actionPt"),
                        "ptPrior": x.get("adjusted_pt_prior") or x.get("pt_prior") or x.get("ptPrior"),
                        "ptCurrent": x.get("adjusted_pt_current") or x.get("pt_current") or x.get("ptCurrent"),
                        "ratingPrior": x.get("rating_prior") or x.get("ratingPrior"),
                        "ratingCurrent": x.get("rating_current") or x.get("ratingCurrent"),
                        "headline": x.get("headline") or x.get("title"),
                        "url": x.get("url") or x.get("link"),
                    })
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(stores)

    uniq: List[dict] = []
    seen = set()
    for e in events:
        key = (e.get("ticker"), e.get("firm"), e.get("actionCompany"), e.get("actionPt"), e.get("ptPrior"), e.get("ptCurrent"), e.get("epoch"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)
    return uniq


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, help="1|2|3")
    ap.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    report = str(args.report).strip()
    lookback_days = max(1, int(args.lookback_days))
    out_path = args.out or f"reports/yahoo_analyst_{report}.json"
    _ensure_dir(os.path.dirname(out_path) or ".")

    tickers = _load_tickers(report)
    since_epoch = int(time.time()) - lookback_days * 86400

    src_analysis = SourceStatus(ok=True, count=0, errors=[])
    src_quote = SourceStatus(ok=True, count=0, errors=[])

    items: List[dict] = []
    targets_snapshot: Dict[str, dict] = {}

    if not tickers:
        payload = {
            "ok": True,
            "type": "yahoo_analyst_events",
            "report": report,
            "generatedAt": _utc_iso(),
            "window": {"lookbackDays": lookback_days},
            "count": 0,
            "items": [],
            "targetsSnapshot": {},
            "sources": {"yahoo_analysis_html": asdict(src_analysis), "yahoo_quote_html": asdict(src_quote)},
            "warning": "No tickers found (set env TICKERS or provide reports/latest_*.json).",
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return 0

    for t in tickers:
        # 1) /analysis page
        url = f"https://finance.yahoo.com/quote/{t}/analysis?p={t}"
        body, status, err = _http_get(url, timeout=25)
        if body is not None:
            html = body.decode("utf-8", errors="replace")
            app, eapp = _extract_root_app_main(html)
            if app:
                snap = _extract_targets_from_app(app)
                if snap:
                    targets_snapshot[t] = snap
                ev = _extract_vendor_events(app, t, since_epoch)
                if ev:
                    items.extend(ev)
                    src_analysis.count += len(ev)
            else:
                src_analysis.ok = False
                src_analysis.errors.append(f"{t}: {eapp or 'root.App.main parse failed'}")
        else:
            src_analysis.ok = False
            src_analysis.errors.append(f"{t}: {err or f'no-body (status={status})'}")

        # 2) quote page fallback for targets snapshot
        if t not in targets_snapshot:
            qurl = f"https://finance.yahoo.com/quote/{t}"
            body2, status2, err2 = _http_get(qurl, timeout=25)
            if body2 is not None:
                html2 = body2.decode("utf-8", errors="replace")
                app2, eapp2 = _extract_root_app_main(html2)
                if app2:
                    snap2 = _extract_targets_from_app(app2)
                    if snap2:
                        targets_snapshot[t] = snap2
                        src_quote.count += 1
                else:
                    src_quote.ok = False
                    src_quote.errors.append(f"{t}: {eapp2 or 'root.App.main parse failed'}")
            else:
                src_quote.ok = False
                src_quote.errors.append(f"{t}: {err2 or f'no-body (status={status2})'}")

    cleaned = []
    for it in items:
        if it.get("firm") or it.get("headline") or it.get("ptCurrent") or it.get("ptPrior") or it.get("actionCompany"):
            cleaned.append(it)
    cleaned = sorted(cleaned, key=lambda x: (x.get("epoch", 0), x.get("ticker", "")), reverse=True)

    payload = {
        "ok": True,
        "type": "yahoo_analyst_events",
        "report": report,
        "generatedAt": _utc_iso(),
        "window": {"lookbackDays": lookback_days},
        "count": len(cleaned),
        "items": cleaned,
        "targetsSnapshot": targets_snapshot,
        "sources": {"yahoo_analysis_html": asdict(src_analysis), "yahoo_quote_html": asdict(src_quote)},
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
