#!/usr/bin/env python3
"""
yahoo_analyst_events_fetcher.py – v2.0.0

Build reports/yahoo_analyst_{report}.json with *recent* analyst actions from Yahoo Finance.

Source:
  - Yahoo quoteSummary: upgradeDowngradeHistory (events)
  - Yahoo quoteSummary: financialData (target snapshot context; not events)

Important:
  Yahoo chart 'price target change' tooltips are not reliably exposed via a public endpoint.
  This module focuses on *rating action events* + target snapshot.

Output:
  reports/yahoo_analyst_{report}.json
"""
from __future__ import annotations

import argparse
import json
import os
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

def _http_get(url: str, timeout: int = 20) -> Tuple[Optional[bytes], Optional[int], Optional[str]]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json,*/*"})
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

def _qs_modules(ticker: str) -> Tuple[Optional[dict], Optional[str]]:
    url = (
        f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
        f"?modules=upgradeDowngradeHistory,financialData&corsDomain=finance.yahoo.com"
    )
    body, status, err = _http_get(url, timeout=20)
    if body is None:
        return None, err or f"no-body (status={status})"
    try:
        return json.loads(body.decode("utf-8", errors="replace")), None
    except Exception as e:
        return None, f"json-parse: {e}"

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
    return []

@dataclass
class SourceStatus:
    ok: bool
    count: int
    errors: List[str]

def _epoch_to_iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

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

    src = SourceStatus(ok=True, count=0, errors=[])
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
            "sources": {"yahoo_quoteSummary_upgradeDowngradeHistory": asdict(src)},
            "warning": "No tickers found (set env TICKERS or provide reports/latest_*.json)."
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return 0

    for t in tickers:
        data, err = _qs_modules(t)
        if not data:
            src.ok = False
            src.errors.append(f"{t}: {err or 'no data'}")
            continue

        try:
            res = data.get("quoteSummary", {}).get("result")
            if not (isinstance(res, list) and res):
                raise ValueError("missing quoteSummary.result")
            r0 = res[0]

            # Events
            udh = r0.get("upgradeDowngradeHistory", {}) or {}
            history = udh.get("history", [])
            if isinstance(history, list):
                for h in history:
                    if not isinstance(h, dict):
                        continue
                    epoch = h.get("epochGradeDate")
                    if not isinstance(epoch, (int, float)):
                        continue
                    epoch = int(epoch)
                    if epoch < since_epoch:
                        continue
                    items.append({
                        "ticker": t,
                        "source": "YahooFinance.quoteSummary.upgradeDowngradeHistory",
                        "dateUtc": _epoch_to_iso(epoch),
                        "epoch": epoch,
                        "firm": h.get("firm"),
                        "action": h.get("action"),
                        "fromGrade": h.get("fromGrade"),
                        "toGrade": h.get("toGrade"),
                    })
                    src.count += 1

            # Target snapshot context (not events)
            fd = r0.get("financialData", {}) or {}
            def _raw(x):
                return x.get("raw") if isinstance(x, dict) else None
            snap = {
                "targetMeanPrice": _raw(fd.get("targetMeanPrice")),
                "targetHighPrice": _raw(fd.get("targetHighPrice")),
                "targetLowPrice": _raw(fd.get("targetLowPrice")),
                "recommendationKey": fd.get("recommendationKey"),
                "recommendationMean": _raw(fd.get("recommendationMean")),
                "numberOfAnalystOpinions": _raw(fd.get("numberOfAnalystOpinions")),
            }
            if any(v is not None for v in snap.values()):
                targets_snapshot[t] = snap

        except Exception as e:
            src.ok = False
            src.errors.append(f"{t}: parse-error: {e}")

    items = [it for it in items if it.get("firm") or it.get("action") or it.get("toGrade")]
    items = sorted(items, key=lambda x: (x["epoch"], x["ticker"], str(x.get("firm") or "")), reverse=True)

    payload = {
        "ok": True,
        "type": "yahoo_analyst_events",
        "report": report,
        "generatedAt": _utc_iso(),
        "window": {"lookbackDays": lookback_days},
        "count": len(items),
        "items": items,
        "targetsSnapshot": targets_snapshot,
        "sources": {"yahoo_quoteSummary_upgradeDowngradeHistory": asdict(src)},
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
