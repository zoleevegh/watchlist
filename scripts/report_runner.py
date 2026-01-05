#!/usr/bin/env python3
# report_runner.py — v4.3.0-chartv8-session-infer-2026-01-05
#
# Yahoo v7 quote KIKAPCSOLVA (401). Csak Yahoo Chart v8.
# FIX: pre/post árak inferálása a chart idősorból a tradingPeriods alapján.
#
# range=2d, interval=1m, includePrePost=true
#
# 1 lista: ha van PM -> PM abs% szerint, különben AH abs% szerint.
#
# Exit:
# - 0 OK
# - 5 ALL_NA (minden ticker PM/AH n/a)
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni.

import csv
import json
import sys
import traceback
import argparse
import urllib.request
from typing import Optional, Tuple, List, Dict, Any

VERSION = "v4.3.0-chartv8-session-infer-2026-01-05"

def pct(a, b):
    if a is None or b in (None, 0):
        return None
    return (a / b - 1) * 100.0

def fmt(x):
    if x is None:
        return "n/a"
    return f"{'+' if x >= 0 else ''}{x:.2f}%"

def http_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())

def load_master(path: str) -> List[str]:
    out = []
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            t = (r.get("ticker") or r.get("Ticker") or r.get("symbol") or r.get("Symbol") or "").strip().upper()
            if t:
                out.append(t)
    seen=set()
    uniq=[]
    for t in out:
        if t not in seen:
            uniq.append(t); seen.add(t)
    return uniq

def _tp(meta: Dict[str, Any], key: str) -> Optional[Tuple[int,int]]:
    tp = meta.get("tradingPeriods")
    if isinstance(tp, dict) and key in tp:
        arr = tp.get(key)
        if isinstance(arr, list) and arr and isinstance(arr[0], list) and arr[0]:
            obj = arr[0][0] if isinstance(arr[0][0], dict) else None
            if obj and "start" in obj and "end" in obj:
                return int(obj["start"]), int(obj["end"])
    ctp = meta.get("currentTradingPeriod")
    if isinstance(ctp, dict) and key in ctp and isinstance(ctp[key], dict):
        d = ctp[key]
        if "start" in d and "end" in d:
            return int(d["start"]), int(d["end"])
    return None

def _last_close_in_window(timestamps: List[int], closes: List[Any], start: int, end: int) -> Optional[float]:
    last = None
    for ts, cl in zip(timestamps, closes):
        if ts is None:
            continue
        if start <= ts <= end:
            try:
                if cl is None:
                    continue
                last = float(cl)
            except Exception:
                continue
    return last

def chart_prices(t: str) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=2d&includePrePost=true"
    j = http_json(url)
    res = j["chart"]["result"][0]
    meta = res.get("meta", {})
    prev = meta.get("previousClose") or meta.get("regularMarketPreviousClose")

    pre = meta.get("preMarketPrice")
    post = meta.get("postMarketPrice")

    ts = res.get("timestamp") or []
    ind = (res.get("indicators") or {}).get("quote") or []
    closes = []
    if ind and isinstance(ind, list) and ind[0].get("close") is not None:
        closes = ind[0].get("close") or []

    notes = []
    if pre is None and ts and closes:
        w = _tp(meta, "pre")
        if w:
            inf = _last_close_in_window(ts, closes, w[0], w[1])
            if inf is not None:
                pre = inf; notes.append("pre=infer")
            else:
                notes.append("pre=none")
        else:
            notes.append("pre=tp_missing")

    if post is None and ts and closes:
        w = _tp(meta, "post")
        if w:
            inf = _last_close_in_window(ts, closes, w[0], w[1])
            if inf is not None:
                post = inf; notes.append("post=infer")
            else:
                notes.append("post=none")
        else:
            notes.append("post=tp_missing")

    return prev, pre, post, ",".join(notes) if notes else "meta"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    args = ap.parse_args()

    tickers = load_master(args.master)
    rows = []
    any_pm = False
    debug_notes = {"meta_only":0, "pre_infer":0, "post_infer":0, "tp_missing":0, "post_none":0, "pre_none":0}

    for t in tickers:
        try:
            prev, pre, post, note = chart_prices(t)
            pm = pct(pre, prev)
            ah = pct(post, prev)
            if pm is not None:
                any_pm = True
            if note == "meta":
                debug_notes["meta_only"] += 1
            else:
                if "pre=infer" in note: debug_notes["pre_infer"] += 1
                if "post=infer" in note: debug_notes["post_infer"] += 1
                if "tp_missing" in note: debug_notes["tp_missing"] += 1
                if "post=none" in note: debug_notes["post_none"] += 1
                if "pre=none" in note: debug_notes["pre_none"] += 1
            rows.append((t, pm, ah))
        except Exception:
            rows.append((t, None, None))

    if any_pm:
        rows.sort(key=lambda r: abs(r[1] or 0.0), reverse=True); mode = "PM"
    else:
        rows.sort(key=lambda r: abs(r[2] or 0.0), reverse=True); mode = "AH"

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write("# #1 — Premarket check (PRICE ENGINE)\n\n")
        f.write(f"Verzió: {VERSION}\n\n")
        f.write(f"Lista — {mode} abs% szerint (1 lista)\n\n")
        for t, pm, ah in rows:
            f.write(f"- {t} — PM {fmt(pm)} | AH {fmt(ah)}\n")
        f.write("\n## Debug\n")
        for k in ["meta_only","pre_infer","post_infer","tp_missing","pre_none","post_none"]:
            f.write(f"- {k}: {debug_notes[k]}\n")

    if all(pm is None and ah is None for _, pm, ah in rows):
        print("ALL_NA: chart v8 nem adott pre/post adatot (meta+infer is üres) — valószínű sessionen kívül vagy feed limit.", file=sys.stderr, flush=True)
        return 5
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
