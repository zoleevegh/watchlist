#!/usr/bin/env python3
# report_runner.py — v4.3.2-chartv8-window-gating-2026-01-05
#
# MI VOLT A HIBA a v4.3.1-ben?
# - A PM "gating" csak azt nézte, hogy now >= pre_start.
# - Ez zárás után is igaz (hiszen a MAI premarket már reggel LEFUTOTT),
#   ezért a runner a "mai" pre ablakból (már rég vége) inferált árat, és PM %-ot számolt.
#
# FIX v4.3.2:
# - Premarket (PM) csak akkor érvényes, ha NOW BENNE VAN a pre ablakban: pre_start <= now <= pre_end.
#   Különben PM = n/a (nem használunk történelmi pre tickeket).
# - After-hours (AH) csak akkor érvényes, ha NOW BENNE VAN a post ablakban: post_start <= now <= post_end.
# - Yahoo v7 quote KIKAPCSOLVA (401). Csak Yahoo Chart v8.
# - 1 lista:
#   - ha van legalább 1 érvényes PM -> PM abs% szerint
#   - különben AH abs% szerint
#
# Logging:
# - A runner.log-ba kiírunk egy rövid összegzést + 10-es debug sample-t, hogy a GH Actionsben is lásd.
#
# Exit:
# - 0 OK
# - 5 ALL_NA (minden ticker PM/AH n/a)
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

import csv
import json
import sys
import traceback
import argparse
import urllib.request
import time
from typing import Optional, Tuple, List, Dict, Any

VERSION = "v4.3.2-chartv8-window-gating-2026-01-05"


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
    seen = set()
    uniq = []
    for t in out:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq


def _tp(meta: Dict[str, Any], key: str) -> Optional[Tuple[int, int]]:
    # prefer currentTradingPeriod (ez a legjobb "mai" periódusokra)
    ctp = meta.get("currentTradingPeriod")
    if isinstance(ctp, dict) and key in ctp and isinstance(ctp[key], dict):
        d = ctp[key]
        if "start" in d and "end" in d:
            return int(d["start"]), int(d["end"])

    tp = meta.get("tradingPeriods")
    if isinstance(tp, dict) and key in tp:
        arr = tp.get(key)
        if isinstance(arr, list) and arr and isinstance(arr[0], list) and arr[0]:
            obj = arr[0][0] if isinstance(arr[0][0], dict) else None
            if obj and "start" in obj and "end" in obj:
                return int(obj["start"]), int(obj["end"])
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


def chart_prices(t: str, now_epoch: int) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=2d&includePrePost=true"
    j = http_json(url)
    res = j["chart"]["result"][0]
    meta = res.get("meta", {})

    prev = meta.get("previousClose") or meta.get("regularMarketPreviousClose")

    wpre = _tp(meta, "pre")
    wpost = _tp(meta, "post")

    pre_start, pre_end = (wpre if wpre else (None, None))
    post_start, post_end = (wpost if wpost else (None, None))

    ts = res.get("timestamp") or []
    ind = (res.get("indicators") or {}).get("quote") or []
    closes = []
    if ind and isinstance(ind, list) and ind[0].get("close") is not None:
        closes = ind[0].get("close") or []

    debug = {
        "now": now_epoch,
        "pre_start": pre_start,
        "pre_end": pre_end,
        "post_start": post_start,
        "post_end": post_end,
        "pre_source": None,
        "post_source": None,
    }

    # --- Premarket ---
    pre = meta.get("preMarketPrice")
    if pre is not None:
        debug["pre_source"] = "meta"
    else:
        # WINDOW GATING: PM csak akkor, ha MOST pre ablakban vagyunk
        if pre_start is None or pre_end is None:
            pre = None
            debug["pre_source"] = "tp_missing"
        elif not (pre_start <= now_epoch <= pre_end):
            pre = None
            debug["pre_source"] = "gated_outside_window"
        else:
            inf = _last_close_in_window(ts, closes, pre_start, pre_end) if (ts and closes) else None
            if inf is not None:
                pre = inf
                debug["pre_source"] = "infer"
            else:
                debug["pre_source"] = "infer_none"

    # --- After-hours (postmarket) ---
    post = meta.get("postMarketPrice")
    if post is not None:
        debug["post_source"] = "meta"
    else:
        # AH csak akkor, ha MOST post ablakban vagyunk
        if post_start is None or post_end is None:
            post = None
            debug["post_source"] = "tp_missing"
        elif not (post_start <= now_epoch <= post_end):
            post = None
            debug["post_source"] = "gated_outside_window"
        else:
            inf = _last_close_in_window(ts, closes, post_start, post_end) if (ts and closes) else None
            if inf is not None:
                post = inf
                debug["post_source"] = "infer"
            else:
                debug["post_source"] = "infer_none"

    return prev, pre, post, debug


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    args = ap.parse_args()

    now_epoch = int(time.time())

    tickers = load_master(args.master)
    rows = []
    any_pm = False

    dbg_counts = {
        "pre_meta": 0,
        "pre_infer": 0,
        "pre_gated": 0,
        "pre_none": 0,
        "post_meta": 0,
        "post_infer": 0,
        "post_gated": 0,
        "post_none": 0,
    }

    sample_dbg = []  # 10 ticker debug

    for t in tickers:
        try:
            prev, pre, post, dbg = chart_prices(t, now_epoch)
            pm = pct(pre, prev)
            ah = pct(post, prev)

            if pm is not None:
                any_pm = True

            ps = dbg.get("pre_source")
            if ps == "meta":
                dbg_counts["pre_meta"] += 1
            elif ps == "infer":
                dbg_counts["pre_infer"] += 1
            elif ps == "gated_outside_window":
                dbg_counts["pre_gated"] += 1
            else:
                dbg_counts["pre_none"] += 1

            qs = dbg.get("post_source")
            if qs == "meta":
                dbg_counts["post_meta"] += 1
            elif qs == "infer":
                dbg_counts["post_infer"] += 1
            elif qs == "gated_outside_window":
                dbg_counts["post_gated"] += 1
            else:
                dbg_counts["post_none"] += 1

            if len(sample_dbg) < 10:
                sample_dbg.append((t, dbg, pm, ah))

            rows.append((t, pm, ah))
        except Exception:
            rows.append((t, None, None))

    # 1 lista rendezés
    if any_pm:
        rows.sort(key=lambda r: abs(r[1] or 0.0), reverse=True)
        mode = "PM"
    else:
        rows.sort(key=lambda r: abs(r[2] or 0.0), reverse=True)
        mode = "AH"

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write("# #1 — Premarket check (PRICE ENGINE)\n\n")
        f.write(f"Verzió: {VERSION}\n\n")
        f.write(f"Lista — {mode} abs% szerint (1 lista)\n\n")
        for t, pm, ah in rows:
            f.write(f"- {t} — PM {fmt(pm)} | AH {fmt(ah)}\n")

        f.write("\n## Debug\n")
        f.write(f"- now_epoch: {now_epoch}\n")
        for k in ["pre_meta","pre_infer","pre_gated","pre_none","post_meta","post_infer","post_gated","post_none"]:
            f.write(f"- {k}: {dbg_counts[k]}\n")

        f.write("\n### Debug sample (first 10 tickers)\n")
        for t, dbg, pm, ah in sample_dbg:
            f.write(
                f"- {t}: PM {fmt(pm)} (pre_source={dbg.get('pre_source')}, pre_start={dbg.get('pre_start')}, pre_end={dbg.get('pre_end')}) | "
                f"AH {fmt(ah)} (post_source={dbg.get('post_source')}, post_start={dbg.get('post_start')}, post_end={dbg.get('post_end')})\n"
            )

    # rövid log a runner.log-ba (GH Actionsben legyen "életjel")
    print(f"RUNNER_VERSION={VERSION}", file=sys.stderr, flush=True)
    print(f"MODE={mode} any_pm={any_pm} tickers={len(rows)}", file=sys.stderr, flush=True)
    print(f"DBG pre_meta={dbg_counts['pre_meta']} pre_infer={dbg_counts['pre_infer']} pre_gated={dbg_counts['pre_gated']} pre_none={dbg_counts['pre_none']}", file=sys.stderr, flush=True)
    print(f"DBG post_meta={dbg_counts['post_meta']} post_infer={dbg_counts['post_infer']} post_gated={dbg_counts['post_gated']} post_none={dbg_counts['post_none']}", file=sys.stderr, flush=True)

    if all(pm is None and ah is None for _, pm, ah in rows):
        print("ALL_NA: chart v8 nem adott használható pre/post adatot (window gating + infer).", file=sys.stderr, flush=True)
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
