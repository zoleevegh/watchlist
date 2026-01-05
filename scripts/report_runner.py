#!/usr/bin/env python3
# report_runner.py — v4.3.1-chartv8-session-gating-2026-01-05
#
# PROBLÉMA (amit most látsz):
# - Zárás után (CE(S)T ~22:xx) a "Premarket" még NEM indult el.
# - Ha range=2d és simán "pre" ablakból inferálunk, könnyen a TEGNAPI premarket tickeket kapjuk,
#   és azt hasonlítjuk a mai prevClose-hoz -> teljesen fals PM %.
#
# FIX:
# - Premarket értéket csak akkor adunk vissza, ha a JELENLEGI premarket ablak már elkezdődött.
#   (now_epoch < pre_start -> PM = n/a, nem használunk történelmi pre adatot!)
# - After-hours (post) értéket akkor is kiadjuk, ha a postmarket már elindult (now_epoch >= post_start),
#   és az utolsó elérhető ticket vesszük a post ablakból.
# - Yahoo v7 quote KIKAPCSOLVA (401). Csak Yahoo Chart v8.
#
# 1 lista:
# - ha van legalább 1 PM (és nem n/a a session gating miatt) -> PM abs% szerint
# - különben AH abs% szerint
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
import time
from typing import Optional, Tuple, List, Dict, Any

VERSION = "v4.3.1-chartv8-session-gating-2026-01-05"


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
    # prefer currentTradingPeriod (ez a "mai" / "következő" sávokat adja)
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

    pre_start, pre_end = (None, None)
    post_start, post_end = (None, None)
    wpre = _tp(meta, "pre")
    wpost = _tp(meta, "post")
    if wpre:
        pre_start, pre_end = wpre
    if wpost:
        post_start, post_end = wpost

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
        # SESSION GATING: ha a premarket még nem indult el, PM = n/a
        if pre_start is not None and now_epoch < pre_start:
            pre = None
            debug["pre_source"] = "gated_not_started"
        else:
            # ha elindult (vagy már lement), inferálhatunk
            if pre_start is not None and pre_end is not None and ts and closes:
                inf = _last_close_in_window(ts, closes, pre_start, pre_end)
                if inf is not None:
                    pre = inf
                    debug["pre_source"] = "infer"
                else:
                    debug["pre_source"] = "infer_none"
            else:
                debug["pre_source"] = "tp_missing_or_no_series"

    # --- After-hours (postmarket) ---
    post = meta.get("postMarketPrice")
    if post is not None:
        debug["post_source"] = "meta"
    else:
        # AH csak akkor releváns, ha a postmarket már elindult
        if post_start is not None and now_epoch < post_start:
            post = None
            debug["post_source"] = "gated_not_started"
        else:
            if post_start is not None and post_end is not None and ts and closes:
                inf = _last_close_in_window(ts, closes, post_start, post_end)
                if inf is not None:
                    post = inf
                    debug["post_source"] = "infer"
                else:
                    debug["post_source"] = "infer_none"
            else:
                debug["post_source"] = "tp_missing_or_no_series"

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

    sample_dbg = []  # pár ticker debug-ot kiírunk

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
            elif ps == "gated_not_started":
                dbg_counts["pre_gated"] += 1
            else:
                dbg_counts["pre_none"] += 1

            qs = dbg.get("post_source")
            if qs == "meta":
                dbg_counts["post_meta"] += 1
            elif qs == "infer":
                dbg_counts["post_infer"] += 1
            elif qs == "gated_not_started":
                dbg_counts["post_gated"] += 1
            else:
                dbg_counts["post_none"] += 1

            if len(sample_dbg) < 5:
                sample_dbg.append((t, dbg))

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
        f.write("\n### Debug sample (first 5 tickers)\n")
        for t, dbg in sample_dbg:
            f.write(f"- {t}: pre_source={dbg.get('pre_source')}, post_source={dbg.get('post_source')}, "
                    f"pre_start={dbg.get('pre_start')}, pre_end={dbg.get('pre_end')}, "
                    f"post_start={dbg.get('post_start')}, post_end={dbg.get('post_end')}\n")

    if all(pm is None and ah is None for _, pm, ah in rows):
        print("ALL_NA: chart v8 nem adott használható pre/post adatot (session gating + infer).", file=sys.stderr, flush=True)
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
