#!/usr/bin/env python3
# report_runner.py — v4.3.4-price-engine-positions-block-ahfirst-cacheclean-2026-01-06
#
# KÉRÉSEID ALAPJÁN (v4.3.4):
# 1) Debug/Debug sample: csak akkor írjuk a report aljára, ha ALL_NA (nincs adat).
# 2) Darabszámos tickerek külön blokkban elöl ("Pozíciók"), alatta "Watchlist".
# 3) A reportban az értékek sorrendje: AH elöl, PM utána.
# 4) Rendezettség: mindkét blokkban abs(AH%) szerint (n/a a végére), tie-breaker abs(PM%).
#
# Megjegyzés:
# - A Yahoo Chart v8 meta gyakran nem ad postMarketPrice / preMarketPrice értéket,
#   ezért ablakon belül inferálunk 1 perces chart close-okból (NOW-cap).
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

VERSION = "v4.3.4-price-engine-positions-block-ahfirst-cacheclean-2026-01-06"


def pct(a, b):
    if a is None or b in (None, 0):
        return None
    try:
        a = float(a)
        b = float(b)
        if b == 0:
            return None
        return (a / b - 1) * 100.0
    except Exception:
        return None


def fmt(x):
    if x is None:
        return "n/a"
    return f"{'+' if x >= 0 else ''}{x:.2f}%"


def http_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def _parse_qty(row: Dict[str, str]) -> float:
    keys = [
        "qty", "Qty", "quantity", "Quantity", "shares", "Shares",
        "db", "Db", "darab", "Darab", "pieces", "Pieces",
        "position", "Position", "count", "Count",
    ]
    for k in keys:
        if k in row and row[k] not in (None, ""):
            v = str(row[k]).strip()
            if not v:
                continue
            v = v.replace(" ", "").replace(",", ".")
            try:
                return float(v)
            except Exception:
                continue
    return 0.0


def load_master_with_groups(path: str) -> Tuple[List[str], List[str]]:
    """
    Visszaad (positions, watchlist) ticker listákat, duplikátum nélkül.
    - positions: darabszám > 0
    - watchlist: egyéb
    Ha nincs qty oszlop a fájlban, akkor minden ticker watchlist-re kerül.
    """
    positions: List[str] = []
    watchlist: List[str] = []

    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            t = (r.get("ticker") or r.get("Ticker") or r.get("symbol") or r.get("Symbol") or "").strip().upper()
            if not t:
                continue
            qty = _parse_qty(r)
            if qty > 0:
                positions.append(t)
            else:
                watchlist.append(t)

    def uniq_keep(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                out.append(x)
                seen.add(x)
        return out

    positions_u = uniq_keep(positions)
    watchlist_u = uniq_keep([t for t in watchlist if t not in set(positions_u)])
    return positions_u, watchlist_u


def _tp(meta: Dict[str, Any], key: str) -> Optional[Tuple[int, int]]:
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


def _last_close_in_window(
    timestamps: List[int],
    closes: List[Any],
    start: int,
    end: int,
    now_epoch: Optional[int] = None,
) -> Optional[float]:
    """
    Utolsó close az ablakban.
    NOW-cap: csak ts <= now_epoch minták.
    """
    last = None
    cap_end = end
    if now_epoch is not None:
        cap_end = min(end, now_epoch)

    for ts, cl in zip(timestamps, closes):
        if ts is None:
            continue
        if now_epoch is not None and ts > now_epoch:
            continue
        if start <= ts <= cap_end:
            try:
                if cl is None:
                    continue
                last = float(cl)
            except Exception:
                continue
    return last


def chart_prices(t: str, now_epoch: int) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=2d&includePrePost=true"
    j = http_json(url)
    res = j["chart"]["result"][0]
    meta = res.get("meta", {})

    prev = meta.get("previousClose") or meta.get("regularMarketPreviousClose")
    regular = meta.get("regularMarketPrice")

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
        "prev": prev,
        "regular": regular,
        "pre_start": pre_start,
        "pre_end": pre_end,
        "post_start": post_start,
        "post_end": post_end,
        "pre_source": None,
        "post_source": None,
    }

    # Premarket
    pre = meta.get("preMarketPrice")
    if pre is not None:
        debug["pre_source"] = "meta"
    else:
        if pre_start is None or pre_end is None:
            pre = None
            debug["pre_source"] = "tp_missing"
        elif not (pre_start <= now_epoch <= pre_end):
            pre = None
            debug["pre_source"] = "gated_outside_window"
        else:
            inf = _last_close_in_window(ts, closes, pre_start, pre_end, now_epoch=now_epoch) if (ts and closes) else None
            if inf is not None:
                pre = inf
                debug["pre_source"] = "infer"
            else:
                debug["pre_source"] = "infer_none"

    # After-hours
    post = meta.get("postMarketPrice")
    if post is not None:
        debug["post_source"] = "meta"
    else:
        if post_start is None or post_end is None:
            post = None
            debug["post_source"] = "tp_missing"
        elif not (post_start <= now_epoch <= post_end):
            post = None
            debug["post_source"] = "gated_outside_window"
        else:
            inf = _last_close_in_window(ts, closes, post_start, post_end, now_epoch=now_epoch) if (ts and closes) else None
            if inf is not None:
                post = inf
                debug["post_source"] = "infer"
            else:
                debug["post_source"] = "infer_none"

    return prev, regular, pre, post, debug


def _sort_key(row: Dict[str, Any]):
    ah = row.get("ah")
    pm = row.get("pm")
    ah_rank = abs(ah) if ah is not None else -1.0
    pm_rank = abs(pm) if pm is not None else -1.0
    return (ah_rank, pm_rank)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    args = ap.parse_args()

    now_epoch = int(time.time())

    positions, watchlist = load_master_with_groups(args.master)
    tickers = positions + watchlist

    rows: Dict[str, Dict[str, Any]] = {}
    any_value = False

    dbg_counts = {
        "pre_meta": 0, "pre_infer": 0, "pre_gated": 0, "pre_none": 0,
        "post_meta": 0, "post_infer": 0, "post_gated": 0, "post_none": 0,
    }
    sample_dbg = []

    for t in tickers:
        try:
            prev, regular, pre, post, dbg = chart_prices(t, now_epoch)

            pm = pct(pre, prev)  # PM: prev close bázis
            ah_base = regular if regular not in (None, 0) else prev
            ah = pct(post, ah_base)  # AH: regular bázis (fallback prev)

            if pm is not None or ah is not None:
                any_value = True

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
                sample_dbg.append((t, dbg, prev, regular, pre, post, pm, ah))

            rows[t] = {"ticker": t, "pm": pm, "ah": ah, "dbg": dbg, "prev": prev, "regular": regular, "pre": pre, "post": post}
        except Exception:
            rows[t] = {"ticker": t, "pm": None, "ah": None}

    pos_rows = [rows[t] for t in positions if t in rows]
    w_rows = [rows[t] for t in watchlist if t in rows]
    pos_rows.sort(key=_sort_key, reverse=True)
    w_rows.sort(key=_sort_key, reverse=True)

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write("# #1 — Premarket check (PRICE ENGINE)\n\n")
        f.write(f"Verzió: {VERSION}\n\n")

        f.write("## Pozíciók (darabszámos)\n\n")
        if pos_rows:
            for r in pos_rows:
                f.write(f"- {r['ticker']} — AH {fmt(r.get('ah'))} | PM {fmt(r.get('pm'))}\n")
        else:
            f.write("- (nincs darabszámos ticker a MASTER-ben / nincs qty oszlop)\n")

        f.write("\n## Watchlist\n\n")
        for r in w_rows:
            f.write(f"- {r['ticker']} — AH {fmt(r.get('ah'))} | PM {fmt(r.get('pm'))}\n")

        # Debug csak akkor, ha nincs adat
        if not any_value:
            f.write("\n## Debug (NO DATA)\n")
            f.write(f"- now_epoch: {now_epoch}\n")
            for k in ["pre_meta","pre_infer","pre_gated","pre_none","post_meta","post_infer","post_gated","post_none"]:
                f.write(f"- {k}: {dbg_counts[k]}\n")

            f.write("\n### Debug sample (first 10 tickers)\n")
            for t, dbg, prev, regular, pre, post, pm, ah in sample_dbg:
                f.write(
                    f"- {t}: prev={prev} regular={regular} pre={pre} post={post} | "
                    f"AH {fmt(ah)} (post_source={dbg.get('post_source')}, post_start={dbg.get('post_start')}, post_end={dbg.get('post_end')}) | "
                    f"PM {fmt(pm)} (pre_source={dbg.get('pre_source')}, pre_start={dbg.get('pre_start')}, pre_end={dbg.get('pre_end')})\n"
                )

    print(f"RUNNER_VERSION={VERSION}", file=sys.stderr, flush=True)
    print(f"positions={len(positions)} watchlist={len(watchlist)} total={len(tickers)}", file=sys.stderr, flush=True)

    if not any_value:
        print("ALL_NA: nincs használható pre/post adat (window gating + infer).", file=sys.stderr, flush=True)
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
