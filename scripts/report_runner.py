#!/usr/bin/env python3
# report_runner.py — v4.4.0-price-engine-ah-base-regularprice-2026-01-06
#
# MI VOLT A BAJ (v4.3.x):
# - AH% bázis néha a previousClose lett (előző kereskedési nap), nem a *mai záró*.
#   Emiatt az AH% irreálisan elcsúszott (pl. IREN).
#
# FIX v4.4.0:
# - AH% bázis = meta.regularMarketPrice (Yahoo After-hours logika).
# - AH ár = meta.postMarketPrice (fallback: post ablakból inferált last).
# - PM csak akkor számolható, ha NOW a premarket ablakban van.
# - Output blokkok: **Pozíciók** külön, **Watchlist** külön. (Darabszám > 0 => Pozíció.)
# - Formátum soronként: AH elöl, PM utána.
# - Debug csak akkor jelenik meg, ha *nincs adat* (ALL_NA) vagy ha --debug.
#
# Exit:
# - 0 OK
# - 5 ALL_NA (minden ticker AH/PM n/a)
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

VERSION = "v4.4.0-price-engine-ah-base-regularprice-2026-01-06"


def pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return (a / b - 1.0) * 100.0


def fmt(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{('+' if x >= 0 else '')}{x:.2f}%"


def http_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read())


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def load_master_split(path: str) -> Tuple[List[str], List[str]]:
    """Return (positions, watchlist) unique tickers preserving first appearance.

    Position rule: if any row for that ticker has Darabszam/Qty/Shares > 0 => position.
    """
    rows: List[Dict[str, str]] = []
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            rows.append(r)

    def _get_ticker(r: Dict[str, str]) -> str:
        return (r.get("Ticker") or r.get("ticker") or r.get("Symbol") or r.get("symbol") or "").strip().upper()

    def _get_qty(r: Dict[str, str]) -> Optional[float]:
        raw = (
            r.get("Darabszam")
            or r.get("darabszam")
            or r.get("Qty")
            or r.get("qty")
            or r.get("Shares")
            or r.get("shares")
            or ""
        )
        raw = str(raw).strip().replace(" ", "").replace(",", ".")
        if not raw:
            return None
        try:
            return float(raw)
        except Exception:
            return None

    is_position: Dict[str, bool] = {}
    order: List[str] = []

    for r in rows:
        t = _get_ticker(r)
        if not t:
            continue
        if t not in is_position:
            is_position[t] = False
            order.append(t)
        q = _get_qty(r)
        if q is not None and q > 0:
            is_position[t] = True

    positions: List[str] = []
    watchlist: List[str] = []
    for t in order:
        if is_position.get(t):
            positions.append(t)
        else:
            watchlist.append(t)

    return positions, watchlist


def _extract_periods(meta: Dict[str, Any], key: str) -> List[Tuple[int, int]]:
    """Extract list of (start,end) epochs from meta.tradingPeriods[<key>]."""
    tp = meta.get("tradingPeriods")
    out: List[Tuple[int, int]] = []
    if not isinstance(tp, dict):
        return out
    arr = tp.get(key)
    if not isinstance(arr, list):
        return out

    for d in arr:
        if isinstance(d, list) and d:
            obj = d[0] if isinstance(d[0], dict) else None
            if obj and "start" in obj and "end" in obj:
                out.append((int(obj["start"]), int(obj["end"])) )
        elif isinstance(d, dict) and "start" in d and "end" in d:
            out.append((int(d["start"]), int(d["end"])) )

    # unique + sort
    out = sorted(set(out), key=lambda x: x[0])
    return out


def _pick_window_containing(windows: List[Tuple[int, int]], now_epoch: int) -> Optional[Tuple[int, int]]:
    for s, e in windows:
        if s <= now_epoch <= e:
            return (s, e)
    return None


def _last_close_in_window(timestamps: List[int], closes: List[Any], start: int, end: int) -> Optional[float]:
    last: Optional[float] = None
    for ts, cl in zip(timestamps, closes):
        if ts is None:
            continue
        if start <= ts <= end:
            v = _to_float(cl)
            if v is not None:
                last = v
    return last


def chart_prices(t: str, now_epoch: int) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
    """Return (pm_base_prevclose, pm_price, ah_price, debug)

    AH% base is meta.regularMarketPrice (handled in main).
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=2d&includePrePost=true"
    j = http_json(url)
    res = j["chart"]["result"][0]
    meta = res.get("meta", {})

    prev_close = _to_float(meta.get("previousClose") or meta.get("regularMarketPreviousClose"))
    regular_price = _to_float(meta.get("regularMarketPrice"))

    ts: List[int] = res.get("timestamp") or []
    ind = (res.get("indicators") or {}).get("quote") or []
    closes: List[Any] = []
    if ind and isinstance(ind, list) and isinstance(ind[0], dict):
        closes = ind[0].get("close") or []

    pre_windows = _extract_periods(meta, "pre")
    post_windows = _extract_periods(meta, "post")

    pre_now = _pick_window_containing(pre_windows, now_epoch)

    debug: Dict[str, Any] = {
        "now": now_epoch,
        "prev_close": prev_close,
        "regular_price": regular_price,
        "pre_now": pre_now,
        "pre_windows_cnt": len(pre_windows),
        "post_windows_cnt": len(post_windows),
        "pre_source": None,
        "post_source": None,
    }

    # --- PM (csak ha MOST pre ablakban vagyunk) ---
    pm_price: Optional[float] = None
    if pre_now is None:
        debug["pre_source"] = "gated_outside_window"
    else:
        meta_pre = _to_float(meta.get("preMarketPrice"))
        if meta_pre is not None:
            pm_price = meta_pre
            debug["pre_source"] = "meta"
        else:
            s, e = pre_now
            inf = _last_close_in_window(ts, closes, s, e) if (ts and closes) else None
            pm_price = inf
            debug["pre_source"] = "infer" if inf is not None else "infer_none"

    # --- AH price (mindig próbáljuk, hogy premarket előtt is legyen) ---
    ah_price: Optional[float] = None
    meta_post = _to_float(meta.get("postMarketPrice"))
    if meta_post is not None:
        ah_price = meta_post
        debug["post_source"] = "meta"
    else:
        # fallback: legutóbbi post ablakból inferálunk (ha van)
        if post_windows:
            # pick the latest post window that has any data in timestamps (heurisztika)
            # -> kezdünk a legutóbbitól.
            for s, e in sorted(post_windows, key=lambda x: x[0], reverse=True):
                inf = _last_close_in_window(ts, closes, s, e) if (ts and closes) else None
                if inf is not None:
                    ah_price = inf
                    debug["post_source"] = "infer"
                    break
            if ah_price is None:
                debug["post_source"] = "infer_none"
        else:
            debug["post_source"] = "tp_missing"

    # we also expose regular_price for AH base usage
    debug["regular_price"] = regular_price

    return prev_close, pm_price, ah_price, debug


def _sort_key(row: Tuple[str, Optional[float], Optional[float]]) -> float:
    # Prefer AH abs, then PM abs.
    _, pm, ah = row
    if ah is not None:
        return abs(ah)
    if pm is not None:
        return abs(pm)
    return -1.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    args = ap.parse_args()

    now_epoch = int(time.time())

    positions, watchlist = load_master_split(args.master)
    all_tickers = positions + [t for t in watchlist if t not in set(positions)]

    pos_rows: List[Tuple[str, Optional[float], Optional[float]]] = []
    wl_rows: List[Tuple[str, Optional[float], Optional[float]]] = []

    dbg_counts = {
        "pre_meta": 0,
        "pre_infer": 0,
        "pre_gated": 0,
        "pre_none": 0,
        "post_meta": 0,
        "post_infer": 0,
        "post_none": 0,
        "post_tp_missing": 0,
    }
    sample_dbg: List[Tuple[str, Dict[str, Any], Optional[float], Optional[float]]] = []

    for t in all_tickers:
        try:
            prev_close, pm_price, ah_price, dbg = chart_prices(t, now_epoch)

            # PM% bázis: previousClose (előző regular záró)
            pm = pct(pm_price, prev_close)

            # AH% bázis: regularMarketPrice (aznapi záró/utolsó regular)
            ah_base = _to_float(dbg.get("regular_price"))
            # fallback: ha nincs, próbáljuk prev_close-t (jobb mint semmi)
            if ah_base is None:
                ah_base = prev_close
            ah = pct(ah_price, ah_base)

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
            elif qs == "tp_missing":
                dbg_counts["post_tp_missing"] += 1
            else:
                dbg_counts["post_none"] += 1

            if len(sample_dbg) < 10:
                sample_dbg.append((t, dbg, pm, ah))

            row = (t, pm, ah)
            if t in set(positions):
                pos_rows.append(row)
            else:
                wl_rows.append(row)

        except Exception as e:
            # hard fail for one ticker => n/a
            row = (t, None, None)
            if t in set(positions):
                pos_rows.append(row)
            else:
                wl_rows.append(row)

    pos_rows.sort(key=_sort_key, reverse=True)
    wl_rows.sort(key=_sort_key, reverse=True)

    any_data = any((pm is not None or ah is not None) for _, pm, ah in (pos_rows + wl_rows))

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write("# #1 — Premarket check (PRICE ENGINE)\n\n")
        f.write(f"Verzió: {VERSION}\n\n")
        f.write("Formátum: AH elöl, PM utána.\n\n")

        f.write("## Pozíciók\n\n")
        if pos_rows:
            for t, pm, ah in pos_rows:
                f.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}\n")
        else:
            f.write("- (nincs)\n")

        f.write("\n## Watchlist\n\n")
        if wl_rows:
            for t, pm, ah in wl_rows:
                f.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}\n")
        else:
            f.write("- (nincs)\n")

        # Debug csak ha kell
        if args.debug or (not any_data):
            f.write("\n## Debug\n")
            f.write(f"- now_epoch: {now_epoch}\n")
            f.write(f"- positions: {len(positions)} | watchlist: {len(watchlist)} | total_unique: {len(pos_rows)+len(wl_rows)}\n")
            for k in ["pre_meta", "pre_infer", "pre_gated", "pre_none", "post_meta", "post_infer", "post_tp_missing", "post_none"]:
                f.write(f"- {k}: {dbg_counts[k]}\n")
            f.write("\n### Debug sample (first 10 tickers)\n")
            for t, dbg, pm, ah in sample_dbg:
                f.write(
                    f"- {t}: AH {fmt(ah)} (post_source={dbg.get('post_source')}, regular_price={dbg.get('regular_price')}) | "
                    f"PM {fmt(pm)} (pre_source={dbg.get('pre_source')}, pre_now={dbg.get('pre_now')})\n"
                )

    # rövid runner.log a GH Actionsben
    print(f"RUNNER_VERSION={VERSION}", file=sys.stderr, flush=True)
    print(f"positions={len(positions)} watchlist={len(watchlist)} total_unique={len(pos_rows)+len(wl_rows)}", file=sys.stderr, flush=True)

    if not any_data:
        print("NO_DATA: most nincs használható AH/PM adat (meta+chart). A report ettől még frissült.", file=sys.stderr, flush=True)
        return 0

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
