#!/usr/bin/env python3
# report_runner.py — v4.3.7-price-engine-ah-carryforward-2026-01-06
#
# FIX v4.3.7:
# - "NO_DATA" helyett: AH-t megpróbáljuk "carry-forward" módon számolni a legutóbbi post ablakból,
#   akkor is, ha most épp NEM vagyunk PM/AH ablakban (pl. reggel európai időben).
# - PM továbbra is csak akkor él, ha MOST a pre ablakban vagyunk (ne használjunk régi pre tickeket).
# - Kimenet: külön blokk "Pozíciók" (Darabszam > 0), alatta "Watchlist" (Darabszam üres/0).
# - Sorformátum: AH elöl, PM utána.
# - Debug csak akkor kerül a riportba, ha --debug vagy ha minden ticker AH/PM n/a.
#
# Exit:
# - 0 OK (mindig 0, még ha minden n/a – ettől még a gist frissül)
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

VERSION = "v4.3.7-price-engine-ah-carryforward-2026-01-06"


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
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read())


def _to_int(x) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(float(str(x).strip().replace(",", ".")))
    except Exception:
        return None


def load_master_rows(path: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Visszaad (positions, watchlist) sorlistákat.
    Felismert oszlopok:
    - ticker: Ticker/ticker/symbol/Symbol
    - darabszam: Darabszam/darabszám/qty/Qty/quantity/Quantity
    """
    positions: List[Dict[str, Any]] = []
    watch: List[Dict[str, Any]] = []

    with open(path, encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            t = (r.get("Ticker") or r.get("ticker") or r.get("Symbol") or r.get("symbol") or "").strip().upper()
            if not t:
                continue

            ds = (
                r.get("Darabszam")
                or r.get("Darabszám")
                or r.get("qty")
                or r.get("Qty")
                or r.get("quantity")
                or r.get("Quantity")
                or ""
            )
            qty = _to_int(ds)

            row = {"ticker": t, "qty": qty or 0, "raw": r}

            if qty is not None and qty > 0:
                positions.append(row)
            else:
                watch.append(row)

    # unique by ticker (first occurrence wins)
    def uniq(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        out = []
        for rr in rows:
            t = rr["ticker"]
            if t in seen:
                continue
            out.append(rr)
            seen.add(t)
        return out

    return uniq(positions), uniq(watch)


def _tps(meta: Dict[str, Any], key: str) -> List[Tuple[int, int]]:
    """
    tradingPeriods alapján több napos (pre/post) ablakok listája.
    """
    out: List[Tuple[int, int]] = []

    # currentTradingPeriod (ha van) – egy ablak
    ctp = meta.get("currentTradingPeriod")
    if isinstance(ctp, dict) and key in ctp and isinstance(ctp[key], dict):
        d = ctp[key]
        if "start" in d and "end" in d:
            try:
                out.append((int(d["start"]), int(d["end"])))
            except Exception:
                pass

    tp = meta.get("tradingPeriods")
    if isinstance(tp, dict) and key in tp:
        arr = tp.get(key)
        if isinstance(arr, list) and arr and isinstance(arr[0], list):
            for day in arr[0]:
                if isinstance(day, dict) and "start" in day and "end" in day:
                    try:
                        out.append((int(day["start"]), int(day["end"])))
                    except Exception:
                        continue

    # unique + sort
    out = sorted(set(out), key=lambda x: x[0])
    return out


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


def _latest_window_before(now_epoch: int, windows: List[Tuple[int, int]]) -> Optional[Tuple[int, int]]:
    """
    Legutóbbi olyan ablak, amelynek vége <= now.
    """
    cand = [w for w in windows if w[1] <= now_epoch]
    if not cand:
        return None
    return max(cand, key=lambda w: w[1])


def chart_prices(t: str, now_epoch: int) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=2d&includePrePost=true"
    j = http_json(url)
    res = j["chart"]["result"][0]
    meta = res.get("meta", {})

    prev = meta.get("previousClose") or meta.get("regularMarketPreviousClose")

    pre_windows = _tps(meta, "pre")
    post_windows = _tps(meta, "post")

    pre_start, pre_end = (pre_windows[-1] if pre_windows else (None, None))  # legutóbbi pre ablak
    post_start, post_end = (post_windows[-1] if post_windows else (None, None))  # legutóbbi post ablak

    ts = res.get("timestamp") or []
    ind = (res.get("indicators") or {}).get("quote") or []
    closes = []
    if ind and isinstance(ind, list) and isinstance(ind[0], dict) and ind[0].get("close") is not None:
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

    # --- Premarket (PM) ---
    pre = meta.get("preMarketPrice")
    if pre is not None:
        # PM-et csak akkor mutatjuk, ha MOST a pre ablakban vagyunk
        if pre_start is not None and pre_end is not None and (pre_start <= now_epoch <= pre_end):
            debug["pre_source"] = "meta_in_window"
        else:
            pre = None
            debug["pre_source"] = "meta_gated_outside_window"
    else:
        # infer, de csak ha MOST a pre ablakban vagyunk
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
                debug["pre_source"] = "infer_in_window"
            else:
                debug["pre_source"] = "infer_none"

    # --- After-hours (AH) ---
    post = meta.get("postMarketPrice")
    if post is not None:
        # ha most post ablakban vagyunk, ok; ha nem, akkor is elfogadjuk (carry-forward),
        # mert a postMarketPrice jellemzően a legutóbbi AH árat hordozza
        if post_start is not None and post_end is not None and (post_start <= now_epoch <= post_end):
            debug["post_source"] = "meta_in_window"
        else:
            debug["post_source"] = "meta_carry"
    else:
        # infer: először ha most post ablakban vagyunk, különben carry-forward a legutóbbi post ablakból
        if not post_windows:
            post = None
            debug["post_source"] = "tp_missing"
        else:
            if post_start is not None and post_end is not None and (post_start <= now_epoch <= post_end):
                inf = _last_close_in_window(ts, closes, post_start, post_end) if (ts and closes) else None
                if inf is not None:
                    post = inf
                    debug["post_source"] = "infer_in_window"
                else:
                    post = None
                    debug["post_source"] = "infer_none"
            else:
                w = _latest_window_before(now_epoch, post_windows)
                if w is None:
                    post = None
                    debug["post_source"] = "no_prior_post_window"
                else:
                    inf = _last_close_in_window(ts, closes, w[0], w[1]) if (ts and closes) else None
                    if inf is not None:
                        post = inf
                        debug["post_source"] = "carry_last_post_window"
                    else:
                        post = None
                        debug["post_source"] = "carry_infer_none"

    return prev, pre, post, debug


def sort_key(row):
    # row: (ticker, ah, pm)
    ah = row[1]
    pm = row[2]
    return (abs(ah) if ah is not None else -1.0, abs(pm) if pm is not None else -1.0)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    args = ap.parse_args()

    now_epoch = int(time.time())

    positions, watchlist = load_master_rows(args.master)
    tickers_positions = [r["ticker"] for r in positions]
    tickers_watch = [r["ticker"] for r in watchlist]

    all_tickers = tickers_positions + [t for t in tickers_watch if t not in set(tickers_positions)]

    pos_rows: List[Tuple[str, Optional[float], Optional[float]]] = []
    wl_rows: List[Tuple[str, Optional[float], Optional[float]]] = []

    dbg_counts: Dict[str, int] = {}
    sample_dbg = []

    def bump(k: str):
        dbg_counts[k] = dbg_counts.get(k, 0) + 1

    for t in all_tickers:
        try:
            prev, pre, post, dbg = chart_prices(t, now_epoch)
            pm = pct(pre, prev)
            ah = pct(post, prev)

            ps = dbg.get("pre_source") or "pre_none"
            qs = dbg.get("post_source") or "post_none"
            bump(f"pre:{ps}")
            bump(f"post:{qs}")

            if len(sample_dbg) < 10:
                sample_dbg.append((t, dbg, ah, pm))

            if t in tickers_positions:
                pos_rows.append((t, ah, pm))
            else:
                wl_rows.append((t, ah, pm))
        except Exception:
            if t in tickers_positions:
                pos_rows.append((t, None, None))
            else:
                wl_rows.append((t, None, None))
            bump("err")

    # sort: abs(AH) desc, then abs(PM) desc
    pos_rows.sort(key=sort_key, reverse=True)
    wl_rows.sort(key=sort_key, reverse=True)

    all_na = all((ah is None and pm is None) for _, ah, pm in (pos_rows + wl_rows))

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write("# #1 — Premarket check (PRICE ENGINE)\n\n")
        f.write(f"Verzió: {VERSION}\n\n")
        f.write("Formátum: AH elöl, PM utána.\n\n")

        f.write("## Pozíciók\n\n")
        for t, ah, pm in pos_rows:
            f.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}\n")

        f.write("\n## Watchlist\n\n")
        for t, ah, pm in wl_rows:
            f.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}\n")

        if args.debug or all_na:
            f.write("\n## Debug\n")
            f.write(f"- now_epoch: {now_epoch}\n")
            for k in sorted(dbg_counts.keys()):
                f.write(f"- {k}: {dbg_counts[k]}\n")

            f.write("\n### Debug sample (first 10 tickers)\n")
            for t, dbg, ah, pm in sample_dbg:
                f.write(
                    f"- {t}: AH {fmt(ah)} (post_source={dbg.get('post_source')}, post_start={dbg.get('post_start')}, post_end={dbg.get('post_end')}) | "
                    f"PM {fmt(pm)} (pre_source={dbg.get('pre_source')}, pre_start={dbg.get('pre_start')}, pre_end={dbg.get('pre_end')})\n"
                )

    # GH Actions log (stderr)
    print(f"RUNNER_VERSION={VERSION}", file=sys.stderr, flush=True)
    print(f"positions={len(pos_rows)} watchlist={len(wl_rows)} total={len(pos_rows)+len(wl_rows)}", file=sys.stderr, flush=True)
    if all_na:
        print("NO_DATA: most nincs friss PM adat (nem pre ablak), és/vagy nincs elérhető AH adat.", file=sys.stderr, flush=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
