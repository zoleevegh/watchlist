#!/usr/bin/env python3
# report_runner.py — v4.4.4-price-engine-base-regularfix-2026-01-06
#
# FIX v4.4.4:
# - AH/PM % bázisa: Yahoo UI-val egyezően a "regular" close/last (meta.regularMarketPrice),
#   fallback: regularMarketPreviousClose -> previousClose.
#   (A korábbi prevClose bázis a "Day Gain" bázissal egyezett, ezért tűntek túl nagynak az AH/PM %-ok.)
# - A pre/post meta árakat NEM gate-eljük (ezek a Yahoo szerint a legutóbbi AH/PM árak),
#   csak az INFER (chart timestamp) esik gate alá.
# - Pozíciók / Watchlist külön blokk (Darabszam > 0 => Pozíció).
# - Debug: csak akkor írjuk ki, ha --debug vagy ALL_NA.
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

VERSION = "v4.4.5-price-engine-windowselect-carryforward-2026-01-06"


def pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return (a / b - 1.0) * 100.0


def fmt(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{'+' if x >= 0 else ''}{x:.2f}%"


def http_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read())


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def load_master_positions_watchlist(path: str) -> Tuple[List[str], List[str]]:
    """
    Google Sheets CSV-ből olvas:
      - ticker oszlop: Ticker / ticker / Symbol / symbol
      - darabszám oszlop: Darabszam / darabszám / Shares / shares / Qty / qty
    Darabszam > 0 => pozíció, különben watchlist.
    """
    positions: List[str] = []
    watch: List[str] = []
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            t = (r.get("Ticker") or r.get("ticker") or r.get("Symbol") or r.get("symbol") or "").strip().upper()
            if not t:
                continue

            qty_raw = (r.get("Darabszam") or r.get("Darabszám") or r.get("Shares") or r.get("shares") or r.get("Qty") or r.get("qty") or "").strip()
            qty = 0.0
            if qty_raw:
                try:
                    qty = float(qty_raw.replace(",", "."))
                except Exception:
                    qty = 0.0

            if qty > 0:
                positions.append(t)
            else:
                watch.append(t)

    # uniq, order-preserving
    def _uniq(xs: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in xs:
            if x not in seen:
                out.append(x)
                seen.add(x)
        return out

    return _uniq(positions), _uniq(watch)


def _tp(meta: Dict[str, Any], key: str, now_epoch: int, mode: str) -> Optional[Tuple[int, int]]:
    """Select a trading-period window.

    mode:
      - "current": only return a window that contains now
      - "current_or_last": return current window if contains now, else the most recent window whose end <= now
    """
    periods: List[Dict[str, Any]] = []

    ctp = meta.get("currentTradingPeriod") or {}
    if isinstance(ctp, dict):
        p = ctp.get(key)
        if isinstance(p, dict) and p.get("start") and p.get("end"):
            periods.append(p)

    tps = (meta.get("tradingPeriods") or {}).get(key)
    if isinstance(tps, list):
        for day in tps:
            if isinstance(day, list):
                for p in day:
                    if isinstance(p, dict) and p.get("start") and p.get("end"):
                        periods.append(p)

    if not periods:
        return None

    norm: List[Tuple[int, int]] = []
    for p in periods:
        try:
            s = int(p["start"])
            e = int(p["end"])
            if e > s:
                norm.append((s, e))
        except Exception:
            continue
    if not norm:
        return None
    norm = sorted(set(norm), key=lambda x: x[0])

    if mode == "current":
        for s, e in norm:
            if s <= now_epoch <= e:
                return (s, e)
        return None

    # current_or_last
    for s, e in norm:
        if s <= now_epoch <= e:
            return (s, e)
    completed = [(s, e) for (s, e) in norm if e <= now_epoch]
    return completed[-1] if completed else None


def _last_close_in_window(timestamps: List[int], closes: List[Any], start: int, end: int) -> Optional[float]:
    last = None
    for ts, cl in zip(timestamps, closes):
        if ts is None:
            continue
        if start <= ts <= end:
            v = _to_float(cl)
            if v is not None:
                last = v
    return last


def chart_prices(t: str, now_epoch: int) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
    """
    Returns: (base_close, pre_price, post_price, debug)
      base_close: regularMarketPrice fallback chain
      pre/post: meta pre/post if present (no gating), else infer within window (gated)
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=2d&includePrePost=true"
    j = http_json(url)
    res = j["chart"]["result"][0]
    meta = res.get("meta", {})

    base_close = (
        _to_float(meta.get("regularMarketPrice"))
        or _to_float(meta.get("regularMarketPreviousClose"))
        or _to_float(meta.get("previousClose"))
        or _to_float(meta.get("chartPreviousClose"))
    )

    wpre = _tp(meta, "pre", now_epoch, "current")
    wpost = _tp(meta, "post", now_epoch, "current_or_last")
    pre_start, pre_end = (wpre if wpre else (None, None))
    post_start, post_end = (wpost if wpost else (None, None))

    ts = res.get("timestamp") or []
    ind = (res.get("indicators") or {}).get("quote") or []
    closes: List[Any] = []
    if ind and isinstance(ind, list) and ind[0].get("close") is not None:
        closes = ind[0].get("close") or []

    debug = {
        "now": now_epoch,
        "base_close": base_close,
        "pre_start": pre_start,
        "pre_end": pre_end,
        "post_start": post_start,
        "post_end": post_end,
        "pre_source": None,
        "post_source": None,
    }

    # --- Premarket ---
    pre = None
    if pre_start is None or pre_end is None:
        debug["pre_source"] = "tp_missing"
    elif not (pre_start <= now_epoch <= pre_end):
        # kívül a pre ablakon: PM legyen n/a (ne használjunk stale meta preMarketPrice-t)
        debug["pre_source"] = "gated_outside_window"
    else:
        pre_meta = _to_float(meta.get("preMarketPrice"))
        if pre_meta is not None:
            pre = pre_meta
            debug["pre_source"] = "meta"
        else:
            inf = _last_close_in_window(ts, closes, pre_start, pre_end) if (ts and closes) else None
            if inf is not None:
                pre = inf
                debug["pre_source"] = "infer"
            else:
                debug["pre_source"] = "infer_none"

    # --- After-hours (postmarket) ---
    post = None
    if post_start is None or post_end is None:
        debug["post_source"] = "tp_missing"
    else:
        # carryforward: a legutóbbi (mostani vagy már lezárt) post ablakból dolgozunk,
        # de ha túl régi, inkább legyen n/a
        max_age_sec = 36 * 3600
        if (now_epoch - post_end) > max_age_sec:
            debug["post_source"] = "too_old"
        else:
            inside_post = (post_start <= now_epoch <= post_end)
            post_meta = _to_float(meta.get("postMarketPrice")) if inside_post else None
            if post_meta is not None:
                post = post_meta
                debug["post_source"] = "meta"
            else:
                inf = _last_close_in_window(ts, closes, post_start, post_end) if (ts and closes) else None
                if inf is not None:
                    post = inf
                    debug["post_source"] = "infer"
                else:
                    debug["post_source"] = "infer_none"

    return base_close, pre, post, debug


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    ap.add_argument("--report", default="1")  # legacy/compat (ignored)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    now_epoch = int(time.time())

    positions, watchlist = load_master_positions_watchlist(args.master)
    tickers = positions + [t for t in watchlist if t not in set(positions)]

    rows: Dict[str, Tuple[Optional[float], Optional[float], Optional[float], Dict[str, Any]]] = {}
    dbg_counts = {
        "pre_meta": 0, "pre_infer": 0, "pre_gated": 0, "pre_none": 0,
        "post_meta": 0, "post_infer": 0, "post_gated": 0, "post_none": 0,
        "base_missing": 0,
    }
    sample_dbg = []  # max 10

    for t in tickers:
        try:
            base_close, pre, post, dbg = chart_prices(t, now_epoch)
            pm = pct(pre, base_close)
            ah = pct(post, base_close)

            if base_close is None:
                dbg_counts["base_missing"] += 1

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

            rows[t] = (base_close, pm, ah, dbg)
        except Exception:
            rows[t] = (None, None, None, {"error": "exception"})

    def write_block(fh, title: str, xs: List[str]):
        fh.write(f"## {title}\n\n")
        for t in xs:
            base_close, pm, ah, _ = rows.get(t, (None, None, None, {}))
            fh.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}\n")
        fh.write("\n")

    all_na = all((pm is None and ah is None) for _, pm, ah, _ in rows.values())

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write("# #1 — Premarket check (PRICE ENGINE)\n\n")
        f.write(f"Verzió: {VERSION}\n\n")
        f.write("Formátum: AH elöl, PM utána.\n\n")

        write_block(f, "Pozíciók", positions)
        write_block(f, "Watchlist", watchlist)

        if args.debug or all_na:
            f.write("## Debug (csak hiba esetén / --debug)\n")
            f.write(f"- now_epoch: {now_epoch}\n")
            for k in ["base_missing","pre_meta","pre_infer","pre_gated","pre_none","post_meta","post_infer","post_gated","post_none"]:
                f.write(f"- {k}: {dbg_counts[k]}\n")
            f.write("\n### Debug sample (first 10 tickers)\n")
            for t, dbg, pm, ah in sample_dbg:
                f.write(
                    f"- {t}: base={dbg.get('base_close')} | "
                    f"AH {fmt(ah)} (post_source={dbg.get('post_source')}, post_start={dbg.get('post_start')}, post_end={dbg.get('post_end')}) | "
                    f"PM {fmt(pm)} (pre_source={dbg.get('pre_source')}, pre_start={dbg.get('pre_start')}, pre_end={dbg.get('pre_end')})\n"
                )

    # short stderr "runner.log" life sign
    print(f"RUNNER_VERSION={VERSION}", file=sys.stderr, flush=True)
    print(f"positions={len(positions)} watchlist={len(watchlist)} total={len(tickers)}", file=sys.stderr, flush=True)

    if all_na:
        print("ALL_NA: nincs használható AH/PM adat (base_close hiány vagy chart/meta hiány).", file=sys.stderr, flush=True)
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
