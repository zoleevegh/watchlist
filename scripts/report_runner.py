#!/usr/bin/env python3
# report_runner.py — v4.4.3-price-engine-ahpm-basefix-2026-01-06
#
# Fixek (v4.4.0 -> v4.4.1):
# - TradingPeriods kiválasztás javítva:
#   - PM: csak akkor számolunk, ha MOST a pre ablakban vagyunk (no carry-forward).
#   - AH: ha MOST post ablakban vagyunk, azt használjuk; különben a legutóbbi LEZÁRT post ablakot (carry-forward),
#     hogy premarketben is legyen tegnap esti AH (ez kell az #1-hez).
# - Százalék-bázis javítva:
#   - PM%: preMarketPrice / regularMarketPreviousClose
#   - AH%: postMarketPrice / regularMarketPrice (a legutóbbi záró)
# - Debug blokk csak ALL_NA esetén kerül a report végére.
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

import argparse
import csv
import json
import sys
import time
import traceback
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

VERSION = "v4.4.3-price-engine-ahpm-basefix-2026-01-06"


def pct(new: Optional[float], base: Optional[float]) -> Optional[float]:
    if new is None or base in (None, 0):
        return None
    return (new / base - 1.0) * 100.0


def fmt(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{'+' if x >= 0 else ''}{x:.2f}%"


def http_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read())


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
            or r.get("darabszám")
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
        (positions if is_position.get(t) else watchlist).append(t)

    return positions, watchlist


def _extract_periods(meta: Dict[str, Any], key: str) -> List[Tuple[int, int]]:
    """Extract list of (start,end) epochs for a given key from meta.tradingPeriods and meta.currentTradingPeriod."""

    out: List[Tuple[int, int]] = []

    ctp = meta.get("currentTradingPeriod")
    if isinstance(ctp, dict) and isinstance(ctp.get(key), dict):
        d = ctp.get(key) or {}
        if "start" in d and "end" in d:
            try:
                out.append((int(d["start"]), int(d["end"])))
            except Exception:
                pass

    tp = meta.get("tradingPeriods")
    if isinstance(tp, dict) and isinstance(tp.get(key), list):
        days = tp.get(key) or []
        # Expected structure: [[{...}], [{...}], ...] but can vary; flatten defensively.
        for day in days:
            if isinstance(day, list):
                for obj in day:
                    if isinstance(obj, dict) and "start" in obj and "end" in obj:
                        try:
                            out.append((int(obj["start"]), int(obj["end"])))
                        except Exception:
                            continue
            elif isinstance(day, dict) and "start" in day and "end" in day:
                try:
                    out.append((int(day["start"]), int(day["end"])))
                except Exception:
                    pass

    # uniq + sort
    uniq = sorted(set(out), key=lambda x: x[0])
    return uniq


def _select_in_window(periods: List[Tuple[int, int]], now_epoch: int) -> Optional[Tuple[int, int]]:
    for s, e in periods:
        if s <= now_epoch <= e:
            return (s, e)
    return None


def _select_last_completed(periods: List[Tuple[int, int]], now_epoch: int) -> Optional[Tuple[int, int]]:
    past = [(s, e) for (s, e) in periods if e < now_epoch]
    if not past:
        return None
    return max(past, key=lambda x: x[1])


def _last_close_in_window(timestamps: List[int], closes: List[Any], start: int, end: int) -> Optional[float]:
    last: Optional[float] = None
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


def chart_prices(t: str, now_epoch: int) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
    """Return (base_prev_close, base_regular_close, pre_price, post_price, debug)."""

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d&includePrePost=true"
    j = http_json(url)
    res = j["chart"]["result"][0]
    meta = res.get("meta", {})

    base_prev = meta.get("regularMarketPreviousClose") or meta.get("previousClose")
    base_regular = meta.get("regularMarketPrice") or meta.get("regularMarketClose") or meta.get("chartPreviousClose")

    # periods
    pre_periods = _extract_periods(meta, "pre")
    post_periods = _extract_periods(meta, "post")

    pre_win = _select_in_window(pre_periods, now_epoch)
    post_win = _select_in_window(post_periods, now_epoch) or _select_last_completed(post_periods, now_epoch)

    ts = res.get("timestamp") or []
    ind = (res.get("indicators") or {}).get("quote") or []
    closes: List[Any] = []
    if ind and isinstance(ind, list) and isinstance(ind[0], dict):
        closes = ind[0].get("close") or []

    debug: Dict[str, Any] = {
        "now": now_epoch,
        "base_prev": base_prev,
        "base_regular": base_regular,
        "pre_win": pre_win,
        "post_win": post_win,
        "pre_source": None,
        "post_source": None,
    }

    # --- Premarket (NO carry-forward) ---
    pre = meta.get("preMarketPrice")
    if pre is not None:
        debug["pre_source"] = "meta"
    else:
        if pre_win is None:
            pre = None
            debug["pre_source"] = "gated_outside_window"
        else:
            s, e = pre_win
            inf = _last_close_in_window(ts, closes, s, e) if (ts and closes) else None
            if inf is not None:
                pre = inf
                debug["pre_source"] = "infer"
            else:
                pre = None
                debug["pre_source"] = "infer_none"

    # --- After-hours (carry-forward last completed post window) ---
    post = meta.get("postMarketPrice")
    if post is not None:
        debug["post_source"] = "meta"
    else:
        if post_win is None:
            post = None
            debug["post_source"] = "tp_missing"
        else:
            s, e = post_win
            inf = _last_close_in_window(ts, closes, s, e) if (ts and closes) else None
            if inf is not None:
                post = inf
                debug["post_source"] = "infer"
            else:
                post = None
                debug["post_source"] = "infer_none"

    # numeric
    try:
        base_prev = float(base_prev) if base_prev is not None else None
    except Exception:
        base_prev = None
    try:
        base_regular = float(base_regular) if base_regular is not None else None
    except Exception:
        base_regular = None
    try:
        pre = float(pre) if pre is not None else None
    except Exception:
        pre = None
    try:
        post = float(post) if post is not None else None
    except Exception:
        post = None

    return base_prev, base_regular, pre, post, debug


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1, help="Back-compat (ignored): report id 1/2/3")
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    now_epoch = int(time.time())
    positions, watchlist = load_master_split(args.master)
    all_tickers = positions + [t for t in watchlist if t not in set(positions)]

    data: Dict[str, Tuple[Optional[float], Optional[float], Dict[str, Any]]] = {}
    debug_samples: List[Tuple[str, Dict[str, Any], Optional[float], Optional[float]]] = []

    counts = {
        "pre_meta": 0,
        "pre_infer": 0,
        "pre_gated": 0,
        "pre_none": 0,
        "post_meta": 0,
        "post_infer": 0,
        "post_none": 0,
    }

    for t in all_tickers:
        try:
            base_prev, base_regular, pre, post, dbg = chart_prices(t, now_epoch)
            pm = pct(pre, base_prev)
            ah = pct(post, base_regular)
            data[t] = (ah, pm, dbg)

            ps = dbg.get("pre_source")
            if ps == "meta":
                counts["pre_meta"] += 1
            elif ps == "infer":
                counts["pre_infer"] += 1
            elif ps == "gated_outside_window":
                counts["pre_gated"] += 1
            else:
                counts["pre_none"] += 1

            qs = dbg.get("post_source")
            if qs == "meta":
                counts["post_meta"] += 1
            elif qs == "infer":
                counts["post_infer"] += 1
            else:
                counts["post_none"] += 1

            if len(debug_samples) < 10:
                debug_samples.append((t, dbg, ah, pm))
        except Exception:
            data[t] = (None, None, {"error": True})

    def _sort_key(t: str) -> float:
        ah, pm, _ = data.get(t, (None, None, {}))
        if ah is not None:
            return abs(ah)
        if pm is not None:
            return abs(pm)
        return -1.0

    positions_sorted = sorted(positions, key=_sort_key, reverse=True)
    watchlist_sorted = sorted(watchlist, key=_sort_key, reverse=True)

    all_na = all((data.get(t, (None, None, {}))[0] is None and data.get(t, (None, None, {}))[1] is None) for t in all_tickers)

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write("# #1 — Premarket check (PRICE ENGINE)\n\n")
        f.write(f"Verzió: {VERSION}\n\n")
        f.write("Formátum: AH elöl, PM utána.\n\n")

        f.write("## Pozíciók\n\n")
        for t in positions_sorted:
            ah, pm, _ = data.get(t, (None, None, {}))
            f.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}\n")

        f.write("\n## Watchlist\n\n")
        for t in watchlist_sorted:
            ah, pm, _ = data.get(t, (None, None, {}))
            f.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}\n")

        if all_na or args.debug:
            f.write("\n## Debug (csak akkor érdekes, ha nincs adat)\n")
            f.write(f"- now_epoch: {now_epoch}\n")
            for k in ["pre_meta", "pre_infer", "pre_gated", "pre_none", "post_meta", "post_infer", "post_none"]:
                f.write(f"- {k}: {counts[k]}\n")
            f.write("\n### Debug sample (first 10 tickers)\n")
            for t, dbg, ah, pm in debug_samples:
                f.write(
                    f"- {t}: AH {fmt(ah)} (post_source={dbg.get('post_source')}, post_win={dbg.get('post_win')}, base_regular={dbg.get('base_regular')}) | "
                    f"PM {fmt(pm)} (pre_source={dbg.get('pre_source')}, pre_win={dbg.get('pre_win')}, base_prev={dbg.get('base_prev')})\n"
                )

    # GH Actions életjel
    print(f"RUNNER_VERSION={VERSION}", file=sys.stderr, flush=True)
    print(f"positions={len(positions)} watchlist={len(watchlist)} total={len(all_tickers)}", file=sys.stderr, flush=True)
    if all_na:
        print("NO_DATA: sem AH sem PM nem elérhető (meta+infer).", file=sys.stderr, flush=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
