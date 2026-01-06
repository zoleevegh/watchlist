#!/usr/bin/env python3
# report_runner.py — v4.3.8-price-engine-windowselect-basefix-2026-01-06
#
# FIXEK v4.3.8:
# 1) HELYES BÁZIS az AH/PM %-hoz:
#    - PM% = (pre_price / prev_regular_close - 1) * 100
#    - AH% = (post_price / same_day_regular_close - 1) * 100   (nem az "previousClose"-hoz!)
# 2) Trading window kiválasztás:
#    - Nem csak az "aktuális nap" currentTradingPeriod-ját nézzük,
#      hanem a tradingPeriods listából kiválasztjuk a NOW-hoz releváns ablakot:
#        • PM: az a pre-ablak, amiben MOST benne vagyunk
#        • AH: ha MOST postmarket → az a post-ablak; ha MOST premarket → az utolsó lezárt post-ablak
# 3) Blokkok:
#    - "Pozíciók" (Darabszam > 0) blokk felül
#    - "Watchlist" alul
# 4) Kimenet:
#    - Formátum: AH elöl, PM utána
#    - Debug csak akkor kerül a report aljára, ha MINDEN ticker n/a.
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


VERSION = "v4.3.8-price-engine-windowselect-basefix-2026-01-06"


def pct(price: Optional[float], base: Optional[float]) -> Optional[float]:
    if price is None or base in (None, 0):
        return None
    return (price / base - 1) * 100.0


def fmt(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{'+' if x >= 0 else ''}{x:.2f}%"


def http_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def _norm(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "").replace("_", "").replace("-", "")


def load_master(path: str) -> List[Dict[str, Any]]:
    """
    MASTER CSV oszlopok:
      - ticker / Ticker / symbol / Symbol
      - darabszam / Darabszam / darabszám (ha >0 -> pozíció)
    """
    rows: List[Dict[str, Any]] = []
    with open(path, encoding="utf-8", newline="") as f:
        dr = csv.DictReader(f)
        for r in dr:
            ticker = (r.get("ticker") or r.get("Ticker") or r.get("symbol") or r.get("Symbol") or "").strip().upper()
            if not ticker:
                continue

            # darabszam oszlop keresése (több névvariáns)
            darab_val = None
            for k, v in r.items():
                nk = _norm(k)
                if nk in ("darabszam", "darabszám", "shares", "qty", "quantity", "db"):
                    darab_val = v
                    break

            is_position = False
            if darab_val is not None:
                try:
                    # "15", "15.0", "15,0" kezelés
                    dv = str(darab_val).strip().replace(",", ".")
                    if dv != "":
                        is_position = float(dv) > 0
                except Exception:
                    is_position = False

            rows.append({"ticker": ticker, "is_position": is_position})

    # uniq ticker (első előfordulás marad)
    seen = set()
    out = []
    for r in rows:
        t = r["ticker"]
        if t not in seen:
            out.append(r)
            seen.add(t)
    return out


def _all_periods(meta: Dict[str, Any], key: str) -> List[Tuple[int, int]]:
    """
    Yahoo v8 chart meta.tradingPeriods formátumából kinyeri az összes (start,end) ablakot.
    """
    out: List[Tuple[int, int]] = []

    tp = meta.get("tradingPeriods")
    if isinstance(tp, dict) and key in tp:
        arr = tp.get(key)
        # tipikusan: [[{start,end,gmtOffset,timezone}, ...], [...]]
        if isinstance(arr, list):
            for day in arr:
                if not isinstance(day, list):
                    continue
                for obj in day:
                    if isinstance(obj, dict) and "start" in obj and "end" in obj:
                        try:
                            out.append((int(obj["start"]), int(obj["end"])))
                        except Exception:
                            pass

    # currentTradingPeriod is lehet, de csak 1 nap – duplikációt nem baj
    ctp = meta.get("currentTradingPeriod")
    if isinstance(ctp, dict) and isinstance(ctp.get(key), dict):
        d = ctp[key]
        if "start" in d and "end" in d:
            try:
                out.append((int(d["start"]), int(d["end"])))
            except Exception:
                pass

    # uniq
    uniq = []
    seen = set()
    for s, e in out:
        k = (s, e)
        if k not in seen:
            uniq.append(k)
            seen.add(k)
    # rendezés időrendbe
    uniq.sort(key=lambda x: x[0])
    return uniq


def _pick_containing(periods: List[Tuple[int, int]], now: int) -> Optional[Tuple[int, int]]:
    for s, e in periods:
        if s <= now <= e:
            return (s, e)
    return None


def _pick_last_ended(periods: List[Tuple[int, int]], now: int) -> Optional[Tuple[int, int]]:
    past = [(s, e) for (s, e) in periods if e < now]
    if not past:
        return None
    return max(past, key=lambda x: x[1])


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


def chart_prices(ticker: str, now_epoch: int) -> Dict[str, Any]:
    """
    Visszaad:
      {
        "ticker": str,
        "pm_pct": Optional[float],
        "ah_pct": Optional[float],
        "dbg": {...}
      }
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=2d&includePrePost=true"
    j = http_json(url)
    res = j["chart"]["result"][0]
    meta = res.get("meta", {})

    ts = res.get("timestamp") or []
    ind = (res.get("indicators") or {}).get("quote") or []
    closes = []
    if ind and isinstance(ind, list) and isinstance(ind[0], dict):
        closes = ind[0].get("close") or []

    periods_pre = _all_periods(meta, "pre")
    periods_post = _all_periods(meta, "post")
    periods_reg = _all_periods(meta, "regular")

    pre_now = _pick_containing(periods_pre, now_epoch)
    post_now = _pick_containing(periods_post, now_epoch)

    # premarketben az "utolsó lezárt" post ablakot akarjuk (tegnap AH)
    post_last = _pick_last_ended(periods_post, now_epoch)

    # PM: csak ha NOW pre ablakban van
    pm_price = None
    pm_base = None
    pm_source = "n/a"
    if pre_now:
        pre_s, pre_e = pre_now
        # bázis: az utolsó lezárt REGULAR ablak close-ja (tegnap záró)
        reg_for_pre = _pick_last_ended(periods_reg, pre_s) or _pick_last_ended(periods_reg, now_epoch)
        if reg_for_pre:
            rs, re = reg_for_pre
            pm_base = _last_close_in_window(ts, closes, rs, re)
        if pm_base is None:
            pm_base = meta.get("previousClose") or meta.get("regularMarketPreviousClose")

        # ár: meta.preMarketPrice, ha van, különben infer a pre ablakból
        if meta.get("preMarketPrice") is not None:
            pm_price = float(meta["preMarketPrice"])
            pm_source = "meta"
        else:
            inf = _last_close_in_window(ts, closes, pre_s, pre_e) if (ts and closes) else None
            if inf is not None:
                pm_price = inf
                pm_source = "infer"
            else:
                pm_source = "infer_none"
    else:
        pm_source = "gated_outside_window"

    # AH: két esetben adunk:
    #  - ha most POST ablakban vagyunk -> a mostani post-ot használjuk
    #  - ha most PRE ablakban vagyunk -> a legutolsó lezárt post-ot használjuk (tegnap AH)
    ah_price = None
    ah_base = None
    ah_source = "n/a"
    selected_post = None
    if post_now:
        selected_post = post_now
    elif pre_now and post_last:
        selected_post = post_last

    if selected_post:
        post_s, post_e = selected_post

        # bázis: az a regular close, ami közvetlenül a post ablak előtt van (same-day close)
        reg_for_post = _pick_last_ended(periods_reg, post_s) or _pick_last_ended(periods_reg, now_epoch)
        if reg_for_post:
            rs, re = reg_for_post
            ah_base = _last_close_in_window(ts, closes, rs, re)

        # fallback (ha infer nem sikerül)
        if ah_base is None:
            # premarketben ez tipikusan tegnapi close, postmarketben a mai close
            ah_base = meta.get("regularMarketPrice") or meta.get("previousClose") or meta.get("regularMarketPreviousClose")

        # ár: meta.postMarketPrice, ha van, különben infer a kiválasztott post ablakból
        if meta.get("postMarketPrice") is not None:
            ah_price = float(meta["postMarketPrice"])
            ah_source = "meta"
        else:
            inf = _last_close_in_window(ts, closes, post_s, post_e) if (ts and closes) else None
            if inf is not None:
                ah_price = inf
                ah_source = "infer"
            else:
                ah_source = "infer_none"
    else:
        ah_source = "gated_outside_window"

    out = {
        "ticker": ticker,
        "pm_pct": pct(pm_price, pm_base),
        "ah_pct": pct(ah_price, ah_base),
        "dbg": {
            "now": now_epoch,
            "pre_now": pre_now,
            "post_now": post_now,
            "post_last": post_last,
            "pm_source": pm_source,
            "pm_price": pm_price,
            "pm_base": pm_base,
            "ah_source": ah_source,
            "ah_price": ah_price,
            "ah_base": ah_base,
        },
    }
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    args = ap.parse_args()

    now_epoch = int(time.time())

    master_rows = load_master(args.master)
    tickers_pos = [r["ticker"] for r in master_rows if r["is_position"]]
    tickers_wl = [r["ticker"] for r in master_rows if not r["is_position"]]

    results: Dict[str, Dict[str, Any]] = {}

    dbg_counts = {
        "pm_meta": 0,
        "pm_infer": 0,
        "pm_gated": 0,
        "pm_none": 0,
        "ah_meta": 0,
        "ah_infer": 0,
        "ah_gated": 0,
        "ah_none": 0,
    }
    sample_dbg: List[Tuple[str, Dict[str, Any], Optional[float], Optional[float]]] = []

    def _count_dbg(dbg: Dict[str, Any]) -> None:
        ps = dbg.get("pm_source")
        if ps == "meta":
            dbg_counts["pm_meta"] += 1
        elif ps == "infer":
            dbg_counts["pm_infer"] += 1
        elif ps == "gated_outside_window":
            dbg_counts["pm_gated"] += 1
        else:
            dbg_counts["pm_none"] += 1

        qs = dbg.get("ah_source")
        if qs == "meta":
            dbg_counts["ah_meta"] += 1
        elif qs == "infer":
            dbg_counts["ah_infer"] += 1
        elif qs == "gated_outside_window":
            dbg_counts["ah_gated"] += 1
        else:
            dbg_counts["ah_none"] += 1

    all_tickers = tickers_pos + [t for t in tickers_wl if t not in set(tickers_pos)]

    for t in all_tickers:
        try:
            r = chart_prices(t, now_epoch)
            results[t] = r
            _count_dbg(r["dbg"])
            if len(sample_dbg) < 10:
                sample_dbg.append((t, r["dbg"], r["ah_pct"], r["pm_pct"]))
        except Exception:
            results[t] = {"ticker": t, "ah_pct": None, "pm_pct": None, "dbg": {"error": "exception"}}

    def _sorted_block(tickers: List[str]) -> List[Tuple[str, Optional[float], Optional[float]]]:
        rows = [(t, results[t].get("ah_pct"), results[t].get("pm_pct")) for t in tickers]
        rows.sort(key=lambda r: (abs(r[1] or 0.0), abs(r[2] or 0.0)), reverse=True)
        return rows

    pos_rows = _sorted_block(tickers_pos)
    wl_rows = _sorted_block(tickers_wl)

    # megállapítjuk, hogy minden n/a-e
    all_na = True
    for t in all_tickers:
        if results[t].get("ah_pct") is not None or results[t].get("pm_pct") is not None:
            all_na = False
            break

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

        if all_na:
            f.write("\n## Debug (csak ALL_NA esetén)\n")
            f.write(f"- now_epoch: {now_epoch}\n")
            for k in ["pm_meta","pm_infer","pm_gated","pm_none","ah_meta","ah_infer","ah_gated","ah_none"]:
                f.write(f"- {k}: {dbg_counts[k]}\n")
            f.write("\n### Debug sample (first 10 tickers)\n")
            for t, dbg, ah, pm in sample_dbg:
                f.write(
                    f"- {t}: AH {fmt(ah)} (ah_source={dbg.get('ah_source')}, ah_base={dbg.get('ah_base')}) | "
                    f"PM {fmt(pm)} (pm_source={dbg.get('pm_source')}, pm_base={dbg.get('pm_base')})\n"
                )

    print(f"RUNNER_VERSION={VERSION}", file=sys.stderr, flush=True)
    print(f"positions={len(tickers_pos)} watchlist={len(tickers_wl)} total={len(all_tickers)}", file=sys.stderr, flush=True)

    if all_na:
        print("ALL_NA: nincs érvényes PM/AH adat a releváns ablakból (vagy nincs chart adat).", file=sys.stderr, flush=True)
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
