#!/usr/bin/env python3
# report_runner.py — v4.6.10-price-engine-sellref-snapshot-2026-02-17
#
# FIX / CÉL:
# - Stabil #1 riport: AH elöl, PM utána, külön blokkban: Pozíciók (Darabszam>0), majd Watchlist.
# - PM "window gating": csak akkor számolunk PM-et, ha MOST premarket ablakban vagyunk.
# - AH "carryforward": premarketben is mutatjuk a tegnapi after-hours árat (meta.postMarketPrice),
#   különben inferálunk a post ablakból, ha MOST post ablakban vagyunk.
# - BASE (prev close) fix:
#   - PM alap: prev close
#   - AH alap: prev close
# - Debug csak akkor kerül a reportba, ha NINCS használható AH és PM se (minden n/a).
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

import csv
import io
import json
import sys
import traceback
import argparse
import urllib.request
import time
import datetime
from typing import Any, Dict, Tuple, Optional, List

def write_header(f, interval_start: str, interval_end: str):
    try:
        from zoneinfo import ZoneInfo
        _tz = ZoneInfo("Europe/Budapest")
        run_time = datetime.datetime.now(_tz).strftime("%H:%M")
    except Exception:
        # Fallback: assume runner uses UTC; add 1h for CET (best effort)
        run_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=1)).strftime("%H:%M")
    header = (
        "# #1 — Premarket check (PRICE ENGINE)\n\n"
        f"Verzió: {VERSION} | Futás ideje: {run_time}\n"
        f"Időintervallum (ellenőrzés): {interval_start} – {interval_end}\n\n"
    )
    f.write(header)




def _budapest_windows(now_epoch: int, last_regular_market_time: int | None = None):
    """Return #1 windows in UTC epoch seconds, derived from America/New_York sessions.

    AH = last regular close (16:00 NY on the last close day) -> today's PM start (04:00 NY)
    PM = today 04:00 NY -> today 09:30 NY

    This keeps windows correct during the US/EU DST mismatch weeks.
    """
    try:
        from zoneinfo import ZoneInfo
        tz_bud = ZoneInfo("Europe/Budapest")
        tz_ny = ZoneInfo("America/New_York")
    except Exception:
        tz_bud = datetime.timezone(datetime.timedelta(hours=1))
        tz_ny = datetime.timezone(datetime.timedelta(hours=-5))

    now_utc = datetime.datetime.fromtimestamp(now_epoch, tz=datetime.timezone.utc)
    now_ny = now_utc.astimezone(tz_ny)
    now_bud = now_utc.astimezone(tz_bud)

    # Last real close day from Yahoo regularMarketTime when available; fallback to yesterday NY date
    if last_regular_market_time is not None:
        try:
            close_day_ny = datetime.datetime.fromtimestamp(
                int(last_regular_market_time), tz=datetime.timezone.utc
            ).astimezone(tz_ny).date()
        except Exception:
            close_day_ny = now_ny.date() - datetime.timedelta(days=1)
    else:
        close_day_ny = now_ny.date() - datetime.timedelta(days=1)

    pm_day_ny = now_ny.date()

    ah_start_ny = datetime.datetime(close_day_ny.year, close_day_ny.month, close_day_ny.day, 16, 0, tzinfo=tz_ny)
    pm_start_ny = datetime.datetime(pm_day_ny.year, pm_day_ny.month, pm_day_ny.day, 4, 0, tzinfo=tz_ny)
    pm_end_ny   = datetime.datetime(pm_day_ny.year, pm_day_ny.month, pm_day_ny.day, 9, 30, tzinfo=tz_ny)

    ah_start = int(ah_start_ny.astimezone(datetime.timezone.utc).timestamp())
    pm_start = int(pm_start_ny.astimezone(datetime.timezone.utc).timestamp())
    pm_end   = int(pm_end_ny.astimezone(datetime.timezone.utc).timestamp())

    return (
        pm_start,
        pm_end,
        ah_start,
        pm_start,  # AH ends where PM starts
        now_bud.isoformat(),
        close_day_ny.isoformat(),
    )

VERSION="v4.6.22-price-engine-usdst-v7pm-2026-03-16"


def pct(a, b):
    if a is None or b in (None, 0):
        return None
    try:
        return (float(a) / float(b) - 1.0) * 100.0
    except Exception:
        return None


def fmt(x):
    if x is None:
        return "n/a"
    return f"{'+' if x >= 0 else ''}{x:.2f}%"


def http_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read())


def http_json_retry(url: str, retries: int = 3, base_sleep: float = 0.6) -> Dict[str, Any]:
    """HTTP JSON with basic retry/backoff for transient errors (incl. 429/5xx)."""
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            return http_json(url)
        except Exception as e:
            last_err = e
            time.sleep(base_sleep * (2 ** i))
    if last_err:
        raise last_err
    raise RuntimeError("http_json_retry: unknown error")



def yahoo_quote_batch(symbols: List[str], retries: int = 2) -> Dict[str, Dict[str, Any]]:
    """Batch quote lookup via Yahoo v7 quote endpoint.

    Returns: {SYMBOL: quote_dict}
    """
    syms = [s.strip().upper() for s in symbols if s and s.strip()]
    out: Dict[str, Dict[str, Any]] = {}
    if not syms:
        return out

    # Yahoo v7 quote supports many symbols in one request (URL length is the main limit).
    # We'll chunk to keep URLs sane.
    CHUNK = 80
    base = "https://query2.finance.yahoo.com/v7/finance/quote?symbols="
    for i in range(0, len(syms), CHUNK):
        chunk = syms[i:i+CHUNK]
        url = base + ",".join(chunk)
        data = http_json_retry(url, retries=retries)
        res = (((data or {}).get("quoteResponse") or {}).get("result")) or []
        for q in res:
            sym = (q.get("symbol") or "").upper()
            if sym:
                out[sym] = q
    return out


def fmt_dt_budapest(epoch_utc: int) -> str:
    """Format UTC epoch into Europe/Budapest local datetime YYYY-MM-DD HH:MM."""
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Budapest")
    except Exception:
        tz = datetime.timezone(datetime.timedelta(hours=1))
    dt = datetime.datetime.fromtimestamp(int(epoch_utc), tz=datetime.timezone.utc).astimezone(tz)
    return dt.strftime("%Y-%m-%d %H:%M")


def _to_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _read_text_from_path_or_url(path_or_url: str) -> str:
    """
    Accepts either a local file path OR a http(s) URL.
    Returns UTF-8 text (errors replaced).
    """
    if path_or_url.lower().startswith(("http://", "https://")):
        req = urllib.request.Request(path_or_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
        return raw.decode("utf-8", errors="replace")
    # local file
    with open(path_or_url, "r", encoding="utf-8", errors="replace", newline="") as f:
        return f.read()


def load_master_rows(path_or_url: str) -> List[Dict[str, Any]]:
    """
    Elvárt oszlopok (Google Sheets CSV): Ticker, Darabszam, Bekerulesi ar ($/db), Broker, Eladasi ar
    Csak a Ticker kötelező. Darabszam ha >0 -> pozíció.

    Megjegyzés: a workflow gyakran URL-t ad át (--master). Ezt is támogatjuk.
    """
    rows: List[Dict[str, Any]] = []
    txt = _read_text_from_path_or_url(path_or_url)

    # csv.DictReader expects a file-like object
    fobj = io.StringIO(txt)
    rdr = csv.DictReader(fobj)
    for r in rdr:
        t = (r.get("Ticker") or r.get("ticker") or r.get("Symbol") or r.get("symbol") or "").strip().upper()
        if not t:
            continue
        qty_raw = (r.get("Darabszam") or r.get("darabszam") or r.get("Qty") or r.get("qty") or "").strip()
        qty = _to_float(qty_raw)
        rows.append({"ticker": t, "qty": qty})

    # de-dup: első előfordulás nyer (sheet-sorrend)
    seen = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        t = r["ticker"]
        if t in seen:
            continue
        out.append(r)
        seen.add(t)
    return out


def _tp(meta: Dict[str, Any], key: str) -> Optional[Tuple[int, int]]:
    # prefer currentTradingPeriod
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


def chart_prices(t: str, now_epoch: int, v7_quote: Optional[Dict[str, Any]] = None) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
    """
    Returns: (prev_close, pre_price, post_price, debug)
    PM primary source: Yahoo v7 preMarketPrice/preMarketTime
    AH primary source: Yahoo meta/v7 postMarketPrice inside the AH window, else chart inference
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=5d&includePrePost=true"
    j = http_json(url)
    res = (j.get("chart") or {}).get("result") or []
    if not res:
        raise ValueError("no chart result")
    res0 = res[0]
    meta = res0.get("meta", {}) or {}

    ts = res0.get("timestamp") or []
    ind = (res0.get("indicators") or {}).get("quote") or []
    closes = []
    if ind and isinstance(ind, list) and ind[0].get("close") is not None:
        closes = ind[0].get("close") or []

    debug = {
        "now": now_epoch,
        "pre_start": None, "pre_end": None,
        "post_start": None, "post_end": None,
        "reg_start": None, "reg_end": None,
        "pre_source": None, "post_source": None,
        "base_prev": None,
    }

    # Correct base: official previous close first
    prev = (
        meta.get("regularMarketPreviousClose")
        or meta.get("previousClose")
        or (v7_quote or {}).get("regularMarketPreviousClose")
        or (v7_quote or {}).get("regularMarketPrice")
        or meta.get("regularMarketPrice")
    )
    debug["base_prev"] = prev

    last_rmt = meta.get("regularMarketTime") or (v7_quote or {}).get("regularMarketTime")
    pm_start, pm_end, ah_start, ah_end, now_local_iso, close_day_iso = _budapest_windows(now_epoch, last_rmt)
    debug["close_day_local"] = close_day_iso
    debug["now_local"] = now_local_iso
    debug["pre_start"] = pm_start
    debug["pre_end"] = pm_end
    debug["post_start"] = ah_start
    debug["post_end"] = ah_end

    # --- PM ---
    pre = None
    if now_epoch < pm_start:
        debug["pre_source"] = "future_window"
    else:
        q = v7_quote or {}
        pre_q = q.get("preMarketPrice")
        pre_qt = q.get("preMarketTime")
        try:
            if pre_q is not None and pre_qt is not None:
                pre_q = float(pre_q)
                pre_qt = int(pre_qt)
            else:
                pre_q = None
                pre_qt = None
        except Exception:
            pre_q = None
            pre_qt = None

        if pre_q is not None and pre_qt is not None and (pm_start <= pre_qt <= pm_end):
            pre = pre_q
            debug["pre_source"] = "v7_preMarketPrice"
            debug["pre_meta_time"] = pre_qt
            debug["pre_meta_price"] = pre_q
        else:
            inf = _last_close_in_window(ts, closes, pm_start, min(now_epoch, pm_end)) if (ts and closes) else None
            if inf is not None:
                pre = inf
                debug["pre_source"] = "infer_fixed"
            else:
                debug["pre_source"] = "none_fixed"

    # --- AH ---
    post = None
    post_meta_price = meta.get("postMarketPrice")
    post_meta_time = meta.get("postMarketTime")
    try:
        if post_meta_price is not None and post_meta_time is not None:
            post_meta_price = float(post_meta_price)
            post_meta_time = int(post_meta_time)
        else:
            post_meta_price = None
            post_meta_time = None
    except Exception:
        post_meta_price = None
        post_meta_time = None

    q = v7_quote or {}
    v7_post = q.get("postMarketPrice")
    v7_post_t = q.get("postMarketTime")
    try:
        if v7_post is not None and v7_post_t is not None:
            v7_post = float(v7_post)
            v7_post_t = int(v7_post_t)
        else:
            v7_post = None
            v7_post_t = None
    except Exception:
        v7_post = None
        v7_post_t = None

    if post_meta_price is not None and post_meta_time is not None and (ah_start <= post_meta_time <= ah_end):
        post = post_meta_price
        debug["post_source"] = "meta_postMarketPrice"
    elif v7_post is not None and v7_post_t is not None and (ah_start <= v7_post_t <= ah_end):
        post = v7_post
        debug["post_source"] = "v7_postMarketPrice"
    else:
        inf = _last_close_in_window(ts, closes, ah_start, ah_end) if (ts and closes) else None
        if inf is not None:
            post = inf
            debug["post_source"] = "infer_fixed"
        else:
            debug["post_source"] = "none_fixed"

    # keep noisy spikes out
    try:
        if debug.get("post_source") == "infer_fixed" and prev not in (None, 0) and post is not None:
            test_ah = (float(post) / float(prev) - 1.0) * 100.0
            if abs(test_ah) > 25.0:
                debug["post_source"] = "suspect_infer"
                post = None
    except Exception:
        pass

    return _to_float(prev), _to_float(pre), _to_float(post), debug
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    now_epoch = int(time.time())

    master_rows = load_master_rows(args.master)
    positions = [r for r in master_rows if (r.get("qty") is not None and r["qty"] > 0)]
    watchlist = [r for r in master_rows if r not in positions]

    # compute
    out_rows_pos = []
    out_rows_wl = []

    all_tickers = [r["ticker"] for r in master_rows]
    quote_map = {}
    try:
        quote_map = yahoo_quote_batch(all_tickers, retries=2)
    except Exception:
        quote_map = {}

    dbg_counts = {
        "pre_meta": 0, "pre_infer": 0, "pre_gated": 0, "pre_none": 0,
        "post_meta": 0, "post_infer": 0, "post_carry": 0, "post_gated": 0, "post_none": 0,
        "errors": 0,
    }
    sample_dbg = []

    # Snapshot for post-processing (SellRef patch): store the actual session prices
    # already derived from Yahoo chart during the runner, so the patch step does not
    # need to hit Yahoo again (avoids 401/challenge).
    price_snapshot: Dict[str, Dict[str, Any]] = {}

    def handle_one(t: str):
        nonlocal sample_dbg
        prev, pre, post, dbg = chart_prices(t, now_epoch, quote_map.get(t))
        pm = pct(pre, prev)
        ah = pct(post, prev)

        price_snapshot[t] = {
            "prev_close": prev,
            "pm_price": pre,
            "ah_price": post,
            # keep a minimal trace for debugging / future extensions
            "meta": {
                "pre_source": dbg.get("pre_source"),
                "post_source": dbg.get("post_source"),
                "pre_start": dbg.get("pre_start"),
                "pre_end": dbg.get("pre_end"),
                "post_start": dbg.get("post_start"),
                "post_end": dbg.get("post_end"),
            },
        }

        ps = dbg.get("pre_source")
        if ps in ("meta", "v7_preMarketPrice"):
            dbg_counts["pre_meta"] += 1
        elif ps in ("infer", "infer_fixed"):
            dbg_counts["pre_infer"] += 1
        elif ps in ("gated_outside_window", "future_window"):
            dbg_counts["pre_gated"] += 1
        else:
            dbg_counts["pre_none"] += 1

        qs = dbg.get("post_source")
        if qs in ("meta", "meta_postMarketPrice", "v7_postMarketPrice"):
            dbg_counts["post_meta"] += 1
        elif qs in ("infer", "infer_fixed"):
            dbg_counts["post_infer"] += 1
        elif qs == "carry_infer":
            dbg_counts["post_carry"] += 1
        elif qs == "gated_outside_window":
            dbg_counts["post_gated"] += 1
        else:
            dbg_counts["post_none"] += 1

        if len(sample_dbg) < 10:
            sample_dbg.append((t, dbg, pm, ah))

        return pm, ah, dbg

    for r in positions:
        t = r["ticker"]
        try:
            pm, ah, dbg = handle_one(t)
            out_rows_pos.append((t, ah, pm))
        except Exception:
            dbg_counts["errors"] += 1
            out_rows_pos.append((t, None, None))

    for r in watchlist:
        t = r["ticker"]
        try:
            pm, ah, dbg = handle_one(t)
            out_rows_wl.append((t, ah, pm))
        except Exception:
            dbg_counts["errors"] += 1
            out_rows_wl.append((t, None, None))

    # sorting (WEBBIBLIA request): value-desc, using PM if available, else AH.
    # - Primary: PM when not n/a, otherwise AH
    # - Secondary (tie-break): the other field (AH/PM)
    # - n/a rows go to the bottom

    def _k(row):
        _, ah, pm = row
        primary = pm if pm is not None else ah
        secondary = ah if pm is not None else pm
        if primary is None:
            return (0, -1e9, -1e9)
        return (1, float(primary), float(secondary) if secondary is not None else -1e9)

    out_rows_pos.sort(key=_k, reverse=True)
    out_rows_wl.sort(key=_k, reverse=True)

    # Determine if we have any data at all
    any_data = any((ah is not None or pm is not None) for _, ah, pm in (out_rows_pos + out_rows_wl))

    # ---- Header interval (global) ----
    # Anchor the displayed interval to the last real US close using a liquid proxy (SPY),
    # so Monday mornings / holidays are correct.
    interval_start = None
    interval_end = None
    try:
        _p, _pre, _post, _dbg = chart_prices("SPY", now_epoch)
        now_iso = _dbg.get("now_local_iso") or _dbg.get("now_local") or _dbg.get("now_local_isoformat")
        close_day = _dbg.get("close_day_local") or _dbg.get("close_day")
        if now_iso and close_day:
            interval_start = f"{close_day} 22:00"
            hhmm = now_iso.split("T")[1][:5] if "T" in now_iso else now_iso[11:16]
            interval_end = f"{now_iso[:10]} {hhmm}"
    except Exception:
        pass

    if interval_start is None or interval_end is None:
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo("Europe/Budapest")
        except Exception:
            tz = datetime.timezone(datetime.timedelta(hours=1))
        _now_local = datetime.datetime.fromtimestamp(now_epoch, tz=datetime.timezone.utc).astimezone(tz)
        interval_end = _now_local.strftime("%Y-%m-%d %H:%M")
        interval_start = (_now_local.date() - datetime.timedelta(days=1)).strftime("%Y-%m-%d") + " 22:00"

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        write_header(f, interval_start, interval_end)

        f.write("## Pozíciók\n\n")
        for t, ah, pm in out_rows_pos:
            f.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}\n")

        f.write("\n## Watchlist\n\n")
        for t, ah, pm in out_rows_wl:
            f.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}\n")
    
        # Debug csak akkor, ha teljesen üres
        if (not any_data) or args.debug:
            f.write("\n## Debug (only if no data / --debug)\n")
            f.write(f"- now_epoch: {now_epoch}\n")
            for k in ["pre_meta","pre_infer","pre_gated","pre_none","post_meta","post_infer","post_carry","post_gated","post_none","errors"]:
                f.write(f"- {k}: {dbg_counts[k]}\n")
            f.write("\n### Debug sample (first 10 tickers)\n")
            for t, dbg, pm, ah in sample_dbg:
                f.write(
                    f"- {t}: AH {fmt(ah)} (post_source={dbg.get('post_source')}, post_start={dbg.get('post_start')}, post_end={dbg.get('post_end')}) | "
                    f"PM {fmt(pm)} (pre_source={dbg.get('pre_source')}, pre_start={dbg.get('pre_start')}, pre_end={dbg.get('pre_end')})\n"
                )

    # Write snapshot alongside the report for SellRef patching.
    # Path convention: reports/price_snapshot_1.json
    try:
        snap_path = "reports/price_snapshot_1.json" if args.out.startswith("reports/") else "price_snapshot_1.json"
        with open(snap_path, "w", encoding="utf-8", newline="\n") as sf:
            json.dump(
                {
                    "version": VERSION,
                    "generated_utc": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "now_epoch": now_epoch,
                    "prices": price_snapshot,
                },
                sf,
                ensure_ascii=False,
                indent=2,
            )
        print(f"SELLREF_SNAPSHOT=OK path={snap_path} tickers={len(price_snapshot)}", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"SELLREF_SNAPSHOT=FAIL err={e}", file=sys.stderr, flush=True)
    
    # stderr log "életjel"
    print(f"RUNNER_VERSION={VERSION}", file=sys.stderr, flush=True)
    print(f"positions={len(out_rows_pos)} watchlist={len(out_rows_wl)} total={len(out_rows_pos)+len(out_rows_wl)}", file=sys.stderr, flush=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
