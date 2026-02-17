#!/usr/bin/env python3
# report_runner.py — v4.7.1-price-engine-watchlist-sellref-2026-02-17
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
    """Return fixed report windows in UTC epoch seconds for Europe/Budapest.

    Anchor logic:
      - Use the *last regular market time* (epoch) from Yahoo meta when available.
        This makes Monday morning (or holiday) runs anchor to the last real close (e.g. Friday),
        instead of using calendar 'yesterday'.

    #1 spec (local):
      AH: last close day 22:00 -> next day 02:00
      PM: today 10:00 -> today 15:30 (only if already started)
    """
    try:
        from zoneinfo import ZoneInfo  # py3.9+
        tz = ZoneInfo("Europe/Budapest")
    except Exception:
        tz = datetime.timezone(datetime.timedelta(hours=1))  # fallback (winter CET)

    now_utc = datetime.datetime.fromtimestamp(now_epoch, tz=datetime.timezone.utc)
    now_local = now_utc.astimezone(tz)

    # Determine "close day" in local terms
    close_day = None
    if last_regular_market_time is not None:
        try:
            close_local = datetime.datetime.fromtimestamp(int(last_regular_market_time), tz=datetime.timezone.utc).astimezone(tz)
            close_day = close_local.date()
        except Exception:
            close_day = None
    if close_day is None:
        close_day = (now_local.date() - datetime.timedelta(days=1))  # fallback

    today = now_local.date()

    def loc_dt(day, hh, mm):
        return datetime.datetime(day.year, day.month, day.day, hh, mm, tzinfo=tz)

    # AH: close_day 22:00 -> next day 02:00 (local)
    ah_start_local = loc_dt(close_day, 22, 0)
    ah_end_local   = loc_dt(close_day + datetime.timedelta(days=1), 2, 0)

    # PM: today 10:00 -> 15:30 (local)
    pm_start_local = loc_dt(today, 10, 0)
    pm_end_local   = loc_dt(today, 15, 30)

    # Convert to UTC epoch seconds
    ah_start = int(ah_start_local.astimezone(datetime.timezone.utc).timestamp())
    ah_end   = int(ah_end_local.astimezone(datetime.timezone.utc).timestamp())
    pm_start = int(pm_start_local.astimezone(datetime.timezone.utc).timestamp())
    pm_end   = int(pm_end_local.astimezone(datetime.timezone.utc).timestamp())

    return (pm_start, pm_end, ah_start, ah_end, now_local.isoformat(), close_day.isoformat())

VERSION="v4.7.1-price-engine-watchlist-sellref-2026-02-17"


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


def fmt_price(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    try:
        return f"${float(x):.2f}"
    except Exception:
        return "n/a"


def fmt_delta_pct(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{'+' if x >= 0 else ''}{x:.2f}%"


def http_json(url: str) -> Dict[str, Any]:
    # Yahoo gyakran blokkol "bot"-szerű kéréseket. Konzervatív böngésző-fejlécek.
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
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
    # query2 endpoint néha 401-et dob GitHub runner környezetben; query1 stabilabb.
    base = "https://query1.finance.yahoo.com/v7/finance/quote?symbols="
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

    ÚJ: Eladasi ar (E oszlop) -> sell_price (float). HU tizedesvesszőt is kezelünk.
    Duplikált tickerek esetén:
      - qty: utolsó nem üres érték nyer
      - sell_price: utolsó nem üres érték nyer
      - sorrend: az első előfordulás sorrendje marad (stabil listázás)
    """
    txt = _read_text_from_path_or_url(path_or_url)

    fobj = io.StringIO(txt)
    rdr = csv.DictReader(fobj)

    order: List[str] = []
    agg: Dict[str, Dict[str, Any]] = {}

    for r in rdr:
        t = (r.get("Ticker") or r.get("ticker") or r.get("Symbol") or r.get("symbol") or "").strip().upper()
        if not t:
            continue

        if t not in agg:
            agg[t] = {"ticker": t, "qty": None, "sell_price": None}
            order.append(t)

        qty_raw = (r.get("Darabszam") or r.get("darabszam") or r.get("Qty") or r.get("qty") or "").strip()
        qty = _to_float(qty_raw)
        if qty is not None:
            agg[t]["qty"] = qty

        sell_raw = (r.get("Eladasi ar") or r.get("Eladási ar") or r.get("eladasi ar") or r.get("Sell") or r.get("sell") or "").strip()
        if sell_raw:
            # HU tizedesvessző -> pont
            sell_raw = sell_raw.replace(" ", "").replace(",", ".")
            sell = _to_float(sell_raw)
            if sell is not None:
                agg[t]["sell_price"] = sell

    out: List[Dict[str, Any]] = []
    for t in order:
        out.append(agg[t])
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


def chart_prices(t: str, now_epoch: int) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
    """
    Returns: (prev_close, pre_price, post_price, debug)
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=2d&includePrePost=true"
    j = http_json(url)
    res = (j.get("chart") or {}).get("result") or []
    if not res:
        raise ValueError("no chart result")
    res0 = res[0]
    meta = res0.get("meta", {}) or {}

    debug: Dict[str, Any] = {}

    # Meta extended-hours fields (more stable than sparse extended-hours candles)
    post_meta_price = meta.get("postMarketPrice")
    post_meta_time = meta.get("postMarketTime")
    if post_meta_price is not None and post_meta_time is not None:
        try:
            post_meta_time = int(post_meta_time)
            post_meta_price = float(post_meta_price)
            debug["post_meta_time"] = post_meta_time
            debug["post_meta_price"] = post_meta_price
        except Exception:
            post_meta_time = None
            post_meta_price = None
    else:
        post_meta_time = None
        post_meta_price = None


    # Base close for AH/PM %: prefer regularMarketPrice when market is closed; fallback to previousClose.
    prev = meta.get("regularMarketPrice") or meta.get("previousClose") or meta.get("regularMarketPreviousClose")

    wpre = _tp(meta, "pre")
    wpost = _tp(meta, "post")
    wreg = _tp(meta, "regular")

    pre_start, pre_end = (wpre if wpre else (None, None))
    post_start, post_end = (wpost if wpost else (None, None))
    reg_start, reg_end = (wreg if wreg else (None, None))

    ts = res0.get("timestamp") or []
    ind = (res0.get("indicators") or {}).get("quote") or []
    closes = []
    if ind and isinstance(ind, list) and ind[0].get("close") is not None:
        closes = ind[0].get("close") or []

    debug.update({
        "now": now_epoch,
        "pre_start": pre_start, "pre_end": pre_end,
        "post_start": post_start, "post_end": post_end,
        "reg_start": reg_start, "reg_end": reg_end,
        "pre_source": None, "post_source": None,
        "base_prev": prev,
    })
    # --- Fixed windows (Europe/Budapest) ---
    last_rmt = meta.get("regularMarketTime")
    pm_start, pm_end, ah_start, ah_end, now_local_iso, close_day_iso = _budapest_windows(now_epoch, last_rmt)
    debug["close_day_local"] = close_day_iso
    debug["now_local"] = now_local_iso
    debug["pre_start"] = pm_start
    debug["pre_end"] = pm_end
    debug["post_start"] = ah_start
    debug["post_end"] = ah_end

    # PM: take last close inside PM window if available; if window is in the future -> n/a
    if now_epoch < pm_start:
        pre = None
        debug["pre_source"] = "future_window"
    else:
        inf = _last_close_in_window(ts, closes, pm_start, pm_end) if (ts and closes) else None
        if inf is not None:
            pre = inf
            debug["pre_source"] = "infer_fixed"
        else:
            pre = None
            debug["pre_source"] = "none_fixed"

    # AH: compute previous session after-hours window (local); prefer meta.postMarketPrice if it belongs to this window.
    inf = _last_close_in_window(ts, closes, ah_start, ah_end) if (ts and closes) else None
    if inf is not None:
        post = inf
        debug["post_source"] = "infer_fixed"
    else:
        post = None
        debug["post_source"] = "none_fixed"

    # Override with meta postMarketPrice when it belongs to this AH window
    if post_meta_time is not None and post_meta_price is not None and (ah_start <= post_meta_time <= ah_end):
        post = post_meta_price
        debug["post_source"] = "meta_postMarketPrice"

    # Sanity filter for inferred AH: if absurd move, drop to avoid noisy spikes
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

    # Session selection (Europe/Budapest fixed windows), anchored to last real US close via SPY regularMarketTime.
    session_label: Optional[str] = None
    session_kind: Optional[str] = None  # "PM" or "AH"
    pm_start = pm_end = ah_start = ah_end = None
    now_local_iso: Optional[str] = None
    close_day_iso: Optional[str] = None
    try:
        spy_q = yahoo_quote_batch(["SPY"]).get("SPY") or {}
        last_rmt = spy_q.get("regularMarketTime")
        pm_start, pm_end, ah_start, ah_end, now_local_iso, close_day_iso = _budapest_windows(now_epoch, int(last_rmt) if last_rmt is not None else None)
        if pm_start is not None and pm_end is not None and pm_start <= now_epoch <= pm_end:
            session_kind = "PM"
        elif ah_start is not None and ah_end is not None and ah_start <= now_epoch <= ah_end:
            session_kind = "AH"
    except Exception:
        session_kind = None

    master_rows = load_master_rows(args.master)
    positions = [r for r in master_rows if (r.get("qty") is not None and r["qty"] > 0)]
    watchlist = [r for r in master_rows if r not in positions]


    all_tickers = [r["ticker"] for r in master_rows]
    # Quote batch nem kritikus a #1 futáshoz; ha Yahoo blokkol, ne dőljön el a teljes report.
    try:
        quote_map = yahoo_quote_batch(all_tickers)
    except Exception:
        quote_map = {}

    # compute

    out_rows_pos = []
    out_rows_wl = []

    dbg_counts = {
        "pre_meta": 0, "pre_infer": 0, "pre_gated": 0, "pre_none": 0,
        "post_meta": 0, "post_infer": 0, "post_carry": 0, "post_gated": 0, "post_none": 0,
        "errors": 0,
    }
    sample_dbg = []

    def handle_one(t: str):
        nonlocal sample_dbg
        prev, pre, post, dbg = chart_prices(t, now_epoch)
        pm = pct(pre, prev)
        ah = pct(post, prev)

        ps = dbg.get("pre_source")
        if ps == "meta":
            dbg_counts["pre_meta"] += 1
        elif ps in ("infer", "infer_fixed"):
            dbg_counts["pre_infer"] += 1
        elif ps in ("gated_outside_window", "future_window"):
            dbg_counts["pre_gated"] += 1
        else:
            dbg_counts["pre_none"] += 1

        qs = dbg.get("post_source")
        if qs == "meta":
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
            out_rows_pos.append((t, ah, pm, r.get("sell_price")))
        except Exception:
            dbg_counts["errors"] += 1
            out_rows_pos.append((t, None, None, r.get("sell_price")))

    for r in watchlist:
        t = r["ticker"]
        try:
            pm, ah, dbg = handle_one(t)
            out_rows_wl.append((t, ah, pm, r.get("sell_price")))
        except Exception:
            dbg_counts["errors"] += 1
            out_rows_wl.append((t, None, None, r.get("sell_price")))

    # sorting (WEBBIBLIA request): value-desc, using PM if available, else AH.
    # - Primary: PM when not n/a, otherwise AH
    # - Secondary (tie-break): the other field (AH/PM)
    # - n/a rows go to the bottom

    def _k(row):
        _, ah, pm, _ = row
        primary = pm if pm is not None else ah
        secondary = ah if pm is not None else pm
        if primary is None:
            return (0, -1e9, -1e9)
        return (1, float(primary), float(secondary) if secondary is not None else -1e9)

    out_rows_pos.sort(key=_k, reverse=True)
    out_rows_wl.sort(key=_k, reverse=True)

    # Determine if we have any data at all
    any_data = any((ah is not None or pm is not None) for _, ah, pm, _ in (out_rows_pos + out_rows_wl))

    # ---- Header interval (global) ----
    interval_start = None
    interval_end = None
    try:
        if now_local_iso and close_day_iso:
            interval_start = f"{close_day_iso} 22:00"
            # now_local_iso is ISO, e.g. 2026-02-17T08:24:11+01:00
            hhmm = now_local_iso.split("T")[1][:5] if "T" in now_local_iso else now_local_iso[11:16]
            interval_end = f"{now_local_iso[:10]} {hhmm}"
    except Exception:
        interval_start = None
        interval_end = None

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
        for t, ah, pm, _sell in out_rows_pos:
            f.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}\n")

        f.write("\n## Watchlist\n\n")
        for t, ah, pm, sell in out_rows_wl:
            extra = ""
            if sell is not None and session_kind in ("PM", "AH"):
                q = quote_map.get(t) or {}
                cur = None
                if session_kind == "PM":
                    cur = _to_float(q.get("preMarketPrice")) or _to_float(q.get("regularMarketPrice"))
                else:
                    cur = _to_float(q.get("postMarketPrice")) or _to_float(q.get("regularMarketPrice"))
                dp = pct(cur, sell) if (cur is not None and sell not in (None, 0)) else None
                if cur is not None and dp is not None:
                    extra = f" | Now({session_kind}) {fmt_price(cur)} vs Sell {fmt_price(sell)} ({fmt_delta_pct(dp)})"
            f.write(f"- {t} — AH {fmt(ah)} | PM {fmt(pm)}{extra}\n")

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
