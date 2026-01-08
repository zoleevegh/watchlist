#!/usr/bin/env python3
# report_runner.py — v4.5.4-price-engine-header-interval-fix-2026-01-07
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

def write_header(f, interval_start, interval_end):
    try:
        from zoneinfo import ZoneInfo
        _tz = ZoneInfo("Europe/Budapest")
        run_time = datetime.datetime.now(_tz).strftime("%H:%M")
    except Exception:
        # Fallback: assume runner uses UTC; add 1h for CET (best effort)
        run_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=1)).strftime("%H:%M")
    header = (
        "# #1 – Premarket check (PRICE ENGINE)\n\n"
        f"Verzió: v4.5.4-price-engine-header-interval-fix-2026-01-07 | Futás ideje: {run_time}\n"
        f"Időintervallum (ellenőrzés): {interval_start} – {interval_end}\n\n"
    )
    f.write(header)
from typing import Optional, Tuple, List, Dict, Any


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

VERSION = "v4.6.1-price-engine-earnings7d-yahoo-2026-01-08"


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


def yahoo_earnings_next(ticker: str) -> Tuple[Optional[int], Optional[str]]:
    """Return next earnings datetime (UTC epoch) from Yahoo quoteSummary calendarEvents."""
    t = ticker.strip().upper()
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{t}?modules=calendarEvents"
    j = http_json_retry(url, retries=3, base_sleep=0.6)
    qs = (j.get("quoteSummary") or {}).get("result") or []
    if not qs:
        return None, None
    cal = (qs[0].get("calendarEvents") or {})
    earnings = (cal.get("earnings") or {})
    ed = earnings.get("earningsDate")

    epoch: Optional[int] = None
    if isinstance(ed, list) and ed:
        raw = ed[0].get("raw") if isinstance(ed[0], dict) else None
        if raw is not None:
            try:
                epoch = int(raw)
            except Exception:
                epoch = None

    call_time = earnings.get("earningsCallTime")
    call_str: Optional[str] = None
    if isinstance(call_time, dict):
        call_str = call_time.get("fmt") or call_time.get("raw")
        if call_str is not None:
            call_str = str(call_str).upper()

    return epoch, call_str


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

    debug = {
        "now": now_epoch,
        "pre_start": pre_start, "pre_end": pre_end,
        "post_start": post_start, "post_end": post_end,
        "reg_start": reg_start, "reg_end": reg_end,
        "pre_source": None, "post_source": None,
        "base_prev": prev,
    }
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

    master_rows = load_master_rows(args.master)
    positions = [r for r in master_rows if (r.get("qty") is not None and r["qty"] > 0)]
    watchlist = [r for r in master_rows if r not in positions]

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

    # sorting: abs(AH) desc, then abs(PM)
    def _k(row):
        _, ah, pm = row
        return (abs(ah) if ah is not None else -1.0, abs(pm) if pm is not None else -1.0)

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

        # -------------------------------
        # 7D EARNINGS AUDIT (Yahoo calendarEvents)
        # -------------------------------
        f.write("\n## Közelgő katalizátorok (7 napon belüli earnings) — ellenőrzött\n\n")

        now_utc = int(time.time())
        horizon_utc = now_utc + 7 * 24 * 60 * 60

        tickers_all = [r["ticker"] for r in (positions + watchlist)]
        checked = 0
        hits = []
        missing = []

        for tkr in tickers_all:
            try:
                checked += 1
                ep, call_time = yahoo_earnings_next(tkr)
                if ep is None:
                    missing.append(tkr)
                    continue
                if ep <= horizon_utc:
                    hits.append((tkr, ep, call_time))
            except Exception:
                missing.append(tkr)
            time.sleep(0.15)

        hits.sort(key=lambda x: x[1])

        if hits:
            for tkr, ep, call_time in hits:
                when = fmt_dt_budapest(ep)
                suffix = f" ({call_time})" if call_time else ""
                f.write(f"- {tkr} — {when}{suffix}\n")
        else:
            f.write("- Nincs 7 napon belüli earnings a listában (ellenőrizve).\n")

        f.write(f"\nEarnings audit lefedettség: {checked}/{len(tickers_all)} ticker ellenőrizve; találat: {len(hits)} db\n")
        if missing:
            sample = ", ".join(missing[:25])
            more = "…" if len(missing) > 25 else ""
            f.write(f"Adat nem elérhető (kihagyva): {sample}{more}\n")


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
