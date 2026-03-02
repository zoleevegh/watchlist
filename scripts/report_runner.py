#!/usr/bin/env python3
# report_runner.py — v4.6.18-price-engine-pm-quotev7-fixshadow-2026-03-02
#
# FIX / CÉL:
# - Stabil #1 riport: AM elöl, PM utána, külön blokkban: Pozíciók (Darabszam>0), majd Watchlist.
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

# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.
VERSION = "v4.6.18-price-engine-pm-quotev7-fixshadow-2026-03-02"

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
        "Bázis: előző hivatalos záróár (Previous Close)\n"
        "Ablakok: AM 22:00–10:00 CET/CEST (rögzítve 10:00-kor) | PM 10:00–15:30 CET/CEST\n"
        f"Időintervallum (ellenőrzés): {interval_start} – {interval_end}\n\n"
    )
    f.write(header)




def _budapest_windows(now_epoch: int, last_regular_market_time: int | None = None):
    """Return fixed report windows in UTC epoch seconds for Europe/Budapest.

    #1 spec (local, Budapest time):
      AM: yesterday 22:00 -> today 10:00   (extended session BEFORE premarket)
      PM: today 10:00 -> today 15:30      (premarket; only if already started)

    IMPORTANT:
      - Window boundaries must be anchored to *calendar time* (Budapest), not to the last regular
        market close timestamp. Otherwise Monday/holiday runs incorrectly anchor PM to Friday.
      - We still use Yahoo's last regular close timestamp elsewhere to pick the correct *prev close*,
        but NOT to define today's PM window.
    """
    try:
        from zoneinfo import ZoneInfo  # py3.9+
        tz = ZoneInfo("Europe/Budapest")
    except Exception:
        tz = datetime.timezone(datetime.timedelta(hours=1))  # best-effort CET fallback

    now_utc = datetime.datetime.fromtimestamp(now_epoch, tz=datetime.timezone.utc)
    now_local = now_utc.astimezone(tz)

    today = now_local.date()
    yesterday = today - datetime.timedelta(days=1)

    am_start_local = datetime.datetime.combine(yesterday, datetime.time(22, 0), tzinfo=tz)
    am_end_local = datetime.datetime.combine(today, datetime.time(10, 0), tzinfo=tz)

    pm_start_local = am_end_local
    pm_end_local = datetime.datetime.combine(today, datetime.time(15, 30), tzinfo=tz)

    am_start = int(am_start_local.astimezone(datetime.timezone.utc).timestamp())
    am_end = int(am_end_local.astimezone(datetime.timezone.utc).timestamp())
    pm_start = int(pm_start_local.astimezone(datetime.timezone.utc).timestamp())
    pm_end = int(pm_end_local.astimezone(datetime.timezone.utc).timestamp())

    now_local_iso = now_local.strftime("%Y-%m-%d %H:%M")
    close_day_iso = am_start_local.strftime("%Y-%m-%d")

    return pm_start, pm_end, am_start, am_end, now_local_iso, close_day_iso
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
    """Compute #1 session prices for a ticker using Yahoo chart v8 (includePrePost=true).

    Returns: (prev_close, pm_price, am_price, debug)

    - prev_close: official previous close (regularMarketPreviousClose/previousClose)
    - AM: last extended price from 22:00 (close-day) to 10:00 local (Europe/Budapest),
          i.e. after-hours + overnight *before* premarket.
    - PM: last price inside 10:00–15:30 local (premarket) if already started; else None.
    """
    debug: Dict[str, Any] = {}
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=2d&includePrePost=true"
    j = http_json(url)
    res = (j.get("chart") or {}).get("result") or []
    if not res:
        raise ValueError("no chart result")
    res0 = res[0]
    meta = res0.get("meta", {}) or {}

    # Prefer official close as base (Previous Close)
    prev = meta.get("regularMarketPreviousClose")
    if prev is None:
        prev = meta.get("previousClose")
    # If v7 quote provides a better previous close, prefer it
    if v7_quote:
        q_prev = v7_quote.get("regularMarketPreviousClose")
        if q_prev is None:
            q_prev = v7_quote.get("previousClose")
        if q_prev not in (None, "", 0):
            prev = q_prev
    prev = float(prev) if prev not in (None, "", 0) else None

    timestamps = res0.get("timestamp") or []
    chart_quote = (((res0.get("indicators") or {}).get("quote") or [{}])[0]) or {}
    closes = chart_quote.get("close") or []

    # Windows
    last_rmt = meta.get("regularMarketTime")
    pm_start, pm_end, am_start, am_end, now_local_iso, close_day_iso = _budapest_windows(now_epoch, last_rmt)
    debug.update({
        "close_day_local": close_day_iso,
        "now_local": now_local_iso,
        "am_start": am_start,
        "am_end": am_end,
        "pm_start": pm_start,
        "pm_end": pm_end,
    })

    # AM (always attempt): last close within AM window; if now is before AM start, return None.
    if now_epoch < am_start:
        am = None
        debug["am_source"] = "future_window"
    else:
        am_window_end = min(now_epoch, am_end)
        am = _last_close_in_window(timestamps, closes, am_start, am_window_end) if (timestamps and closes) else None
        debug["am_source"] = "infer_chart" if am is not None else "none_chart"

    # PM (only when started) — prefer Yahoo v7 quote preMarketPrice (matches UI better)
    if now_epoch < pm_start:
        pm = None
        debug["pm_source"] = "future_window"
    else:
        pm_window_end = min(now_epoch, pm_end)
        pm = None
        # Prefer v7 quote fields (matches Yahoo UI better) — do NOT shadow this with chart "quote".
        if v7_quote:
            q_pmp = v7_quote.get("preMarketPrice")
            q_pmt = v7_quote.get("preMarketTime")
            # Only accept if the quote is actually a premarket quote inside the PM window.
            if q_pmp not in (None, "", 0) and q_pmt:
                try:
                    q_pmt_i = int(q_pmt)
                except Exception:
                    q_pmt_i = None
                if q_pmt_i is not None and pm_start <= q_pmt_i <= pm_window_end:
                    pm = float(q_pmp)
                    debug["pm_source"] = "quote_v7"
                    debug["pm_time"] = q_pmt_i
        if pm is None:
            pm = _last_close_in_window(timestamps, closes, pm_start, pm_window_end) if (timestamps and closes) else None
            debug["pm_source"] = "infer_chart" if pm is not None else "none_chart"

    return prev, pm, am, debug
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    now_epoch = int(time.time())

    master_rows = load_master_rows(args.master)

    # Batch Yahoo v7 quote lookup (for live preMarketPrice)
    tickers_all = [r['ticker'] for r in master_rows]
    quote_map = {}
    try:
        quote_map = yahoo_quote_batch(tickers_all + ['SPY'])
    except Exception:
        quote_map = {}
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
    sample_dbg: List[Tuple[str, Dict[str, Any], Optional[float], Optional[float]]] = []

    # Snapshot for post-processing (SellRef patch): store the actual session prices
    # already derived from Yahoo chart during the runner, so the patch step does not
    # need to hit Yahoo again (avoids 401/challenge).
    price_snapshot: Dict[str, Dict[str, Any]] = {}

    def handle_one(t: str):
        nonlocal sample_dbg
        prev, pm_price, am_price, dbg = chart_prices(t, now_epoch, quote_map.get(t))

        pm = pct(pm_price, prev)
        am = pct(am_price, prev)

        price_snapshot[t] = {
            "prev_close": prev,
            "pm_price": pm_price,
            "am_price": am_price,
        }

        # Keep a small sample for debugging output (first 10 tickers).
        if len(sample_dbg) < 10:
            # store as (ticker, debug, pm, am) to avoid name confusion in the report
            sample_dbg.append((t, dbg, pm, am))

        return pm, am, dbg



    for r in positions:
        t = r["ticker"]
        try:
            pm, am, dbg = handle_one(t)
            out_rows_pos.append((t, am, pm))
        except Exception:
            dbg_counts["errors"] += 1
            out_rows_pos.append((t, None, None))

    for r in watchlist:
        t = r["ticker"]
        try:
            pm, am, dbg = handle_one(t)
            out_rows_wl.append((t, am, pm))
        except Exception:
            dbg_counts["errors"] += 1
            out_rows_wl.append((t, None, None))

    # sorting (WEBBIBLIA request): value-desc, using PM if available, else AH.
    # - Primary: PM when not n/a, otherwise AH
    # - Secondary (tie-break): the other field (AH/PM)
    # - n/a rows go to the bottom

    def _k(row):
        _, am, pm = row
        primary = pm if pm is not None else am
        secondary = am if pm is not None else pm
        if primary is None:
            return (0, -1e9, -1e9)
        return (1, float(primary), float(secondary) if secondary is not None else -1e9)

    out_rows_pos.sort(key=_k, reverse=True)
    out_rows_wl.sort(key=_k, reverse=True)

    # Determine if we have any data at all
    any_data = any((am is not None or pm is not None) for _, am, pm in (out_rows_pos + out_rows_wl))

    # ---- Header interval (global) ----
    # Anchor the displayed interval to the last real US close using a liquid proxy (SPY),
    # so Monday mornings / holidays are correct.
    interval_start = None
    interval_end = None
    try:
        _p, _pre, _post, _dbg = chart_prices("SPY", now_epoch, quote_map.get("SPY"))
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

        # Label: 10:00 után az AM egy "befagyasztott" érték (22:00->10:00), hogy ne keveredjen a PM-mel.
        try:
            pm_start, pm_end, am_start, am_end, _now_local_iso, _close_day_iso = _budapest_windows(now_epoch)
            am_label = "AM" if now_epoch < pm_start else "AM(10:00-ig)"
        except Exception:
            am_label = "AM"

        f.write("## Pozíciók\n\n")
        for t, am, pm in out_rows_pos:
            f.write(f"- {t} — {am_label} {fmt(am)} | PM {fmt(pm)}\n")

        f.write("\n## Watchlist\n\n")
        for t, am, pm in out_rows_wl:
            f.write(f"- {t} — {am_label} {fmt(am)} | PM {fmt(pm)}\n")
    
        # Debug csak akkor, ha teljesen üres
        if (not any_data) or args.debug:
            f.write("\n## Debug (only if no data / --debug)\n")
            f.write(f"- now_epoch: {now_epoch}\n")
            for k in ["pre_meta","pre_infer","pre_gated","pre_none","post_meta","post_infer","post_carry","post_gated","post_none","errors"]:
                f.write(f"- {k}: {dbg_counts[k]}\n")
            f.write("\n### Debug sample (first 10 tickers)\n")
            for t, dbg, pm_val, am_val in sample_dbg:
                f.write(
                    f"- {t}: {am_label} {fmt(am_val)} (am_source={dbg.get('am_source')}, am_start={dbg.get('am_start')}, am_end={dbg.get('am_end')}) | "
                    f"PM {fmt(pm_val)} (pm_source={dbg.get('pm_source')}, pm_start={dbg.get('pm_start')}, pm_end={dbg.get('pm_end')})\n"
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
