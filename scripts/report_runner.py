#!/usr/bin/env python3
# report_runner.py — v4.1.1-price-engine-debug-yahoo-2026-01-05
#
# DEBUG fókusz: lássuk, melyik Yahoo endpoint hasal el GitHub Actions-en.
#
# Forrás-sorrend (tickenként):
#  1) Yahoo Chart v8 (includePrePost=true)
#  2) Yahoo Quote v7 (batch, includePrePost=true)
#  3) Yahoo HTML quote (finance.yahoo.com/quote/...)
#
# Output:
#  - 1 lista (PM abs% szerint, ha van PM; különben AH abs% szerint)
#  - Reportban "Forrás-statisztika" + "Debug összefoglaló"
#  - Runner logban (stderr) tömör debug sorok, ha DEBUG=1 vagy --debug
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import urllib.request
import urllib.error


VERSION = "v4.1.1-price-engine-debug-yahoo-2026-01-05"


# -----------------------------
# Basics
# -----------------------------

def _now_local() -> dt.datetime:
    return dt.datetime.now().astimezone()

def _tz_label(now: dt.datetime) -> str:
    return (now.tzname() or "LOCAL") + f" ({now.strftime('%z')})"

def _fmt_pct(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "n/a"
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.2f}%"

def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace(",", ".")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def _abs_or_zero(x: Optional[float]) -> float:
    return abs(x) if x is not None else 0.0


# -----------------------------
# MASTER CSV
# -----------------------------

def _read_master_csv(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({(k or "").strip(): (v.strip() if isinstance(v, str) else v) for k, v in (r or {}).items()})
    return rows

def _norm_ticker(x: str) -> str:
    return (x or "").strip().upper()

def _get_first_key(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def _parse_qty(row: Dict[str, Any]) -> float:
    v = _get_first_key(row, [
        "quantity", "qty", "shares", "share", "position",
        "darab", "db", "darabszám", "darabszam", "mennyiség", "mennyiseg",
        "Darab", "DB", "Darabszám", "Darabszam", "Mennyiség", "Mennyiseg",
    ])
    f = _safe_float(v)
    return float(f) if f is not None else 0.0


# -----------------------------
# HTTP with DEBUG
# -----------------------------

def _is_debug_enabled(cli_debug: bool) -> bool:
    if cli_debug:
        return True
    return (os.getenv("DEBUG", "").strip() in ("1", "true", "TRUE", "yes", "YES"))

def _dbg(msg: str, enabled: bool) -> None:
    if enabled:
        print(msg, file=sys.stderr)

def _http_get(url: str, timeout: int, debug: bool, label: str) -> Tuple[int, bytes, str]:
    """
    Returns: (status_code, body_bytes, note)
    note: short reason (e.g. HTTPError/timeout)
    """
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "close",
        },
        method="GET",
    )
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            body = resp.read()
            dt_ms = int((time.time() - t0) * 1000)
            _dbg(f"[DEBUG] {label} OK {status} {len(body)}B {dt_ms}ms :: {url}", debug)
            return status, body, "OK"
    except urllib.error.HTTPError as e:
        try:
            body = e.read() or b""
        except Exception:
            body = b""
        dt_ms = int((time.time() - t0) * 1000)
        _dbg(f"[DEBUG] {label} HTTPError {e.code} {len(body)}B {dt_ms}ms :: {url}", debug)
        return int(getattr(e, "code", 0) or 0), body, f"HTTPError:{getattr(e,'code',None)}"
    except Exception as e:
        dt_ms = int((time.time() - t0) * 1000)
        _dbg(f"[DEBUG] {label} EXC {type(e).__name__} {dt_ms}ms :: {url}", debug)
        return 0, b"", f"EXC:{type(e).__name__}"

def _http_get_json(url: str, timeout: int, debug: bool, label: str) -> Tuple[int, Optional[Dict[str, Any]], str]:
    status, body, note = _http_get(url, timeout, debug, label)
    if not body:
        return status, None, note
    try:
        return status, json.loads(body.decode("utf-8", errors="replace")), note
    except Exception:
        # show small snippet to debug in log
        if debug:
            snippet = body[:200].decode("utf-8", errors="replace").replace("\n", " ")[:200]
            _dbg(f"[DEBUG] {label} JSON parse FAIL, snippet: {snippet}", debug)
        return status, None, "JSONParseFail"


# -----------------------------
# Yahoo sources
# -----------------------------

@dataclass
class PriceSlice:
    prev_close: Optional[float]
    pre: Optional[float]
    post: Optional[float]
    source: str

def _yahoo_chart_v8(ticker: str, debug: bool) -> Tuple[int, Optional[Dict[str, Any]], str]:
    return _http_get_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d&includePrePost=true",
        timeout=20,
        debug=debug,
        label=f"chart_v8:{ticker}",
    )

def _yahoo_quote_v7(symbols_csv: str, debug: bool) -> Tuple[int, Optional[Dict[str, Any]], str]:
    # batch endpoint
    return _http_get_json(
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_csv}&includePrePost=true",
        timeout=20,
        debug=debug,
        label=f"quote_v7:batch({min(len(symbols_csv),80)}c)",
    )

def _last_close_in_window(timestamps: List[int], closes: List[Any], start: int, end: int) -> Optional[float]:
    last: Optional[float] = None
    for ts, c in zip(timestamps, closes):
        if ts < start or ts > end:
            continue
        fv = _safe_float(c)
        if fv is not None:
            last = fv
    return last

def _extract_from_chart(payload: Dict[str, Any]) -> PriceSlice:
    res = (payload.get("chart") or {}).get("result") or []
    r0 = res[0] if res else {}
    meta = (r0.get("meta") or {}) if isinstance(r0, dict) else {}
    timestamps = r0.get("timestamp") or []
    indicators = r0.get("indicators") or {}
    quotes = indicators.get("quote") or []
    q0 = quotes[0] if quotes else {}
    closes = q0.get("close") or []
    trading = meta.get("tradingPeriods") or {}

    prev_close = _safe_float(meta.get("previousClose")) or _safe_float(meta.get("regularMarketPreviousClose"))
    pre_price = _safe_float(meta.get("preMarketPrice"))
    post_price = _safe_float(meta.get("postMarketPrice"))

    try:
        pre0 = (trading.get("pre") or [None])[0]
        post0 = (trading.get("post") or [None])[0]
        if pre_price is None and isinstance(pre0, dict) and timestamps and closes:
            pre_price = _last_close_in_window(timestamps, closes, int(pre0["start"]), int(pre0["end"]))
        if post_price is None and isinstance(post0, dict) and timestamps and closes:
            post_price = _last_close_in_window(timestamps, closes, int(post0["start"]), int(post0["end"]))
    except Exception:
        pass

    return PriceSlice(prev_close=prev_close, pre=pre_price, post=post_price, source="chart_v8")

def _extract_from_quote_v7(quote_payload: Dict[str, Any], ticker: str) -> PriceSlice:
    results = ((quote_payload.get("quoteResponse") or {}).get("result") or [])
    q = None
    for item in results:
        if (item.get("symbol") or "").upper() == ticker.upper():
            q = item
            break
    if not q:
        return PriceSlice(prev_close=None, pre=None, post=None, source="quote_v7")

    prev = _safe_float(q.get("regularMarketPreviousClose")) or _safe_float(q.get("regularMarketPrevClose"))
    pre = _safe_float(q.get("preMarketPrice"))
    post = _safe_float(q.get("postMarketPrice"))
    return PriceSlice(prev_close=prev, pre=pre, post=post, source="quote_v7")

def _yahoo_quote_html_prices(ticker: str, debug: bool) -> Tuple[PriceSlice, str]:
    url = f"https://finance.yahoo.com/quote/{ticker}?p={ticker}"
    status, body, note = _http_get(url, timeout=20, debug=debug, label=f"html:{ticker}")
    html = body.decode("utf-8", errors="replace") if body else ""
    # quick detection
    detect = ""
    if "consent" in html.lower() or "gdpr" in html.lower():
        detect = "CONSENT_WALL"
    elif "captcha" in html.lower():
        detect = "CAPTCHA"
    elif "sign in" in html.lower() and "account" in html.lower():
        detect = "LOGIN_WALL"

    def grab(patterns: List[str]) -> Optional[float]:
        for pat in patterns:
            m = re.search(pat, html)
            if m:
                return _safe_float(m.group(1))
        return None

    prev = grab([
        r'"regularMarketPreviousClose"\s*:\s*\{\s*"raw"\s*:\s*([0-9.]+)',
        r'"previousClose"\s*:\s*\{\s*"raw"\s*:\s*([0-9.]+)',
    ])
    pre = grab([r'"preMarketPrice"\s*:\s*\{\s*"raw"\s*:\s*([0-9.]+)'])
    post = grab([r'"postMarketPrice"\s*:\s*\{\s*"raw"\s*:\s*([0-9.]+)'])

    ps = PriceSlice(prev_close=prev, pre=pre, post=post, source=f"html({status})")
    tag = detect or note
    return ps, tag


# -----------------------------
# Calculations
# -----------------------------

def _pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1.0) * 100.0

@dataclass
class TickerMove:
    ticker: str
    qty: float
    ah_pct: Optional[float]
    pm_pct: Optional[float]
    src: str

@dataclass
class DebugAgg:
    chart_status_counts: Dict[int, int]
    quote_status: int
    html_status_counts: Dict[int, int]
    html_flags: Dict[str, int]
    notes: List[str]

def _compute_moves(master_rows: List[Dict[str, Any]], debug: bool) -> Tuple[List[TickerMove], List[str], int, Dict[str, int], DebugAgg]:
    moves: List[TickerMove] = []
    missing: List[str] = []
    seen: set[str] = set()
    skipped_dupes = 0

    tickers: List[str] = []
    rows_norm: List[Tuple[str, float]] = []

    for row in master_rows:
        t = _norm_ticker(_get_first_key(row, ["ticker", "Ticker", "symbol", "Symbol"]) or "")
        if not t:
            continue
        if t in seen:
            skipped_dupes += 1
            continue
        seen.add(t)
        qty = _parse_qty(row)
        tickers.append(t)
        rows_norm.append((t, qty))

    # Quote v7 batch once
    quote_payload = None
    quote_status = 0
    quote_note = ""
    if tickers:
        quote_status, quote_payload, quote_note = _yahoo_quote_v7(",".join(tickers), debug=debug)
        if quote_payload is None:
            _dbg(f"[DEBUG] quote_v7 batch payload missing (status={quote_status}, note={quote_note})", debug)

    status_map: Dict[str, int] = {"chart_v8": 0, "quote_v7": 0, "html": 0, "none": 0}
    dbg = DebugAgg(
        chart_status_counts={},
        quote_status=int(quote_status or 0),
        html_status_counts={},
        html_flags={},
        notes=[],
    )

    for t, qty in rows_norm:
        # 1) chart v8
        ps: PriceSlice = PriceSlice(prev_close=None, pre=None, post=None, source="none")
        chart_status, chart_payload, chart_note = _yahoo_chart_v8(t, debug=debug)
        dbg.chart_status_counts[chart_status] = dbg.chart_status_counts.get(chart_status, 0) + 1

        if chart_payload is not None:
            try:
                ps = _extract_from_chart(chart_payload)
            except Exception:
                ps = PriceSlice(prev_close=None, pre=None, post=None, source="none")
        if ps.prev_close is not None:
            status_map["chart_v8"] += 1
        else:
            # 2) quote v7
            if quote_payload is not None:
                ps2 = _extract_from_quote_v7(quote_payload, t)
                if ps2.prev_close is not None:
                    ps = ps2
                    status_map["quote_v7"] += 1
                else:
                    # 3) html
                    ps3, flag = _yahoo_quote_html_prices(t, debug=debug)
                    ps = ps3
                    status_map["html"] += 1 if ps.prev_close is not None else 0
                    st = 0
                    try:
                        m = re.match(r"html\((\d+)\)", ps.source)
                        st = int(m.group(1)) if m else 0
                    except Exception:
                        st = 0
                    dbg.html_status_counts[st] = dbg.html_status_counts.get(st, 0) + 1
                    if flag:
                        dbg.html_flags[flag] = dbg.html_flags.get(flag, 0) + 1

        if ps.prev_close is None:
            status_map["none"] += 1
            missing.append(t)

        ah = _pct_change(ps.post, ps.prev_close) if (ps.post is not None and ps.prev_close is not None) else None
        pm = _pct_change(ps.pre, ps.prev_close) if (ps.pre is not None and ps.prev_close is not None) else None
        moves.append(TickerMove(ticker=t, qty=qty, ah_pct=ah, pm_pct=pm, src=ps.source))

    return moves, sorted(list(set(missing))), skipped_dupes, status_map, dbg


def _sort_single_list(moves: List[TickerMove]) -> Tuple[str, List[TickerMove]]:
    any_pm = any(m.pm_pct is not None for m in moves)
    if any_pm:
        return "PM", sorted(moves, key=lambda m: (_abs_or_zero(m.pm_pct), _abs_or_zero(m.ah_pct), m.ticker), reverse=True)
    return "AH", sorted(moves, key=lambda m: (_abs_or_zero(m.ah_pct), _abs_or_zero(m.pm_pct), m.ticker), reverse=True)


def _render_report(moves: List[TickerMove], missing: List[str], skipped_dupes: int, status_map: Dict[str, int], dbg: DebugAgg) -> str:
    now = _now_local()
    run_id = os.getenv("GITHUB_RUN_ID", "")
    run_attempt = os.getenv("GITHUB_RUN_ATTEMPT", "")
    run_tag = f"{run_id}/{run_attempt}" if run_id and run_attempt else (run_id or "n/a")

    if len(missing) == 0:
        cov = f"Lefedettség: TELJES — ellenőrizve: {len(moves)}/{len(moves)} ticker"
    else:
        cov = f"Lefedettség: HIÁNYOS — hiányos prevClose/adat: {', '.join(missing)} (ok: árfeed/forrás hiba)"

    mode, sorted_moves = _sort_single_list(moves)
    all_na = all((m.pm_pct is None and m.ah_pct is None) for m in moves)

    def fmt_counts(d: Dict[int, int]) -> str:
        items = sorted(d.items(), key=lambda x: x[0])
        return ", ".join([f"{k}:{v}" for k, v in items]) if items else "n/a"

    def fmt_str_counts(d: Dict[str, int]) -> str:
        items = sorted(d.items(), key=lambda x: (-x[1], x[0]))
        return ", ".join([f"{k}:{v}" for k, v in items]) if items else "n/a"

    lines: List[str] = []
    lines.append("# #1 — Premarket check (PRICE ENGINE)")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Run: {run_tag}")
    lines.append(f"Időbélyeg: {now.strftime('%Y-%m-%d %H:%M:%S')} {_tz_label(now)}")
    lines.append(cov)
    lines.append(f"Universe: total={len(moves)} (duplikált kihagyva: {skipped_dupes})")
    lines.append(f"Forrás-statisztika: chart_v8_ok={status_map.get('chart_v8',0)}, quote_v7_ok={status_map.get('quote_v7',0)}, html_ok={status_map.get('html',0)}, none={status_map.get('none',0)}")
    lines.append("")
    lines.append(f"## Lista — {mode} abs% szerint rendezve (1 lista, kész)")
    lines.append("")
    for m in sorted_moves:
        lines.append(f"- {m.ticker} — PM {_fmt_pct(m.pm_pct)} | AH {_fmt_pct(m.ah_pct)}")

    lines.append("")
    lines.append("## Debug összefoglaló (Yahoo)")
    lines.append(f"- Chart v8 HTTP státuszok: {fmt_counts(dbg.chart_status_counts)}")
    lines.append(f"- Quote v7 batch HTTP státusz: {dbg.quote_status or 'n/a'}")
    lines.append(f"- HTML HTTP státuszok: {fmt_counts(dbg.html_status_counts)}")
    lines.append(f"- HTML fal/flag detekt: {fmt_str_counts(dbg.html_flags)}")
    lines.append("")
    if all_na:
        lines.append("## ⚠️ FIGYELEM — minden érték n/a")
        lines.append("Ez tipikusan Yahoo blokkolás / rate-limit / cookie wall / geo issue a GitHub runneren.")
        lines.append("A debug sorokból látni fogod: 401/403/429/999 vagy consent/login/captcha jelzések.")
        lines.append("")
    lines.append(f"Job summary generated at run-time ({now.strftime('%Y-%m-%dT%H:%M:%S%z')})")
    return ("\n".join(lines) + "\n").replace("\r\n", "\n").replace("\r", "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1, choices=[1])
    ap.add_argument("--master", type=str, default="reports/master.csv")
    ap.add_argument("--out", type=str, default="reports/summary_report_1.md")
    ap.add_argument("--debug", action="store_true", help="Extra debug a runner logban (stderr).")
    args = ap.parse_args()

    debug = _is_debug_enabled(args.debug)

    if not os.path.exists(args.master):
        print(f"HIBA: MASTER CSV nem található: {args.master}", file=sys.stderr)
        return 2

    rows = _read_master_csv(args.master)

    try:
        moves, missing, skipped_dupes, status_map, dbg = _compute_moves(rows, debug=debug)
    except Exception as e:
        # hard crash guard (so we see the exception in logs)
        print(f"FATAL: report_runner crash: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    md = _render_report(moves, missing, skipped_dupes, status_map, dbg)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write(md)

    all_na = all((m.pm_pct is None and m.ah_pct is None) for m in moves)
    if all_na:
        return 5
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
