#!/usr/bin/env python3
# report_runner.py — v4.1.2-price-engine-nodataclasses-trace-2026-01-05
#
# Fix: ne legyen "néma exit 1". Mindig írunk traceback-et és okot stderr-re.
# Fix: dataclasses teljesen kiiktatva (GitHub runneren láttunk furcsa dataclass import/exec edge-case-t).
#
# Forrás-sorrend (tickenként):
#  1) Yahoo Chart v8 (includePrePost=true)
#  2) Yahoo Quote v7 (batch, includePrePost=true)
#  3) Yahoo HTML quote (finance.yahoo.com/quote/...)
#
# Output:
#  - 1 lista (PM abs% szerint, ha van PM; különben AH abs% szerint)
#  - Reportban forrás-statisztika + debug összefoglaló
#
# Exit code:
#  0 = OK
#  5 = minden ticker n/a (Yahoo/forrás blokk)
#  1/2/3/... = valódi hiba (mindig logoljuk)
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
import traceback
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


VERSION = "v4.1.2-price-engine-nodataclasses-trace-2026-01-05"


# -----------------------------
# Helpers
# -----------------------------

def now_local() -> dt.datetime:
    return dt.datetime.now().astimezone()

def tz_label(now: dt.datetime) -> str:
    return (now.tzname() or "LOCAL") + f" ({now.strftime('%z')})"

def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace(",", ".")
        if not s:
            return None
        return float(s)
    except Exception:
        return None

def fmt_pct(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "n/a"
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.2f}%"

def abs_or_zero(x: Optional[float]) -> float:
    return abs(x) if x is not None else 0.0

def pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1.0) * 100.0

def debug_enabled(cli_debug: bool) -> bool:
    if cli_debug:
        return True
    return os.getenv("DEBUG", "").strip().lower() in ("1", "true", "yes", "y")

def dbg(msg: str, enabled: bool) -> None:
    if enabled:
        print(msg, file=sys.stderr, flush=True)


# -----------------------------
# MASTER CSV
# -----------------------------

def read_master_csv(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({(k or "").strip(): (v.strip() if isinstance(v, str) else v) for k, v in (r or {}).items()})
    return rows

def norm_ticker(x: str) -> str:
    return (x or "").strip().upper()

def get_first(row: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return None

def parse_qty(row: Dict[str, Any]) -> float:
    v = get_first(row, [
        "quantity", "qty", "shares", "share", "position",
        "darab", "db", "darabszám", "darabszam", "mennyiség", "mennyiseg",
        "Darab", "DB", "Darabszám", "Darabszam", "Mennyiség", "Mennyiseg",
    ])
    f = safe_float(v)
    return float(f) if f is not None else 0.0


# -----------------------------
# HTTP
# -----------------------------

def http_get(url: str, timeout: int, debug: bool, label: str) -> Tuple[int, bytes, str, int]:
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
            ms = int((time.time() - t0) * 1000)
            dbg(f"[DEBUG] {label} OK {status} {len(body)}B {ms}ms", debug)
            return int(status), body, "OK", ms
    except urllib.error.HTTPError as e:
        try:
            body = e.read() or b""
        except Exception:
            body = b""
        ms = int((time.time() - t0) * 1000)
        dbg(f"[DEBUG] {label} HTTPError {e.code} {len(body)}B {ms}ms", debug)
        return int(getattr(e, "code", 0) or 0), body, f"HTTPError:{getattr(e,'code',None)}", ms
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        dbg(f"[DEBUG] {label} EXC {type(e).__name__} {ms}ms", debug)
        return 0, b"", f"EXC:{type(e).__name__}", ms

def http_get_json(url: str, timeout: int, debug: bool, label: str) -> Tuple[int, Optional[Dict[str, Any]], str]:
    status, body, note, _ = http_get(url, timeout, debug, label)
    if not body:
        return status, None, note
    try:
        return status, json.loads(body.decode("utf-8", errors="replace")), note
    except Exception:
        if debug:
            snippet = body[:200].decode("utf-8", errors="replace").replace("\n", " ")[:200]
            dbg(f"[DEBUG] {label} JSON parse FAIL snippet: {snippet}", debug)
        return status, None, "JSONParseFail"


# -----------------------------
# Yahoo sources
# -----------------------------

def yahoo_chart_v8(ticker: str, debug: bool) -> Tuple[int, Optional[Dict[str, Any]], str]:
    return http_get_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d&includePrePost=true",
        timeout=20,
        debug=debug,
        label=f"chart_v8:{ticker}",
    )

def yahoo_quote_v7(symbols_csv: str, debug: bool) -> Tuple[int, Optional[Dict[str, Any]], str]:
    return http_get_json(
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_csv}&includePrePost=true",
        timeout=20,
        debug=debug,
        label=f"quote_v7:batch({min(len(symbols_csv),80)}c)",
    )

def extract_from_chart(payload: Dict[str, Any]) -> Dict[str, Optional[float]]:
    res = (payload.get("chart") or {}).get("result") or []
    r0 = res[0] if res else {}
    meta = (r0.get("meta") or {}) if isinstance(r0, dict) else {}

    prev = safe_float(meta.get("previousClose")) or safe_float(meta.get("regularMarketPreviousClose"))
    pre = safe_float(meta.get("preMarketPrice"))
    post = safe_float(meta.get("postMarketPrice"))
    return {"prev": prev, "pre": pre, "post": post, "src": "chart_v8"}

def extract_from_quote_v7(quote_payload: Dict[str, Any], ticker: str) -> Dict[str, Optional[float]]:
    results = ((quote_payload.get("quoteResponse") or {}).get("result") or [])
    q = None
    tU = ticker.upper()
    for item in results:
        if (item.get("symbol") or "").upper() == tU:
            q = item
            break
    if not q:
        return {"prev": None, "pre": None, "post": None, "src": "quote_v7"}
    prev = safe_float(q.get("regularMarketPreviousClose")) or safe_float(q.get("regularMarketPrevClose"))
    pre = safe_float(q.get("preMarketPrice"))
    post = safe_float(q.get("postMarketPrice"))
    return {"prev": prev, "pre": pre, "post": post, "src": "quote_v7"}

def yahoo_html_prices(ticker: str, debug: bool) -> Tuple[Dict[str, Optional[float]], str, int]:
    url = f"https://finance.yahoo.com/quote/{ticker}?p={ticker}"
    status, body, note, _ = http_get(url, timeout=20, debug=debug, label=f"html:{ticker}")
    html = body.decode("utf-8", errors="replace") if body else ""
    detect = ""
    low = html.lower()
    if "consent" in low or "gdpr" in low:
        detect = "CONSENT_WALL"
    elif "captcha" in low:
        detect = "CAPTCHA"
    elif "sign in" in low and "account" in low:
        detect = "LOGIN_WALL"

    def grab(pats: List[str]) -> Optional[float]:
        for pat in pats:
            m = re.search(pat, html)
            if m:
                return safe_float(m.group(1))
        return None

    prev = grab([
        r'"regularMarketPreviousClose"\s*:\s*\{\s*"raw"\s*:\s*([0-9.]+)',
        r'"previousClose"\s*:\s*\{\s*"raw"\s*:\s*([0-9.]+)',
    ])
    pre = grab([r'"preMarketPrice"\s*:\s*\{\s*"raw"\s*:\s*([0-9.]+)'])
    post = grab([r'"postMarketPrice"\s*:\s*\{\s*"raw"\s*:\s*([0-9.]+)'])

    return {"prev": prev, "pre": pre, "post": post, "src": f"html({status})"}, (detect or note), int(status or 0)


# -----------------------------
# Core
# -----------------------------

def compute_moves(rows: List[Dict[str, Any]], debug: bool) -> Tuple[List[Dict[str, Any]], List[str], int, Dict[str, int], Dict[str, Any]]:
    seen = set()
    tickers: List[str] = []
    pairs: List[Tuple[str, float]] = []
    skipped_dupes = 0

    for row in rows:
        t = norm_ticker(get_first(row, ["ticker", "Ticker", "symbol", "Symbol"]) or "")
        if not t:
            continue
        if t in seen:
            skipped_dupes += 1
            continue
        seen.add(t)
        qty = parse_qty(row)
        tickers.append(t)
        pairs.append((t, qty))

    quote_payload = None
    quote_status = 0
    quote_note = ""
    if tickers:
        quote_status, quote_payload, quote_note = yahoo_quote_v7(",".join(tickers), debug=debug)
        if quote_payload is None:
            dbg(f"[DEBUG] quote_v7 batch missing payload (status={quote_status}, note={quote_note})", debug)

    status_map = {"chart_v8_ok": 0, "quote_v7_ok": 0, "html_ok": 0, "none": 0}
    chart_status_counts: Dict[int, int] = {}
    html_status_counts: Dict[int, int] = {}
    html_flags: Dict[str, int] = {}

    moves: List[Dict[str, Any]] = []
    missing: List[str] = []

    for t, qty in pairs:
        # 1) chart
        cs, cp, _ = yahoo_chart_v8(t, debug=debug)
        chart_status_counts[cs] = chart_status_counts.get(cs, 0) + 1
        ps = {"prev": None, "pre": None, "post": None, "src": "none"}

        if cp is not None:
            try:
                ps = extract_from_chart(cp)
            except Exception:
                ps = {"prev": None, "pre": None, "post": None, "src": "none"}

        if ps["prev"] is not None:
            status_map["chart_v8_ok"] += 1
        else:
            # 2) quote v7
            if quote_payload is not None:
                ps2 = extract_from_quote_v7(quote_payload, t)
                if ps2["prev"] is not None:
                    ps = ps2
                    status_map["quote_v7_ok"] += 1
                else:
                    # 3) html
                    ps3, flag, hs = yahoo_html_prices(t, debug=debug)
                    ps = ps3
                    if ps["prev"] is not None:
                        status_map["html_ok"] += 1
                    html_status_counts[hs] = html_status_counts.get(hs, 0) + 1
                    if flag:
                        html_flags[flag] = html_flags.get(flag, 0) + 1

        if ps["prev"] is None:
            status_map["none"] += 1
            missing.append(t)

        ah = pct_change(ps.get("post"), ps.get("prev"))
        pm = pct_change(ps.get("pre"), ps.get("prev"))
        moves.append({"ticker": t, "qty": qty, "ah": ah, "pm": pm, "src": ps.get("src")})

    dbg_info = {
        "quote_status": quote_status,
        "chart_status_counts": chart_status_counts,
        "html_status_counts": html_status_counts,
        "html_flags": html_flags,
    }
    return moves, sorted(list(set(missing))), skipped_dupes, status_map, dbg_info

def sort_single_list(moves: List[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
    any_pm = any(m.get("pm") is not None for m in moves)
    if any_pm:
        return "PM", sorted(moves, key=lambda m: (abs_or_zero(m.get("pm")), abs_or_zero(m.get("ah")), m.get("ticker")), reverse=True)
    return "AH", sorted(moves, key=lambda m: (abs_or_zero(m.get("ah")), abs_or_zero(m.get("pm")), m.get("ticker")), reverse=True)

def fmt_counts_int(d: Dict[int, int]) -> str:
    items = sorted(d.items(), key=lambda x: x[0])
    return ", ".join([f"{k}:{v}" for k, v in items]) if items else "n/a"

def fmt_counts_str(d: Dict[str, int]) -> str:
    items = sorted(d.items(), key=lambda x: (-x[1], x[0]))
    return ", ".join([f"{k}:{v}" for k, v in items]) if items else "n/a"

def render_report(moves: List[Dict[str, Any]], missing: List[str], skipped_dupes: int, status_map: Dict[str, int], dbg_info: Dict[str, Any]) -> str:
    now = now_local()
    run_id = os.getenv("GITHUB_RUN_ID", "")
    run_attempt = os.getenv("GITHUB_RUN_ATTEMPT", "")
    run_tag = f"{run_id}/{run_attempt}" if run_id and run_attempt else (run_id or "n/a")

    if not missing:
        cov = f"Lefedettség: TELJES — ellenőrizve: {len(moves)}/{len(moves)} ticker"
    else:
        cov = f"Lefedettség: HIÁNYOS — hiányos prevClose/adat: {', '.join(missing)} (ok: árfeed/forrás hiba)"

    mode, sorted_moves = sort_single_list(moves)
    all_na = all((m.get("pm") is None and m.get("ah") is None) for m in moves)

    lines: List[str] = []
    lines.append("# #1 — Premarket check (PRICE ENGINE)")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Run: {run_tag}")
    lines.append(f"Időbélyeg: {now.strftime('%Y-%m-%d %H:%M:%S')} {tz_label(now)}")
    lines.append(cov)
    lines.append(f"Universe: total={len(moves)} (duplikált kihagyva: {skipped_dupes})")
    lines.append(f"Forrás-statisztika: chart_v8_ok={status_map.get('chart_v8_ok',0)}, quote_v7_ok={status_map.get('quote_v7_ok',0)}, html_ok={status_map.get('html_ok',0)}, none={status_map.get('none',0)}")
    lines.append("")
    lines.append(f"## Lista — {mode} abs% szerint rendezve (1 lista)")
    lines.append("")
    for m in sorted_moves:
        lines.append(f"- {m['ticker']} — PM {fmt_pct(m.get('pm'))} | AH {fmt_pct(m.get('ah'))}")

    lines.append("")
    lines.append("## Debug összefoglaló (Yahoo)")
    lines.append(f"- Chart v8 HTTP státuszok: {fmt_counts_int(dbg_info.get('chart_status_counts',{}))}")
    lines.append(f"- Quote v7 batch HTTP státusz: {dbg_info.get('quote_status','n/a')}")
    lines.append(f"- HTML HTTP státuszok: {fmt_counts_int(dbg_info.get('html_status_counts',{}))}")
    lines.append(f"- HTML fal/flag detekt: {fmt_counts_str(dbg_info.get('html_flags',{}))}")
    lines.append("")
    if all_na:
        lines.append("## ⚠️ FIGYELEM — minden érték n/a")
        lines.append("Ez tipikusan Yahoo blokkolás / rate-limit / cookie wall / geo issue a GitHub runneren.")
        lines.append("")
    lines.append(f"Job summary generated at run-time ({now.strftime('%Y-%m-%dT%H:%M:%S%z')})")
    return ("\n".join(lines) + "\n").replace("\r\n", "\n").replace("\r", "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1, choices=[1])
    ap.add_argument("--master", type=str, default="reports/master.csv")
    ap.add_argument("--out", type=str, default="reports/summary_report_1.md")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    debug = debug_enabled(args.debug)

    if not os.path.exists(args.master):
        print(f"HIBA: MASTER CSV nem található: {args.master}", file=sys.stderr, flush=True)
        return 2

    rows = read_master_csv(args.master)
    moves, missing, skipped_dupes, status_map, dbg_info = compute_moves(rows, debug=debug)

    md = render_report(moves, missing, skipped_dupes, status_map, dbg_info)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write(md)

    all_na = all((m.get("pm") is None and m.get("ah") is None) for m in moves)
    if all_na:
        return 5
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        print("FATAL: report_runner crash (traceback lent):", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise SystemExit(1)
