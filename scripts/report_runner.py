#!/usr/bin/env python3
# report_runner.py — v4.0.4-price-engine-yahoohtml-fallback-2026-01-05
#
# PRICE ENGINE (#1): After-hours & Premarket % (AH/PM) + K küszöb + lefedettség
# - Primary: Yahoo Chart v8 (includePrePost=true)
# - Fallback: Yahoo Finance quote HTML (embedded JSON) — pre/post/prevClose
# - Dedupe tickers
# - TZ-safe (no tzdata required)
# - Report header includes: script version + GitHub run_id/run_attempt (ha elérhető)

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


VERSION = "v4.0.4-price-engine-yahoohtml-fallback-2026-01-05"


# -----------------------------
# TZ-safe time helpers
# -----------------------------

def _now_local() -> dt.datetime:
    return dt.datetime.now().astimezone()

def _tz_label(now: dt.datetime) -> str:
    return (now.tzname() or "LOCAL") + f" ({now.strftime('%z')})"


# -----------------------------
# Formatting helpers
# -----------------------------

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

def _parse_threshold_k(row: Dict[str, Any]) -> float:
    v = _get_first_key(row, ["K", "k", "K_%", "min_move", "min_move_pct"])
    f = _safe_float(v)
    return float(f) if (f is not None and f > 0) else 3.0


# -----------------------------
# HTTP helpers
# -----------------------------

def _http_get(url: str, timeout: int = 25) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()

def _http_get_json(url: str, timeout: int = 25) -> Dict[str, Any]:
    return json.loads(_http_get(url, timeout=timeout).decode("utf-8"))


# -----------------------------
# Yahoo sources
# -----------------------------

@dataclass
class PriceSlice:
    prev_close: Optional[float]
    pre: Optional[float]
    post: Optional[float]


def _yahoo_chart_v8(ticker: str) -> Dict[str, Any]:
    return _http_get_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?interval=1m&range=1d&includePrePost=true&events=div%7Csplit"
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
    try:
        res = payload.get("chart", {}).get("result", [])
        r0 = res[0] if res else {}
        meta = r0.get("meta", {}) if isinstance(r0, dict) else {}
        timestamps = r0.get("timestamp", []) or []
        indicators = r0.get("indicators", {}) or {}
        quotes = indicators.get("quote", []) or []
        q0 = quotes[0] if quotes else {}
        closes = q0.get("close", []) or []
        trading = meta.get("tradingPeriods", {}) or {}
    except Exception:
        meta, timestamps, closes, trading = {}, [], [], {}

    prev_close = _safe_float(meta.get("previousClose")) or _safe_float(meta.get("regularMarketPreviousClose"))
    pre_price = _safe_float(meta.get("preMarketPrice"))
    post_price = _safe_float(meta.get("postMarketPrice"))

    # Compute from tradingPeriods if possible
    try:
        pre0 = (trading.get("pre") or [None])[0]
        post0 = (trading.get("post") or [None])[0]
        if pre_price is None and isinstance(pre0, dict) and timestamps and closes:
            pre_price = _last_close_in_window(timestamps, closes, int(pre0["start"]), int(pre0["end"]))
        if post_price is None and isinstance(post0, dict) and timestamps and closes:
            post_price = _last_close_in_window(timestamps, closes, int(post0["start"]), int(post0["end"]))
    except Exception:
        pass

    return PriceSlice(prev_close=prev_close, pre=pre_price, post=post_price)


def _yahoo_quote_html_prices(ticker: str) -> PriceSlice:
    """
    HTML fallback: finance.yahoo.com quote oldal embedded JSON-ból próbáljuk kinyerni:
    - regularMarketPreviousClose
    - preMarketPrice
    - postMarketPrice
    """
    url = f"https://finance.yahoo.com/quote/{ticker}?p={ticker}"
    html = _http_get(url, timeout=25).decode("utf-8", errors="ignore")

    # Fast regexes (raw values)
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
    pre = grab([
        r'"preMarketPrice"\s*:\s*\{\s*"raw"\s*:\s*([0-9.]+)',
    ])
    post = grab([
        r'"postMarketPrice"\s*:\s*\{\s*"raw"\s*:\s*([0-9.]+)',
    ])

    return PriceSlice(prev_close=prev, pre=pre, post=post)


def _pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return (a / b - 1.0) * 100.0


@dataclass
class TickerMove:
    ticker: str
    qty: float
    k: float
    ah_pct: Optional[float]
    pm_pct: Optional[float]


def _fetch_prices(ticker: str, retries: int = 2, sleep_s: float = 0.6) -> PriceSlice:
    # 1) Chart v8
    last_err: Optional[Exception] = None
    for _ in range(retries + 1):
        try:
            ps = _extract_from_chart(_yahoo_chart_v8(ticker))
            # If we at least have prev_close, accept; pre/post may still be None.
            if ps.prev_close is not None:
                return ps
            return ps
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)

    # 2) HTML fallback
    for _ in range(retries + 1):
        try:
            return _yahoo_quote_html_prices(ticker)
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)

    raise last_err or RuntimeError("Unknown fetch error")


def _compute_moves(master_rows: List[Dict[str, Any]]) -> Tuple[List[TickerMove], List[str], int]:
    moves: List[TickerMove] = []
    missing: List[str] = []
    seen: set[str] = set()
    skipped_dupes = 0

    for row in master_rows:
        t = _norm_ticker(_get_first_key(row, ["ticker", "Ticker", "symbol", "Symbol"]) or "")
        if not t:
            continue
        if t in seen:
            skipped_dupes += 1
            continue
        seen.add(t)

        qty = _parse_qty(row)
        k = _parse_threshold_k(row)

        try:
            ps = _fetch_prices(t)
        except Exception:
            missing.append(t)
            moves.append(TickerMove(ticker=t, qty=qty, k=k, ah_pct=None, pm_pct=None))
            continue

        if ps.prev_close is None:
            missing.append(t)

        ah_pct = _pct_change(ps.post, ps.prev_close) if ps.post is not None else None
        pm_pct = _pct_change(ps.pre, ps.prev_close) if ps.pre is not None else None

        moves.append(TickerMove(ticker=t, qty=qty, k=k, ah_pct=ah_pct, pm_pct=pm_pct))

    return moves, sorted(list(set(missing))), skipped_dupes


def _split_positions_watchlist(moves: List[TickerMove]) -> Tuple[List[TickerMove], List[TickerMove]]:
    positions = [m for m in moves if m.qty and m.qty > 0]
    watch = [m for m in moves if not (m.qty and m.qty > 0)]
    positions.sort(key=lambda x: x.ticker)
    watch.sort(key=lambda x: x.ticker)
    return positions, watch


def _is_trigger(m: TickerMove) -> bool:
    k = float(m.k or 3.0)
    return (m.ah_pct is not None and abs(m.ah_pct) >= k) or (m.pm_pct is not None and abs(m.pm_pct) >= k)


def _render_report_1(moves: List[TickerMove], missing: List[str], skipped_dupes: int) -> str:
    now = _now_local()
    positions, watch = _split_positions_watchlist(moves)

    run_id = os.getenv("GITHUB_RUN_ID", "")
    run_attempt = os.getenv("GITHUB_RUN_ATTEMPT", "")
    run_tag = f"{run_id}/{run_attempt}" if run_id and run_attempt else (run_id or "n/a")

    if len(missing) == 0:
        cov = f"Lefedettség: TELJES — ellenőrizve: {len(moves)}/{len(moves)} ticker"
    else:
        cov = f"Lefedettség: HIÁNYOS — nem elérhető / hiányos adat: {', '.join(missing)} (ok: árfeed/forrás hiba)"

    lines: List[str] = []
    lines.append("# #1 — After-hours & Premarket (PRICE ENGINE)")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Run: {run_tag}")
    lines.append(f"Időbélyeg: {now.strftime('%Y-%m-%d %H:%M:%S')} {_tz_label(now)}")
    lines.append(cov)
    lines.append(f"Universe: positions={len(positions)}, watchlist={len(watch)}, total={len(moves)}")
    if skipped_dupes:
        lines.append(f"Duplikált tickerek kihagyva: {skipped_dupes}")
    lines.append("")
    lines.append("## 🎯 Megjegyzés")
    lines.append("Ez a riport SZÁNDÉKOSAN csak az AH/PM számokat és küszöb-triggereket tartalmazza. "
                 "Makró/FED/politika + bejelentések/elemzői lépések + katalizátorok + high-conv: manuális/webes elemzés.")
    lines.append("")

    lines.append("### 📊 Darabszámos tickerek — After-hours & Premarket mozgások (teljes lista)")
    if not positions:
        lines.append("Nincs darabszámos pozíció a MASTER-ben. (ellenőrizd a darabszám oszlop nevét a CSV-ben)")
    else:
        for m in positions:
            tag = " 🔔" if _is_trigger(m) else ""
            lines.append(f"- {m.ticker} — AH {_fmt_pct(m.ah_pct)} | PM {_fmt_pct(m.pm_pct)} — K={m.k:.2f}%{tag}")
    lines.append("")

    lines.append("### 👀 Watchlist — Küszöb feletti AH/PM mozgások (|%| ≥ K)")
    watch_trig = [m for m in watch if _is_trigger(m)]
    if not watch_trig:
        lines.append("Nincs küszöb feletti watchlist mozgás.")
    else:
        def key_abs(m: TickerMove) -> float:
            a = abs(m.pm_pct) if m.pm_pct is not None else 0.0
            b = abs(m.ah_pct) if m.ah_pct is not None else 0.0
            return max(a, b)
        watch_trig.sort(key=key_abs, reverse=True)
        for m in watch_trig:
            lines.append(f"- {m.ticker} — AH {_fmt_pct(m.ah_pct)} | PM {_fmt_pct(m.pm_pct)} — K={m.k:.2f}% 🔔")
    lines.append("")

    lines.append("### 📄 Watchlist — After-hours & Premarket mozgások (teljes lista)")
    if not watch:
        lines.append("Nincs watchlist ticker a MASTER-ben.")
    else:
        for m in watch:
            tag = " 🔔" if _is_trigger(m) else ""
            lines.append(f"- {m.ticker} — AH {_fmt_pct(m.ah_pct)} | PM {_fmt_pct(m.pm_pct)} — K={m.k:.2f}%{tag}")
    lines.append("")
    lines.append(f"Job summary generated at run-time ({now.strftime('%Y-%m-%dT%H:%M:%S%z')})")

    # Normalize newlines to \n (GitHub raw + JSON patch safe)
    return ("\n".join(lines) + "\n").replace("\r\n", "\n").replace("\r", "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1, choices=[1])
    ap.add_argument("--master", type=str, default="reports/master.csv")
    ap.add_argument("--out", type=str, default="reports/summary_report_1.md")
    args = ap.parse_args()

    if not os.path.exists(args.master):
        print(f"HIBA: MASTER CSV nem található: {args.master}", file=sys.stderr)
        return 2

    rows = _read_master_csv(args.master)
    moves, missing, skipped_dupes = _compute_moves(rows)

    md = _render_report_1(moves, missing, skipped_dupes)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write(md)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
