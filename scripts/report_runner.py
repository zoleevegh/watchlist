#!/usr/bin/env python3
# report_runner.py — v4.0.2-price-engine-chartv8-2026-01-05
# PRICE ENGINE: AH/PM % + küszöb + lefedettség
# FIX: A /v7/finance/quote GitHub Actions alatt gyakran üres/korlátozott.
#      Átváltunk Yahoo Chart v8 endpoint-ra (includePrePost=true), ami a régi "v8" logikához áll közelebb.

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import urllib.request
import urllib.error


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
            rows.append({k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items()})
    return rows

def _norm_ticker(x: str) -> str:
    return (x or "").strip().upper()

def _get_first_key(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def _parse_qty(row: Dict[str, Any]) -> float:
    v = _get_first_key(row, ["quantity", "qty", "darab", "db", "shares"])
    f = _safe_float(v)
    return float(f) if f is not None else 0.0

def _parse_threshold_k(row: Dict[str, Any]) -> float:
    v = _get_first_key(row, ["K", "k", "K_%", "min_move", "min_move_pct"])
    f = _safe_float(v)
    return float(f) if (f is not None and f > 0) else 3.0


# -----------------------------
# Yahoo Chart v8
# -----------------------------

@dataclass
class PriceSlice:
    prev_close: Optional[float]
    post: Optional[float]
    pre: Optional[float]

def _http_get_json(url: str, timeout: int = 25) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))

def _yahoo_chart_v8(ticker: str) -> Dict[str, Any]:
    # includePrePost=true adja a pre/post meta értékeket, és a prevClose-t is.
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?interval=1m&range=1d&includePrePost=true&events=div%7Csplit"
    )
    return _http_get_json(url)

def _extract_prices_from_chart(payload: Dict[str, Any]) -> PriceSlice:
    """
    A v8 chart válaszban a meta rész tipikusan tartalmazza:
      - previousClose
      - preMarketPrice
      - postMarketPrice
    Kulcsnevek változhatnak; több opciót próbálunk.
    """
    try:
        res = payload.get("chart", {}).get("result", [])
        r0 = res[0] if res else {}
        meta = r0.get("meta", {}) if isinstance(r0, dict) else {}
    except Exception:
        meta = {}

    prev_close = _safe_float(meta.get("previousClose")) or _safe_float(meta.get("regularMarketPreviousClose"))
    pre = _safe_float(meta.get("preMarketPrice"))
    post = _safe_float(meta.get("postMarketPrice"))

    # Ha nincs pre/post a meta-ban (előfordulhat), akkor marad None.
    return PriceSlice(prev_close=prev_close, pre=pre, post=post)

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


def _compute_moves(master_rows: List[Dict[str, Any]], retries: int = 2, sleep_s: float = 0.6) -> Tuple[List[TickerMove], List[str]]:
    moves: List[TickerMove] = []
    missing: List[str] = []

    for row in master_rows:
        t = _norm_ticker(_get_first_key(row, ["ticker", "Ticker", "symbol", "Symbol"]) or "")
        if not t:
            continue

        qty = _parse_qty(row)
        k = _parse_threshold_k(row)

        payload = None
        for _ in range(retries + 1):
            try:
                payload = _yahoo_chart_v8(t)
                break
            except Exception:
                time.sleep(sleep_s)

        if payload is None:
            missing.append(t)
            moves.append(TickerMove(ticker=t, qty=qty, k=k, ah_pct=None, pm_pct=None))
            continue

        ps = _extract_prices_from_chart(payload)

        if ps.prev_close is None:
            missing.append(t)

        ah_pct = _pct_change(ps.post, ps.prev_close) if ps.post is not None else None
        pm_pct = _pct_change(ps.pre, ps.prev_close) if ps.pre is not None else None

        moves.append(TickerMove(ticker=t, qty=qty, k=k, ah_pct=ah_pct, pm_pct=pm_pct))

    return moves, sorted(list(set(missing)))


def _split_positions_watchlist(moves: List[TickerMove]) -> Tuple[List[TickerMove], List[TickerMove]]:
    positions = [m for m in moves if m.qty and m.qty > 0]
    watch = [m for m in moves if not (m.qty and m.qty > 0)]
    positions.sort(key=lambda x: x.ticker)
    watch.sort(key=lambda x: x.ticker)
    return positions, watch


def _is_trigger(m: TickerMove) -> bool:
    k = float(m.k or 3.0)
    return (m.ah_pct is not None and abs(m.ah_pct) >= k) or (m.pm_pct is not None and abs(m.pm_pct) >= k)


def _render_report_1(moves: List[TickerMove], missing: List[str]) -> str:
    now = _now_local()
    positions, watch = _split_positions_watchlist(moves)

    if len(missing) == 0:
        cov = f"Lefedettség: TELJES — ellenőrizve: {len(moves)}/{len(moves)} ticker"
    else:
        cov = f"Lefedettség: HIÁNYOS — nem elérhető / hiányos adat: {', '.join(missing)} (ok: árfeed/forrás hiba)"

    lines: List[str] = []
    lines.append("# #1 — After-hours & Premarket (PRICE ENGINE)")
    lines.append("")
    lines.append(f"Időbélyeg: {now.strftime('%Y-%m-%d %H:%M:%S')} {_tz_label(now)}")
    lines.append(cov)
    lines.append(f"Universe: positions={len(positions)}, watchlist={len(watch)}, total={len(moves)}")
    lines.append("")
    lines.append("## 🎯 Megjegyzés")
    lines.append("Ez a riport SZÁNDÉKOSAN csak az AH/PM számokat és küszöb-triggereket tartalmazza. "
                 "Makró/FED/politika + bejelentések/elemzői lépések + katalizátorok + high-conv: manuális/webes elemzés.")
    lines.append("")

    lines.append("### 📊 Darabszámos tickerek — After-hours & Premarket mozgások (teljes lista)")
    if not positions:
        lines.append("_Nincs darabszámos pozíció a MASTER-ben._")
    else:
        for m in positions:
            tag = " 🔔" if _is_trigger(m) else ""
            lines.append(f"- {m.ticker} — AH {_fmt_pct(m.ah_pct)} | PM {_fmt_pct(m.pm_pct)} — Küszöb K={m.k:.2f}%{tag}")
    lines.append("")

    lines.append("### 👀 Watchlist — Küszöb feletti AH/PM mozgások (|%| ≥ K)")
    watch_trig = [m for m in watch if _is_trigger(m)]
    if not watch_trig:
        lines.append("_Jelenleg nincs küszöb feletti watchlist mozgás._")
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
        lines.append("_Nincs watchlist ticker a MASTER-ben._")
    else:
        for m in watch:
            tag = " 🔔" if _is_trigger(m) else ""
            lines.append(f"- {m.ticker} — AH {_fmt_pct(m.ah_pct)} | PM {_fmt_pct(m.pm_pct)} — K={m.k:.2f}%{tag}")
    lines.append("")
    lines.append(f"Job summary generated at run-time ({now.strftime('%Y-%m-%dT%H:%M:%S%z')})")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1, choices=[1], help="PRICE ENGINE: csak report=1 (AH/PM).")
    ap.add_argument("--master", type=str, default="reports/master.csv", help="MASTER CSV.")
    ap.add_argument("--out", type=str, default="reports/summary_report_1.md", help="Kimeneti markdown.")
    args = ap.parse_args()

    if not os.path.exists(args.master):
        print(f"HIBA: MASTER CSV nem található: {args.master}", file=sys.stderr)
        return 2

    rows = _read_master_csv(args.master)
    moves, missing = _compute_moves(rows)

    md = _render_report_1(moves, missing)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(md)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
