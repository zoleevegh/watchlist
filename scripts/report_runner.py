#!/usr/bin/env python3
# report_runner.py — v4.0.0-price-engine-2026-01-05
# Cél: csak azokat a részeket csináljuk, amit webesen nem tudsz megbízhatóan lekérni:
#      ár/% számítások (AH/PM, Open→Close, Open→Most) + küszöb/lefödöttség.
# A makró / analyst / catalyst / high-conv blokkokat SZÁNDÉKOSAN nem gyártjuk.

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
from zoneinfo import ZoneInfo

import urllib.request
import urllib.error


BUDAPEST_TZ = ZoneInfo("Europe/Budapest")


# -----------------------------
# Helpers
# -----------------------------

def _now_budapest() -> dt.datetime:
    return dt.datetime.now(tz=BUDAPEST_TZ)


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


def _read_master_csv(path: str) -> List[Dict[str, Any]]:
    """
    Elvárt minimum oszlopok:
      - ticker (vagy Ticker)
    Opcionális:
      - quantity / darab / qty
      - buy_price / beker / avg
      - K, L, M (küszöbök) — ha nincs: default K=3, L=2, M=1
    Bármi extra jöhet, figyelmen kívül hagyjuk.
    """
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
    # Biblia: ha üres/érvénytelen: K=3 (%)
    v = _get_first_key(row, ["K", "k", "K_%", "min_move", "min_move_pct"])
    f = _safe_float(v)
    return float(f) if (f is not None and f > 0) else 3.0


@dataclass
class PriceSlice:
    last: Optional[float]
    prev_close: Optional[float]
    post: Optional[float]       # after-hours last / postMarketPrice
    pre: Optional[float]        # preMarketPrice
    reg_open: Optional[float]
    reg_close: Optional[float]
    reg_last: Optional[float]   # regularMarketPrice
    ts: Optional[int]


def _yahoo_quote(ticker: str, timeout: int = 20) -> Dict[str, Any]:
    """
    Yahoo quote endpoint. Szükség esetén a GitHub Actions IP-k miatt előfordulhat 401/403/429.
    Ilyenkor a runner a lefedettségben HIÁNYOS-t fog jelezni (ticker szinten).
    """
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; price-engine/1.0; +https://github.com/)",
            "Accept": "application/json,text/plain,*/*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))


def _extract_prices(payload: Dict[str, Any]) -> PriceSlice:
    try:
        result = payload.get("quoteResponse", {}).get("result", [])
        q = result[0] if result else {}
    except Exception:
        q = {}
    return PriceSlice(
        last=_safe_float(q.get("regularMarketPrice")),
        prev_close=_safe_float(q.get("regularMarketPreviousClose")),
        post=_safe_float(q.get("postMarketPrice")) or _safe_float(q.get("postMarketLastPrice")),
        pre=_safe_float(q.get("preMarketPrice")),
        reg_open=_safe_float(q.get("regularMarketOpen")),
        reg_close=_safe_float(q.get("regularMarketPreviousClose")),
        reg_last=_safe_float(q.get("regularMarketPrice")),
        ts=int(q.get("regularMarketTime")) if q.get("regularMarketTime") else None,
    )


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
    # Debug / raw
    prev_close: Optional[float]
    post_price: Optional[float]
    pre_price: Optional[float]


def _compute_moves(master_rows: List[Dict[str, Any]], retries: int = 2, sleep_s: float = 0.7) -> Tuple[List[TickerMove], List[str]]:
    moves: List[TickerMove] = []
    missing: List[str] = []

    for row in master_rows:
        t = _norm_ticker(_get_first_key(row, ["ticker", "Ticker", "symbol", "Symbol"]) or "")
        if not t:
            continue

        qty = _parse_qty(row)
        k = _parse_threshold_k(row)

        payload = None
        last_err = None
        for attempt in range(retries + 1):
            try:
                payload = _yahoo_quote(t)
                last_err = None
                break
            except Exception as e:
                last_err = e
                time.sleep(sleep_s)

        if payload is None:
            missing.append(t)
            moves.append(TickerMove(
                ticker=t, qty=qty, k=k,
                ah_pct=None, pm_pct=None,
                prev_close=None, post_price=None, pre_price=None
            ))
            continue

        ps = _extract_prices(payload)
        # AH: postMarketPrice vs prev_close
        ah_pct = _pct_change(ps.post, ps.prev_close) if ps.post is not None else None
        # PM: preMarketPrice vs prev_close
        pm_pct = _pct_change(ps.pre, ps.prev_close) if ps.pre is not None else None

        if ps.prev_close is None:
            missing.append(t)

        moves.append(TickerMove(
            ticker=t, qty=qty, k=k,
            ah_pct=ah_pct, pm_pct=pm_pct,
            prev_close=ps.prev_close, post_price=ps.post, pre_price=ps.pre
        ))

    return moves, sorted(list(set(missing)))


def _split_positions_watchlist(moves: List[TickerMove]) -> Tuple[List[TickerMove], List[TickerMove]]:
    positions = [m for m in moves if m.qty and m.qty > 0]
    watch = [m for m in moves if not (m.qty and m.qty > 0)]
    # darabszámosak előre, utána ABC
    positions.sort(key=lambda x: x.ticker)
    watch.sort(key=lambda x: x.ticker)
    return positions, watch


def _is_trigger(m: TickerMove) -> bool:
    k = float(m.k or 3.0)
    return (m.ah_pct is not None and abs(m.ah_pct) >= k) or (m.pm_pct is not None and abs(m.pm_pct) >= k)


def _render_report_1(moves: List[TickerMove], missing: List[str]) -> str:
    now = _now_budapest()
    positions, watch = _split_positions_watchlist(moves)

    # Coverage
    if len(missing) == 0:
        cov = f"Lefedettség: TELJES — ellenőrizve: {len(moves)}/{len(moves)} ticker"
    else:
        cov = f"Lefedettség: HIÁNYOS — nem elérhető / hiányos adat: {', '.join(missing)} (ok: árfeed/forrás hiba)"

    # Output
    lines: List[str] = []
    lines.append(f"# #1 — After-hours & Premarket (PRICE ENGINE)")
    lines.append("")
    lines.append(f"Időbélyeg: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    lines.append(cov)
    lines.append(f"Universe: positions={len(positions)}, watchlist={len(watch)}, total={len(moves)}")
    lines.append("")
    lines.append("## 🎯 Megjegyzés")
    lines.append("Ez a riport SZÁNDÉKOSAN csak az AH/PM számokat és küszöb-triggereket tartalmazza. "
                 "Makró/FED/politika + bejelentések/elemzői lépések + katalizátorok + high-conv: manuális/webes elemzés.")
    lines.append("")

    # Positions (full list, but mark triggers)
    lines.append("### 📊 Darabszámos tickerek — After-hours & Premarket mozgások (teljes lista)")
    if not positions:
        lines.append("_Nincs darabszámos pozíció a MASTER-ben._")
    else:
        for m in positions:
            tag = " 🔔" if _is_trigger(m) else ""
            lines.append(f"- {m.ticker} — AH {_fmt_pct(m.ah_pct)} | PM {_fmt_pct(m.pm_pct)} — Küszöb K={m.k:.2f}%{tag}")
    lines.append("")

    # Watchlist triggers first, then full list (optional)
    lines.append("### 👀 Watchlist — Küszöb feletti AH/PM mozgások (|%| ≥ K)")
    watch_trig = [m for m in watch if _is_trigger(m)]
    if not watch_trig:
        lines.append("_Jelenleg nincs küszöb feletti watchlist mozgás._")
    else:
        # nagyobb abs mozgás előre
        def key_abs(m: TickerMove) -> float:
            a = abs(m.pm_pct) if m.pm_pct is not None else 0.0
            b = abs(m.ah_pct) if m.ah_pct is not None else 0.0
            return max(a, b)
        watch_trig.sort(key=key_abs, reverse=True)
        for m in watch_trig:
            lines.append(f"- {m.ticker} — AH {_fmt_pct(m.ah_pct)} | PM {_fmt_pct(m.pm_pct)} — K={m.k:.2f}% 🔔")
    lines.append("")

    # Full watchlist (so you can decide to check news for ALL tickers)
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
    ap.add_argument("--report", type=int, default=1, choices=[1], help="Most csak report=1 (AH/PM) price-engine mód.")
    ap.add_argument("--master", type=str, default="reports/master.csv", help="MASTER CSV (publish-to-web CSV).")
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
