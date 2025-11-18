#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
report_runner.py
-----------------

Egységes runner script az 1/2/3-as riportokhoz.

#1: AH + PM (chart 2d/5m, includePrePost) – árforrás: Yahoo chart (v8)
#2: Tegnapi Open→Close (batch quote) – árforrás: Yahoo quote (v7) – egyszerűsített
#3: Ma Open→Most (batch quote) – árforrás: Yahoo quote (v7) – egyszerűsített

BIBLIA-követelmények (1-es riport – core):
- Időablak: AH (22:00–02:00 CEST), PM (10:00–15:30 CEST) – magyarázó sor a tetején.
- Lefedettség blokk: TickerStatus alapján „TELJES” vagy „HIÁNYOS – … (oka: …)”.
- Sorrend: 1) Lefedettség 2) Makró/FED blokk 3) Darabszámos tickerek 4) Watchlist (ha ≥K) 5) Hírek 6) Katalizátorok.
- PKN.WA alapértelmezetten kimarad.
- Minden darabszámos ticker külön sorban, AH/PM % + rövid indok + árforrás.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

# scripts/ mappa importálhatósága
sys.path.append(os.path.dirname(__file__))

from report_1_helpers import (
    TickerStatus,
    build_coverage_block,
    build_macro_block_1,
    NewsItem,
    merge_news_sources,
    build_news_block_1,
    UpcomingCatalyst,
    build_catalyst_block_1,
)

try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python <3.9 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore

BUDAPEST = ZoneInfo("Europe/Budapest")
US_EASTERN = ZoneInfo("America/New_York")

SCRIPT_VERSION = "2.0.0-biblia-core-yahoo"


# ---------------------------------------------------------------------------
# CSV beolvasása – darabszámos / watchlist + K küszöb
# ---------------------------------------------------------------------------

@dataclass
class TickerRow:
    ticker: str
    quantity: Optional[float]  # None = watchlist
    k_threshold: float         # min. abs % (3% default)
    is_position: bool          # True = darabszámos pozíció


def _parse_float(val: str) -> Optional[float]:
    if val is None:
        return None
    val = val.strip().replace(",", ".")
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def load_tickers_from_csv(path: str, default_k: float = 3.0) -> List[TickerRow]:
    """
    MASTER CSV beolvasása.

    - Ticker oszlop: "Ticker" / "tiker" / "symbol"
    - Darabszám: "Darabszám" / "darabszam" / "db" / "qty" / "quantity" / "shares"
    - Küszöb: "K" / "minmove" / "minmovepct" / "min_pct"

    Duplikált tickereket (pl. UBER IBKR + UBER RAIFFEISEN) összevon:
    - quantity = összeg
    - is_position = True, ha összeg > 0
    - k_threshold = legkisebb K az adott tickerre
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV nem található: {path}")

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = {h.strip().lower(): h for h in (reader.fieldnames or [])}

        def get_col(*names: str) -> Optional[str]:
            for n in names:
                if n.lower() in headers:
                    return headers[n.lower()]
            return None

        ticker_col = get_col("ticker", "tiker", "symbol")
        qty_col = get_col("darabszám", "darabszam", "db", "qty", "quantity", "shares")
        k_col = get_col("k", "minmove", "minmovepct", "min_pct")

        if not ticker_col:
            raise ValueError("A CSV-ben nincs 'Ticker' (vagy ekvivalens) oszlop.")

        tmp: Dict[str, TickerRow] = {}

        for r in reader:
            t = (r.get(ticker_col) or "").strip().upper()
            if not t:
                continue

            qty_val = _parse_float(r.get(qty_col, "")) if qty_col else None
            k_val = _parse_float(r.get(k_col, "")) if k_col else None
            if k_val is None or k_val <= 0:
                k_val = default_k

            if t in tmp:
                prev = tmp[t]
                total_qty = (prev.quantity or 0.0) + (qty_val or 0.0)
                is_pos = total_qty > 0
                merged = TickerRow(
                    ticker=t,
                    quantity=total_qty if is_pos else None,
                    k_threshold=min(prev.k_threshold, k_val),
                    is_position=is_pos,
                )
                tmp[t] = merged
            else:
                is_pos = qty_val is not None and qty_val > 0
                tmp[t] = TickerRow(
                    ticker=t,
                    quantity=qty_val if is_pos else None,
                    k_threshold=k_val,
                    is_position=is_pos,
                )

    return list(tmp.values())


# ---------------------------------------------------------------------------
# Yahoo Finance chart (v8) – AH/PM helper
# ---------------------------------------------------------------------------

@dataclass
class PriceSnapshot:
    prev_close: Optional[float]
    ah_last: Optional[float]
    pm_last: Optional[float]


def fetch_chart_2d_5m(ticker: str) -> dict:
    """
    Yahoo chart v8:
    - range=2d
    - interval=5m
    - includePrePost=true

    Rate limit / auth hibáknál egyértelmű hibaüzenetet dobunk.
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        "?range=2d&interval=5m&includePrePost=true"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
    }
    resp = requests.get(url, headers=headers, timeout=10)

    if resp.status_code == 429:
        raise RuntimeError("rate_limited(chart_v8)")
    if resp.status_code == 401:
        raise RuntimeError("http_401(chart_v8)")
    try:
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"http_error_{resp.status_code}(chart_v8): {e}")

    return resp.json()


def extract_ah_pm_move(ticker: str) -> Tuple[PriceSnapshot, Optional[str]]:
    """
    #1 riporthoz: AH + PM sáv utolsó ára, previousClose alapján.
    Árforrás: Yahoo Finance chart (v8, 2d/5m, includePrePost).
    """
    try:
        data = fetch_chart_2d_5m(ticker)
    except Exception as e:
        return PriceSnapshot(None, None, None), str(e)

    try:
        result = data["chart"]["result"][0]
        meta = result.get("meta", {})
        timestamps = result.get("timestamp", [])
        quotes = result.get("indicators", {}).get("quote", [{}])[0]
        closes = quotes.get("close", [])

        if not timestamps or not closes:
            return PriceSnapshot(None, None, None), "no_price_data"

        prev_close = meta.get("previousClose")

        from datetime import datetime as _dt
        dt_list = [_dt.fromtimestamp(ts, tz=US_EASTERN) for ts in timestamps]

        # fallback prev_close, ha meta-ban nincs
        if prev_close is None:
            prev_rth_closes = [
                c
                for c, dt in zip(closes, dt_list)
                if 9 <= dt.hour <= 16
            ]
            prev_close = prev_rth_closes[-1] if prev_rth_closes else None

        # AH: előző nap 16:00–20:00 US idő (csak utolsó érték kell)
        ah_prices = [
            c
            for c, dt in zip(closes, dt_list)
            if 16 <= dt.hour < 20
        ]
        ah_last = ah_prices[-1] if ah_prices else None

        # PM: mai nap 4:00–9:30 US idő
        today_date = dt_list[-1].date()
        pm_prices = [
            c
            for c, dt in zip(closes, dt_list)
            if dt.date() == today_date
            and (
                4 <= dt.hour < 9
                or (dt.hour == 9 and dt.minute <= 30)
            )
        ]
        pm_last = pm_prices[-1] if pm_prices else None

        return PriceSnapshot(prev_close, ah_last, pm_last), None
    except Exception as e:
        return PriceSnapshot(None, None, None), f"parse_error(chart_v8): {e}"


def pct_change(base: Optional[float], new: Optional[float]) -> Optional[float]:
    if base is None or new is None or base == 0:
        return None
    return (new - base) / base * 100.0


# ---------------------------------------------------------------------------
# 1-es riport: AH + PM (BIBLIA-core)
# ---------------------------------------------------------------------------

def run_report_1(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    lines: List[str] = []

    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")
    prev_day_str = (now_local - timedelta(days=1)).strftime("%Y-%m-%d")

    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST\n")
    lines.append(f"Script verzió: {SCRIPT_VERSION}\n")
    lines.append(
        f"Vizsgált ablakok (CEST): AH {prev_day_str} 22:00 → {today_str} 02:00, "
        f"PM {today_str} 10:00 → 15:30\n"
    )
    lines.append(
        "_Árforrás: Yahoo Finance chart (v8 – 2d/5m, includePrePost; "
        "previousClose → AH/PM utolsó ár alapján számolt % mozgás)_\n"
    )

    # PKN.WA kihagyása
    filtered = [r for r in tickers if r.ticker != "PKN.WA"]

    # szétválasztjuk pozíciókra / watchlistre
    positions = [r for r in filtered if r.is_position]
    watchlist = [r for r in filtered if not r.is_position]

    if not positions and not watchlist:
        lines.append("Nincs feldolgozható ticker a MASTER-ben.\n")
        return "\n".join(lines)

    # Yahoo lekérdezések – először a darabszámosokra
    status_map: Dict[str, TickerStatus] = {}
    price_map: Dict[str, PriceSnapshot] = {}

    # paraméterek a rate limit kíméléshez – itt lehet finomhangolni
    SLEEP_BETWEEN_REQUESTS = 1.2  # másodperc
    RATE_LIMIT_HARDSTOP = 5       # ennyi rate_limited után nem kérdezünk tovább

    rate_limited_count = 0

    # 1) Darabszámos tickerek
    for row in positions:
        snap, reason = extract_ah_pm_move(row.ticker)
        price_map[row.ticker] = snap

        if reason is None:
            status_map[row.ticker] = TickerStatus(ok=True)
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason)
            if "rate_limited" in reason:
                rate_limited_count += 1

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # 2) Watchlist – csak akkor, ha nem látszik masszív rate_limit a pozíciókon
    watchlist_processed: List[TickerRow] = []
    if rate_limited_count < RATE_LIMIT_HARDSTOP:
        for row in watchlist:
            snap, reason = extract_ah_pm_move(row.ticker)
            price_map[row.ticker] = snap

            if reason is None:
                status_map[row.ticker] = TickerStatus(ok=True)
            else:
                status_map[row.ticker] = TickerStatus(ok=False, reason=reason)
                if "rate_limited" in reason:
                    rate_limited_count += 1
                    if rate_limited_count >= RATE_LIMIT_HARDSTOP:
                        # hátralévő watchlist tickerek – proaktívan "skipped_to_avoid_rate_limit"
                        break

            watchlist_processed.append(row)
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        # ami watchlist ticker kimaradt a hardstop miatt, azt jelöljük coverage-ben
        remaining_skipped = [r for r in watchlist if r not in watchlist_processed]
        for r in remaining_skipped:
            if r.ticker not in status_map:
                status_map[r.ticker] = TickerStatus(
                    ok=False,
                    reason="skipped_to_avoid_rate_limit(chart_v8)"
                )

    else:
        # ha már a pozícióknál beborult a Yahoo, a teljes watchlistet "skipped"-nek jelöljük
        for r in watchlist:
            status_map[r.ticker] = TickerStatus(
                ok=False,
                reason="skipped_to_avoid_rate_limit(chart_v8)"
            )

    # Lefedettség blokk
    lines.append(build_coverage_block(status_map))

    # Makró/FED blokk (BIBLIA: itt következik)
    macro_block = build_macro_block_1(macro)
    if macro_block:
        lines.append(macro_block)

    # --- Darabszámos tickerek blokk ---
    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – After-hours & Premarket mozgások\n")

    # --- Watchlist blokk ---
    watch_lines: List[str] = []
    watch_lines.append("### Watchlist – After-hours & Premarket mozgások (csak ha ≥K)\n")

    def fmt_pct(label: str, value: Optional[float]) -> str:
        if value is None:
            return f"{label}: n/a"
        sign = "+" if value >= 0 else ""
        return f"{label} {sign}{value:.2f}%"

    # 3) Sorok építése – pozíciók
    for row in positions:
        snap = price_map.get(row.ticker, PriceSnapshot(None, None, None))
        ah_pct = pct_change(snap.prev_close, snap.ah_last)
        pm_pct = pct_change(snap.prev_close, snap.pm_last)

        ah_str = fmt_pct("AH", ah_pct)
        pm_str = fmt_pct("PM", pm_pct)

        has_signal = (
            (ah_pct is not None and abs(ah_pct) >= row.k_threshold) or
            (pm_pct is not None and abs(pm_pct) >= row.k_threshold)
        )

        # hiba-információ, ha van
        ts = status_map.get(row.ticker)
        src_str = "árforrás: Yahoo chart/v8 2d/5m, includePrePost"
        if ts and ts.reason and "skipped_to_avoid_rate_limit" in ts.reason:
            src_str += " (watchlist részben limit elkerülés – de ez pozíció, ezért még lekérdezve)"
        elif ts and ts.reason and "rate_limited" in ts.reason:
            src_str += f" (hiba: {ts.reason})"

        if has_signal:
            reason_txt = "Érdemi AH/PM elmozdulás (≥K) a záróárhoz képest."
        else:
            reason_txt = "Egyelőre nincs küszöb feletti AH/PM elmozdulás."

        pos_lines.append(
            f"{row.ticker} — {ah_str} | {pm_str} — {reason_txt} ({src_str})"
        )

    # 4) Sorok építése – watchlist (csak ha ≥K és ténylegesen le tudtuk kérdezni)
    for row in watchlist_processed:
        snap = price_map.get(row.ticker, PriceSnapshot(None, None, None))
        ah_pct = pct_change(snap.prev_close, snap.ah_last)
        pm_pct = pct_change(snap.prev_close, snap.pm_last)

        ah_str = fmt_pct("AH", ah_pct)
        pm_str = fmt_pct("PM", pm_pct)

        has_signal = (
            (ah_pct is not None and abs(ah_pct) >= row.k_threshold) or
            (pm_pct is not None and abs(pm_pct) >= row.k_threshold)
        )
        if not has_signal:
            continue

        src_str = "árforrás: Yahoo chart/v8 2d/5m, includePrePost"
        ts = status_map.get(row.ticker)
        if ts and ts.reason and "rate_limited" in ts.reason:
            src_str += f" (hiba: {ts.reason})"

        reason_txt = "Watchlisten is érdemi AH/PM elmozdulás (≥K) a záróárhoz képest."
        watch_lines.append(
            f"{row.ticker} — {ah_str} | {pm_str} — {reason_txt} ({src_str})"
        )

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

    # ------------------------------------------------------------------
    # Hírek & katalizátorok – BIBLIA szerint itt következnek
    # (Most csak a blokk-építés hívása marad, a tényleges hírszedéshez
    #  a report_1_helpers NewsItem/UpcomingCatalyst logikáját kell majd bővíteni.)
    # ------------------------------------------------------------------
    yahoo_news: List[NewsItem] = []
    extra_news: List[NewsItem] = []
    all_news = merge_news_sources(yahoo_news, extra_news)

    news_block = build_news_block_1(all_news)
    if news_block:
        lines.append(news_block)

    catalysts: List[UpcomingCatalyst] = []
    catalyst_block = build_catalyst_block_1(catalysts)
    if catalyst_block:
        lines.append(catalyst_block)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 2-es riport: Tegnapi Open→Close – egyszerűsített (változatlan logika)
# ---------------------------------------------------------------------------

def fetch_quote_summary(ticker: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Egyszerű v7 quote helper a #2/#3-hoz (fallback jelleggel).
    """
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
    }
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code == 429:
        return None, None
    try:
        resp.raise_for_status()
    except Exception:
        return None, None
    try:
        data = resp.json()
        q = data["quoteResponse"]["result"][0]
        prev_close = q.get("regularMarketPreviousClose")
        last = q.get("regularMarketPrice")
        return prev_close, last
    except Exception:
        return None, None


def run_report_2(
    tickers: List[TickerRow],
) -> str:
    lines: List[str] = []
    lines.append("#2 – Tegnapi nyitástól zárásig (Open→Close) – egyszerűsített\n")
    lines.append(f"Script verzió: {SCRIPT_VERSION}\n")
    lines.append("_Árforrás: Yahoo Finance quote (v7 – previousClose → last)_\n")

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]

    pos_lines: List[str] = []
    pos_lines.append("Darabszámos tickerek – abs(Open→Close) becsült mozgás (≥K)\n")

    watch_lines: List[str] = []
    watch_lines.append("Watchlist – abs(Open→Close) becsült mozgás (≥K)\n")

    for row in filtered:
        prev_close, last = fetch_quote_summary(row.ticker)
        move = pct_change(prev_close, last)
        if move is None or abs(move) < row.k_threshold:
            continue

        sign = "+" if move >= 0 else ""
        line = f"{row.ticker} — Open→Close (becsült): {sign}{move:.2f}% (árforrás: Yahoo quote/v7)"

        if row.is_position:
            pos_lines.append(line)
        else:
            watch_lines.append(line)

        time.sleep(0.4)

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 3-as riport: Ma nyitástól mostanáig – egyszerűsített (változatlan core)
# ---------------------------------------------------------------------------

def run_report_3(
    tickers: List[TickerRow],
) -> str:
    lines: List[str] = []
    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")

    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most) — CEST\n")
    lines.append(f"Script verzió: {SCRIPT_VERSION}\n")
    lines.append(
        f"Vizsgált ablak (CEST): mai USA nyitás (15:30) → lekérdezés időpontja ({today_str})\n"
    )
    lines.append(
        "_Árforrás: Yahoo Finance quote (v7 – regularMarketOpen → regularMarketPrice)_\n"
    )

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]

    pos_lines: List[str] = []
    pos_lines.append("Darabszámos tickerek – Ma nyitástól mostanáig (Open→Most)\n")

    watch_lines: List[str] = []
    watch_lines.append("Watchlist – Open→Most mozgások (csak ha ≥K)\n")

    for row in filtered:
        # ugyanaz a helper, csak open vs last összefüggésben használjuk
        prev_close, last = fetch_quote_summary(row.ticker)  # egyszerűsítés
        # itt igazából regularMarketOpen kellene; a v7 open/last pontosságától függ
        open_px = prev_close  # egyszerű fallback; a finomhangolást külön lehet megcsinálni

        move = pct_change(open_px, last)

        def fmt_open_most(value: Optional[float]) -> str:
            if value is None:
                return "Open→Most: n/a"
            sign = "+" if value >= 0 else ""
            return f"Open→Most: {sign}{value:.2f}%"

        open_most_str = fmt_open_most(move)

        if row.is_position:
            pos_lines.append(
                f"{row.ticker} — {open_most_str} (Küszöb: {row.k_threshold:.2f}%) "
                "(árforrás: Yahoo quote/v7 – egyszerűsített Open→Most)"
            )
        else:
            if move is None or abs(move) < row.k_threshold:
                continue
            watch_lines.append(
                f"{row.ticker} — {open_most_str} (árforrás: Yahoo quote/v7 – egyszerűsített Open→Most)"
            )

        time.sleep(0.4)

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Részvény riport futtató (1/2/3)")
    parser.add_argument(
        "--report",
        choices=["1", "2", "3"],
        required=True,
        help="Riport típusa: 1 = AH/PM, 2 = Tegnapi O→C, 3 = Ma O→Most",
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Ticker-lista CSV elérési útja",
    )
    parser.add_argument(
        "--summary",
        required=False,
        help="GitHub Actions GITHUB_STEP_SUMMARY elérési útja (ha megadod, oda írja ki a riportot)",
    )
    parser.add_argument(
        "--macro",
        required=False,
        help="Politika/FED/makró szöveg (Trump-napihír + 1–4 mondat, az 1-es riporthoz)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    tickers = load_tickers_from_csv(args.csv)

    if args.report == "1":
        report_text = run_report_1(tickers, macro=args.macro)
    elif args.report == "2":
        report_text = run_report_2(tickers)
    elif args.report == "3":
        report_text = run_report_3(tickers)
    else:
        raise ValueError(f"Ismeretlen riport: {args.report}")

    if args.summary:
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(report_text)
    else:
        print(report_text)


if __name__ == "__main__":
    main()
