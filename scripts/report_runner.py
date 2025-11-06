#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
report_runner.py
-----------------

Egységes runner script az 1/2/3-as riportokhoz.

#1: AH + PM (chart 2d/5m, includePrePost) – árforrás: Yahoo chart (v8)
#2: Tegnapi Open→Close (batch quote) – árforrás: Yahoo quote (v7)
#3: Ma Open→Most (batch quote, regularMarketOpen→regularMarketPrice) – árforrás: Yahoo quote (v7)
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
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


# ---------------------------------------------------------------------------
# CSV beolvasása
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
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV nem található: {path}")

    rows: List[TickerRow] = []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = {h.strip().lower(): h for h in reader.fieldnames or []}

        def get_col(*names: str) -> Optional[str]:
            for n in names:
                if n.lower() in headers:
                    return headers[n.lower()]
            return None

        ticker_col = get_col("ticker", "tiker", "symbol")
        qty_col = get_col("darabszám", "db", "qty", "quantity")
        k_col = get_col("k", "minmove", "minmovepct", "min_pct")

        if not ticker_col:
            raise ValueError("A CSV-ben nincs 'Ticker' (vagy ekvivalens) oszlop.")

        for r in reader:
            t = (r.get(ticker_col) or "").strip().upper()
            if not t:
                continue

            qty_val = _parse_float(r.get(qty_col, "")) if qty_col else None
            k_val = _parse_float(r.get(k_col, "")) if k_col else None
            if k_val is None or k_val <= 0:
                k_val = default_k

            is_position = qty_val is not None and qty_val > 0

            rows.append(
                TickerRow(
                    ticker=t,
                    quantity=qty_val,
                    k_threshold=k_val,
                    is_position=is_position,
                )
            )

    return rows


# ---------------------------------------------------------------------------
# Yahoo Finance chart / quote helper-ek
# ---------------------------------------------------------------------------

@dataclass
class PriceSnapshot:
    prev_close: Optional[float]
    ah_last: Optional[float]
    pm_last: Optional[float]


def fetch_chart_2d_5m(ticker: str) -> dict:
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        "?range=2d&interval=5m&includePrePost=true"
    )
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def extract_ah_pm_move(ticker: str) -> Tuple[PriceSnapshot, Optional[str]]:
    """
    #1 riporthoz: AH + PM sáv utolsó ára, previousClose alapján.
    Árforrás: Yahoo Finance chart (v8)
    """
    try:
        data = fetch_chart_2d_5m(ticker)
    except Exception as e:
        return PriceSnapshot(None, None, None), f"fetch_error: {e}"

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
                if dt.hour >= 9 and dt.hour <= 16
            ]
            prev_close = prev_rth_closes[-1] if prev_rth_closes else None

        # AH: 16:00–20:00 US idő
        ah_prices = [
            c
            for c, dt in zip(closes, dt_list)
            if dt.hour >= 16 and dt.hour < 20
        ]
        ah_last = ah_prices[-1] if ah_prices else None

        # PM: ma 4:00–9:30 US idő
        today_date = dt_list[-1].date()
        pm_prices = [
            c
            for c, dt in zip(closes, dt_list)
            if dt.date() == today_date
            and (
                dt.hour >= 4
                and (dt.hour < 9 or (dt.hour == 9 and dt.minute <= 30))
            )
        ]
        pm_last = pm_prices[-1] if pm_prices else None

        return PriceSnapshot(prev_close, ah_last, pm_last), None
    except Exception as e:
        return PriceSnapshot(None, None, None), f"parse_error: {e}"


def pct_change(base: Optional[float], new: Optional[float]) -> Optional[float]:
    if base is None or new is None or base == 0:
        return None
    return (new - base) / base * 100.0


# ---------------------------------------------------------------------------
# Quote batch helper (#2 és #3 riporthoz)
# ---------------------------------------------------------------------------

import time  # a file elején legyen importálva


def fetch_quotes_batch(
    tickers: List[str],
    batch_size: int = 20,
) -> Dict[str, Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]]:
    """
    Yahoo quote API batch-ben (#2 és #3 riporthoz).

    Visszaad:
        { "AAPL": (prev_close, last, regular_open, error_reason_str_or_None), ... }

    Árforrás: Yahoo Finance quote (v7)

    Rate limit kímélés:
    - kisebb batch_size (20),
    - batch-ek között 2 másodperc sleep.
    """
    results: Dict[str, Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]] = {}
    if not tickers:
        return results

    for t in tickers:
        results[t] = (None, None, None, None)

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        symbols_str = ",".join(batch)
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_str}"

        try:
            resp = requests.get(url, timeout=10)
        except Exception as e:
            for t in batch:
                results[t] = (None, None, None, f"network_error: {e}")
            print(f"[WARN] fetch_quotes_batch network error for {symbols_str}: {e}", file=sys.stderr)
            # következő batch előtt is pihenjünk egy kicsit
            time.sleep(2)
            continue

        if resp.status_code == 429:
            for t in batch:
                results[t] = (None, None, None, "rate_limited")
            print(f"[WARN] fetch_quotes_batch rate limited (429) for {symbols_str}", file=sys.stderr)
            # ha már rate limit, akkor várjunk kicsit többet
            time.sleep(5)
            continue

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            for t in batch:
                results[t] = (None, None, None, f"http_error: {e}")
            print(f"[WARN] fetch_quotes_batch HTTP error for {symbols_str}: {e}", file=sys.stderr)
            time.sleep(2)
            continue

        try:
            data = resp.json()
            qlist = data.get("quoteResponse", {}).get("result", []) or []
        except Exception as e:
            for t in batch:
                results[t] = (None, None, None, f"parse_error: {e}")
            print(f"[WARN] fetch_quotes_batch parse error for {symbols_str}: {e}", file=sys.stderr)
            time.sleep(2)
            continue

        seen_in_batch = set()
        for q in qlist:
            sym = q.get("symbol")
            if not sym:
                continue

            prev_close = q.get("regularMarketPreviousClose")
            last = q.get("regularMarketPrice")
            open_px = q.get("regularMarketOpen")

            results[sym] = (prev_close, last, open_px, None)
            seen_in_batch.add(sym)

        for t in batch:
            if t not in seen_in_batch:
                _, _, _, reason = results.get(t, (None, None, None, None))
                if reason is None:
                    results[t] = (None, None, None, "no_quote_result")

        # MINDIG pihenjünk egy kicsit a batch-ek között
        time.sleep(2)

    return results

# ---------------------------------------------------------------------------
# 1-es riport: AH + PM
# ---------------------------------------------------------------------------

def run_report_1(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    lines: List[str] = []

    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")
    prev_day = (now_local - timedelta(days=1)).strftime("%Y-%m-%d")

    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST\n")
    lines.append(
        f"Vizsgált ablakok (CEST): AH {prev_day} 22:00 → {today_str} 02:00, "
        f"PM {today_str} 10:00 → 15:30\n"
    )
    lines.append("_Árforrás: Yahoo Finance chart (v8, 2d/5m, includePrePost)_\n")

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]

    status_map: Dict[str, TickerStatus] = {}
    price_map: Dict[str, PriceSnapshot] = {}

    for row in filtered:
        snap, reason = extract_ah_pm_move(row.ticker)
        price_map[row.ticker] = snap
        if reason is None:
            status_map[row.ticker] = TickerStatus(ok=True)
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason)

    lines.append(build_coverage_block(status_map))
    lines.append(build_macro_block_1(macro))

    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – After-hours & Premarket mozgások\n")

    watch_lines: List[str] = []
    watch_lines.append("### Watchlist – After-hours & Premarket mozgások (csak ha ≥K vagy van hír)\n")

    for row in filtered:
        snap = price_map[row.ticker]
        ah_pct = pct_change(snap.prev_close, snap.ah_last)
        pm_pct = pct_change(snap.prev_close, snap.pm_last)

        def fmt_pct(label: str, value: Optional[float]) -> str:
            if value is None:
                return f"{label}: n/a"
            sign = "+" if value >= 0 else ""
            return f"{label} {sign}{value:.2f}%"

        has_signal = (
            (ah_pct is not None and abs(ah_pct) >= row.k_threshold) or
            (pm_pct is not None and abs(pm_pct) >= row.k_threshold)
        )

        ah_str = fmt_pct("AH", ah_pct)
        pm_str = fmt_pct("PM", pm_pct)

        src_str = "árforrás: Yahoo chart"

        if row.is_position:
            if has_signal:
                reason = "Érdemi AH/PM elmozdulás (≥K)."
            else:
                reason = "Egyelőre nincs küszöb feletti AH/PM elmozdulás."
            pos_lines.append(f"{row.ticker} — {ah_str} | {pm_str} — {reason} ({src_str})")
        else:
            if not has_signal:
                continue
            reason = "Watchlisten is érdemi AH/PM elmozdulás (≥K)."
            watch_lines.append(f"{row.ticker} — {ah_str} | {pm_str} — {reason} ({src_str})")

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

    # Hírek – egyelőre üresen (később Yahoo + extra forrás)
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
# 2-es riport: Tegnapi Open→Close (batch quote, egyszerűsített)
# ---------------------------------------------------------------------------

def run_report_2(
    tickers: List[TickerRow],
) -> str:
    lines: List[str] = []
    lines.append("#2 – Tegnapi nyitástól zárásig (Open→Close) – egyszerűsített\n")
    lines.append("_Árforrás: Yahoo Finance quote (v7 – previousClose → last)_\n")

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    ticker_list = [r.ticker for r in filtered]
    quote_map = fetch_quotes_batch(ticker_list)

    pos_lines: List[str] = []
    pos_lines.append("Darabszámos tickerek – abs(Open→Close) becsült mozgás (≥K)\n")

    watch_lines: List[str] = []
    watch_lines.append("Watchlist – abs(Open→Close) becsült mozgás (≥K)\n")

    for row in filtered:
        prev_close, last, open_px, reason = quote_map.get(row.ticker, (None, None, None, None))
        move = pct_change(prev_close, last)

        if move is None:
            continue
        if abs(move) < row.k_threshold:
            continue

        sign = "+" if move >= 0 else ""
        src_str = "árforrás: Yahoo quote"
        line = f"{row.ticker} — Open→Close (becsült): {sign}{move:.2f}% ({src_str})"

        if row.is_position:
            pos_lines.append(line)
        else:
            watch_lines.append(line)

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 3-as riport: Ma nyitástól mostanáig (Open→Most, batch quote, minden pozíció)
# ---------------------------------------------------------------------------

def run_report_3(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    lines: List[str] = []

    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")

    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most) — CEST\n")
    lines.append(
        f"Vizsgált ablak (CEST): mai USA nyitás (15:30) → lekérdezés időpontja ({today_str})\n"
    )
    lines.append("_Árforrás: Yahoo Finance quote (v7 – regularMarketOpen → regularMarketPrice)_\n")

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    ticker_list = [r.ticker for r in filtered]
    quote_map = fetch_quotes_batch(ticker_list)

    # Lefedettség a quote alapján
    status_map: Dict[str, TickerStatus] = {}
    ok_count = 0
    for row in filtered:
        prev_close, last, open_px, reason = quote_map.get(row.ticker, (None, None, None, None))
        if reason is None and open_px is not None and last is not None:
            status_map[row.ticker] = TickerStatus(ok=True)
            ok_count += 1
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason or "no_open_or_last")

    total = len(filtered)
    lines.append(build_coverage_block(status_map))

    # HA GYAKORLATILAG MINDEN RATE_LIMITED → inkább rövid, őszinte üzenet
    if ok_count == 0:
        lines.append(
            "\nA 3-as intranapi riport ma **nem értelmezhető**, mert a Yahoo Finance "
            "quote API-ja minden tickerre `rate_limited` (429 Too Many Requests) státuszt adott. "
            "Ilyenkor az Open→Most mozgásokra nincs megbízható adat ebből a forrásból.\n"
        )
        return "\n".join(lines)

    # ha nem 0, akkor jöhet a „normál” folytatás (makró + darabszámos + watchlist)
    lines.append(build_macro_block_1(macro))
    
    # Darabszámos – MINDEN pozíció listázása
    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – Ma nyitástól mostanáig (Open→Most)\n")

    # Watchlist – csak ahol ≥K az Open→Most
    watch_lines: List[str] = []
    watch_lines.append("### Watchlist – Open→Most mozgások (csak ha ≥K vagy anyagilag lényeges hír)\n")

    for row in filtered:
        prev_close, last, open_px, reason = quote_map.get(row.ticker, (None, None, None, None))
        open_most = pct_change(open_px, last)

        def fmt_open_most(value: Optional[float]) -> str:
            if value is None:
                return "Open→Most: n/a"
            sign = "+" if value >= 0 else ""
            return f"Open→Most: {sign}{value:.2f}%"

        open_most_str = fmt_open_most(open_most)
        src_base = "árforrás: Yahoo quote"

        if row.is_position:
            # MINDEN darabszámos pozíció
            if open_most is None:
                if reason:
                    reason_str = f"Hiányzó intranapi adat (oka: {reason})."
                else:
                    reason_str = "Hiányzó intranapi adat (nincs értelmezhető Open→Most)."
            elif abs(open_most) >= row.k_threshold:
                reason_str = "Érdemi intranapi elmozdulás (≥K) nyitáshoz képest."
            else:
                reason_str = "Mérsékelt intranapi mozgás, egyelőre nincs küszöb feletti elmozdulás."

            # ha volt hiba, jelezzük a forrásnál is
            if reason:
                src_str = f"{src_base} (hiba: {reason})"
            else:
                src_str = src_base

            pos_lines.append(f"{row.ticker} — {open_most_str} — {reason_str} ({src_str})")
        else:
            # Watchlist: csak jelzésnél (≥K)
            has_signal = open_most is not None and abs(open_most) >= row.k_threshold
            if not has_signal:
                continue
            reason_str = "Watchlisten is érdemi intranapi mozgás (≥K) nyitáshoz képest."

            if reason:
                src_str = f"{src_base} (hiba: {reason})"
            else:
                src_str = src_base

            watch_lines.append(f"{row.ticker} — {open_most_str} — {reason_str} ({src_str})")

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
        help="Politika/FED/makró szöveg (Trump-napihír + 1–4 mondat, az 1-es/3-as riporthoz)",
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
        report_text = run_report_3(tickers, macro=args.macro)
    else:
        raise ValueError(f"Ismeretlen riport: {args.report}")

    if args.summary:
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(report_text)
    else:
        print(report_text)


if __name__ == "__main__":
    main()
