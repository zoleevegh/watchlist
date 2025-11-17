#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
report_runner.py
-----------------

Egységes runner script az 1/2/3-as riportokhoz.

#1: AH + PM (pre/post) – árforrás: Yahoo Finance quote (v7 – preMarketPrice/postMarketPrice, previousClose)
#2: Tegnapi Open→Close – árforrás: Yahoo Finance quote (v7 – previousClose → last)
#3: Ma Open→Most – árforrás: Yahoo Finance quote (v7 – regularMarketOpen → regularMarketPrice)

FIGYELEM:
- #1 jelenleg csak a DARABSZÁMOS pozíciókra fut (hard stop logika, kevesebb Yahoo hívás).
- A teljes watchlistet továbbra is a CSV-ből olvassa, de #1 csak a pozíciókat kérdezi le.
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

SCRIPT_VERSION = "1.7.1-yahoo-v7-prepost-positions-only"


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
# Közös helper-ek
# ---------------------------------------------------------------------------

def pct_change(base: Optional[float], new: Optional[float]) -> Optional[float]:
    if base is None or new is None or base == 0:
        return None
    return (new - base) / base * 100.0


# v7 quote batch helper – minden riport ezt használja
def fetch_quotes_batch_v7(
    tickers: List[str],
    batch_size: int = 20,
    sleep_sec: float = 2.0,
) -> Dict[str, Tuple[
    Optional[float],  # previousClose
    Optional[float],  # last (regularMarketPrice)
    Optional[float],  # regularMarketOpen
    Optional[float],  # preMarketPrice
    Optional[float],  # postMarketPrice
    Optional[str],    # error_reason
]]:
    """
    Yahoo quote API batch-ben, v7-es endpointtal.

    Visszaad:
        {
          "AAPL": (previousClose, last, regularOpen, preMarketPrice, postMarketPrice, error_reason),
          ...
        }

    Árforrás: https://query1.finance.yahoo.com/v7/finance/quote?symbols=...
    Rate limit kímélés:
      - kisebb batch_size (20),
      - batch-ek között sleep.
    """
    results: Dict[str, Tuple[
        Optional[float],
        Optional[float],
        Optional[float],
        Optional[float],
        Optional[float],
        Optional[str],
    ]] = {}

    if not tickers:
        return results

    for t in tickers:
        results[t] = (None, None, None, None, None, "not_fetched")

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        symbols_str = ",".join(batch)
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_str}&region=US&lang=en-US"

        try:
            resp = requests.get(
                url,
                timeout=10,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0 Safari/537.36"
                    )
                },
            )
        except Exception as e:
            for t in batch:
                results[t] = (None, None, None, None, None, f"network_error: {e}")
            print(f"[WARN] fetch_quotes_batch_v7 network error for {symbols_str}: {e}", file=sys.stderr)
            time.sleep(sleep_sec)
            continue

        if resp.status_code == 429:
            for t in batch:
                results[t] = (None, None, None, None, None, "rate_limited")
            print(f"[WARN] fetch_quotes_batch_v7 rate limited (429) for {symbols_str}", file=sys.stderr)
            time.sleep(sleep_sec * 2)
            continue

        if resp.status_code == 401:
            for t in batch:
                results[t] = (None, None, None, None, None, "http_401")
            print(f"[WARN] fetch_quotes_batch_v7 unauthorized (401) for {symbols_str}", file=sys.stderr)
            time.sleep(sleep_sec)
            continue

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            for t in batch:
                results[t] = (None, None, None, None, None, f"http_error: {e}")
            print(f"[WARN] fetch_quotes_batch_v7 HTTP error for {symbols_str}: {e}", file=sys.stderr)
            time.sleep(sleep_sec)
            continue

        try:
            data = resp.json()
            qlist = data.get("quoteResponse", {}).get("result", []) or []
        except Exception as e:
            for t in batch:
                results[t] = (None, None, None, None, None, f"parse_error: {e}")
            print(f"[WARN] fetch_quotes_batch_v7 parse error for {symbols_str}: {e}", file=sys.stderr)
            time.sleep(sleep_sec)
            continue

        seen_in_batch = set()
        for q in qlist:
            sym = q.get("symbol")
            if not sym:
                continue

            prev_close = q.get("regularMarketPreviousClose")
            last = q.get("regularMarketPrice")
            open_px = q.get("regularMarketOpen")
            pre_px = q.get("preMarketPrice")
            post_px = q.get("postMarketPrice")

            results[sym] = (prev_close, last, open_px, pre_px, post_px, None)
            seen_in_batch.add(sym)

        for t in batch:
            if t not in seen_in_batch:
                _, _, _, _, _, reason = results.get(t, (None, None, None, None, None, None))
                if reason is None or reason == "not_fetched":
                    results[t] = (None, None, None, None, None, "no_quote_result")

        time.sleep(sleep_sec)

    return results


# ---------------------------------------------------------------------------
# 1-es riport: AH + PM (jelenleg csak darabszámos pozíciókra)
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
        "_Árforrás: Yahoo Finance quote (v7 – preMarketPrice/postMarketPrice, "
        "regularMarketPreviousClose alapú százalékos mozgás)_\n"
    )

    # Csak darabszámos pozíciók → kevesebb Yahoo hívás, kisebb rate limit esély
    positions = [r for r in tickers if r.is_position and r.ticker != "PKN.WA"]

    if not positions:
        lines.append("Lefedettség: NINCS DARABSZÁMOS POZÍCIÓ – az 1-es riport jelenleg csak a pozíciókra fut.\n")
        return "\n".join(lines)

    ticker_list = [r.ticker for r in positions]
    quote_map = fetch_quotes_batch_v7(ticker_list)

    # Lefedettség – TickerStatus a darabszámosokra
    status_map: Dict[str, TickerStatus] = {}
    ok_count = 0
    for row in positions:
        prev_close, last, open_px, pre_px, post_px, reason = quote_map.get(
            row.ticker, (None, None, None, None, None, "no_data")
        )

        # akkor tekintjük "ok"-nak, ha van previousClose ÉS van legalább pre vagy post ár
        if prev_close is not None and (pre_px is not None or post_px is not None) and reason is None:
            status_map[row.ticker] = TickerStatus(ok=True)
            ok_count += 1
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason or "no_pre_or_post")

    # Lefedettség blokk (csak a pozíciókra)
    lines.append(build_coverage_block(status_map))

    # Ha MINDEN darabszámosnál hibás a pre/post lekérdezés → értelmezhetetlen #1
    if ok_count == 0:
        lines.append(
            "\nA #1 AH/PM riport ma **nem értelmezhető**, mert a Yahoo Finance quote (v7) "
            "végpont a darabszámos pozícióknál nem adott használható pre/post árat "
            "(pl. rate limit / hiányzó preMarketPrice/postMarketPrice). "
            "Ilyenkor az after-hours és premarket mozgásokra ebből a forrásból nincs megbízható adat.\n"
        )
        return "\n".join(lines)

    # Politika/FED blokk
    lines.append(build_macro_block_1(macro))

    # Darabszámos tickerek – részletes lista
    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – After-hours & Premarket mozgások\n")

    for row in positions:
        prev_close, last, open_px, pre_px, post_px, reason = quote_map.get(
            row.ticker, (None, None, None, None, None, None)
        )

        ah_pct = pct_change(prev_close, post_px)  # AH: previousClose → postMarketPrice
        pm_pct = pct_change(prev_close, pre_px)   # PM: previousClose → preMarketPrice

        def fmt_pct(label: str, value: Optional[float]) -> str:
            if value is None:
                return f"{label} n/a"
            sign = "+" if value >= 0 else ""
            return f"{label} {sign}{value:.2f}%"

        ah_str = fmt_pct("AH", ah_pct)
        pm_str = fmt_pct("PM", pm_pct)

        # Indoklás (1–2 mondat, biblia-kompatibilis rövid narratíva)
        if ah_pct is None and pm_pct is None:
            if reason:
                reason_str = f"Hiányzó AH/PM adat (oka: {reason})."
            else:
                reason_str = "Hiányzó AH/PM adat (nincs értelmezhető pre/post ár)."
        else:
            has_signal = (
                (ah_pct is not None and abs(ah_pct) >= row.k_threshold) or
                (pm_pct is not None and abs(pm_pct) >= row.k_threshold)
            )
            if has_signal:
                reason_str = "Érdemi AH/PM elmozdulás (≥K) a záróárhoz képest."
            else:
                reason_str = "Egyelőre nincs küszöb feletti AH/PM elmozdulás."

        pos_lines.append(
            f"{row.ticker} — {ah_str} | {pm_str} — {reason_str} "
            "(árforrás: Yahoo quote/v7 pre/post)"
        )

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")

    # Hírek – egyelőre üres váz (később Yahoo + extra forrás mix)
    yahoo_news: List[NewsItem] = []
    extra_news: List[NewsItem] = []
    all_news = merge_news_sources(yahoo_news, extra_news)

    news_block = build_news_block_1(all_news)
    if news_block:
        lines.append(news_block)

    # Közeli katalizátorok – váz, későbbre
    catalysts: List[UpcomingCatalyst] = []
    catalyst_block = build_catalyst_block_1(catalysts)
    if catalyst_block:
        lines.append(catalyst_block)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 2-es riport: Tegnapi Open→Close (egyszerűsített)
# ---------------------------------------------------------------------------

def run_report_2(
    tickers: List[TickerRow],
) -> str:
    lines: List[str] = []
    lines.append("#2 – Tegnapi nyitástól zárásig (Open→Close) – egyszerűsített\n")
    lines.append("_Árforrás: Yahoo Finance quote (v7 – previousClose → regularMarketPrice)_\n")

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    ticker_list = [r.ticker for r in filtered]
    quote_map = fetch_quotes_batch_v7(ticker_list)

    pos_lines: List[str] = []
    pos_lines.append("Darabszámos tickerek – abs(Open→Close) becsült mozgás (≥K)\n")

    watch_lines: List[str] = []
    watch_lines.append("Watchlist – abs(Open→Close) becsült mozgás (≥K)\n")

    for row in filtered:
        prev_close, last, open_px, pre_px, post_px, reason = quote_map.get(
            row.ticker, (None, None, None, None, None, None)
        )

        move = pct_change(prev_close, last)
        if move is None or abs(move) < row.k_threshold:
            continue

        sign = "+" if move >= 0 else ""
        line = f"{row.ticker} — Open→Close (becsült): {sign}{move:.2f}% (árforrás: Yahoo quote/v7)"

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
    quote_map = fetch_quotes_batch_v7(ticker_list)

    # Lefedettség a quote alapján
    status_map: Dict[str, TickerStatus] = {}
    ok_count = 0
    for row in filtered:
        prev_close, last, open_px, pre_px, post_px, reason = quote_map.get(
            row.ticker, (None, None, None, None, None, None)
        )
        if reason is None and open_px is not None and last is not None:
            status_map[row.ticker] = TickerStatus(ok=True)
            ok_count += 1
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason or "no_open_or_last")

    lines.append(build_coverage_block(status_map))

    if ok_count == 0:
        lines.append(
            "\nA 3-as intranapi riport ma **nem értelmezhető**, mert a Yahoo Finance "
            "quote API-ja minden tickerre hibát adott (pl. rate limit / hiányzó open/last). "
            "Ilyenkor az Open→Most mozgásokra nincs megbízható adat ebből a forrásból.\n"
        )
        return "\n".join(lines)

    lines.append(build_macro_block_1(macro))

    # Darabszámos – MINDEN pozíció listázása
    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – Ma nyitástól mostanáig (Open→Most)\n")

    # Watchlist – csak ahol ≥K az Open→Most
    watch_lines: List[str] = []
    watch_lines.append("### Watchlist – Open→Most mozgások (csak ha ≥K)\n")

    for row in filtered:
        prev_close, last, open_px, pre_px, post_px, reason = quote_map.get(
            row.ticker, (None, None, None, None, None, None)
        )

        open_most = pct_change(open_px, last)

        def fmt_open_most(value: Optional[float]) -> str:
            if value is None:
                return "Open→Most: n/a"
            sign = "+" if value >= 0 else ""
            return f"Open→Most: {sign}{value:.2f}%"

        open_most_str = fmt_open_most(open_most)
        src_base = "árforrás: Yahoo quote/v7"

        if row.is_position:
            if open_most is None:
                if reason:
                    reason_str = f"Hiányzó intranapi adat (oka: {reason})."
                else:
                    reason_str = "Hiányzó intranapi adat (nincs értelmezhető Open→Most)."
            elif abs(open_most) >= row.k_threshold:
                reason_str = "Érdemi intranapi elmozdulás (≥K) nyitáshoz képest."
            else:
                reason_str = "Mérsékelt intranapi mozgás, egyelőre nincs küszöb feletti elmozdulás."

            if reason:
                src_str = f"{src_base} (hiba: {reason})"
            else:
                src_str = src_base

            pos_lines.append(f"{row.ticker} — {open_most_str} — {reason_str} ({src_str})")
        else:
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
        # 2-eshez jelenleg nincs makróblokk / hírrész külön
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
