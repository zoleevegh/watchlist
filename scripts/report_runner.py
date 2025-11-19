#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
report_runner.py 2.1.6
-----------------------

Egységes runner script az 1/2/3-as riportokhoz.

#1: AH + PM (pre/post-market %) – árforrás: Yahoo chart (v8, meta.pre/postMarketChangePercent)
#2: Tegnapi Open→Close – árforrás: Yahoo quote (v7)
#3: Ma Open→Most – árforrás: Yahoo quote (v7)
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import time
import requests

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
except ImportError:  # Python <3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

BUDAPEST = ZoneInfo("Europe/Budapest")
US_EASTERN = ZoneInfo("America/New_York")

SCRIPT_VERSION = "2.1.7-biblia-yahoo-us-time-chart-meta-prevclose"


HEADERS_YF = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
}

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
        headers = {h.strip().lower(): h for h in (reader.fieldnames or [])}

        def get_col(*names: str) -> Optional[str]:
            for n in names:
                key = n.lower()
                if key in headers:
                    return headers[key]
            return None

        ticker_col = get_col("ticker", "tiker", "symbol")
        qty_col = get_col("darabszám", "darabszam", "db", "qty", "quantity")
        k_col = get_col("k", "minmove", "minmovepct", "min_pct", "küszöb", "kuszob")

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

def pct_change(base: Optional[float], new: Optional[float]) -> Optional[float]:
    if base is None or new is None or base == 0:
        return None
    return (new - base) / base * 100.0

# ---------------------------------------------------------------------------
# Yahoo chart meta alapú AH/PM (#1 riport)
# ---------------------------------------------------------------------------

def fetch_ahpm_chart_batch(
    tickers: List[str],
    delay_sec: float = 1.2,
) -> Dict[str, Tuple[Optional[float], Optional[float], Optional[str]]]:
    """
    Yahoo Finance chart (v8) alapján lekéri az AH/PM százalékokat (#1 riport).

    meta.preMarketChangePercent
    meta.postMarketChangePercent

    Visszaad:
        { "AAPL": (ah_pct, pm_pct, error_or_None), ... }
    """
    results: Dict[str, Tuple[Optional[float], Optional[float], Optional[str]]] = {}
    if not tickers:
        return results

    uniq = list(dict.fromkeys(tickers))
    for t in uniq:
        results[t] = (None, None, None)

    for sym in uniq:
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
            "?range=1d&interval=1m&includePrePost=true"
        )
        try:
            resp = requests.get(url, headers=HEADERS_YF, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            result = data.get("chart", {}).get("result")
            if not result:
                results[sym] = (None, None, "no_chart_result")
                continue
            meta = result[0].get("meta", {})

            ah_raw = meta.get("postMarketChangePercent")
            pm_raw = meta.get("preMarketChangePercent")

            def _norm(v):
                if v is None:
                    return None
                try:
                    return float(v)
                except Exception:
                    return None

            ah_pct = _norm(ah_raw)
            pm_pct = _norm(pm_raw)

            if ah_pct is None and pm_pct is None:
                results[sym] = (None, None, "no_prepost_data")
            else:
                results[sym] = (ah_pct, pm_pct, None)

        except Exception as e:
            results[sym] = (None, None, f"chart_error: {e}")

        time.sleep(delay_sec)

    return results

# ---------------------------------------------------------------------------
# Quote batch helper (#2/#3)
# ---------------------------------------------------------------------------

def fetch_quotes_batch(
    tickers: List[str],
    batch_size: int = 20,
) -> Dict[str, Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]]:
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
            resp = requests.get(url, headers=HEADERS_YF, timeout=10)
        except Exception as e:
            for t in batch:
                results[t] = (None, None, None, f"network_error: {e}")
            time.sleep(2)
            continue

        if resp.status_code == 429:
            for t in batch:
                results[t] = (None, None, None, "rate_limited")
            time.sleep(5)
            continue

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            for t in batch:
                results[t] = (None, None, None, f"http_error: {e}")
            time.sleep(2)
            continue

        try:
            data = resp.json()
            qlist = data.get("quoteResponse", {}).get("result", []) or []
        except Exception as e:
            for t in batch:
                results[t] = (None, None, None, f"parse_error: {e}")
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
                _pc, _l, _o, reason = results.get(t, (None, None, None, None))
                if reason is None:
                    results[t] = (None, None, None, "no_quote_result")

        time.sleep(2)

    return results

# ---------------------------------------------------------------------------
# 1-es riport
# ---------------------------------------------------------------------------

def run_report_1(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    lines: List[str] = []

    now_local = datetime.now(BUDAPEST)
    now_us = now_local.astimezone(US_EASTERN)

    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST\n")
    lines.append(f"Script verzió: {SCRIPT_VERSION}\n")

    today_local = now_local.date()
    prev_local = today_local - timedelta(days=1)
    lines.append(
        f"Vizsgált ablakok (CEST): AH {prev_local} 22:00 → {today_local} 02:00, "
        f"PM {today_local} 10:00 → 15:30\n"
    )
    lines.append(
        "_Árforrás: Yahoo Finance chart (v8 – meta.postMarketChangePercent / "
        "meta.preMarketChangePercent; mindegyik az utolsó RTH záróhoz viszonyított %)_\n"
    )

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]

    status_map: Dict[str, TickerStatus] = {}

    symbols = [r.ticker for r in filtered]
    ahpm_map = fetch_ahpm_chart_batch(symbols)

    us_premarket_started = (now_us.hour > 4) or (now_us.hour == 4 and now_us.minute >= 0)

    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – After-hours & Premarket mozgások\n")

    watch_lines: List[str] = []
    watch_lines.append("### Watchlist – After-hours & Premarket mozgások (csak ha ≥K)\n")

    for row in filtered:
        ah_pct, pm_pct, err = ahpm_map.get(row.ticker, (None, None, "no_chart_entry"))
        if err is None and ((ah_pct is not None) or (pm_pct is not None)):
            status_map[row.ticker] = TickerStatus(ok=True)
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=err or "no_prepost_data")

    lines.append(build_coverage_block(status_map))
    lines.append(build_macro_block_1(macro))

    for row in filtered:
        ah_pct, pm_pct, err = ahpm_map.get(row.ticker, (None, None, "no_chart_entry"))

        if not us_premarket_started:
            effective_pm = None
        else:
            effective_pm = pm_pct

        def fmt_pct(label: str, value: Optional[float]) -> str:
            if label == "PM" and not us_premarket_started:
                return "PM: n/a (a mai premarket még nem indult el US/Eastern)"
            if value is None:
                return f"{label}: n/a"
            sign = "+" if value >= 0 else ""
            return f"{label} {sign}{value:.2f}%"

        ah_str = fmt_pct("AH", ah_pct)
        pm_str = fmt_pct("PM", effective_pm)

        has_signal = (
            (ah_pct is not None and abs(ah_pct) >= row.k_threshold) or
            (effective_pm is not None and abs(effective_pm) >= row.k_threshold)
        )

        src_str = "árforrás: Yahoo chart/v8 meta (pre-/post-market %)"

        if row.is_position:
            if err is not None and (ah_pct is None and effective_pm is None):
                reason = f"Hiányzó vagy nem értelmezhető AH/PM adat (oka: {err})."
            elif has_signal:
                reason = "Érdemi AH/PM elmozdulás (≥K) az utolsó RTH záróhoz képest."
            else:
                reason = "Egyelőre nincs küszöb feletti AH/PM elmozdulás."
            pos_lines.append(f"{row.ticker} — {ah_str} | {pm_str} — {reason} ({src_str})")
        else:
            if not has_signal:
                continue
            if err is not None and (ah_pct is None and effective_pm is None):
                reason = (
                    "Watchlisten is érdemi AH/PM elmozdulás (≥K), "
                    f"de adatminőségi hiba: {err}."
                )
            else:
                reason = (
                    "Watchlisten is érdemi AH/PM elmozdulás (≥K) "
                    "az utolsó RTH záróhoz képest."
                )
            watch_lines.append(f"{row.ticker} — {ah_str} | {pm_str} — {reason} ({src_str})")

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

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
# 2-es riport
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

        if move is None or abs(move) < row.k_threshold:
            continue

        sign = "+" if move >= 0 else ""
        src_str = "árforrás: Yahoo quote/v7"
        if reason:
            src_str += f" (hiba: {reason})"

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
# 3-as riport
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

    status_map: Dict[str, TickerStatus] = {}
    ok_count = 0
    for row in filtered:
        _prev_close, last, open_px, reason = quote_map.get(row.ticker, (None, None, None, None))
        if reason is None and open_px is not None and last is not None:
            status_map[row.ticker] = TickerStatus(ok=True)
            ok_count += 1
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason or "no_open_or_last")

    lines.append(build_coverage_block(status_map))

    if ok_count == 0:
        lines.append(
            "\nA 3-as intranapi riport ma **nem értelmezhető**, mert a Yahoo Finance "
            "quote API-ja minden tickerre hibát adott (rate limit vagy hiányzó Open/Last).\n"
        )
        return "\n".join(lines)

    lines.append(build_macro_block_1(macro))

    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – Ma nyitástól mostanáig (Open→Most)\n")

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
        src_base = "árforrás: Yahoo quote/v7"
        if reason:
            src_str = f"{src_base} (hiba: {reason})"
        else:
            src_str = src_base

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

            pos_lines.append(f"{row.ticker} — {open_most_str} — {reason_str} ({src_str})")
        else:
            has_signal = open_most is not None and abs(open_most) >= row.k_threshold
            if not has_signal:
                continue
            reason_str = "Watchlisten is érdemi intranapi mozgás (≥K) nyitáshoz képest."
            watch_lines.append(f"{row.ticker} — {open_most_str} — {reason_str} ({src_str})")

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

    return "\n".join(lines)

# ---------------------------------------------------------------------------
# CLI
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
        help="GitHub Actions GITHUB_STEP_SUMMARY elérési útja",
    )
    parser.add_argument(
        "--macro",
        required=False,
        help="Politika/FED/makró szöveg az 1-es/3-as riporthoz",
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
