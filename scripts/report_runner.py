#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
report_runner.py
-----------------

Egységes runner script az 1/2/3-as riportokhoz.

#1: AH + PM (chart 2d/5m, includePrePost) – ELSŐDLEGESEN Yahoo quote (v7 pre/post),
    chart (v8) csak fallback, CSAK darabszámos tickerekre.
#2: Tegnapi Open→Close (batch quote) – árforrás: Yahoo Finance quote (v7)
#3: Ma Open→Most (batch quote, regularMarketOpen→regularMarketPrice) – árforrás: Yahoo Finance quote (v7)

Script verzió: 1.5.0-positions-quote-first
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

SCRIPT_VERSION = "1.5.0-positions-quote-first"

# Egy normális, böngésző-szerű UA – ezzel látszólag kevésbé tilt a Yahoo
YF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
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
# Segédfüggvények – százalékváltozás
# ---------------------------------------------------------------------------

def pct_change(base: Optional[float], new: Optional[float]) -> Optional[float]:
    if base is None or new is None or base == 0:
        return None
    return (new - base) / base * 100.0


# ---------------------------------------------------------------------------
# Yahoo Finance chart (v8) – fallback AH/PM-hez
# ---------------------------------------------------------------------------

@dataclass
class PriceSnapshot:
    prev_close: Optional[float]
    ah_last: Optional[float]
    pm_last: Optional[float]


def fetch_chart_2d_5m(ticker: str) -> Tuple[Optional[dict], Optional[str]]:
    """
    Nyers chart JSON (v8) 2 nap / 5 perces, includePrePost.
    Hiba esetén (429, network, parse) None + reason-str.
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        "?range=2d&interval=5m&includePrePost=true"
    )
    try:
        resp = requests.get(url, headers=YF_HEADERS, timeout=10)
    except Exception as e:
        return None, f"network_error(chart_v8): {e}"

    if resp.status_code == 429:
        return None, "rate_limited(chart_v8)"

    try:
        resp.raise_for_status()
    except Exception as e:
        return None, f"http_error(chart_v8): {e}"

    try:
        data = resp.json()
    except Exception as e:
        return None, f"parse_error(chart_v8): {e}"

    return data, None


def extract_ah_pm_move_from_chart(ticker: str) -> Tuple[PriceSnapshot, Optional[str]]:
    """
    Fallback a #1 riporthoz: AH + PM sáv utolsó ára chartból.

    - prev_close: előző napi RTH záró (meta.previousClose vagy chartból becsülve)
    - ah_last: AH utolsó ár (RTH záró után 20:00 US-ig)
    - pm_last: PM utolsó ár (mai nap 4:00–9:30 US)
    """
    data, err = fetch_chart_2d_5m(ticker)
    if err is not None or data is None:
        return PriceSnapshot(None, None, None), err

    try:
        result = data["chart"]["result"][0]
        meta = result.get("meta", {})
        timestamps = result.get("timestamp", [])
        quotes = result.get("indicators", {}).get("quote", [{}])[0]
        closes = quotes.get("close", [])

        if not timestamps or not closes:
            return PriceSnapshot(None, None, None), "no_price_data(chart_v8)"

        prev_close = meta.get("previousClose")

        from datetime import datetime as _dt
        dt_list = [_dt.fromtimestamp(ts, tz=US_EASTERN) for ts in timestamps]

        # fallback prev_close, ha meta-ban nincs – előző napi RTH utolsó záró
        if prev_close is None:
            prev_rth_closes = [
                c
                for c, dt in zip(closes, dt_list)
                if 9 <= dt.hour <= 16
            ]
            prev_close = prev_rth_closes[-1] if prev_rth_closes else None

        # AH: előző napi 16:00–20:00 US idő
        ah_prices = [
            c
            for c, dt in zip(closes, dt_list)
            if dt.hour >= 16 and dt.hour < 20
        ]
        ah_last = ah_prices[-1] if ah_prices else None

        # PM: mai nap 4:00–9:30 US idő
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
        return PriceSnapshot(None, None, None), f"parse_error(chart_v8): {e}"


# ---------------------------------------------------------------------------
# Yahoo Finance quote (v7) – közös batch helper #1/#2/#3-hoz
# ---------------------------------------------------------------------------

@dataclass
class QuoteSnapshot:
    prev_close: Optional[float] = None
    last: Optional[float] = None
    open_px: Optional[float] = None
    post_price: Optional[float] = None
    post_change_pct: Optional[float] = None
    pre_price: Optional[float] = None
    pre_change_pct: Optional[float] = None
    error: Optional[str] = None


def fetch_quotes_batch(
    tickers: List[str],
    batch_size: int = 15,
) -> Dict[str, QuoteSnapshot]:
    """
    Yahoo quote API batch-ben (#1/#2/#3 riporthoz).

    Visszaad:
        { "AAPL": QuoteSnapshot(...), ... }

    Elsődleges forrás:
    - regularMarketPreviousClose
    - regularMarketOpen
    - regularMarketPrice
    - postMarketPrice / postMarketChangePercent
    - preMarketPrice / preMarketChangePercent
    """
    results: Dict[str, QuoteSnapshot] = {}
    if not tickers:
        return results

    # deduplikált, de az eredeti sorrendet megtartjuk
    unique: List[str] = list(dict.fromkeys(tickers))

    for t in unique:
        results[t] = QuoteSnapshot(error="no_data_yet")

    for i in range(0, len(unique), batch_size):
        batch = unique[i : i + batch_size]
        symbols_str = ",".join(batch)
        url = (
            "https://query1.finance.yahoo.com/v7/finance/quote"
            f"?symbols={symbols_str}&lang=en-US&region=US"
        )

        try:
            resp = requests.get(url, headers=YF_HEADERS, timeout=10)
        except Exception as e:
            for t in batch:
                results[t].error = f"network_error(quote_v7): {e}"
            # kis pihenő, hogy ne öljük meg a Yahoo-t
            time.sleep(1.2)
            continue

        if resp.status_code == 429:
            for t in batch:
                results[t].error = "rate_limited(quote_v7)"
            # ha 429, akkor is várjunk
            time.sleep(2.0)
            continue

        try:
            resp.raise_for_status()
        except Exception as e:
            for t in batch:
                results[t].error = f"http_error(quote_v7): {e}"
            time.sleep(1.2)
            continue

        try:
            data = resp.json()
            qlist = data.get("quoteResponse", {}).get("result", []) or []
        except Exception as e:
            for t in batch:
                results[t].error = f"parse_error(quote_v7): {e}"
            time.sleep(1.2)
            continue

        seen_in_batch = set()
        for q in qlist:
            sym = q.get("symbol")
            if not sym:
                continue
            if sym not in results:
                results[sym] = QuoteSnapshot()

            snap = results[sym]
            snap.prev_close = q.get("regularMarketPreviousClose")
            snap.last = q.get("regularMarketPrice")
            snap.open_px = q.get("regularMarketOpen")
            snap.post_price = q.get("postMarketPrice")
            snap.post_change_pct = q.get("postMarketChangePercent")
            snap.pre_price = q.get("preMarketPrice")
            snap.pre_change_pct = q.get("preMarketChangePercent")
            snap.error = None
            results[sym] = snap
            seen_in_batch.add(sym)

        for t in batch:
            if t not in seen_in_batch and (results[t].error is None or results[t].error == "no_data_yet"):
                results[t].error = "no_quote_result(quote_v7)"

        # batch-ek között pihenés
        time.sleep(1.2)

    return results


# ---------------------------------------------------------------------------
# 1-es riport: AH + PM (pozíciók, Yahoo quote elsődleges, chart fallback)
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
    lines.append(f"Script verzió: {SCRIPT_VERSION}\n\n")
    lines.append(
        f"Vizsgált ablakok (CEST): AH {prev_day} 22:00 → {today_str} 02:00, "
        f"PM {today_str} 10:00 → 15:30\n\n"
    )
    lines.append(
        "Árforrás: elsődlegesen Yahoo Finance quote (v7 pre/post – "
        "preMarket/postMarket mezők), fallback: Yahoo Finance chart "
        "(v8, 2d/5m, includePrePost).\n"
    )

    # Csak a darabszámos tickerekre fut az 1-es riport, PKN.WA kivéve.
    positions = [
        r for r in tickers
        if r.is_position and r.ticker != "PKN.WA"
    ]

    if not positions:
        lines.append(
            "\nLefedettség: NINCS DARABSZÁMOS POZÍCIÓ – az 1-es riport jelenleg csak "
            "a pozíciókra fut.\n"
        )
        return "\n".join(lines)

    ticker_list = [r.ticker for r in positions]
    quote_map = fetch_quotes_batch(ticker_list)

    # Első kör: csak a quote alapján lefedettség + AH/PM, aztán ahol kell, chart fallback
    status_map: Dict[str, TickerStatus] = {}
    ah_pm_map: Dict[str, Tuple[Optional[float], Optional[float], Optional[str]]] = {}

    valid_cnt = 0

    for row in positions:
        sym = row.ticker
        snap = quote_map.get(sym, QuoteSnapshot(error="missing_from_quote_map"))

        ah_pct: Optional[float] = None
        pm_pct: Optional[float] = None
        reason_parts: List[str] = []

        if snap.error:
            reason_parts.append(snap.error)

        base = snap.prev_close

        # AH – először a direkt % mező, aztán az ár + base
        if snap.post_change_pct is not None:
            ah_pct = float(snap.post_change_pct)
        elif base is not None and snap.post_price is not None:
            ah_pct = pct_change(base, snap.post_price)

        # PM – hasonló logika
        if snap.pre_change_pct is not None:
            pm_pct = float(snap.pre_change_pct)
        elif base is not None and snap.pre_price is not None:
            pm_pct = pct_change(base, snap.pre_price)

        # Ha semmilyen AH/PM nincs quote-ból, akkor chart fallback
        if ah_pct is None and pm_pct is None:
            chart_snap, chart_err = extract_ah_pm_move_from_chart(sym)
            if chart_err:
                reason_parts.append(chart_err)

            # ha a chartból jobb prev_close jön, azt használjuk
            if base is None and chart_snap.prev_close is not None:
                base = chart_snap.prev_close

            if base is not None and ah_pct is None and chart_snap.ah_last is not None:
                ah_pct = pct_change(base, chart_snap.
