#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
report_runner.py
-----------------

Egységes runner script az 1/2/3-as riportokhoz, „biblia” szerinti struktúrával.

#1: AH + PM (chart 2d/5m, includePrePost) – elsődleges árforrás: Yahoo chart (v8)
#2: Tegnapi Open→Close – elsődleges árforrás: Yahoo quote (v7), fallback: Stooq (EOD)
#3: Ma Open→Most – elsődleges árforrás: Yahoo quote (v7), fallback: Stooq (EOD)

Yahoo-hiba / rate limit esetén:
- #1: csak jelzem a lefedettségben (nincs értelmes AH/PM fallback).
- #2/#3: megpróbálok EOD árat hozni Stooq-ról; ha az is sikertelen, a ticker HIÁNYOS marad.
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


# ---------------------------------------------------------------------------
# CSV beolvasása
# ---------------------------------------------------------------------------

@dataclass
class TickerRow:
    ticker: str
    quantity: Optional[float]  # None = watchlist
    k_threshold: float         # min. abs % (3% default)
    is_position: bool          # True = darabszámos pozíció


def _parse_float(val: Optional[str]) -> Optional[float]:
    if val is None:
        return None
    s = val.strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_tickers_from_csv(path: str, default_k: float = 3.0) -> List[TickerRow]:
    """
    MASTER CSV beolvasása.
    Elvárt oszlopok (kis/nagybetű, ékezet nem számít):
      - Ticker / Tiker / Symbol
      - Darabszám / Db / Qty / Quantity  (opcionális – ha üres, watchlist)
      - K / MinMove / MinMovePct / Min_Pct (opcionális, default = 3.0)
    """
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
        qty_col = get_col("darabszám", "db", "qty", "quantity")
        k_col = get_col("k", "minmove", "minmovepct", "min_pct")

        if not ticker_col:
            raise ValueError("A CSV-ben nincs 'Ticker' (vagy ekvivalens) oszlop.")

        for r in reader:
            t = (r.get(ticker_col) or "").strip().upper()
            if not t:
                continue

            qty_val = _parse_float(r.get(qty_col)) if qty_col else None
            k_val = _parse_float(r.get(k_col)) if k_col else None
            if k_val is None or k_val <= 0:
                k_val = default_k

            is_position = qty_val is not None and qty_val > 0.0

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


def _fetch_chart_2d_5m_yahoo(ticker: str) -> dict:
    """
    Yahoo chart (v8) – 2 nap / 5 perces gyertyák, pre/post piaccal.
    Csak #1 riporthoz használjuk (AH + PM).
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        "?range=2d&interval=5m&includePrePost=true"
    )
    resp = requests.get(url, timeout=10)
    # itt direkt nem hívunk raise_for_status()-t, hogy 429-et felismerjük
    if resp.status_code == 429:
        raise requests.exceptions.HTTPError(
            f"429 Too Many Requests for {ticker}", response=resp
        )
    resp.raise_for_status()
    return resp.json()


def extract_ah_pm_move(ticker: str) -> Tuple[PriceSnapshot, Optional[str]]:
    """
    #1 riporthoz: AH + PM sáv utolsó ára, previousClose alapján.
    Árforrás: Yahoo Finance chart (v8).
    Hiba / rate limit esetén reason-ben visszaadja az okot.
    """
    try:
        data = _fetch_chart_2d_5m_yahoo(ticker)
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if getattr(e, "response", None) is not None else None
        if status == 429:
            return PriceSnapshot(None, None, None), "rate_limited"
        return PriceSnapshot(None, None, None), f"http_error:{status}"
    except Exception as e:
        return PriceSnapshot(None, None, None), f"fetch_error:{e}"

    try:
        result = data["chart"]["result"][0]
        meta = result.get("meta", {})
        timestamps = result.get("timestamp", []) or []
        quotes = result.get("indicators", {}).get("quote", [{}])[0]
        closes = quotes.get("close", []) or []

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

        # AH: 16:00–20:00 US idő
        ah_prices = [
            c
            for c, dt in zip(closes, dt_list)
            if 16 <= dt.hour < 20
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
        return PriceSnapshot(None, None, None), f"parse_error:{e}"


def pct_change(base: Optional[float], new: Optional[float]) -> Optional[float]:
    if base is None or new is None or base == 0:
        return None
    return (new - base) / base * 100.0


# ---------------------------------------------------------------------------
# Yahoo quote + Stooq fallback (#2 és #3 riporthoz)
# ---------------------------------------------------------------------------

def fetch_quotes_batch_yahoo(
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
        results[t] = (None, None, None, "not_fetched")

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        symbols_str = ",".join(batch)
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbols_str}"

        try:
            resp = requests.get(url, timeout=10)
        except Exception as e:
            for t in batch:
                results[t] = (None, None, None, f"network_error:{e}")
            print(f"[WARN] yahoo batch network error for {symbols_str}: {e}", file=sys.stderr)
            time.sleep(2)
            continue

        if resp.status_code == 429:
            for t in batch:
                results[t] = (None, None, None, "rate_limited")
            print(f"[WARN] yahoo batch rate limited (429) for {symbols_str}", file=sys.stderr)
            time.sleep(5)
            continue

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status = resp.status_code
            for t in batch:
                results[t] = (None, None, None, f"http_error:{status}")
            print(f"[WARN] yahoo batch HTTP error {status} for {symbols_str}: {e}", file=sys.stderr)
            time.sleep(2)
            continue

        try:
            data = resp.json()
            qlist = data.get("quoteResponse", {}).get("result", []) or []
        except Exception as e:
            for t in batch:
                results[t] = (None, None, None, f"parse_error:{e}")
            print(f"[WARN] yahoo batch parse error for {symbols_str}: {e}", file=sys.stderr)
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
                if reason is None or reason == "not_fetched":
                    results[t] = (None, None, None, "no_quote_result")

        # batch-ek között pihenjünk
        time.sleep(2)

    return results


def fetch_quote_stooq_single(ticker: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    """
    Stooq EOD fallback (napi adatok).
    URL minta: https://stooq.com/q/d/l/?s=AAPL.US&i=d
    Visszaad: (prev_close, last_close, last_open, error_reason_or_None)
    """
    # nagyon egyszerű US mapping – a legtöbb ticker .US-sel működik
    symbol = f"{ticker.lower()}.us"
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"

    try:
        resp = requests.get(url, timeout=10)
    except Exception as e:
        return None, None, None, f"stooq_network_error:{e}"

    if resp.status_code != 200:
        return None, None, None, f"stooq_http_error:{resp.status_code}"

    text = resp.text.strip()
    lines = text.splitlines()
    if len(lines) < 2:
        return None, None, None, "stooq_no_rows"

    reader = csv.DictReader(lines)
    data_rows = list(reader)
    if not data_rows:
        return None, None, None, "stooq_no_rows_parsed"

    last_row = data_rows[-1]
    prev_row = data_rows[-2] if len(data_rows) >= 2 else None

    def _f(row: Optional[dict], key: str) -> Optional[float]:
        if row is None:
            return None
        v = row.get(key)
        if v is None:
            return None
        v = v.strip()
        if not v:
            return None
        try:
            return float(v)
        except ValueError:
            return None

    last_close = _f(last_row, "Close")
    last_open = _f(last_row, "Open")
    prev_close = _f(prev_row, "Close")

    if last_close is None and last_open is None:
        return None, None, None, "stooq_no_price"

    return prev_close, last_close, last_open, None


def fetch_quotes_batch_with_fallback(
    tickers: List[str],
) -> Dict[str, Tuple[Optional[float], Optional[float], Optional[float], Optional[str], str]]:
    """
    Kombinált árforrás (#2/#3):
      1) Yahoo quote (v7)
      2) fallback: Stooq EOD (q/d/l)

    Visszaad:
      {ticker: (prev_close, last, open_px, error_reason, source_tag)}

    source_tag:
      - "yahoo"  – Yahoo adat
      - "stooq"  – Yahoo hibás → Stooq fallback
      - ""       – egyikből sem érkezett értelmes adat
    """
    results: Dict[str, Tuple[Optional[float], Optional[float], Optional[float], Optional[str], str]] = {}
    if not tickers:
        return results

    yahoo_map = fetch_quotes_batch_yahoo(tickers)

    for t in tickers:
        prev_close, last, open_px, reason = yahoo_map.get(t, (None, None, None, "not_fetched"))

        if last is not None or open_px is not None:
            # Yahoo adat elég jó, akkor ezt használjuk
            results[t] = (prev_close, last, open_px, None, "yahoo")
            continue

        # próbálkozunk Stooq EOD fallback-kel
        fb_prev, fb_last, fb_open, fb_reason = fetch_quote_stooq_single(t)

        if fb_last is not None or fb_open is not None:
            results[t] = (fb_prev, fb_last, fb_open, None, "stooq")
        else:
            combined_reason = fb_reason or reason or "no_data"
            results[t] = (None, None, None, combined_reason, "")

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
    lines.append("_Árforrás: elsődlegesen Yahoo Finance chart (v8, 2d/5m, includePrePost)._  \n")

    # PKN.WA alapértelmezetten kimarad
    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    status_map: Dict[str, TickerStatus] = {}
    price_map: Dict[str, PriceSnapshot] = {}

    for row in filtered:
        snap, reason = extract_ah_pm_move(row.ticker)
        price_map[row.ticker] = snap
        if reason is None:
            status_map[row.ticker] = TickerStatus(ok=True)
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason)

    # Lefedettség blokk (biblia szerint) – de ha MINDEN rate_limited, akkor ne falat írjunk
    all_rate_limited = (
        len(status_map) > 0 and
        all((not s.ok and (s.reason or "").startswith("rate_limited") for s in status_map.values()))
    )

    if all_rate_limited:
        lines.append(
            "Lefedettség: HIÁNYOS – a Yahoo Finance chart API (v8) "
            "gyakorlatilag minden tickerre 429 (rate_limited) státuszt adott, "
            "ezért az AH/PM riport ma nem értelmezhető.\n"
        )
        # és kész, nem soroljuk fel a 100 tickert
    else:
        lines.append(build_coverage_block(status_map))

    # Politika/FED blokk (makró)
    macro_block = build_macro_block_1(macro)
    if macro_block:
        lines.append(macro_block)

    # Darabszámos / watchlist szekciók
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

        src_str = "árforrás: Yahoo chart (v8)"

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
# 2-es riport: Tegnapi Open→Close (batch quote, Yahoo + Stooq fallback)
# ---------------------------------------------------------------------------

def run_report_2(
    tickers: List[TickerRow],
) -> str:
    lines: List[str] = []
    lines.append("## #2 – Tegnapi nyitástól zárásig (Open→Close) — CEST\n")
    lines.append("_Árforrás: elsődlegesen Yahoo Finance quote (v7 – previousClose → last), "
                 "fallback: Stooq EOD (q/d/l)._  \n")
    # … eddig:
    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    ticker_list = [r.ticker for r in filtered]
    quote_map = fetch_quotes_batch_with_fallback(ticker_list)

    # Forrásbontás: hány ticker jött Yahoo-ról és hány Stooq-fallbackről
    yahoo_ok = sum(
        1
        for row in filtered
        if quote_map.get(row.ticker, (None, None, None, "no_data", ""))[4] == "yahoo"
           and quote_map[row.ticker][3] is None
    )
    stooq_ok = sum(
        1
        for row in filtered
        if quote_map.get(row.ticker, (None, None, None, "no_data", ""))[4] == "stooq"
           and quote_map[row.ticker][3] is None
    )
    lines.append(
        f"Árforrás bontás: Yahoo = {yahoo_ok} ticker, Stooq fallback = {stooq_ok} ticker.\n"
    )


    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    ticker_list = [r.ticker for r in filtered]
    quote_map = fetch_quotes_batch_with_fallback(ticker_list)

    # Lefedettség blokk
    status_map: Dict[str, TickerStatus] = {}
    for row in filtered:
        prev_close, last, open_px, err, source_tag = quote_map.get(
            row.ticker, (None, None, None, "no_data", "")
        )
        if err is None and last is not None and prev_close is not None:
            status_map[row.ticker] = TickerStatus(ok=True)
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=err or "no_prev_or_last")

    lines.append(build_coverage_block(status_map))

    pos_lines: List[str] = []
    pos_lines.append("Darabszámos tickerek – abs(Open→Close) becsült mozgás (≥K)\n")

    watch_lines: List[str] = []
    watch_lines.append("Watchlist – abs(Open→Close) becsült mozgás (≥K)\n")

    for row in filtered:
        prev_close, last, open_px, err, source_tag = quote_map.get(
            row.ticker, (None, None, None, None, "")
        )
        move = pct_change(prev_close, last)

        if move is None or abs(move) < row.k_threshold:
            continue

        sign = "+" if move >= 0 else ""
        if source_tag == "yahoo":
            src_str = "árforrás: Yahoo quote"
        elif source_tag == "stooq":
            src_str = "árforrás: Stooq EOD (Yahoo fallback)"
        else:
            src_str = "árforrás: ismeretlen/hiba"

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
# 3-as riport: Ma nyitástól mostanáig (Open→Most, Yahoo + Stooq fallback)
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
    lines.append("_Árforrás: elsődlegesen Yahoo Finance quote (v7 – regularMarketOpen → "
                 "regularMarketPrice), fallback: Stooq EOD (q/d/l)._  \n")

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    ticker_list = [r.ticker for r in filtered]
    quote_map = fetch_quotes_batch_with_fallback(ticker_list)
        ticker_list = [r.ticker for r in filtered]
    quote_map = fetch_quotes_batch_with_fallback(ticker_list)

    # Forrásbontás: hány ticker jött Yahoo-ról és hány Stooq-fallbackről
    yahoo_ok = sum(
        1
        for row in filtered
        if quote_map.get(row.ticker, (None, None, None, "no_data", ""))[4] == "yahoo"
           and quote_map[row.ticker][3] is None
    )
    stooq_ok = sum(
        1
        for row in filtered
        if quote_map.get(row.ticker, (None, None, None, "no_data", ""))[4] == "stooq"
           and quote_map[row.ticker][3] is None
    )
    lines.append(
        f"Árforrás bontás: Yahoo = {yahoo_ok} ticker, Stooq fallback = {stooq_ok} ticker.\n"
    )

    # Lefedettség a kombinált quote alapján
    status_map: Dict[str, TickerStatus] = {}
    ok_count = 0
    for row in filtered:
        prev_close, last, open_px, err, source_tag = quote_map.get(
            row.ticker, (None, None, None, "no_data", "")
        )
        if err is None and open_px is not None and last is not None:
            status_map[row.ticker] = TickerStatus(ok=True)
            ok_count += 1
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=err or "no_open_or_last")

    lines.append(build_coverage_block(status_map))

    # Ha gyakorlatilag semmire nincs adat (pl. globális rate limit), akkor őszintén jelezzük
    if ok_count == 0:
        lines.append(
            "\nA 3-as intranapi riport ma **nem értelmezhető**, mert sem a Yahoo Finance quote "
            "API-jából, sem a Stooq EOD fallback-ből nem érkezett használható Open→Most adat "
            "egyik tickerre sem. Ilyenkor intranapi mozgásokra nincs megbízható adat ebből a "
            "lekérdezésből.\n"
        )
        return "\n".join(lines)

    # Makró blokk
    macro_block = build_macro_block_1(macro)
    if macro_block:
        lines.append(macro_block)

    # Darabszámos – MINDEN pozíció listázása
    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – Ma nyitástól mostanáig (Open→Most)\n")

    # Watchlist – csak ahol ≥K az Open→Most
    watch_lines: List[str] = []
    watch_lines.append("### Watchlist – Open→Most mozgások (csak ha ≥K vagy anyagilag lényeges hír)\n")

    for row in filtered:
        prev_close, last, open_px, err, source_tag = quote_map.get(
            row.ticker, (None, None, None, None, "")
        )

        open_most = pct_change(open_px, last)

        def fmt_open_most(value: Optional[float]) -> str:
            if value is None:
                return "Open→Most: n/a"
            sign = "+" if value >= 0 else ""
            return f"Open→Most: {sign}{value:.2f}%"

        open_most_str = fmt_open_most(open_most)

        if source_tag == "yahoo":
            src_base = "árforrás: Yahoo quote"
        elif source_tag == "stooq":
            src_base = "árforrás: Stooq EOD (Yahoo fallback)"
        else:
            src_base = "árforrás: ismeretlen/hiba"

        if row.is_position:
            # MINDEN darabszámos pozíció
            if open_most is None:
                if err:
                    reason_str = f"Hiányzó intranapi adat (oka: {err})."
                else:
                    reason_str = "Hiányzó intranapi adat (nincs értelmezhető Open→Most)."
            elif abs(open_most) >= row.k_threshold:
                reason_str = "Érdemi intranapi elmozdulás (≥K) nyitáshoz képest."
            else:
                reason_str = "Mérsékelt intranapi mozgás, egyelőre nincs küszöb feletti elmozdulás."

            pos_lines.append(f"{row.ticker} — {open_most_str} — {reason_str} ({src_base})")
        else:
            # Watchlist: csak jelzésnél (≥K)
            has_signal = open_most is not None and abs(open_most) >= row.k_threshold
            if not has_signal:
                continue
            reason_str = "Watchlisten is érdemi intranapi mozgás (≥K) nyitáshoz képest."
            watch_lines.append(f"{row.ticker} — {open_most_str} — {reason_str} ({src_base})")

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
