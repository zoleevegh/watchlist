#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
report_runner.py
-----------------

Egységes runner script az 1/2/3-as riportokhoz.

#1: AH + PM – ELSŐDLEGESEN Yahoo Finance quote (v7 pre/post),
    chart (v8, 2d/5m, includePrePost) csak fallback.
    CSAK darabszámos tickerekre fut, PKN.WA kivéve.

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

try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python <3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

BUDAPEST = ZoneInfo("Europe/Budapest")
US_EASTERN = ZoneInfo("America/New_York")

SCRIPT_VERSION = "1.5.0-positions-quote-first"

# Egy böngészős User-Agent – így a Yahoo kevésbé tilt
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
    quantity: Optional[float]        # None = watchlist
    k_threshold: float               # min. abs % mozgás (default 3.0)
    is_position: bool                # True = darabszámos pozíció


def _parse_float(val: Optional[str]) -> Optional[float]:
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
# Segédfüggvény – százalékváltozás
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


def extract_ah_pm_from_chart(ticker: str) -> Tuple[PriceSnapshot, Optional[str]]:
    """
    Fallback a #1 riporthoz: AH + PM sáv utolsó ára chartból.

    - prev_close: előző napi RTH záró (meta.previousClose vagy chartból becsülve)
    - ah_last: AH utolsó ár (RTH záró utáni 16:00–20:00 US)
    - pm_last: PM utolsó ár (mai nap 04:00–09:30 US)
    """
    data, err = fetch_chart_2d_5m(ticker)
    if err or data is None:
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

        dt_list = [
            datetime.fromtimestamp(ts, tz=US_EASTERN) for ts in timestamps
        ]

        # ha nincs previousClose, becsüljük az előző napi RTH záróra
        if prev_close is None:
            # utolsó olyan ár, ami 15:30–16:00 között volt (nagyon közelítő)
            prev_rth = [
                c
                for c, dt in zip(closes, dt_list)
                if dt.hour == 16 or (dt.hour == 15 and dt.minute >= 30)
            ]
            prev_close = prev_rth[0] if prev_rth else None

        # tegnapi dátum az USA szerint
        last_dt = dt_list[-1]
        today_date = last_dt.date()
        yesterday_date = (last_dt - timedelta(days=1)).date()

        ah_prices = [
            c
            for c, dt in zip(closes, dt_list)
            if dt.date() == yesterday_date and 16 <= dt.hour < 20
        ]
        ah_last = ah_prices[-1] if ah_prices else None

        pm_prices = [
            c
            for c, dt in zip(closes, dt_list)
            if dt.date() == today_date
            and (
                (4 <= dt.hour < 9)
                or (dt.hour == 9 and dt.minute <= 30)
            )
        ]
        pm_last = pm_prices[-1] if pm_prices else None

        return PriceSnapshot(prev_close, ah_last, pm_last), None
    except Exception as e:
        return PriceSnapshot(None, None, None), f"parse_error(chart_v8): {e}"


# ---------------------------------------------------------------------------
# Yahoo Finance quote (v7) – batch helper (#1)
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
    Yahoo quote API batch-ben az 1-es riporthoz.

    Visszaad:
        { "AAPL": QuoteSnapshot(...), ... }
    """
    results: Dict[str, QuoteSnapshot] = {}
    if not tickers:
        return results

    # deduplikálás sorrend megtartásával
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
            time.sleep(1.2)
            continue

        if resp.status_code == 429:
            for t in batch:
                results[t].error = "rate_limited(quote_v7)"
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

        # ha a batch valamelyik tagjára nem jött vissza semmi
        for t in batch:
            if t not in seen_in_batch and (
                results[t].error is None or results[t].error == "no_data_yet"
            ):
                results[t].error = "no_quote_result(quote_v7)"

        time.sleep(1.0)

    return results


# ---------------------------------------------------------------------------
# 1-es riport: AH + PM csak pozíciókra, quote-first + chart fallback
# ---------------------------------------------------------------------------

def _fmt_pct(val: Optional[float]) -> str:
    if val is None:
        return "n/a"
    return f"{val:+.2f}%"


def run_report_1(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    lines: List[str] = []

    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")
    prev_day = (now_local - timedelta(days=1)).strftime("%Y-%m-%d")

    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST\n")
    lines.append(f"\nScript verzió: {SCRIPT_VERSION}\n\n")
    lines.append(
        f"Vizsgált ablakok (CEST): AH {prev_day} 22:00 → {today_str} 02:00, "
        f"PM {today_str} 10:00 → 15:30\n\n"
    )
    lines.append(
        "Árforrás: elsődlegesen Yahoo Finance quote (v7 pre/post – "
        "preMarket/postMarket mezők), fallback: Yahoo Finance chart "
        "(v8, 2d/5m, includePrePost).\n"
    )

    # Csak a darabszámos tickerekre fusson, PKN.WA kizárva
    positions = [
        r for r in tickers if r.is_position and r.ticker != "PKN.WA"
    ]

    if not positions:
        lines.append(
            "\nLefedettség: NINCS DARABSZÁMOS POZÍCIÓ – az 1-es riport jelenleg csak "
            "a pozíciókra fut.\n"
        )
        return "\n".join(lines)

    ticker_list = [r.ticker for r in positions]
    quote_map = fetch_quotes_batch(ticker_list)

    coverage_missing: List[str] = []
    coverage_reasons: Dict[str, str] = {}

    moves: Dict[str, Tuple[Optional[float], Optional[float], float]] = {}

    # Első kör: quote alapján számolunk, szükség esetén chart fallback
    for row in positions:
        sym = row.ticker
        snap = quote_map.get(sym, QuoteSnapshot(error="missing_from_quote_map"))

        base = snap.prev_close
        ah_pct: Optional[float] = None
        pm_pct: Optional[float] = None
        reason_parts: List[str] = []

        if snap.error:
            reason_parts.append(snap.error)

        # AH – először a direkt % mező, aztán ár + base
        if snap.post_change_pct is not None:
            ah_pct = float(snap.post_change_pct)
        elif base is not None and snap.post_price is not None:
            ah_pct = pct_change(base, snap.post_price)

        # PM – hasonló
        if snap.pre_change_pct is not None:
            pm_pct = float(snap.pre_change_pct)
        elif base is not None and snap.pre_price is not None:
            pm_pct = pct_change(base, snap.pre_price)

        # Ha se AH, se PM nincs quote-ból, próbáljuk charttal
        if ah_pct is None and pm_pct is None:
            chart_snap, chart_err = extract_ah_pm_from_chart(sym)
            if chart_err:
                reason_parts.append(chart_err)

            if base is None and chart_snap.prev_close is not None:
                base = chart_snap.prev_close

            if base is not None and chart_snap.ah_last is not None:
                ah_pct = pct_change(base, chart_snap.ah_last)
            if base is not None and chart_snap.pm_last is not None:
                pm_pct = pct_change(base, chart_snap.pm_last)

        # Ha még mindig nincs adat, akkor coverage hibának számít
        if ah_pct is None and pm_pct is None:
            reason = "; ".join(reason_parts) if reason_parts else "no_ah_pm_data"
            coverage_missing.append(sym)
            coverage_reasons[sym] = reason
        else:
            moves[sym] = (ah_pct, pm_pct, row.k_threshold)

    # Lefedettség blokk
    if not coverage_missing:
        lines.append("\nLefedettség: TELJES\n")
    else:
        parts = []
        for sym in coverage_missing:
            reason = coverage_reasons.get(sym, "ismeretlen ok")
            parts.append(f"{sym} (oka: {reason})")
        joined = ", ".join(parts)
        lines.append(f"\nLefedettség: HIÁNYOS – nem elérhető ticker(ek): {joined}\n")

    # Darabszámos tickerek eredményei
    lines.append("\nDarabszámos tickerek – After-hours & Premarket mozgások\n\n")

    # a pozíciókat a CSV-ben lévő sorrendben listázzuk
    for row in positions:
        sym = row.ticker
        if sym not in moves:
            # lefedettség hibás ticker – már feljebb jeleztük
            lines.append(
                f"{sym} — AH: n/a | PM: n/a — Nincs értelmezhető AH/PM adat "
                f"(oka: {coverage_reasons.get(sym, 'ismeretlen ok')}).\n"
            )
            continue

        ah_pct, pm_pct, k_thr = moves[sym]
        ah_str = _fmt_pct(ah_pct)
        pm_str = _fmt_pct(pm_pct)

        max_abs = max(
            abs(ah_pct) if ah_pct is not None else 0.0,
            abs(pm_pct) if pm_pct is not None else 0.0,
        )

        if max_abs >= k_thr:
            comment = f"Érdemi AH/PM elmozdulás (≥{k_thr:.2f}%)."
        else:
            comment = "Egyelőre nincs küszöb feletti AH/PM elmozdulás."

        lines.append(
            f"{sym} — AH {ah_str} | PM {pm_str} — {comment} "
            "(árforrás: Yahoo quote/v7, chart/v8 fallback)\n"
        )

    # Makró blokk
    lines.append("\nPolitika/FED / Trump-napihír\n\n")
    if macro and macro.strip() and macro.strip().lower() != "auto":
        lines.append(macro.strip() + "\n")
    else:
        lines.append(
            "(Auto mód még nincs implementálva – add meg a makró összefoglalót "
            "a workflow 'macro' mezőjében.)\n"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Placeholder a 2/3-as riportokra – most nem használjuk
# ---------------------------------------------------------------------------

def run_report_2(tickers: List[TickerRow], macro: Optional[str]) -> str:
    return (
        "## #2 – Tegnapi nyitástól zárásig\n\n"
        "Script verzió: " + SCRIPT_VERSION + "\n\n"
        "A #2 riport ebben a verzióban még nincs implementálva.\n"
    )


def run_report_3(tickers: List[TickerRow], macro: Optional[str]) -> str:
    return (
        "## #3 – Ma nyitástól mostanáig\n\n"
        "Script verzió: " + SCRIPT_VERSION + "\n\n"
        "A #3 riport ebben a verzióban még nincs implementálva.\n"
    )


# ---------------------------------------------------------------------------
# CLI belépési pont
# ---------------------------------------------------------------------------

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--report",
        type=int,
        choices=[1, 2, 3],
        required=True,
        help="Riport típusa: 1 / 2 / 3",
    )
    p.add_argument(
        "--csv",
        type=str,
        required=True,
        help="Tickerlista CSV útvonala (pl. master.csv)",
    )
    p.add_argument(
        "--macro",
        type=str,
        default="auto",
        help="Makró/FED összefoglaló szöveg, vagy 'auto'",
    )
    return p.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])
    tickers = load_tickers_from_csv(args.csv)

    if args.report == 1:
        out = run_report_1(tickers, args.macro)
    elif args.report == 2:
        out = run_report_2(tickers, args.macro)
    else:
        out = run_report_3(tickers, args.macro)

    # A GitHub Action „Run summary”-t a stdout-ból veszi
    sys.stdout.write(out)


if __name__ == "__main__":
    main()
