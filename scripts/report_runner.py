#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
report_runner.py
-----------------

Egységes runner script az 1/2/3-as riportokhoz.

#1: AH + PM – Yahoo Finance quote v7 streamer (pre/post %), teljes MASTER-re.
#2: Tegnapi Open→Close – Yahoo Finance quote v7 (previousClose → last).
#3: Ma Open→Most – Yahoo Finance quote v7 (regularMarketOpen → regularMarketPrice).

PKN.WA – a régi szabály szerint nem szerepel a riportokban, csak ha külön kéred.
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

# Verzió – a run summary-ban is megjelenik
SCRIPT_VERSION = "1.7.0-yahoo-streamer-fullsheet"


# ---------------------------------------------------------------------------
# CSV beolvasása – K küszöbbel, darabszámos flaggel
# ---------------------------------------------------------------------------

@dataclass
class TickerRow:
    ticker: str
    quantity: Optional[float]  # None = nincs darabszám (watchlist / üres)
    k_threshold: float         # min. abs % (3% default, ha nincs értelmes K)
    is_position: bool          # True = darabszámos pozíció


def _parse_float(val: str) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_tickers_from_csv(path: str, default_k: float = 3.0) -> List[TickerRow]:
    """
    MASTER CSV beolvasása.
    Várható oszlopok (esetérzéketlen, ékezet mindegy):
        - Ticker / Symbol
        - Darabszám / qty / quantity / db
        - K (küszöb %)
    Ha K üres/hibás → default_k (3%).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV nem található: {path}")

    rows: List[TickerRow] = []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        headers = {h.strip().lower(): h for h in fieldnames}

        def get_col(*names: str) -> Optional[str]:
            for n in names:
                ln = n.lower()
                if ln in headers:
                    return headers[ln]
            return None

        ticker_col = get_col("ticker", "tiker", "symbol")
        qty_col = get_col("darabszám", "darabszam", "db", "qty", "quantity")
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
# Yahoo Finance – streamer (quote v7) AH/PM (pre/post) százalékok
# ---------------------------------------------------------------------------

YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9,hu;q=0.8",
    "Referer": "https://finance.yahoo.com/",
}


def fetch_ah_pm_from_yahoo_streamer(
    tickers: List[str],
    max_batch_size: int = 40,
) -> Tuple[Dict[str, Dict[str, Optional[float]]], Dict[str, Optional[str]]]:
    """
    Yahoo Finance quote (v7) pre/post százalékok (#1 riport).

    Fields:
        - preMarketChangePercent  → PM
        - postMarketChangePercent → AH

    Visszatér:
        data:   {ticker: {"ah": float|None, "pm": float|None}}
        errors: {ticker: str|None}  # hiba oka (pl. 'http_429', 'no_quote_result', stb.)
    """
    session = requests.Session()
    session.headers.update(YAHOO_HEADERS)

    data: Dict[str, Dict[str, Optional[float]]] = {
        t: {"ah": None, "pm": None} for t in tickers
    }
    errors: Dict[str, Optional[str]] = {t: None for t in tickers}

    def chunks(seq, n):
        for i in range(0, len(seq), n):
            yield seq[i:i + n]

    for batch in chunks(list(tickers), max_batch_size):
        params = {
            "symbols": ",".join(batch),
            "fields": "symbol,postMarketChangePercent,preMarketChangePercent",
        }

        attempt = 0
        resp: Optional[requests.Response] = None

        while attempt < 3:
            try:
                resp = session.get(YAHOO_QUOTE_URL, params=params, timeout=10)
            except Exception as e:
                for t in batch:
                    errors[t] = f"request_error:{e}"
                resp = None
                break

            if resp.status_code == 200:
                break

            if resp.status_code == 429:
                # rate limit – kicsi backoff, max 3 próbálkozás
                time.sleep(1.5 * (attempt + 1))
                attempt += 1
                continue

            # más HTTP hiba – nem próbálkozunk tovább
            for t in batch:
                errors[t] = f"http_{resp.status_code}"
            resp = None
            break

        if resp is None or resp.status_code != 200:
            continue

        try:
            payload = resp.json()
            results = payload.get("quoteResponse", {}).get("result", []) or []
        except Exception as e:
            for t in batch:
                errors[t] = f"json_error:{e}"
            continue

        by_symbol = {}
        for item in results:
            sym = (item.get("symbol") or "").upper()
            if sym:
                by_symbol[sym] = item

        for t in batch:
            item = by_symbol.get(t)
            if not item:
                if errors[t] is None:
                    errors[t] = "no_quote_result"
                continue

            ah = item.get("postMarketChangePercent")
            pm = item.get("preMarketChangePercent")

            if isinstance(ah, (int, float)):
                data[t]["ah"] = float(ah)
            if isinstance(pm, (int, float)):
                data[t]["pm"] = float(pm)

    return data, errors


# ---------------------------------------------------------------------------
# Általános segéd – százalék formázás
# ---------------------------------------------------------------------------

def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{x:+.2f}%"


def pct_change(base: Optional[float], new: Optional[float]) -> Optional[float]:
    if base is None or new is None or base == 0:
        return None
    return (new - base) / base * 100.0


# ---------------------------------------------------------------------------
# #1 riport – AH/PM – TELJES MASTER-re, streamerrel
# ---------------------------------------------------------------------------

def run_report_1(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    """
    #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30).

    - A teljes MASTER-re fut (pozíciók + watchlist, PKN.WA kivéve).
    - Árforrás: Yahoo quote v7 streamer (pre/post %).
    - Darabszámos tickerek: mindig listázva.
    - Watchlist: csak ha abs(AH) vagy abs(PM) ≥ K (ha K hiányzik → 3.00%).
    """
    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")
    prev_day = (now_local - timedelta(days=1)).strftime("%Y-%m-%d")

    # Fejléc
    lines: List[str] = []
    lines.append("#1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST\n")
    lines.append(f"Script verzió: {SCRIPT_VERSION}\n")
    lines.append(
        f"Vizsgált ablakok (CEST): AH {prev_day} 22:00 → {today_str} 02:00, "
        f"PM {today_str} 10:00 → 15:30\n"
    )
    lines.append(
        "Árforrás: Yahoo Finance quote (v7 streamer – "
        "preMarketChangePercent/postMarketChangePercent).\n"
    )

    # PKN.WA kizárás a riportból (régi szabály)
    filtered = [r for r in tickers if r.ticker != "PKN.WA"]

    if not filtered:
        lines.append("Lefedettség: NINCS FELDOLGOZHATÓ TICKER (PKN.WA nélkül).")
        return "\n".join(lines)

    ticker_list = [r.ticker for r in filtered]

    # Yahoo streamer lekérdezés
    ah_pm_data, errors = fetch_ah_pm_from_yahoo_streamer(ticker_list)

    # Lefedettség blokk – TickerStatus + build_coverage_block (Biblia szerint)
    status_map: Dict[str, TickerStatus] = {}
    for r in filtered:
        err = errors.get(r.ticker)
        if err:
            status_map[r.ticker] = TickerStatus(ok=False, reason=err)
        else:
            status_map[r.ticker] = TickerStatus(ok=True)

    lines.append(build_coverage_block(status_map))
    lines.append("")

    # Politika/FED blokk – a helper kezeli a macro paramétert
    macro_block = build_macro_block_1(macro)
    if macro_block:
        lines.append(macro_block)
        lines.append("")

    # Darabszámos / watchlist szétválasztás
    pos_lines: List[str] = []
    pos_lines.append("Darabszámos tickerek – After-hours & Premarket mozgások\n")

    wl_lines: List[str] = []
    wl_lines.append(
        "Watchlist – After-hours & Premarket mozgások (csak ha ≥K a mozgás)\n"
    )

    for row in filtered:
        t = row.ticker
        d = ah_pm_data.get(t, {"ah": None, "pm": None})
        ah = d.get("ah")
        pm = d.get("pm")

        ah_txt = fmt_pct(ah)
        pm_txt = fmt_pct(pm)

        # K küszöb
        k_thr = row.k_threshold if row.k_threshold and row.k_threshold > 0 else 3.0

        max_move = max(
            abs(ah) if isinstance(ah, (int, float)) else 0.0,
            abs(pm) if isinstance(pm, (int, float)) else 0.0,
        )

        has_signal = max_move >= k_thr

        if row.is_position:
            if not (isinstance(ah, (int, float)) or isinstance(pm, (int, float))):
                reason = "Hiányzó AH/PM adat (streamer nem adott értelmezhető változást)."
            elif has_signal:
                reason = f"Érdemi AH/PM elmozdulás (≥{k_thr:.2f}%)."
            else:
                reason = "Egyelőre nincs küszöb feletti AH/PM elmozdulás."

            pos_lines.append(
                f"{t} — AH {ah_txt} | PM {pm_txt} — {reason} "
                "(árforrás: Yahoo quote/v7 streamer)"
            )
        else:
            # watchlist – csak jelzésnél
            if not has_signal:
                continue
            reason = f"Watchlisten is érdemi AH/PM elmozdulás (≥{k_thr:.2f}%)."
            wl_lines.append(
                f"{t} — AH {ah_txt} | PM {pm_txt} — {reason} "
                "(árforrás: Yahoo quote/v7 streamer)"
            )

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(wl_lines) > 1:
        lines.append("\n".join(wl_lines) + "\n")

    # Hírek + katalizátor blokk – jelenleg üres placeholder (Biblia szerint ide fognak kerülni)
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
# #2 / #3 – Yahoo quote v7 batch (previousClose / last / open)
# ---------------------------------------------------------------------------

def fetch_quotes_batch(
    tickers: List[str],
    batch_size: int = 20,
) -> Dict[str, Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]]:
    """
    Yahoo quote API batch-ben (#2 és #3 riporthoz).

    Visszaad:
        {
          "AAPL": (prev_close, last, regular_open, error_reason_str_or_None),
          ...
        }
    """
    results: Dict[str, Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]] = {}
    if not tickers:
        return results

    for t in tickers:
        results[t] = (None, None, None, None)

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        symbols_str = ",".join(batch)
        url = f"{YAHOO_QUOTE_URL}?symbols={symbols_str}"

        try:
            resp = requests.get(url, headers=YAHOO_HEADERS, timeout=10)
        except Exception as e:
            for t in batch:
                results[t] = (None, None, None, f"network_error:{e}")
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
                results[t] = (None, None, None, f"http_error:{e}")
            time.sleep(2)
            continue

        try:
            data = resp.json()
            qlist = data.get("quoteResponse", {}).get("result", []) or []
        except Exception as e:
            for t in batch:
                results[t] = (None, None, None, f"parse_error:{e}")
            time.sleep(2)
            continue

        seen = set()
        for q in qlist:
            sym = (q.get("symbol") or "").upper()
            if not sym:
                continue
            prev_close = q.get("regularMarketPreviousClose")
            last = q.get("regularMarketPrice")
            open_px = q.get("regularMarketOpen")
            results[sym] = (prev_close, last, open_px, None)
            seen.add(sym)

        for t in batch:
            if t not in seen:
                _, _, _, reason = results.get(t, (None, None, None, None))
                if reason is None:
                    results[t] = (None, None, None, "no_quote_result")

        time.sleep(2)

    return results


# ---------------------------------------------------------------------------
# #2 riport – Tegnapi Open→Close
# ---------------------------------------------------------------------------

def run_report_2(
    tickers: List[TickerRow],
) -> str:
    lines: List[str] = []
    lines.append("#2 – Tegnapi nyitástól zárásig (Open→Close) – egyszerűsített\n")
    lines.append(f"Script verzió: {SCRIPT_VERSION}\n")
    lines.append(
        "_Árforrás: Yahoo Finance quote (v7 – previousClose → regularMarketPrice)_\n"
    )

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Lefedettség: NINCS FELDOLGOZHATÓ TICKER (PKN.WA nélkül).")
        return "\n".join(lines)

    ticker_list = [r.ticker for r in filtered]
    quote_map = fetch_quotes_batch(ticker_list)

    # Lefedettség blokk
    status_map: Dict[str, TickerStatus] = {}
    for row in filtered:
        prev_close, last, open_px, reason = quote_map.get(row.ticker, (None, None, None, None))
        if reason is None and prev_close is not None and last is not None:
            status_map[row.ticker] = TickerStatus(ok=True)
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason or "no_price_data")

    lines.append(build_coverage_block(status_map))
    lines.append("")

    pos_lines: List[str] = []
    pos_lines.append("Darabszámos tickerek – abs(Open→Close) becsült mozgás (≥K)\n")

    watch_lines: List[str] = []
    watch_lines.append("Watchlist – abs(Open→Close) becsült mozgás (≥K)\n")

    for row in filtered:
        prev_close, last, open_px, reason = quote_map.get(row.ticker, (None, None, None, None))
        move = pct_change(prev_close, last)
        if move is None:
            continue

        k_thr = row.k_threshold if row.k_threshold and row.k_threshold > 0 else 3.0
        if abs(move) < k_thr:
            continue

        sign = "+" if move >= 0 else ""
        src_str = "árforrás: Yahoo quote/v7"
        line = f"{row.ticker} — Open→Close (becsült): {sign}{move:.2f}% (Küszöb: {k_thr:.2f}%) ({src_str})"

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
# #3 riport – Ma Open→Most
# ---------------------------------------------------------------------------

def run_report_3(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    lines: List[str] = []

    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")

    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most) — CEST\n")
    lines.append(f"Script verzió: {SCRIPT_VERSION}\n")
    lines.append(
        f"Vizsgált ablak (CEST): mai USA nyitás (15:30) → lekérdezés időpontja ({today_str})\n"
    )
    lines.append("_Árforrás: Yahoo Finance quote (v7 – regularMarketOpen → regularMarketPrice)_\n")

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Lefedettség: NINCS FELDOLGOZHATÓ TICKER (PKN.WA nélkül).")
        return "\n".join(lines)

    ticker_list = [r.ticker for r in filtered]
    quote_map = fetch_quotes_batch(ticker_list)

    # Lefedettség
    status_map: Dict[str, TickerStatus] = {}
    ok_count = 0
    for row in filtered:
        prev_close, last, open_px, reason = quote_map.get(row.ticker, (None, None, None, None))
        if reason is None and open_px is not None and last is not None:
            status_map[row.ticker] = TickerStatus(ok=True)
            ok_count += 1
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason or "no_open_or_last")

    lines.append(build_coverage_block(status_map))
    lines.append("")

    # Ha minden hibás (429, stb.) → őszinte üzenet
    if ok_count == 0:
        lines.append(
            "\nA 3-as intranapi riport ma **nem értelmezhető**, mert a Yahoo Finance "
            "quote API-ja minden tickerre hibát adott (pl. rate limit / nincs intranapi adat). "
            "Ilyenkor az Open→Most mozgásokra ebből a forrásból nincs megbízható adat.\n"
        )
        return "\n".join(lines)

    # Makró blokk
    macro_block = build_macro_block_1(macro)
    if macro_block:
        lines.append(macro_block)
        lines.append("")

    # Darabszámos – MINDEN pozíció
    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – Ma nyitástól mostanáig (Open→Most)\n")

    # Watchlist – csak jelzésnél
    watch_lines: List[str] = []
    watch_lines.append("### Watchlist – Open→Most mozgások (csak ha ≥K)\n")

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

        k_thr = row.k_threshold if row.k_threshold and row.k_threshold > 0 else 3.0

        if row.is_position:
            # minden pozíció
            if open_most is None:
                if reason:
                    reason_str = f"Hiányzó intranapi adat (oka: {reason})."
                else:
                    reason_str = "Hiányzó intranapi adat (nincs értelmezhető Open→Most)."
            elif abs(open_most) >= k_thr:
                reason_str = f"Érdemi intranapi elmozdulás (≥{k_thr:.2f}%) nyitáshoz képest."
            else:
                reason_str = "Mérsékelt intranapi mozgás, egyelőre nincs küszöb feletti elmozdulás."

            if reason:
                src_str = f"{src_base} (hiba: {reason})"
            else:
                src_str = src_base

            pos_lines.append(f"{row.ticker} — {open_most_str} — {reason_str} ({src_str})")
        else:
            has_signal = open_most is not None and abs(open_most) >= k_thr
            if not has_signal:
                continue
            reason_str = f"Watchlisten is érdemi intranapi mozgás (≥{k_thr:.2f}%) nyitáshoz képest."

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
        help="Ticker-lista CSV elérési útja (MASTER)",
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
