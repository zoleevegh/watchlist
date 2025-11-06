#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
report_runner.py
-----------------

ÁTMENETI VERZIÓ – YAHOO FINANCE IDEIGLENESEN KIKAPCSOLVA

- A Yahoo Finance (chart + quote) hívások most NEM futnak, hogy ne üssük tovább a rate limitet.
- #1 és #3 riport: csak tájékoztató szöveget írnak ki, érdemi ármozgás-számítás nélkül.
- #2 riport: Stooq napi adatokból (EOD) számolja a tegnapi Open→Close becsült változását.

HA KÉSŐBB ÚJRA AKAROD HASZNÁLNI A YAHOO ALAPÚ LEKÉRDEZÉST:
- ezt a fájlt tedd félre (pl. report_runner_yahoo_off.py),
- és állítsd vissza a korábbi, Yahoo-t használó report_runner.py-t.
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
    # TickerStatus,            # YAHOO-OFF módban nem használjuk
    # build_coverage_block,    # csak Yahoo-s lefedettséghez kellene
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
# Közös helper – százalékszámítás
# ---------------------------------------------------------------------------

def pct_change(base: Optional[float], new: Optional[float]) -> Optional[float]:
    if base is None or new is None or base == 0:
        return None
    return (new - base) / base * 100.0


# ---------------------------------------------------------------------------
# STOOQ helper – csak #2 riporthoz (tegnapi Open→Close)
# ---------------------------------------------------------------------------

@dataclass
class DailyBar:
    date: str
    open: Optional[float]
    close: Optional[float]


def to_stooq_symbol(ticker: str) -> str:
    """
    Egyszerű ticker→Stooq szimbólum konverzió.

    - US tickereknél (nincs pont a névben): AAPL -> aapl.us, NVDA -> nvda.us
    - Ha már van benne pont (pl. PKN.WA), azt változatlanul hagyjuk (lowercase):
      pkn.wa, baba.sw stb.

    Ez nem lesz tökéletes minden esetben, de a legtöbb nagy US névre működni fog.
    """
    t = ticker.strip().lower()
    if "." in t:
        return t
    return f"{t}.us"


def fetch_prev_day_oc_stooq(ticker: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Stooq napi adatokból (https://stooq.com/q/d/l/?s=...&i=d) visszaadja
    az ELŐZŐ kereskedési nap Open és Close értékét.

    Visszatérés:
        (open_prev, close_prev, error_reason_or_None)
    """
    symbol = to_stooq_symbol(ticker)
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"

    try:
        resp = requests.get(url, timeout=10)
    except Exception as e:
        return None, None, f"stooq_network_error: {e}"

    if resp.status_code != 200:
        return None, None, f"stooq_http_error: {resp.status_code}"

    text = resp.text.strip()
    lines = text.splitlines()
    if len(lines) < 2:
        return None, None, "stooq_no_data"

    # első sor header: Date,Open,High,Low,Close,Volume
    data_rows = lines[1:]

    # ha csak 1 napi adat van, akkor azt használjuk "tegnapként"
    if len(data_rows) == 1:
        row = data_rows[0].split(",")
        if len(row) < 5:
            return None, None, "stooq_row_parse_error(single)"
        o = _parse_float(row[1])
        c = _parse_float(row[4])
        return o, c, None

    # különben az utolsó előtti sor az előző nap
    prev_row = data_rows[-2].split(",")
    if len(prev_row) < 5:
        return None, None, "stooq_row_parse_error(prev)"

    o = _parse_float(prev_row[1])
    c = _parse_float(prev_row[4])
    return o, c, None


# ---------------------------------------------------------------------------
# 1-es riport: AH + PM – YAHOO OFF MÓDBAN CSAK TÁJÉKOZTATÓ
# ---------------------------------------------------------------------------

def run_report_1(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    """
    FIGYELEM: YAHOO-OFF VERZIÓ

    Ebben az átmeneti verzióban az 1-es riport NEM húz le AH/PM adatot Yahoo-ról.
    Csak egy rövid szöveg jelzi, hogy a Yahoo-t ideiglenesen kikapcsoltuk
    a rate limit miatt.

    HA KÉSŐBB ÚJRA AKAROD HASZNÁLNI A YAHOO-ALAPÚ AH/PM LEKÉRDEZÉST:
    - cseréld le ezt a függvényt a korábbi, Yahoo chartot használó run_report_1-re.
    """
    lines: List[str] = []

    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")
    prev_day = (now_local - timedelta(days=1)).strftime("%Y-%m-%d")

    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST\n")
    lines.append(
        f"Vizsgált ablakok (CEST): AH {prev_day} 22:00 → {today_str} 02:00, "
        f"PM {today_str} 10:00 → 15:30\n"
    )
    lines.append(
        "\nFIGYELEM: A Yahoo Finance API jelenleg rate-limited / letiltott ebben a környezetben, "
        "ezért az AH/PM ármozgásokat most nem számolom ki.\n"
        "Amint újra elérhető lesz a Yahoo (429 nélkül), érdemes visszaállítani a korábbi, "
        "Yahoo chart (v8) alapú run_report_1 implementációt.\n"
    )

    # Makróblokk opcionálisan, hogy legyen keret (ha adtál --macro-t)
    macro_block = build_macro_block_1(macro)
    if macro_block:
        lines.append(macro_block)

    # Hírek / katalizátorok – ha van más forrás, a helperen keresztül később beépíthető
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
# 2-es riport: Tegnapi Open→Close – STOOQ ALAPÚ
# ---------------------------------------------------------------------------

def run_report_2(
    tickers: List[TickerRow],
) -> str:
    """
    #2 – Tegnapi nyitástól zárásig (Open→Close) – Stooq napi adatokkal.

    Árforrás:
    - Stooq (https://stooq.com) napi OHLC,
    - előző kereskedési nap Open→Close alapján számolom a becsült %-ot.

    Ez nem perces pontosságú intraday, hanem EOD jellegű adat (ami az 1–2 napos
    késleltetés szempontjából vállalható).
    """
    lines: List[str] = []
    lines.append("#2 – Tegnapi nyitástól zárásig (Open→Close) – Stooq alapú\n")
    lines.append("_Árforrás: Stooq napi adatok (tegnapi Open→Close) – https://stooq.com_\n")

    # PKN.WA továbbra is ki van zárva
    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    pos_lines: List[str] = []
    pos_lines.append("Darabszámos tickerek – abs(Open→Close) becsült mozgás (≥K)\n")

    watch_lines: List[str] = []
    watch_lines.append("Watchlist – abs(Open→Close) becsült mozgás (≥K)\n")

    error_lines: List[str] = []
    error_lines.append("Technikai megjegyzések (Stooq hibák / hiányzó adatok):\n")

    for row in filtered:
        open_prev, close_prev, err = fetch_prev_day_oc_stooq(row.ticker)
        if err is not None:
            error_lines.append(f"- {row.ticker}: {err}")
            continue

        move = pct_change(open_prev, close_prev)
        if move is None:
            error_lines.append(f"- {row.ticker}: nincs értelmezhető Open/Close adat Stooq-ból")
            continue

        if abs(move) < row.k_threshold:
            continue

        sign = "+" if move >= 0 else ""
        src_str = "árforrás: Stooq (tegnapi Open→Close)"

        line = f"{row.ticker} — Open→Close (Stooq, tegnapi nap): {sign}{move:.2f}% ({src_str})"

        if row.is_position:
            pos_lines.append(line)
        else:
            watch_lines.append(line)

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

    # Ha volt bármilyen ticker, amin elhasalt a Stooq, azt a végén jelezzük
    if len(error_lines) > 1:
        lines.append("\n".join(error_lines) + "\n")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 3-as riport: Ma Open→Most – YAHOO OFF MÓDBAN CSAK TÁJÉKOZTATÓ
# ---------------------------------------------------------------------------

def run_report_3(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    """
    FIGYELEM: YAHOO-OFF VERZIÓ

    A 3-as riport (Ma nyitástól mostanáig, intraday Open→Most) korábban
    Yahoo quote / chart API-t használt (regularMarketOpen → regularMarketPrice).

    Mivel a Yahoo jelenleg mindenre 429 (rate_limited) választ ad, ebben az
    átmeneti verzióban NEM próbálunk intranapi adatot számolni, csak egy
    rövid magyarázatot írunk ki.

    HA KÉSŐBB ÚJRA AKAROD HASZNÁLNI AZ INTRADAY OPEN→MOST RIPORTOT YAHOO-VAL:
    - cseréld le ezt a függvényt a korábbi, Yahoo-alapú run_report_3 implementációra.
    """
    lines: List[str] = []

    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")

    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most) — CEST\n")
    lines.append(
        f"Vizsgált ablak (CEST): mai USA nyitás (15:30) → lekérdezés időpontja ({today_str})\n"
    )
    lines.append(
        "_Árforrás: intranapi adatra korábban Yahoo Finance quote (v7 – regularMarketOpen → "
        "regularMarketPrice) szolgált, de a Yahoo jelenleg rate-limited, ezért ezt most nem hívjuk._\n"
    )

    lines.append(
        "\nA 3-as intranapi riport ebben az átmeneti verzióban **nem számol Open→Most mozgást**, "
        "mert a Yahoo Finance API tartósan 429 (Too Many Requests) státuszt ad vissza. "
        "Ahhoz, hogy megbízható intraday adat legyen, fennakadás nélkül kellene Yahoo-hoz "
        "kapcsolódni, vagy beépíteni egy alternatív, intraday adatforrást.\n"
        "\nAmint a Yahoo újra stabilan elérhető (429 nélkül), érdemes visszaállítani a korábbi, "
        "Yahoo-alapú run_report_3 implementációt, vagy kialakítani egy új intraday forrást.\n"
    )

    macro_block = build_macro_block_1(macro)
    if macro_block:
        lines.append(macro_block)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Részvény riport futtató (1/2/3) – YAHOO-OFF átmeneti verzió")
    parser.add_argument(
        "--report",
        choices=["1", "2", "3"],
        required=True,
        help="Riport típusa: 1 = AH/PM (Yahoo OFF), 2 = Tegnapi O→C (Stooq), 3 = Ma O→Most (Yahoo OFF)",
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
