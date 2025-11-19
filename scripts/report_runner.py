#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
report_runner_2_1_7.py

Egyszerűsített, önállóan futtatható verzió a #1-es (After-hours + Premarket) jelentéshez.
Fő fókusz: Yahoo Finance v7 /quote endpoint használata tickerenként,
           pontos preMarketChangePercent és postMarketChangePercent értékekkel.

Használat (példa):
    python report_runner_2_1_7.py --mode 1 --positions AAPL,TVTX,TMUS --watchlist AMD,NVDA,CGON

Ha nem adsz meg paramétereket, a script egy beépített minta-portfóliót és watchlistet használ.
A kimenetet STDOUT-ra írja, ugyanabban a markdown stílusban, amit a workflow is használ.
"""

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import json
import math
import textwrap

try:
    import requests  # type: ignore
except Exception as e:  # pragma: no cover
    print("Hiányzik a 'requests' csomag. Telepítés: pip install requests", file=sys.stderr)
    raise


YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"


# --------- Adatszerkezetek ---------


@dataclass
class TickerInfo:
    symbol: str
    has_position: bool  # True = darabszámos pozíció, False = watchlist
    k_threshold: float = 3.0  # alapértelmezett K (abszolút % küszöb)


@dataclass
class AhPmMove:
    ah_change: Optional[float]  # százalék, pl. -1.23
    pm_change: Optional[float]
    error: Optional[str] = None  # ha bármilyen hiba volt a lekérésnél / parsingnál


# --------- Yahoo helper függvények ---------


def fetch_quote(symbol: str, session: Optional["requests.Session"] = None) -> Dict:
    """
    Lekéri egyetlen ticker adatait a v7 /quote endpointtal.

    Az egyes tickereket külön-külön kérjük le, hogy elkerüljük a
    több-szimbólumos 401/Unauthorized problémákat.
    """
    sess = session or requests.Session()
    params = {"symbols": symbol}
    resp = sess.get(YAHOO_QUOTE_URL, params=params, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"http_error: {resp.status_code} for symbol {symbol}")

    data = resp.json()
    result = data.get("quoteResponse", {}).get("result", [])
    if not result:
        raise RuntimeError(f"empty_result")
    return result[0]


def parse_ah_pm_from_quote(quote: Dict) -> AhPmMove:
    """
    Kinyeri a preMarketChangePercent és postMarketChangePercent értékeket.
    Csak akkor tekintjük valós adatnak, ha van hozzá preMarketPrice / postMarketPrice is.
    """
    def _extract(prefix: str) -> Optional[float]:
        price = quote.get(f"{prefix}Price")
        change_pct = quote.get(f"{prefix}ChangePercent")
        # Nincs pre/post session: None vagy 0 price + None változás
        if price in (None, 0) or change_pct is None:
            return None
        try:
            return float(change_pct)
        except Exception:
            return None

    pm = _extract("preMarket")
    ah = _extract("postMarket")
    return AhPmMove(ah_change=ah, pm_change=pm, error=None)


def fetch_ah_pm_for_ticker(symbol: str, session: Optional["requests.Session"] = None) -> AhPmMove:
    """
    Teljes pipeline: quote → AH/PM százalékok.
    Hibánál AhPmMove(error=...) tér vissza.
    """
    try:
        q = fetch_quote(symbol, session=session)
        return parse_ah_pm_from_quote(q)
    except Exception as e:
        return AhPmMove(ah_change=None, pm_change=None, error=str(e))


# --------- Jelentésgenerálás ---------


def format_pct(val: Optional[float]) -> str:
    if val is None or math.isnan(val):
        return "n/a"
    return f"{val:+.2f}%"


def build_header(script_version: str, window_desc: str) -> str:
    header = f"""#1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST

Script verzió: {script_version}

Vizsgált ablakok (CEST): {window_desc}

Árforrás: Yahoo Finance quote (v7 – preMarketChangePercent / postMarketChangePercent; mindegyik az utolsó RTH záróhoz viszonyított %)

"""
    return header


def classify_coverage(ticker_results: Dict[str, AhPmMove]) -> Tuple[str, List[str]]:
    """
    Lefedettség blokk szöveg + hiányos tickerek listája.
    """
    missing = []
    for sym, res in ticker_results.items():
        if res.error is not None:
            missing.append(f"{sym} (oka: {res.error})")
    if missing:
        return (
            "Lefedettség: HIÁNYOS – nem elérhető ticker(ek): "
            + ", ".join(sorted(missing))
        ), missing
    else:
        return "Lefedettség: TELJES", []


def generate_report(tickers: List[TickerInfo], script_version: str) -> str:
    """
    Lekérdez minden tickert és összerakja a teljes #1-es jelentést.
    """
    session = requests.Session()
    results: Dict[str, AhPmMove] = {}

    for t in tickers:
        # kis alvás, hogy kvázi udvariasak legyünk a Yahoo felé
        time.sleep(0.15)
        results[t.symbol] = fetch_ah_pm_for_ticker(t.symbol, session=session)

    # Fejléc
    window_desc = "AH előző nap 22:00 → 02:00, PM ma 10:00 → 15:30"
    parts: List[str] = []
    parts.append(build_header(script_version, window_desc))

    # Lefedettség blokk
    coverage_line, missing = classify_coverage(results)
    parts.append(coverage_line)
    parts.append("Politika/FED / Trump-napihír\n")
    parts.append("(Auto mód még nincs implementálva – add meg a makró összefoglalót a workflow 'macro' mezőjében.)")

    # Darabszámos + watchlist szétválasztása
    positions = [t for t in tickers if t.has_position]
    watchlist = [t for t in tickers if not t.has_position]

    parts.append("Darabszámos tickerek – After-hours & Premarket mozgások\n")
    for t in positions:
        res = results.get(t.symbol)
        if res is None:
            continue
        ah_s = format_pct(res.ah_change)
        pm_s = format_pct(res.pm_change)

        # Küszöbérték logika (abs(ah) vagy abs(pm) >= K?)
        max_move = 0.0
        if res.ah_change is not None:
            max_move = max(max_move, abs(res.ah_change))
        if res.pm_change is not None:
            max_move = max(max_move, abs(res.pm_change))

        if res.error is not None:
            reason = f"Hiányzó vagy nem értelmezhető AH/PM adat (oka: {res.error})."
        elif max_move >= t.k_threshold:
            reason = "Érdemi AH/PM elmozdulás (≥K) az utolsó RTH záróhoz képest."
        else:
            reason = "Egyelőre nincs küszöb feletti AH/PM elmozdulás."

        line = f"{t.symbol} — AH {ah_s} | PM {pm_s} — {reason} (árforrás: Yahoo quote/v7 (pre-/post-market %))"
        parts.append(line)

    parts.append("Watchlist – After-hours & Premarket mozgások (csak ha ≥K)\n")
    for t in watchlist:
        res = results.get(t.symbol)
        if res is None or res.error is not None:
            continue

        ah = res.ah_change
        pm = res.pm_change
        max_move = 0.0
        if ah is not None:
            max_move = max(max_move, abs(ah))
        if pm is not None:
            max_move = max(max_move, abs(pm))

        if max_move < t.k_threshold:
            continue  # nincs küszöb felett, nem kerül jelentésbe

        ah_s = format_pct(ah)
        pm_s = format_pct(pm)
        line = (
            f"{t.symbol} — AH {ah_s} | PM {pm_s} — "
            f"Watchlisten is érdemi AH/PM elmozdulás (≥K) az utolsó RTH záróhoz képest. "
            f"(árforrás: Yahoo quote/v7 (pre-/post-market %))"
        )
        parts.append(line)

    parts.append("Job summary generated at run-time")
    return "\n".join(parts)


# --------- CLI / main ---------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="#1-es (AH+PM) jelentés Yahoo Finance v7 quote alapján.")
    parser.add_argument("--mode", type=int, default=1, help="Jelentés típusa (jelenleg csak 1 támogatott).")
    parser.add_argument(
        "--positions",
        type=str,
        default="AAPL,TVTX,TMUS,NFLX,UBER,GOOG,AVGO,MU,TSM,SOUN,CRML,MSTR,BTDR",
        help="Darabszámos tickerek vesszővel elválasztva.",
    )
    parser.add_argument(
        "--watchlist",
        type=str,
        default="ACHR,ADBE,ALL,AMD,AMZN,ANET,APP,ARM,BABA,BIDU,BIRK,CELH,CGON,COIN,COP,CRM,CRWD,CRWV,CVX,CYBR,CSCO,"
                "DAL,DDOG,DELL,DHR,DIS,DRUG,ERJ,FDX,FSLR,GAP,HAS,HOOD,IBM,INTC,IONQ,IREN,JD,KLAC,LCID,LNTH,LRCX,LLY,"
                "MA,MLM,MNDY,MRK,MRNA,MRVL,MSI,NBIS,NET,NKE,NUE,NVO,OKTA,ORCL,PANW,PDD,PFE,PLTR,QBTS,QCOM,RGTI,RKLB,"
                "SCHW,SMCI,SNOW,SNPS,SPOT,STX,T,TCEHY,TEVA,TLN,TSLA,TXN,UEC,UNH,UPS,USAR,V,VLO,WDAY,WMT",
        help="Watchlist tickerek vesszővel elválasztva.",
    )
    parser.add_argument(
        "--k-default",
        type=float,
        default=3.0,
        help="Alapértelmezett K küszöb (abszolút %).",
    )
    parser.add_argument(
        "--script-version",
        type=str,
        default="2.1.7-biblia-yahoo-us-time-quote-single",
        help="Verziósztring, ami a jelentés elejére kerül.",
    )
    return parser.parse_args(argv)


def build_ticker_list(args: argparse.Namespace) -> List[TickerInfo]:
    def _split(s: str) -> List[str]:
        return [x.strip().upper() for x in s.split(",") if x.strip()]

    pos_syms = _split(args.positions)
    wl_syms = _split(args.watchlist)

    tickers: List[TickerInfo] = []
    for sym in pos_syms:
        tickers.append(TickerInfo(symbol=sym, has_position=True, k_threshold=args.k_default))
    for sym in wl_syms:
        # ha véletlenül átfed, akkor a "pozíció" az erősebb
        if sym in pos_syms:
            continue
        tickers.append(TickerInfo(symbol=sym, has_position=False, k_threshold=args.k_default))
    return tickers


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if args.mode != 1:
        print("Ez az egyszerűsített script jelenleg csak a #1-es (AH+PM) jelentést támogatja (mode=1).", file=sys.stderr)
        return 1

    tickers = build_ticker_list(args)
    report = generate_report(tickers, script_version=args.script_version)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
