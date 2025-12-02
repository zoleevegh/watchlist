"""
highconv_builder.py

3–12 hónapos „high-conviction” jelöltek automatikus azonosítása és high_conv_1.json generálása.

ALAPELV
-------
A script az Apps Scriptes analyst feedet használja bemenetként, emellett Yahoo Finance
snapshotot kér le, és a biblia szerinti 5 jelből épít pontszámot:

1) 2–3+ friss felminősítés / céláremelés nagy házaktól
2) Iránymutatás-emelés / pozitív guide (kulcsszavak a megjegyzésben)
3) Konszenzus EPS / árbevétel felfelé módosul  [JELENLEG PLACEHOLDER, 0 pont]
4) Közelgő konkrét katalizátor (3–12 hónap): event / launch / approval jellegű kulcsszavak
5) Relatív erő / 52w csúcs közeli teljesítmény (aktuális ár max. 5%-on belül a 52w high-tól)

Legalább 2 jelnek teljesülnie kell, és a pontszámnak el kell érnie egy küszöböt (0.6).
A kimenet a high_conv_1.json, amelyet a macro_highconv_helpers_v2.py tud beolvasni.

HASZNÁLAT
---------
1) Töltsd ki az ANALYST_FEED_URL konstansban az Apps Script URL-t, pl.:
       ANALYST_FEED_URL = "https://script.google.com/macros/s/AKfycbxxCqoEMGbvMayN4iz6JpfXQzaR9m5tobVmzw_CopDtPnjfRDdnX2Os2289ZCp25uez/exec"
   A script automatikusan ?type=analyst&report=1&days=DAYS_BACK paraméterekkel hívja.

2) Add meg az EXCLUDE_TICKERS_FILE utat, amely egy sima szöveges / CSV fájl,
   és tartalmazza azokat a tickereket (portfólió + watchlist), amelyeket NEM
   akarunk high-conv jelöltként látni (egy sor = egy ticker, vagy vesszővel
   elválasztva).

3) Futtasd:
       python highconv_builder.py
   A script a futtatási könyvtárba írja a high_conv_1.json fájlt.

4) A report_runner-ben a macro_highconv_helpers_v2.inject_macro_and_highconv_blocks
   már tudja használni ezt a JSON-t a #1-es jelentés végén.

MEGJEGYZÉS
----------
- A script `requests` modult használ HTTP hívásokhoz. Ha nincs telepítve:
      pip install requests
- A Yahoo Finance quote API egy publikus endpointot használ, extra lib nélkül.
"""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import requests


# --- KONFIGURÁCIÓ --------------------------------------------------------------

# IDE ÍRD BE AZ APPS SCRIPT ANALYST FEED URL-JÉT (alap, paraméterek nélkül)
ANALYST_FEED_URL = "https://script.google.com/macros/s/AKfycbxxCqoEMGbvMayN4iz6JpfXQzaR9m5tobVmzw_CopDtPnjfRDdnX2Os2289ZCp25uez/exec"

# Hány napra visszamenőleg nézzük az analyst eseményeket a high-convhez
DAYS_BACK = 30

# Kizárandó tickerek (portfólió + watchlist)
# Alternatív megoldásként használd az EXCLUDE_TICKERS_FILE fájlt.
EXCLUDE_TICKERS: Set[str] = set()

# Ha megadsz itt egy fájlnevet, abból beolvassa a kizárandó tickereket.
# Formátum: egy sor = egy ticker, vagy 1 sorban több ticker vesszővel elválasztva.
EXCLUDE_TICKERS_FILE = "exclude_tickers.txt"

# Yahoo Finance quote endpoint (több ticker egyszerre)
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

# High-conv pontszám küszöb
MIN_SCORE = 0.6

# Min. jel szám (az 5 biblia-jelölés közül)
MIN_SIGNAL_COUNT = 2

# high_conv_1.json kimeneti fájl
OUTPUT_PATH = Path("high_conv_1.json")


# --- ADATSTRUKTÚRÁK ------------------------------------------------------------


@dataclass
class AnalystEvent:
    ticker: str
    date: datetime
    firm: str
    action: str
    from_rating: str
    to_rating: str
    price_target: Optional[float]
    notes: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AnalystEvent":
        # Várt kulcsok: ticker, date, firm, action, from_rating, to_rating, price_target, notes
        ticker = str(data.get("ticker") or "").upper()
        date_str = str(data.get("date") or "")
        date = _parse_date(date_str)
        firm = str(data.get("firm") or "").strip()
        action = str(data.get("action") or "").strip()
        from_rating = str(data.get("from_rating") or "").strip()
        to_rating = str(data.get("to_rating") or "").strip()
        pt = data.get("price_target")
        try:
            price_target = float(pt) if pt not in (None, "", "null") else None
        except Exception:
            price_target = None
        notes = str(data.get("notes") or "").strip()

        return cls(
            ticker=ticker,
            date=date,
            firm=firm,
            action=action,
            from_rating=from_rating,
            to_rating=to_rating,
            price_target=price_target,
            notes=notes,
        )


@dataclass
class TickerSignals:
    ticker: str
    positive_analyst_events: int = 0
    has_guidance_upgrade: bool = False
    has_consensus_revision_up: bool = False  # jelenleg placeholder
    has_future_catalyst: bool = False
    near_52w_high: bool = False
    score: float = 0.0


# --- SEGÉD FÜGGVÉNYEK ----------------------------------------------------------


def _parse_date(value: str) -> datetime:
    """
    ISO vagy 'YYYY-MM-DD' -> datetime (UTC). Ha nem érthető, 'most'.
    """
    if not value:
        return datetime.now(timezone.utc)
    try:
        if "T" in value:
            # ISO datetime
            v = value.replace("Z", "+00:00")
            return datetime.fromisoformat(v)
        # Csak dátum
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def load_exclude_tickers() -> Set[str]:
    tickers: Set[str] = set(EXCLUDE_TICKERS)
    path = Path(EXCLUDE_TICKERS_FILE)
    if path.is_file():
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            for part in line.replace(";", ",").split(","):
                t = part.strip().upper()
                if t:
                    tickers.add(t)
    return tickers


def fetch_analyst_events() -> List[AnalystEvent]:
    """
    Apps Script analyst feed hívása.
    Várja, hogy a válasz JSON-ben:
        { "ok": true, "events": [ ... ] }
    legyen.
    """
    params = {
        "type": "analyst",
        "report": "1",
        "days": str(DAYS_BACK),
    }
    resp = requests.get(ANALYST_FEED_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    events_raw = []
    if isinstance(data, dict):
        events_raw = data.get("events") or data.get("items") or []
    elif isinstance(data, list):
        events_raw = data
    else:
        events_raw = []

    events: List[AnalystEvent] = []
    for item in events_raw:
        try:
            ev = AnalystEvent.from_dict(item)
        except Exception:
            continue
        if not ev.ticker:
            continue
        events.append(ev)

    return events


def classify_events(events: Iterable[AnalystEvent]) -> Dict[str, TickerSignals]:
    """
    Analyst eseményekből biblia-jelölések és pontszám számítása ticker szinten.
    """
    by_ticker: Dict[str, List[AnalystEvent]] = defaultdict(list)
    for ev in events:
        by_ticker[ev.ticker].append(ev)

    result: Dict[str, TickerSignals] = {}

    for ticker, evs in by_ticker.items():
        sig = TickerSignals(ticker=ticker)

        # 1) Pozitív analyst események (upgrade, PT emelés, pozitív coverage)
        positive_count = 0
        for ev in evs:
            act = ev.action.lower()
            notes = ev.notes.lower()
            # egyszerű szabályok: upgrade / initiate / overweight / buy stb.
            if any(kw in act for kw in ["upgrade", "initiated", "overweight", "outperform", "buy"]):
                positive_count += 1
                continue
            # PT emelés kulcsszó a megjegyzésben
            if "pt emelés" in notes or "price target raised" in notes or "pt raised" in notes:
                positive_count += 1
        sig.positive_analyst_events = positive_count

        # 2) Guidance / outlook emelés
        for ev in evs:
            notes = ev.notes.lower()
            if any(
                kw in notes
                for kw in [
                    "guidance raised",
                    "raises guidance",
                    "emelt guidance",
                    "iránymutatás-emel",
                    "outlook raised",
                    "outlook improved",
                ]
            ):
                sig.has_guidance_upgrade = True
                break

        # 3) Konszenzus EPS/árbevétel felfelé módosul – jelenleg placeholder
        sig.has_consensus_revision_up = False

        # 4) Közelgő katalizátor (3–12 hónap)
        for ev in evs:
            notes = ev.notes.lower()
            if any(
                kw in notes
                for kw in [
                    "product launch",
                    "terméklaunch",
                    "pdufa",
                    "phase 3 readout",
                    "earnings event",
                    "capital markets day",
                    "investor day",
                    "approval",
                    "jóváhagyás",
                ]
            ):
                sig.has_future_catalyst = True
                break

        result[ticker] = sig

    return result


def fetch_yahoo_snapshot(tickers: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """
    Yahoo Finance quote snapshot – több tickerre egyszerre.
    Visszaad egy dictet: {ticker: quote_dict}.
    """
    tickers_list = sorted({t.upper() for t in tickers if t})
    if not tickers_list:
        return {}

    symbols = ",".join(tickers_list)
    resp = requests.get(
        YAHOO_QUOTE_URL,
        params={"symbols": symbols},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()

    result: Dict[str, Dict[str, Any]] = {}
    quotes = data.get("quoteResponse", {}).get("result", [])
    for q in quotes:
        symbol = str(q.get("symbol") or "").upper()
        if not symbol:
            continue
        result[symbol] = q

    return result


def apply_52w_high_signal(signals: Dict[str, TickerSignals], yahoo_quotes: Dict[str, Dict[str, Any]]) -> None:
    """
    Beállítja a near_52w_high jelzőt a Yahoo-ból jövő adatok alapján:
    aktuális ár max. 5%-nál legyen távol a 12 havi csúcstól.
    """
    for ticker, sig in signals.items():
        q = yahoo_quotes.get(ticker)
        if not q:
            continue

        price = q.get("regularMarketPrice") or q.get("postMarketPrice")
        high_52w = q.get("fiftyTwoWeekHigh")

        try:
            price_f = float(price)
            high_f = float(high_52w)
        except Exception:
            continue

        if high_f <= 0:
            continue

        distance = (high_f - price_f) / high_f
        if distance <= 0.05:
            sig.near_52w_high = True


def compute_scores(signals: Dict[str, TickerSignals]) -> None:
    """
    A biblia 5 jelölését pontszámokká alakítja.
    A pontszám kézzel hangolható; most egy egyszerű lineáris modell:
      - 1) pozitív analyst események (>=2): +0.4
      - 2) guidance upgrade: +0.3
      - 3) konszenzus felhúzás (placeholder): +0.2
      - 4) közelgő katalizátor: +0.2
      - 5) near 52w high: +0.2
    """
    for sig in signals.values():
        score = 0.0
        # 1)
        if sig.positive_analyst_events >= 2:
            score += 0.4
        # 2)
        if sig.has_guidance_upgrade:
            score += 0.3
        # 3)
        if sig.has_consensus_revision_up:
            score += 0.2
        # 4)
        if sig.has_future_catalyst:
            score += 0.2
        # 5)
        if sig.near_52w_high:
            score += 0.2

        sig.score = score


def select_highconv(
    signals: Dict[str, TickerSignals],
    exclude_tickers: Set[str],
) -> List[TickerSignals]:
    """
    Kiválasztja a high-conv jelölteket:
      - nincs a kizárandó halmazban,
      - elég jel (MIN_SIGNAL_COUNT),
      - elég pont (MIN_SCORE).
    """
    candidates: List[TickerSignals] = []

    for ticker, sig in signals.items():
        if ticker in exclude_tickers:
            continue

        # jel-szám
        signal_flags = [
            sig.positive_analyst_events >= 2,
            sig.has_guidance_upgrade,
            sig.has_consensus_revision_up,
            sig.has_future_catalyst,
            sig.near_52w_high,
        ]
        signal_count = sum(1 for f in signal_flags if f)

        if signal_count < MIN_SIGNAL_COUNT:
            continue
        if sig.score < MIN_SCORE:
            continue

        candidates.append(sig)

    # score szerint csökkenőben rendezve
    candidates.sort(key=lambda s: s.score, reverse=True)
    return candidates


def build_highconv_json(
    candidates: List[TickerSignals],
) -> List[Dict[str, Any]]:
    """
    A TickerSignals listát high_conv_1.json kompatibilis dict-listává alakítja.
    """
    items: List[Dict[str, Any]] = []
    for sig in candidates:
        thesis_parts: List[str] = []

        if sig.positive_analyst_events >= 2:
            thesis_parts.append("Több friss felminősítés / céláremelés nagy házaktól.")
        if sig.has_guidance_upgrade:
            thesis_parts.append("A vállalat emelte az iránymutatását / pozitív guide-ot adott.")
        if sig.has_future_catalyst:
            thesis_parts.append("Közelgő 3–12 hónapos katalizátor (esemény / terméklaunch / jóváhagyás).")
        if sig.near_52w_high:
            thesis_parts.append("Árfolyam 52 hetes csúcs közelében, relatív erő fennáll.")

        thesis = " ".join(thesis_parts).strip()

        signals_list: List[str] = []
        if sig.positive_analyst_events >= 2:
            signals_list.append(f"{sig.positive_analyst_events} friss pozitív analyst esemény (upgrade / PT emelés).")
        if sig.has_guidance_upgrade:
            signals_list.append("Iránymutatás / outlook emelése.")
        if sig.has_consensus_revision_up:
            signals_list.append("EPS / árbevétel konszenzus felfelé módosult.")
        if sig.has_future_catalyst:
            signals_list.append("Közelgő specifikus katalizátor 3–12 hónapon belül.")
        if sig.near_52w_high:
            signals_list.append("Árfolyam 5%-on belül az 52 hetes csúcstól.")

        item = {
            "ticker": sig.ticker,
            "company": "",  # opcionálisan később kitölthető Yahoo 'shortName'-ből
            "thesis": thesis,
            "score": round(sig.score, 2),
            "signals": signals_list,
        }
        items.append(item)

    return items


def save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    exclude_tickers = load_exclude_tickers()

    print(f"[highconv_builder] Kizárandó tickerek: {len(exclude_tickers)} db")

    print(f"[highconv_builder] Analyst feed letöltése (utolsó {DAYS_BACK} nap)...")
    events = fetch_analyst_events()
    print(f"[highconv_builder] Analyst események: {len(events)} db")

    if not events:
        print("[highconv_builder] Nincs analyst esemény – high_conv_1.json üres lesz.")
        save_json(OUTPUT_PATH, [])
        return

    sigs = classify_events(events)

    print("[highconv_builder] Yahoo snapshot lekérése...")
    yahoo_quotes = fetch_yahoo_snapshot(sigs.keys())
    apply_52w_high_signal(sigs, yahoo_quotes)

    compute_scores(sigs)

    candidates = select_highconv(sigs, exclude_tickers)
    print(f"[highconv_builder] High-conv jelöltek: {len(candidates)} db")

    items = build_highconv_json(candidates)
    save_json(OUTPUT_PATH, items)
    print(f"[highconv_builder] high_conv_1.json elkészült: {OUTPUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
