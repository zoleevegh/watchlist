"""
highconv_builder.py
(Sheet-alapú, kizárásos high-conv builder – FRISSÍTETT LINKKEL + ÚJ OUTPUT ÚTTAL)

3–12 hónapos „high-conviction” jelöltek automatikus azonosítása és high_conv_1.json generálása.

FRISSÍTÉSEK EHHEZ A VERZIÓHOZ
-----------------------------
- ANALYST_FEED_URL: a JELENLEGI, működő Apps Script analyst endpointod
    https://script.google.com/macros/s/AKfycbx5geIQ-eAnAqzjbPmTd5k59Hn_MnLiKh_9c8ft0nSWd2P3BQt1o5Dv6JQqNJi4q7X4Ow/exec

- EXCLUDE_TICKERS_CSV_URL: a kézi futásoknál is használt MASTER CSV linked
    https://docs.google.com/spreadsheets/d/e/2PACX-1vS0vpBd1ADF3_Godyflgh3-TbJoj_CCRBJ4QHeLiZCY12tHPWuTIL5OZBTByMApdT92vjS2pRpI1koM/pub?output=csv

- OUTPUT_PATH: a #1 biblia-struktúrához igazítva most már
    reports/1/high_conv_1.json

A kizárandó tickereket (portfólió + watchlist) dinamikusan a fenti CSV-ből olvassa,
nem kell semmit txt-be írogatnod. A txt csak opcionális extra rásegítés.

HASZNÁLAT
---------
1) Tedd ezt a fájlt a projekted scripts/ mappájába:
       scripts/highconv_builder.py

2) Futtatás (repo gyökeréből, ahol a reports/ mappa is van):
       pip install requests   (ha még nincs)
       python scripts/highconv_builder.py

3) Eredmény:
       reports/1/high_conv_1.json

   Ezt a macro_highconv_helpers_v2.py script fogja beolvasni, és a
   „Listán kívüli, 3–12 hónapos high-conviction jelöltek” blokkot a #1-es riport
   végére bevarrja.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import time
import random

import requests

VERSION = "v1.0.3-rate-limit-safe-yahoochart-fallback"

# Yahoo rate-limit / retry config
YAHOO_BATCH_SIZE = 50
YAHOO_MAX_RETRIES = 6
YAHOO_BASE_SLEEP_SEC = 1.0

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
})



# --- KONFIGURÁCIÓ --------------------------------------------------------------

# Apps Script analyst feed URL – FRISSÍTETT, JELENLEGI LINK
ANALYST_FEED_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbx5geIQ-eAnAqzjbPmTd5k59Hn_MnLiKh_9c8ft0nSWd2P3BQt1o5Dv6JQqNJi4q7X4Ow"
    "/exec"
)

# Hány napra visszamenőleg nézzük az analyst eseményeket a high-convhez
DAYS_BACK = 30

# Dinamikus kizárási lista Google Sheets CSV-ből
# A kézi futásoknál is használt MASTER CSV linked
EXCLUDE_TICKERS_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vS0vpBd1ADF3_Godyflgh3-TbJoj_CCRBJ4QHeLiZCY12tHPWuTIL5OZBTByMApdT92vjS2pRpI1koM/"
    "pub?output=csv"
)

# Opcionális extra kizárási txt fájl (egy sor = ticker, vagy vesszővel elválasztott lista)
EXCLUDE_TICKERS_FILE = "exclude_tickers.txt"

# Alapból üres kézi halmaz; ha akarsz, itt is megadhatsz pár tickert fixen
EXTRA_EXCLUDE_TICKERS: Set[str] = set()

# Yahoo Finance quote endpoint (több ticker egyszerre)
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

# High-conv pontszám küszöb
MIN_SCORE = 0.6

# Min. jel szám (az 5 biblia-jelölés közül)
MIN_SIGNAL_COUNT = 2

# high_conv_1.json kimeneti fájl – BIBLIA SZERINTI HELYRE RAKVA
OUTPUT_PATH = Path("reports/1/high_conv_1.json")


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
            v = value.replace("Z", "+00:00")
            return datetime.fromisoformat(v)
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def load_exclude_tickers() -> Set[str]:
    """
    Dinamikus kizárási lista építése:
      - EXTRA_EXCLUDE_TICKERS (hardcode)
      - EXCLUDE_TICKERS_CSV_URL (Google Sheets CSV)
      - EXCLUDE_TICKERS_FILE (opcionális txt)
    """
    tickers: Set[str] = set(t.upper() for t in EXTRA_EXCLUDE_TICKERS)

    # 1) CSV-ből
    if EXCLUDE_TICKERS_CSV_URL and EXCLUDE_TICKERS_CSV_URL.startswith("http"):
        try:
            resp = requests.get(EXCLUDE_TICKERS_CSV_URL, timeout=30)
            resp.raise_for_status()
            text = resp.text
            f = StringIO(text)
            reader = csv.reader(f)
            rows = list(reader)
            if rows:
                header = [h.strip() for h in rows[0]]
                data_rows = rows[1:] if any(header) else rows

                # Ticker oszlop index
                ticker_idx = 0
                for i, name in enumerate(header):
                    if name.lower() == "ticker":
                        ticker_idx = i
                        break

                for row in data_rows:
                    if len(row) <= ticker_idx:
                        continue
                    t = row[ticker_idx].strip().upper()
                    if t:
                        tickers.add(t)
        except Exception as e:
            print(f"[highconv_builder] FIGYELEM: CSV kizárási lista betöltése sikertelen: {e}")

    # 2) Opcionális txt fájl
    path = Path(EXCLUDE_TICKERS_FILE)
    if path.is_file():
        try:
            text = path.read_text(encoding="utf-8")
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                for part in line.replace(";", ",").split(","):
                    t = part.strip().upper()
                    if t:
                        tickers.add(t)
        except Exception as e:
            print(f"[highconv_builder] FIGYELEM: txt kizárási lista betöltése sikertelen: {e}")

    return tickers


def fetch_analyst_events() -> List[AnalystEvent]:
    """
    Apps Script analyst feed hívása.
    Várja, hogy a válasz JSON-ben:
        { "ok": true, "events": [ ... ] }
    legyen.
    """
    params = {"type": "analyst", "report": "1", "days": str(DAYS_BACK)}
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
            if any(kw in act for kw in ["upgrade", "initiated", "overweight", "outperform", "buy"]):
                positive_count += 1
                continue
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


def _sleep_backoff(attempt: int) -> None:
    # Exponential backoff + jitter
    base = YAHOO_BASE_SLEEP_SEC * (2 ** max(0, attempt))
    jitter = random.uniform(0.0, 0.25 * base)
    time.sleep(min(30.0, base + jitter))


def fetch_yahoo_chart_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Yahoo Finance chart fallback – akkor használjuk, ha a quote endpoint (v7/finance/quote) 401/403/429 miatt
    nem ad adatot. A cél minimálisan ez:
      - regularMarketPrice (vagy utolsó close)
      - fiftyTwoWeekHigh (1y napi close max)
    """
    t = (ticker or "").upper().strip()
    if not t:
        return {}
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?range=1y&interval=1d&includePrePost=false"
    try:
        resp = requests.get(url, headers=YAHOO_HEADERS, timeout=YAHOO_TIMEOUT)
        
                # Ha a quote endpoint blokkol (401/403) vagy rate-limit (429), próbáljuk chart fallback-kal tickerenként.
                if resp.status_code in (401, 403, 429):
                    for t in batch:
                        if t not in result:
                            q = fetch_yahoo_chart_snapshot(t)
                            if q:
                                result[t] = q
                    last_err = f"HTTP {resp.status_code}"
                    break
# Ha ez is blokkolt, hagyjuk üresen
        if resp.status_code >= 400:
            return {}
        data = resp.json()
        res = (((data.get("chart") or {}).get("result") or [])[:1] or [None])[0] or {}
        meta = res.get("meta") or {}
        price = meta.get("regularMarketPrice") or meta.get("previousClose")
        closes = (((res.get("indicators") or {}).get("quote") or [])[:1] or [None])[0] or {}
        close_list = closes.get("close") or []
        try:
            max_close = max([c for c in close_list if isinstance(c, (int, float))])
        except Exception:
            max_close = None
        # Biztonság: ha nincs meta-price, vegyük az utolsó valid close-t
        if price is None:
            try:
                price = [c for c in close_list if isinstance(c, (int, float))][-1]
            except Exception:
                price = None
        out = {"symbol": t}
        if price is not None:
            out["regularMarketPrice"] = price
        if max_close is not None:
            out["fiftyTwoWeekHigh"] = max_close
        return out
    except Exception:
        return {}


def fetch_yahoo_snapshot(tickers: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """
    Yahoo Finance quote snapshot – több tickerre egyszerre.

    FONTOS: A Yahoo 429 / rate-limit esetén NEM dobjuk el a teljes futást.
    - batch-eljük a tickereket (YAHOO_BATCH_SIZE)
    - 429/5xx esetén retry + backoff
    - végső sikertelenségnél a batch kimarad, a futás megy tovább
    """
    tickers_list = sorted({t.upper() for t in tickers if t})
    if not tickers_list:
        return {}

    result: Dict[str, Dict[str, Any]] = {}

    # Batches
    for i in range(0, len(tickers_list), YAHOO_BATCH_SIZE):
        batch = tickers_list[i : i + YAHOO_BATCH_SIZE]
        symbols = ",".join(batch)

        last_err: Optional[str] = None
        for attempt in range(YAHOO_MAX_RETRIES):
            try:
                resp = SESSION.get(
                    YAHOO_QUOTE_URL,
                    params={"symbols": symbols},
                    timeout=20,
                    headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
                )

                # Rate limit
                if resp.status_code == 429:
                    last_err = "HTTP 429 Too Many Requests"
                    _sleep_backoff(attempt)
                    continue

                # Transient server errors
                if 500 <= resp.status_code < 600:
                    last_err = f"HTTP {resp.status_code}"
                    _sleep_backoff(attempt)
                    continue

                resp.raise_for_status()

                data = resp.json()
                quotes = data.get("quoteResponse", {}).get("result", []) or []
                for q in quotes:
                    symbol = str(q.get("symbol") or "").upper()
                    if symbol:
                        result[symbol] = q

                # Ha a batch-ből hiányzik ticker, próbáljuk chart fallback-kal pótolni
                for t in batch:
                    if t not in result:
                        q2 = fetch_yahoo_chart_snapshot(t)
                        if q2:
                            result[t] = q2

                last_err = None
                break
            except Exception as e:
                last_err = str(e)
                _sleep_backoff(attempt)

        if last_err:
            print(f"[highconv_builder] FIGYELEM: Yahoo snapshot batch kihagyva ({len(batch)} ticker): {last_err}")

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
    Egyszerű súlyozás:
      - 1) pozitív analyst események (>=2): +0.4
      - 2) guidance upgrade: +0.3
      - 3) konszenzus felhúzás (placeholder): +0.2
      - 4) közelgő katalizátor: +0.2
      - 5) near 52w high: +0.2
    """
    for sig in signals.values():
        score = 0.0
        if sig.positive_analyst_events >= 2:
            score += 0.4
        if sig.has_guidance_upgrade:
            score += 0.3
        if sig.has_consensus_revision_up:
            score += 0.2
        if sig.has_future_catalyst:
            score += 0.2
        if sig.near_52w_high:
            score += 0.2
        sig.score = score


def select_highconv(signals: Dict[str, TickerSignals], exclude_tickers: Set[str]) -> List[TickerSignals]:
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

    candidates.sort(key=lambda s: s.score, reverse=True)
    return candidates


def build_highconv_json(candidates: List[TickerSignals]) -> List[Dict[str, Any]]:
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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    print(f"[highconv_builder] Verzió: {VERSION}")
    exclude_tickers = load_exclude_tickers()
    print(f"[highconv_builder] Kizárandó tickerek összesen: {len(exclude_tickers)} db")

    print(f"[highconv_builder] Analyst feed letöltése (utolsó {DAYS_BACK} nap)...")
    events = fetch_analyst_events()
    print(f"[highconv_builder] Analyst események: {len(events)} db")

    if not events:
        print("[highconv_builder] Nincs analyst esemény – high_conv_1.json üres lesz.")
        save_json(OUTPUT_PATH, [])
        return

    sigs = classify_events(events)

    print("[highconv_builder] Yahoo snapshot lekérése...")
    try:
        yahoo_quotes = fetch_yahoo_snapshot(sigs.keys())
        apply_52w_high_signal(sigs, yahoo_quotes)
    except Exception as e:
        print(f"[highconv_builder] FIGYELEM: Yahoo snapshot sikertelen (folytatom 52w jel nélkül): {e}")

    compute_scores(sigs)

    candidates = select_highconv(sigs, exclude_tickers)
    print(f"[highconv_builder] High-conv jelöltek: {len(candidates)} db")

    items = build_highconv_json(candidates)
    save_json(OUTPUT_PATH, items)
    print(f"[highconv_builder] high_conv_1.json elkészült: {OUTPUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
