VERSION = "v1.0.6-biblia-version-top-no-future"

"""highconv_builder.py

(Sheet-alapú, kizárásos high-conv builder – TICKER-GUARD + SNAPSHOT SAFE)

3–12 hónapos „high-conviction” jelöltek automatikus azonosítása és high_conv_1.json generálása.

FRISSÍTÉSEK EHHEZ A VERZIÓHOZ
-----------------------------
- Verziószám a fájl ELEJÉN (biblia-szabály: folytatólagos verziózás).
- Ticker-guard: UPGRADE/DOWNGRADE stb. nem lehet ticker (stopword + regex).
- Yahoo snapshot 401/403/429 esetén nem törik a futás (best-effort + chart fallback).
- Biblia-kompatibilis: „listán kívüli” jelöltek = kizárjuk a MASTER (watchlist+pozíció) tickereit.

HASZNÁLAT
---------
1) Tedd ezt a fájlt a projekted scripts/ mappájába:
       scripts/highconv_builder.py

2) Futtatás (repo gyökeréből, ahol a reports/ mappa is van):
       pip install requests   (ha még nincs)
       python scripts/highconv_builder.py

3) Eredmény:
       reports/1/high_conv_1.json

# IMÁDSÁG
# Bocsáss meg uram, mert balfék voltam, és action szót tickernek néztem (UPGRADE).
# Add uram, hogy ez a build csak valódi tickereket engedjen át, és a Yahoo 401 ne törje el a futást.
"""

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
import re

import requests

# --- TICKER VALIDÁCIÓ ----------------------------------------------------------

_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,11}$")

_TICKER_STOPWORDS = {
    "UPGRADE","DOWNGRADE","UPGRADED","DOWNGRADED",
    "RAISE","RAISED","LOWER","LOWERED",
    "INITIATE","INITIATED","INITIATES","INITIATING",
    "MAINTAIN","MAINTAINED","REITERATE","REITERATED",
    "OUTPERFORM","UNDERPERFORM","NEUTRAL","BUY","SELL","HOLD",
    "OVERWEIGHT","UNDERWEIGHT","EQUALWEIGHT",
    "TARGET","PT","PRICE","ACTION","RATING",
}

def is_valid_ticker(sym: str) -> bool:
    if not sym:
        return False
    s = sym.strip().upper()
    if s in _TICKER_STOPWORDS:
        return False
    return bool(_TICKER_RE.match(s))


# --- KONFIGURÁCIÓ --------------------------------------------------------------

ANALYST_FEED_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbx5geIQ-eAnAqzjbPmTd5k59Hn_MnLiKh_9c8ft0nSWd2P3BQt1o5Dv6JQqNJi4q7X4Ow"
    "/exec"
)

DAYS_BACK = 30

EXCLUDE_TICKERS_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vS0vpBd1ADF3_Godyflgh3-TbJoj_CCRBJ4QHeLiZCY12tHPWuTIL5OZBTByMApdT92vjS2pRpI1koM/"
    "pub?output=csv"
)

EXCLUDE_TICKERS_FILE = "exclude_tickers.txt"
EXTRA_EXCLUDE_TICKERS: Set[str] = set()

YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

MIN_SCORE = 0.6
MIN_SIGNAL_COUNT = 2

OUTPUT_PATH = Path("reports/1/high_conv_1.json")

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
        ticker = str(data.get("ticker") or "").upper().strip()
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
    has_consensus_revision_up: bool = False  # placeholder
    has_future_catalyst: bool = False
    near_52w_high: bool = False
    score: float = 0.0


# --- SEGÉD FÜGGVÉNYEK ----------------------------------------------------------

def _parse_date(value: str) -> datetime:
    """ISO vagy 'YYYY-MM-DD' -> datetime (UTC). Ha nem érthető, 'most'."""
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
    """MASTER CSV + opcionális txt + extra set."""
    tickers: Set[str] = set(t.upper() for t in EXTRA_EXCLUDE_TICKERS if t)

    if EXCLUDE_TICKERS_CSV_URL and EXCLUDE_TICKERS_CSV_URL.startswith("http"):
        try:
            resp = requests.get(EXCLUDE_TICKERS_CSV_URL, timeout=30)
            resp.raise_for_status()
            f = StringIO(resp.text)
            reader = csv.reader(f)
            rows = list(reader)
            if rows:
                header = [h.strip() for h in rows[0]]
                data_rows = rows[1:] if any(header) else rows

                ticker_idx = 0
                for i, name in enumerate(header):
                    if name.lower() == "ticker":
                        ticker_idx = i
                        break

                for row in data_rows:
                    if len(row) <= ticker_idx:
                        continue
                    t = row[ticker_idx].strip().upper()
                    if is_valid_ticker(t):
                        tickers.add(t)
        except Exception as e:
            print(f"[highconv_builder] FIGYELEM: CSV kizárási lista betöltése sikertelen: {e}")

    path = Path(EXCLUDE_TICKERS_FILE)
    if path.is_file():
        try:
            txt = path.read_text(encoding="utf-8")
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue
                for part in line.replace(";", ",").split(","):
                    t = part.strip().upper()
                    if is_valid_ticker(t):
                        tickers.add(t)
        except Exception as e:
            print(f"[highconv_builder] FIGYELEM: txt kizárási lista betöltése sikertelen: {e}")

    return tickers


def fetch_analyst_events() -> List[AnalystEvent]:
    """Apps Script analyst feed hívása."""
    params = {"type": "analyst", "report": "1", "days": str(DAYS_BACK)}
    resp = requests.get(ANALYST_FEED_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

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
        if not is_valid_ticker(ev.ticker):
            continue
        events.append(ev)

    return events


def classify_events(events: Iterable[AnalystEvent]) -> Dict[str, TickerSignals]:
    """Analyst események -> biblia-jelölések ticker szinten."""
    by_ticker: Dict[str, List[AnalystEvent]] = defaultdict(list)
    for ev in events:
        if not is_valid_ticker(ev.ticker):
            continue
        by_ticker[ev.ticker].append(ev)

    result: Dict[str, TickerSignals] = {}

    for ticker, evs in by_ticker.items():
        sig = TickerSignals(ticker=ticker)

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

        sig.has_consensus_revision_up = False

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
    base = YAHOO_BASE_SLEEP_SEC * (2 ** max(0, attempt))
    jitter = random.uniform(0.0, 0.25 * base)
    time.sleep(min(30.0, base + jitter))


def fetch_yahoo_chart_snapshot(ticker: str) -> Dict[str, Any]:
    """Yahoo chart fallback 1y/1d: regularMarketPrice + fiftyTwoWeekHigh."""
    t = (ticker or "").upper().strip()
    if not is_valid_ticker(t):
        return {}
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?range=1y&interval=1d&includePrePost=false"
    try:
        resp = SESSION.get(url, timeout=20, headers={"Cache-Control": "no-cache", "Pragma": "no-cache"})
        if resp.status_code >= 400:
            return {}
        data = resp.json()
        res = (((data.get("chart") or {}).get("result") or [])[:1] or [None])[0] or {}
        meta = res.get("meta") or {}
        price = meta.get("regularMarketPrice") or meta.get("previousClose")
        closes = (((res.get("indicators") or {}).get("quote") or [])[:1] or [None])[0] or {}
        close_list = closes.get("close") or []
        max_close = None
        try:
            max_close = max([c for c in close_list if isinstance(c, (int, float))])
        except Exception:
            max_close = None
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
    """Yahoo quote snapshot batch. 401/403/429 esetén best-effort, nem dob."""
    tickers_list = sorted({t.upper() for t in tickers if is_valid_ticker(t)})
    if not tickers_list:
        return {}

    result: Dict[str, Dict[str, Any]] = {}

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

                if resp.status_code in (401, 403):
                    last_err = f"HTTP {resp.status_code} Unauthorized/Forbidden"
                    break

                if resp.status_code == 429:
                    last_err = "HTTP 429 Too Many Requests"
                    _sleep_backoff(attempt)
                    continue

                if 500 <= resp.status_code < 600:
                    last_err = f"HTTP {resp.status_code}"
                    _sleep_backoff(attempt)
                    continue

                resp.raise_for_status()
                data = resp.json()
                quotes = (data.get("quoteResponse") or {}).get("result") or []
                for q in quotes:
                    symbol = str(q.get("symbol") or "").upper()
                    if is_valid_ticker(symbol):
                        result[symbol] = q

                last_err = None
                break
            except Exception as e:
                last_err = str(e)
                _sleep_backoff(attempt)

        # Chart fallback for missing ones OR if quote blocked
        for t in batch:
            if t not in result:
                q2 = fetch_yahoo_chart_snapshot(t)
                if q2:
                    result[t] = q2

        if last_err:
            print(f"[highconv_builder] FIGYELEM: Yahoo snapshot batch issue ({len(batch)} ticker): {last_err}")

    return result


def apply_52w_high_signal(signals: Dict[str, TickerSignals], yahoo_quotes: Dict[str, Dict[str, Any]]) -> None:
    """near_52w_high: ár max 5%-ra az 52W csúcstól."""
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
    """Biblia jelölések -> pontszám."""
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
    """High-conv kiválasztás: exclude + signal_count + score."""
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
    """Kimeneti items lista."""
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

        items.append({
            "ticker": sig.ticker,
            "company": "",
            "thesis": thesis,
            "score": round(sig.score, 2),
            "signals": signals_list,
        })

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
