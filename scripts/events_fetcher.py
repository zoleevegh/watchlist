#!/usr/bin/env python3
"""
events_fetcher.py

Szerep:
- A #1/#2/#3 jelentésekhez tartozó 5-ös és 6-os blokk (elemzői lépések, közeli katalizátorok)
  JSON inputjait generálja:
    * reports/analyst_1.json, reports/analyst_2.json, reports/analyst_3.json
    * reports/catalysts_1.json, reports/catalysts_2.json, reports/catalysts_3.json

Fontos:
- Ez a modul NEM maga scrapel/letölt az internetről konkrét oldalt.
- Rá tudsz kötni bármilyen strukturált adatforrást (MarketBeat export, saját crawler, API).
- A biblia szerinti szűrés/összeállítás (portfolio vs. watchlist, anyagi lényegesség, ≥±3%)
  itt történik a NYERS eseményekre.

Várt nyers input (raw JSON) – opcionális, ha van külön crawler:
- reports/raw_analyst_{report}.json
- reports/raw_catalysts_{report}.json

Ajánlott séma egy nyers elemre:
{
    "ticker": "NVDA",
    "scope": "portfolio|watchlist|other",
    "event_type": "upgrade|downgrade|pt_raise|pt_cut|guide|M&A|dividend|buyback|mgmt",
    "headline": "Raymond James strong-buy-ra emelte az NVDA-t",
    "summary": "PT 230→260, AI-kereslet továbbra is erős...",
    "source": "MarketBeat",
    "ts": "2025-11-25T10:30:00Z"
}
"""

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional


# ---------- Adatszerkezetek ----------

@dataclass
class TickerScope:
    ticker: str
    scope: str  # "portfolio" | "watchlist" | "other"


@dataclass
class RawEvent:
    ticker: str
    scope: str
    event_type: str
    headline: str
    summary: str
    source: str
    ts: datetime


# ---------- Segédfüggvények ----------

def _load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_ts(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        # ISO + "Z" támogatás
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def load_raw_events(path: str, default_scope: str) -> List[RawEvent]:
    """
    Nyers események betöltése rugalmas JSON-ból.

    Elfogadott formátumok:
    - lista dict-ekkel (RawEvent-séma)
    - vagy lista plain stringekkel (ticker/scope nélkül, 'other'-ként kezeljük)
    """
    data = _load_json(path)
    events: List[RawEvent] = []
    if data is None:
        return events

    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                events.append(
                    RawEvent(
                        ticker="",
                        scope=default_scope,
                        event_type="text_only",
                        headline=item,
                        summary="",
                        source="raw" if default_scope else "",
                        ts=datetime.now(timezone.utc),
                    )
                )
            elif isinstance(item, dict):
                ticker = item.get("ticker", "")
                scope = item.get("scope") or default_scope or "other"
                ev_type = item.get("event_type", "unknown")
                headline = item.get("headline") or item.get("text") or ""
                summary = item.get("summary", "")
                source = item.get("source", "")
                ts_raw = item.get("ts") or item.get("time") or item.get("datetime") or ""
                ts = _parse_ts(ts_raw) or datetime.now(timezone.utc)
                events.append(
                    RawEvent(
                        ticker=ticker,
                        scope=scope,
                        event_type=ev_type,
                        headline=headline,
                        summary=summary,
                        source=source,
                        ts=ts,
                    )
                )
    return events


def detect_ticker_scope(universe_json: str) -> Dict[str, TickerScope]:
    """
    Ticker-univerzum beolvasása.

    Input lehet pl. a reports/latest_1.json, ahol:
    - dict: positions + watchlist
    - vagy lista dict-ekkel ('ticker', opcionálisan 'scope'/'type')
    """
    data = _load_json(universe_json)
    mapping: Dict[str, TickerScope] = {}
    if data is None:
        return mapping

    def add(t: str, scope: str):
        t = (t or "").upper().strip()
        if not t:
            return
        if t not in mapping:
            mapping[t] = TickerScope(ticker=t, scope=scope)

    if isinstance(data, dict):
        positions = data.get("positions") or data.get("portfolio") or []
        watchlist = data.get("watchlist") or []
        for item in positions:
            if isinstance(item, dict):
                add(item.get("ticker"), "portfolio")
            elif isinstance(item, str):
                add(item, "portfolio")
        for item in watchlist:
            if isinstance(item, dict):
                add(item.get("ticker"), "watchlist")
            elif isinstance(item, str):
                add(item, "watchlist")

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                t = item.get("ticker") or item.get("symbol") or ""
                scope = item.get("scope") or item.get("type") or "other"
                add(t, scope)
            elif isinstance(item, str):
                add(item, "other")

    return mapping


def filter_events_for_report(
    events: List[RawEvent],
    report: int,
    scope_map: Dict[str, TickerScope],
) -> Tuple[List[str], List[str]]:
    """
    Biblia-szerinti szűrés v1 (alap skeleton):

    - 5-ös blokk (Bejelentések & elemzői lépések):
      * minden portfólió + watchlist esemény
      * + bármi, ami upgrade/downgrade/PT-módosítás (akkor is, ha scope ismeretlen)

    - 6-os blokk (Közeli katalizátorok 3–12 hónap):
      * event_type ∈ {guide, M&A, dividend, buyback, mgmt}
      * vagy a headline tartalmazza az 'earnings' szót

    A későbbiekben ide lehet betenni:
    - időablak-szűrést (ts alapján),
    - ≥±3% mozgás integrálását (ha a price-runner is ad JSON-t),
    - anyagi lényegességi thresholdokat.
    """
    analyst_lines: List[str] = []
    catalyst_lines: List[str] = []

    for ev in events:
        t = ev.ticker.upper().strip() if ev.ticker else ""
        scope = scope_map.get(t, TickerScope(ticker=t, scope=ev.scope or "other")).scope

        prefix = f"{t} – " if t else ""
        src = f" ({ev.source})" if ev.source else ""
        base_text = prefix + (ev.headline or ev.summary)
        if not base_text:
            continue
        full_text = base_text + src

        # 5-ös blokk
        if scope in ("portfolio", "watchlist") or ev.event_type in ("upgrade", "downgrade", "pt_raise", "pt_cut"):
            analyst_lines.append(full_text)

        # 6-os blokk
        head_lower = (ev.headline or "").lower()
        if ev.event_type in ("guide", "M&A", "dividend", "buyback", "mgmt") or "earnings" in head_lower:
            catalyst_lines.append(full_text)

    def dedup(seq: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for s in seq:
            if s not in seen:
                seen.add(s)
                out.append(s)
        return out

    return dedup(analyst_lines), dedup(catalyst_lines)


# ---------- CLI ----------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyst & catalyst events filter (5/6. blokk JSON generátor)"
    )
    parser.add_argument(
        "--report", type=int, required=True, choices=[1, 2, 3], help="Report # (1/2/3)"
    )
    parser.add_argument(
        "--universe-json",
        required=True,
        help="Ticker-univerzumot tartalmazó JSON (positions + watchlist)",
    )
    parser.add_argument(
        "--raw-analyst",
        default=None,
        help="Opcionális nyers analyst-events JSON (reports/raw_analyst_{report}.json)",
    )
    parser.add_argument(
        "--raw-catalysts",
        default=None,
        help="Opcionális nyers catalysts JSON (reports/raw_catalysts_{report}.json)",
    )
    parser.add_argument(
        "--analyst-out",
        default=None,
        help="Kimeneti path a reports/analyst_{report}.json fájlhoz",
    )
    parser.add_argument(
        "--catalysts-out",
        default=None,
        help="Kimeneti path a reports/catalysts_{report}.json fájlhoz",
    )

    args = parser.parse_args()

    report = args.report
    analyst_out = args.analyst_out or f"reports/analyst_{report}.json"
    catalysts_out = args.catalysts_out or f"reports/catalysts_{report}.json"

    scope_map = detect_ticker_scope(args.universe_json)

    raw_analyst_path = args.raw_analyst or f"reports/raw_analyst_{report}.json"
    raw_catalysts_path = args.raw_catalysts or f"reports/raw_catalysts_{report}.json"

    raw_events: List[RawEvent] = []
    raw_events += load_raw_events(raw_analyst_path, default_scope="other")
    raw_events += load_raw_events(raw_catalysts_path, default_scope="other")

    analyst_lines, catalyst_lines = filter_events_for_report(
        raw_events, report=report, scope_map=scope_map
    )

    os.makedirs(os.path.dirname(analyst_out) or ".", exist_ok=True)

    with open(analyst_out, "w", encoding="utf-8") as f:
        json.dump(analyst_lines, f, ensure_ascii=False, indent=2)

    with open(catalysts_out, "w", encoding="utf-8") as f:
        json.dump(catalyst_lines, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
