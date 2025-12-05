#!/usr/bin/env python3
"""Builds the 'Bejelentések & fel/lemínősítések' markdown block from analyst JSON.

Verzió: v1.2.0

Várt JSON (rugalmasan kezelve):
    {
        "events": [
            {
                "ticker": "NVDA",
                "date": "2025-11-25",
                "firm": "Morgan Stanley",
                "action": "Upgrade",
                "from_rating": "EW",
                "to_rating": "OW",
                "price_target": 160,
                "notes": "price target raised",
                "url": "https://example.com | MarketBeat",
                "source": "MarketBeat"
            },
            ...
        ]
    }

- A gyökér lehet lista is.
- A kulcs lehet 'events', 'items', 'analyst', 'data' stb.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

__version__ = "1.2.0"

CANDIDATE_KEYS: Sequence[str] = ("events", "items", "analyst", "data")


def _load_json(path: Path) -> Any:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _extract_events(data: Any) -> List[Dict[str, Any]]:
    """Return a flat list of event dicts from a flexible JSON structure."""
    if data is None:
        return []

    # 1) Ha eleve lista
    if isinstance(data, list):
        base = data
    # 2) Ha dict, próbáljuk az ismert kulcsokat
    elif isinstance(data, dict):
        base = None
        for k in CANDIDATE_KEYS:
            v = data.get(k)
            if isinstance(v, list):
                base = v
                break
        # 3) Ha még mindig nincs, keressünk bárhol listát
        if base is None:
            collected: List[Dict[str, Any]] = []
            for v in data.values():
                if isinstance(v, list):
                    for e in v:
                        if isinstance(e, dict):
                            collected.append(e)
            base = collected
    else:
        base = []

    events: List[Dict[str, Any]] = []
    for raw in base or []:
        if isinstance(raw, dict):
            events.append(raw)
    return events


def _fmt_pt(ev: Dict[str, Any]) -> str:
    pt = ev.get("price_target")
    if pt in (None, ""):
        return ""
    try:
        return f"PT: {float(pt):.0f}"
    except Exception:
        return f"PT: {pt}"


def _fmt_ratings(ev: Dict[str, Any]) -> str:
    fr = (ev.get("from_rating") or "").strip()
    tr = (ev.get("to_rating") or "").strip()
    if fr and tr and fr != tr:
        return f"{fr} → {tr}"
    if tr:
        return tr
    return fr


def _parse_date(ev: Dict[str, Any]) -> datetime:
    raw = (ev.get("date") or ev.get("date_str") or "").strip()
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            continue
    # Ha nem értelmezhető, adjunk nagyon régi dátumot, hogy a lista végére essen
    return datetime.min


def build_block_from_events(events: Iterable[Dict[str, Any]]) -> str:
    """Build markdown block from iterable of analyst event dicts.

    Újdonságok v1.1.0:
    - Események *ticker szerint* csoportosítva
    - Egy ticker alatt több sor is szerepelhet (időrendben, legfrissebb elöl)
    - Robusztusabb JSON-kezelés / forráshoz kötött mezők
    """
    events = list(events)
    if not events:
        return ""

    # Szűrés: legyen ticker
    valid_events: List[Dict[str, Any]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        ticker = (ev.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        ev = dict(ev)
        ev["_ticker"] = ticker
        ev["_dt"] = _parse_date(ev)
        valid_events.append(ev)

    if not valid_events:
        return ""

    # Csoportosítás ticker szerint
    by_ticker: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for ev in valid_events:
        by_ticker[ev["_ticker"]].append(ev)

    # Rendezzük ticker szerint ABC-ben, azon belül dátum szerint (legújabb elöl)
    for evs in by_ticker.values():
        evs.sort(key=lambda e: e.get("_dt"), reverse=True)

    lines: List[str] = ["### Bejelentések & fel/lemínősítések", ""]

    for ticker in sorted(by_ticker.keys()):
        lines.append(f"**{ticker}**")
        for ev in by_ticker[ticker]:
            firm = (ev.get("firm") or "").strip()
            action = (ev.get("action") or "").strip()
            ratings = _fmt_ratings(ev)
            pt = _fmt_pt(ev)
            notes = (ev.get("notes") or "").strip()
            source = (ev.get("source") or "").strip()

            segments: List[str] = []

            main_parts: List[str] = []
            if firm:
                main_parts.append(firm)
            change_parts: List[str] = []
            if action:
                change_parts.append(action)
            if ratings:
                change_parts.append(ratings)
            if change_parts:
                main_parts.append(" ".join(change_parts))

            if main_parts:
                segments.append(" – ".join(main_parts))

            if pt:
                segments.append(pt)
            if notes:
                segments.append(notes)
            if source:
                segments.append(f"[{source}]")

            if segments:
                lines.append("- " + " – ".join(segments))

        lines.append("")  # üres sor ticker blokkok között

    return "\n".join(lines).rstrip() + "\n\n"


def build_block_from_file(path: Path) -> str:
    """Load JSON from *path* and build markdown block. Returns '' if nothing."""
    data = _load_json(path)
    events = _extract_events(data)
    return build_block_from_events(events)


__all__ = ["build_block_from_events", "build_block_from_file", "__version__"]
