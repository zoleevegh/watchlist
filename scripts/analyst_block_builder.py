#!/usr/bin/env python3
"""Builds the 'Bejelentések & fel/lemínősítések' markdown block from analyst JSON.

Expected JSON structure (tolerant):
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

Top-level can also be a list, or the key can be 'items' instead of 'events'.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


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

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = None
        for key in CANDIDATE_KEYS:
            v = data.get(key)
            if isinstance(v, list):
                items = v
                break
        if items is None:
            # maybe dict-of-ticker → list[events]
            collected: List[Dict[str, Any]] = []
            for v in data.values():
                if isinstance(v, list):
                    for e in v:
                        if isinstance(e, dict):
                            collected.append(e)
            items = collected
    else:
        items = []

    events: List[Dict[str, Any]] = []
    for raw in items or []:
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


def build_block_from_events(events: Iterable[Dict[str, Any]]) -> str:
    """Build markdown block from iterable of analyst event dicts."""
    events = list(events)
    if not events:
        return ""

    lines: List[str] = ["### Bejelentések & fel/lemínősítések", ""]
    for ev in events:
        ticker = (ev.get("ticker") or "").strip().upper()
        firm = (ev.get("firm") or "").strip()
        action = (ev.get("action") or "").strip()
        ratings = _fmt_ratings(ev)
        pt = _fmt_pt(ev)
        notes = (ev.get("notes") or "").strip()
        source = (ev.get("source") or "").strip()

        parts: List[str] = []
        if ticker:
            parts.append(ticker)
        if firm:
            parts.append(firm)
        if action:
            parts.append(action)
        if ratings:
            parts.append(ratings)
        if pt:
            parts.append(pt)
        if notes:
            parts.append(notes)
        if source:
            parts.append(f"[{source}]")

        if parts:
            lines.append("- " + " – ".join(parts))

    lines.append("")
    return "\n".join(lines)


def build_block_from_file(path: Path) -> str:
    """Load JSON from *path* and build markdown block. Returns '' if nothing."""
    data = _load_json(path)
    events = _extract_events(data)
    return build_block_from_events(events)


__all__ = ["build_block_from_events", "build_block_from_file"]
