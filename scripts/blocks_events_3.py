# blocks_events_3.py
# Version: v1.1.0
"""Analyst / Catalyst / High-Conv block builder for Report #3 (intraday).

Defensive, structured skeleton with partial real logic:
- Normalizes JSON inputs.
- Filters by #3 time window (placeholder).
- Applies basic relevance rules (upgrade/downgrade/PT-change).
- Builds markdown for 3 blocks:
    * Analyst
    * Catalysts
    * High-conv (listán kívüli)

Full biblia-level relevance scoring comes in v1.2.0.
"""

import json
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# JSON utility
# ---------------------------------------------------------------------------

def _ensure_list(data: Any) -> List[Dict[str, Any]]:
    """Normalizes JSON-like input into list of dict items."""
    if data is None:
        return []

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return []

    if isinstance(data, dict):
        # Try common keys
        items = (
            data.get("items")
            or data.get("events")
            or data.get("data")
            or data.get("feed")
        )
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
        return []

    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]

    return []


# ---------------------------------------------------------------------------
# Timestamp filter placeholder for #3 window
# ---------------------------------------------------------------------------

def _is_in_intraday_window(ts: str) -> bool:
    """Determines if timestamp belongs to #3 window (15:30 → now).
    Placeholder in v1.1.0: any non-empty timestamp passes.
    """
    if not ts:
        return False
    return True


# ---------------------------------------------------------------------------
# BASIC relevance rules for v1.1.0
# ---------------------------------------------------------------------------

_MAJOR_HOUSES = {
    "morgan stanley", "jpmorgan", "jp morgan", "bank of america", "bofa",
    "ubs", "goldman", "goldman sachs", "citi", "citigroup",
    "barclays", "deutsche bank", "jefferies"
}

def _is_major_house(text: str) -> bool:
    t = text.lower()
    return any(h in t for h in _MAJOR_HOUSES)


def _is_relevant_analyst_event(ev: Dict[str, Any]) -> bool:
    action = str(ev.get("action") or ev.get("type") or "").lower()
    house = str(ev.get("source") or ev.get("analyst") or "").lower()

    if not _is_major_house(house):
        return False

    if any(k in action for k in ["upgrade", "downgrade", "initiation", "pt", "price target"]):
        return True

    return False


def _is_relevant_catalyst(ev: Dict[str, Any]) -> bool:
    desc = (ev.get("desc") or ev.get("title") or ev.get("event") or "").lower()
    if any(k in desc for k in ["fda", "pdufa", "earnings", "guidance", "launch", "investor day", "merger", "m&a"]):
        return True
    return False


# ---------------------------------------------------------------------------
# Markdown builders
# ---------------------------------------------------------------------------

def build_analyst_block(analyst_json: Any) -> str:
    items = _ensure_list(analyst_json)
    relevant: List[str] = []

    for ev in items:
        ts = str(ev.get("timestamp") or ev.get("time") or "")
        if not _is_in_intraday_window(ts):
            continue
        if not _is_relevant_analyst_event(ev):
            continue

        ticker = str(ev.get("ticker") or "").upper()
        house = str(ev.get("source") or ev.get("analyst") or "Ismeretlen")
        action = str(ev.get("action") or ev.get("type") or "").strip()
        note = str(ev.get("note") or ev.get("reason") or "").strip()

        line = f"- {ticker} – {house}: {action}"
        if note:
            line += f" – {note}"
        relevant.append(line)

    if not relevant:
        return "### Bejelentések és fel/lemínősítések (nyitástól mostanáig)
Nincs releváns analyst esemény ma.
"

    block = "### Bejelentések és fel/lemínősítések (nyitástól mostanáig)
" + "
".join(relevant) + "
"
    return block


def build_catalyst_block(catalysts_json: Any) -> str:
    items = _ensure_list(catalysts_json)
    relevant: List[str] = []

    for ev in items:
        ts = str(ev.get("timestamp") or ev.get("time") or "")
        if not _is_in_intraday_window(ts):
            continue
        if not _is_relevant_catalyst(ev):
            continue

        ticker = str(ev.get("ticker") or "").upper()
        desc = str(ev.get("desc") or ev.get("title") or "").strip()
        relevant.append(f"- {ticker} – {desc}")

    if not relevant:
        return "### Közelgő katalizátorok (mai módosítások)
Nincs ma friss releváns katalizátor.
"

    block = "### Közelgő katalizátorok (mai módosítások)
" + "
".join(relevant) + "
"
    return block


def build_highconv_block(highconv_json: Any) -> str:
    items = _ensure_list(highconv_json)
    # v1.1.0: basic pass-through; v1.2.0 gets full scoring + filtering
    relevant: List[str] = []

    for ev in items:
        ticker = str(ev.get("ticker") or ev.get("symbol") or "").upper()
        score = ev.get("score")
        note = str(ev.get("note") or "").strip()

        if score is None:
            continue

        if score >= 0.6:
            line = f"- {ticker} – score={score:.2f}"
            if note:
                line += f" – {note}"
            relevant.append(line)

    if not relevant:
        return "### Listán kívüli, 3–12 hónapos high-conv jelöltek
Nincs releváns jelölt.
"

    block = "### Listán kívüli, 3–12 hónapos high-conv jelöltek
" + "
".join(relevant) + "
"
    return block
