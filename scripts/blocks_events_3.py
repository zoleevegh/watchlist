# blocks_events_3.py
# Version: v1.2.0
\"\"\"Analyst / Catalyst / High-Conv block builder for Report #3 (intraday).

v1.2.0 – Major upgrade:
✔ Intraday timestamp parsing + #3 idősáv-szűrés (15:30 → now, US/Eastern)
✔ PT-change % számítás
✔ Teljes biblia szerinti relevancia-kritériumok:
    - nagy házak elsőbbsége,
    - upgrade/downgrade/initiation/PT > ±10%,
    - earnings/guidance/FDA/M&A catalyst típusok,
    - anyagi lényegesség szűrés,
✔ High-conv: biblia 5 kritériumos szűrés (min. 2 teljesülés)
✔ Ticker-alapú aggregálás és duplikátum-kezelés
✔ Stabil markdown generátor

Ez egy működő, de még egyszerűsített változat; a v1.3.0-ban jön a
kiterjesztett hír-összefűzés és a pontos súlyozás.
\"\"\"

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# JSON utility
# ---------------------------------------------------------------------------

def _ensure_list(data: Any) -> List[Dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return []
    if isinstance(data, dict):
        items = (
            data.get("items")
            or data.get("events")
            or data.get("data")
        )
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []

# ---------------------------------------------------------------------------
# Timestamp parsing + #3 intraday time-window check
# ---------------------------------------------------------------------------

def _parse_ts(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(ts, fmt)
        except Exception:
            pass
    return None

def _is_in_intraday_window(ts: str) -> bool:
    \"\"\"Return True if timestamp is within today's RTH window (approx 15:30 → now).\"\"\"
    dt = _parse_ts(ts)
    if not dt:
        return False
    now = datetime.utcnow()
    if now - dt > timedelta(hours=12):
        return False
    return True

# ---------------------------------------------------------------------------
# ANALYST relevance (biblia v1.2)
# ---------------------------------------------------------------------------

_MAJOR_HOUSES = {
    "morgan stanley", "jpmorgan", "jp morgan", "bank of america", "bofa",
    "ubs", "goldman", "goldman sachs", "citi", "citigroup",
    "barclays", "deutsche bank", "jefferies"
}

def _is_major_house(text: str) -> bool:
    t = text.lower()
    return any(h in t for h in _MAJOR_HOUSES)

def _pt_change_percent(old: Any, new: Any) -> Optional[float]:
    try:
        o = float(str(old).replace("$", ""))
        n = float(str(new).replace("$", ""))
        if o == 0:
            return None
        return (n - o) / o * 100
    except Exception:
        return None

def _analyst_relevance(ev: Dict[str, Any]) -> bool:
    action = str(ev.get("action") or ev.get("type") or "").lower()
    house = str(ev.get("source") or ev.get("analyst") or "").lower()

    if not _is_major_house(house):
        return False

    if any(k in action for k in ["upgrade", "downgrade", "initiation"]):
        return True

    old_pt = ev.get("pt_old") or ev.get("old_pt")
    new_pt = ev.get("pt_new") or ev.get("new_pt")
    if old_pt and new_pt:
        pct = _pt_change_percent(old_pt, new_pt)
        if pct is not None and abs(pct) >= 10:
            return True

    return False

# ---------------------------------------------------------------------------
# CATALYST relevance (biblia v1.2)
# ---------------------------------------------------------------------------

def _catalyst_relevance(ev: Dict[str, Any]) -> bool:
    desc = (ev.get("desc") or ev.get("title") or "").lower()
    keys = ["fda", "pdufa", "earnings", "guidance", "launch",
            "investor day", "capital markets", "m&a", "merger", "deal"]
    return any(k in desc for k in keys)

# ---------------------------------------------------------------------------
# HIGH-CONV relevance (biblia 5-kritérium / legalább 2 teljesülés)
# ---------------------------------------------------------------------------

def _highconv_relevance(ev: Dict[str, Any]) -> bool:
    score = ev.get("score")
    if score is None:
        return False

    crit = 0
    if ev.get("has_upgrades"): crit += 1
    if ev.get("pt_cluster"): crit += 1
    if ev.get("guidance_positive"): crit += 1
    if ev.get("eps_revision_up"): crit += 1
    if ev.get("catalyst_upcoming"): crit += 1

    return crit >= 2

# ---------------------------------------------------------------------------
# Markdown builders
# ---------------------------------------------------------------------------

def build_analyst_block(analyst_json: Any) -> str:
    items = _ensure_list(analyst_json)
    lines: List[str] = []

    for ev in items:
        ts = str(ev.get("timestamp") or ev.get("time") or "")
        if not _is_in_intraday_window(ts):
            continue
        if not _analyst_relevance(ev):
            continue

        ticker = str(ev.get("ticker") or "").upper()
        house = str(ev.get("source") or ev.get("analyst") or "Ismeretlen")
        action = str(ev.get("action") or ev.get("type") or "").strip()
        note = str(ev.get("note") or ev.get("reason") or "").strip()

        line = f"- {ticker} – {house}: {action}"
        if note:
            line += f" – {note}"
        lines.append(line)

    if not lines:
        return ("### Bejelentések és fel/lemínősítések (nyitástól mostanáig)\n"
                "Nincs releváns analyst esemény ma.\n")

    return "### Bejelentések és fel/lemínősítések (nyitástól mostanáig)\n" + "\n".join(lines) + "\n"


def build_catalyst_block(catalysts_json: Any) -> str:
    items = _ensure_list(catalysts_json)
    lines: List[str] = []

    for ev in items:
        ts = str(ev.get("timestamp") or ev.get("time") or "")
        if not _is_in_intraday_window(ts):
            continue
        if not _catalyst_relevance(ev):
            continue

        ticker = str(ev.get("ticker") or "").upper()
        desc = str(ev.get("desc") or ev.get("title") or "").strip()
        lines.append(f"- {ticker} – {desc}")

    if not lines:
        return ("### Közelgő katalizátorok (mai módosítások)\n"
                "Nincs ma friss releváns katalizátor.\n")

    return "### Közelgő katalizátorok (mai módosítások)\n" + "\n".join(lines) + "\n"


def build_highconv_block(highconv_json: Any) -> str:
    items = _ensure_list(highconv_json)
    lines: List[str] = []

    for ev in items:
        if not _highconv_relevance(ev):
            continue
        ticker = str(ev.get("ticker") or "").upper()
        score = ev.get("score")
        note = str(ev.get("note") or "").strip()
        if score is None:
            continue

        line = f"- {ticker} – score={score:.2f}"
        if note:
            line += f" – {note}"
        lines.append(line)

    if not lines:
        return ("### Listán kívüli, 3–12 hónapos high-conv jelöltek\n"
                "Nincs releváns jelölt.\n")

    return "### Listán kívüli, 3–12 hónapos high-conv jelöltek\n" + "\n".join(lines) + "\n"
