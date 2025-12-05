# blocks_events_3.py
# Version: v1.1.0
"""Analyst, catalyst és high‑conv blokkok építése a #3-as riporthoz.

Ez a modul NEM a nyers webes feedet hívja meg, hanem
a már előkészített, Apps Script által normalizált JSON-okból dolgozik:

- analyst_json:  a ?type=analyst&report=3 hívás eredménye (fájl: analyst_3.json)
- catalysts_json: a ?type=catalyst&report=3 hívás eredménye (fájl: catalysts_3.json)
- highconv_json:  a high_conv_1.json (vagy később high_conv_3.json) tartalma

A cél:
- „Bejelentések és fel/lemínősítések (nyitástól mostanáig)”
- „Közelgő katalizátorok”
- „Listán kívüli, 3–12 hónapos high‑conv jelöltek”

A kód szándékosan defenzív:
- elfogad JSON stringet, dictet, listát;
- ha egy blokkhoz nincs értelmes adat, akkor is ad értelmes szöveges fallbacket;
- inkább üres, de olvasható blokkot ad, mint tracebacket dob.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Tuple


# ---- Közös segédfüggvények -------------------------------------------------


def _ensure_list(obj: Any, key: str | None = None) -> List[Dict[str, Any]]:
    """Lazán normalizálja az inputot list-of-dict formára.

    Elfogadott input:
    - JSON string
    - dict (pl. {"items": [...]} vagy {"events": [...]})
    - lista, amely dict elemeket tartalmaz
    """
    if obj is None:
        return []

    if isinstance(obj, str):
        s = obj.strip()
        if not s:
            return []
        try:
            obj = json.loads(s)
        except Exception:
            return []

    if isinstance(obj, dict):
        if key is not None:
            items = obj.get(key) or []
        else:
            items = (
                obj.get("items")
                or obj.get("events")
                or obj.get("data")
                or obj.get("results")
                or []
            )
    else:
        items = obj

    if not isinstance(items, list):
        return []

    out: List[Dict[str, Any]] = []
    for it in items:
        if isinstance(it, dict):
            out.append(it)
    return out


def _norm_ticker(ev: Dict[str, Any]) -> str:
    t = ev.get("ticker") or ev.get("symbol") or ev.get("underlying") or ""
    t = str(t).strip().upper()
    return t or "ISMERETLEN"


def _safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


# ---- Analyst blokk (#3) -----------------------------------------------------


def _is_material_analyst_event(ev: Dict[str, Any]) -> bool:
    """Anyagi lényegesség, erős szűrés a biblia szerint.

    Heurisztikusan megnézzük:
    - action: 'upgrade', 'downgrade', 'initiation', 'reiterated', stb.
    - price_target_change: van‑e érdemi PT-változás
    - impact_flag: explicit 'major' / 'significant' jelölés az Apps Scriptből
    """
    action = _safe_str(ev.get("action")).lower()
    impact = _safe_str(ev.get("impact") or ev.get("impact_flag")).lower()

    # Nagyobb súly: upgrade/downgrade/initiation
    if any(k in action for k in ["upgrade", "downgrade", "initiation", "resume"]):
        return True

    # Explicit impact flag
    if impact in {"major", "significant"}:
        return True

    # Ha egyáltalán nincs akció, ne kerüljenek be.
    if not action:
        return False

    return False


def build_analyst_block(analyst_json: Any) -> str:
    """Bejelentések & fel/lemínősítések blokk a #3‑as riporthoz.

    A #3‑nál csak a mai, nyitástól mostanáig releváns eseményeket várjuk
    (időablak-szűrést az Apps Script + feed_parser végzi),
    itt már csak a lényeges eseményeket listázzuk.
    """
    header = "### Bejelentések és fel/lemínősítések (nyitástól mostanáig)"
    events = _ensure_list(analyst_json)

    # Szűrés anyagi lényegességre
    material: List[Dict[str, Any]] = [ev for ev in events if _is_material_analyst_event(ev)]

    if not events:
        return header + "\nMa eddig nem érkezett elemzői bejelentés a #3‑as időablakban.\n"
    if not material:
        return header + "\nMa volt ugyan elemzői aktivitás, de a biblia szerinti szűrés után nem maradt anyagilag lényeges esemény.\n"

    lines: List[str] = [header]

    # Csoportosítsuk ticker szerint, majd időrendben, ha van timestamp.
    # A pontos időbélyeg kezelését a feed_parser végzi, itt csak stringként használjuk.
    def sort_key(ev: Dict[str, Any]) -> Tuple[str, str]:
        return _norm_ticker(ev), _safe_str(ev.get("time") or ev.get("timestamp") or ev.get("date"))

    for ev in sorted(material, key=sort_key):
        ticker = _norm_ticker(ev)
        action = _safe_str(ev.get("action")) or "értékelés frissítve"
        rating_from = _safe_str(ev.get("from")) or _safe_str(ev.get("from_rating"))
        rating_to = _safe_str(ev.get("to")) or _safe_str(ev.get("to_rating"))
        broker = _safe_str(ev.get("broker") or ev.get("firm") or ev.get("source"))

        # Egysoros összefoglaló (Apps Script‑től jöhet 'headline' vagy 'note')
        note = _safe_str(ev.get("headline") or ev.get("note") or ev.get("comment"))

        bits: List[str] = [ticker]

        if broker:
            bits.append(f"– {broker}")
        if action:
            bits.append(f" {action}")
        if rating_from or rating_to:
            if rating_from and rating_to:
                bits.append(f" ({rating_from} → {rating_to})")
            elif rating_to:
                bits.append(f" ({rating_to})")

        line = "".join(bits)
        if note:
            line += f" – {note}"

        lines.append(f"- {line}")

    return "\n".join(lines) + "\n"


# ---- Katalizátor blokk (#3) -------------------------------------------------


def _is_relevant_catalyst(ev: Dict[str, Any]) -> bool:
    """Csak a ma releváns, közelgő katalizátorok maradhatnak bent.

    Feltételek (bármelyik elég lehet, biblia-szerű heurisztika):
    - a 'window' / 'timeframe' mező tartalmazza a mai napot,
    - vagy explicit 'today_flag' / 'updated_today' jelölés,
    - vagy a 'change_type' jelzi, hogy ma módosították / erősítették meg a dátumot.
    """
    today_flag = str(ev.get("today_flag") or ev.get("updated_today") or "").lower()
    if today_flag in {"1", "true", "yes", "y"}:
        return True

    change_type = _safe_str(ev.get("change_type")).lower()
    if any(k in change_type for k in ["date_change", "confirmed", "reconfirmed", "update"]):
        return True

    window = _safe_str(ev.get("window") or ev.get("timeframe") or ev.get("when"))

    if "today" in window.lower():
        return True

    return False


def build_catalyst_block(catalysts_json: Any) -> str:
    """Közelgő katalizátorok blokk a #3‑as riporthoz.

    Csak akkor listáz, ha a katalizátor kifejezetten ma vált relevánssá
    (új dátum, megerősített időpont, PDUFA update stb.).
    """
    header = "### Közelgő katalizátorok"
    events = _ensure_list(catalysts_json)

    relevant: List[Dict[str, Any]] = [ev for ev in events if _is_relevant_catalyst(ev)]

    if not events:
        return header + "\nMa nem érkezett katalizátor-információ a #3‑as időablakban.\n"
    if not relevant:
        return header + "\nMa érkezett katalizátor-adat, de egyik sem volt elég releváns a biblia szerinti szűréshez.\n"

    lines: List[str] = [header]

    def sort_key(ev: Dict[str, Any]) -> Tuple[str, str]:
        return _norm_ticker(ev), _safe_str(ev.get("date") or ev.get("event_date") or ev.get("when"))

    for ev in sorted(relevant, key=sort_key):
        ticker = _norm_ticker(ev)
        kind = _safe_str(ev.get("type") or ev.get("event_type") or ev.get("label")) or "katalizátor"
        date = _safe_str(ev.get("date") or ev.get("event_date") or ev.get("when"))
        descr = _safe_str(ev.get("headline") or ev.get("note") or ev.get("description"))

        core = f"{ticker} – {kind}"
        if date:
            core += f" – {date}"
        if descr:
            core += f" – {descr}"

        lines.append(f"- {core}")

    return "\n".join(lines) + "\n"


# ---- High‑conv blokk (#3) ---------------------------------------------------


def _is_highconv_candidate(ev: Dict[str, Any]) -> bool:
    """High‑conv jelölt szűrés – biblia szerinti heurisztika.

    A feedben elvárt mezők:
    - score: 0 és 1 közötti skálázott pontszám
    - signals: lista, amely felsorolja, mely biblia‑kritériumok teljesültek
    Legalább 2 jelzésnek teljesülnie kell, és a score‑nak tipikusan ≥0.6‑nak.
    """
    try:
        score = float(ev.get("score", 0.0))
    except Exception:
        score = 0.0

    signals = ev.get("signals") or ev.get("flags") or []
    if isinstance(signals, str):
        # Pl. "upgrade, target_hike" → lista
        signals = [s.strip() for s in signals.split(",") if s.strip()]

    if not isinstance(signals, list):
        signals = []

    # Legalább két érdemi jelzés.
    if len(signals) < 2:
        return False

    if score < 0.6:
        return False

    return True


def build_highconv_block(highconv_json: Any, excluded_tickers: Iterable[str] | None = None) -> str:
    """High‑conv blokk a #3‑as riporthoz.

    excluded_tickers:
        olyan tickerek halmaza (portfólió + watchlist), amelyek SOHA nem
        kerülhetnek a „listán kívüli” high‑conv blokkba.
    """
    header = "### Listán kívüli, 3–12 hónapos high‑conv jelöltek"
    items = _ensure_list(highconv_json)

    if excluded_tickers is None:
        excluded: set[str] = set()
    else:
        excluded = {str(t).strip().upper() for t in excluded_tickers}

    candidates: List[Dict[str, Any]] = []
    for ev in items:
        ticker = _norm_ticker(ev)
        if ticker in excluded:
            continue
        if _is_highconv_candidate(ev):
            candidates.append(ev)

    if not items:
        return header + "\nNincs elérhető high‑conv adat ehhez a futáshoz.\n"
    if not candidates:
        return header + "\nA biblia szerinti szűrés után ma nincs olyan listán kívüli név, amely erős 3–12 hónapos high‑conv jelölt lenne.\n"

    lines: List[str] = [header]

    def sort_key(ev: Dict[str, Any]) -> float:
        try:
            return float(ev.get("score", 0.0))
        except Exception:
            return 0.0

    for ev in sorted(candidates, key=sort_key, reverse=True):
        ticker = _norm_ticker(ev)
        try:
            score = float(ev.get("score", 0.0))
        except Exception:
            score = 0.0

        signals = ev.get("signals") or ev.get("flags") or []
        if isinstance(signals, str):
            signals = [s.strip() for s in signals.split(",") if s.strip()]
        if not isinstance(signals, list):
            signals = []

        short_signals = ", ".join(str(s) for s in signals[:4])

        line = f"- {ticker} – score={score:.2f}"
        if short_signals:
            line += f" – jelek: {short_signals}"

        lines.append(line)

    return "\n".join(lines) + "\n"
