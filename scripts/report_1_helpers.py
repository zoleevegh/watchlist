# -*- coding: utf-8 -*-
"""
report_1_helpers.py
--------------------

Segédfüggvények az #1/#2/#3 riporthoz:
- Lefedettség blokk
- Politika/FED (makró) blokk
- Hírek / bejelentések & fel/lemínősítések
- Közeli katalizátorok
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Lefedettség blokk
# ---------------------------------------------------------------------------

@dataclass
class TickerStatus:
    ok: bool
    reason: Optional[str] = None


def build_coverage_block(status_map: Dict[str, TickerStatus]) -> str:
    """
    Biblia: minden riport ELEJÉN:

      • „Lefedettség: TELJES”
      • „Lefedettség: HIÁNYOS – nem elérhető ticker(ek): AAPL (oka: rate_limited), NVDA (oka: no_price_data)…”

    Itt ticker-szinten kiírjuk az okot is, de 1 sorban, tömören.
    """
    if not status_map:
        return "Lefedettség: HIÁNYOS – nincs feldolgozott ticker.\n"

    missing = [
        f"{t} (oka: {s.reason})"
        for t, s in status_map.items()
        if not s.ok
    ]

    if not missing:
        return "Lefedettség: TELJES\n"

    missing_str = ", ".join(missing)
    return f"Lefedettség: HIÁNYOS – nem elérhető ticker(ek): {missing_str}\n"


# ---------------------------------------------------------------------------
# Politika/FED blokk (#1 és #3 riport)
# ---------------------------------------------------------------------------

def build_macro_block_1(macro: Optional[str]) -> str:
    """
    Makró blokk kezelése.

    macro:
      - None / "" / "off"  → nincs blokk
      - "auto"             → jelzés, hogy auto mód még nincs implementálva
      - bármi más szöveg   → ezt tesszük be a blokk tartalmának

    Használat:
      workflow-ban a 'macro' inputba beírhatod a 1–4 mondatos Trump/FED/makró összefoglalót.
    """
    if macro is None:
        return ""

    m = macro.strip()
    if not m or m.lower() == "off":
        return ""

    if m.lower() == "auto":
        return (
            "### Politika/FED / Trump-napihír\n"
            "(_Auto mód még nincs implementálva – add meg a makró összefoglalót a workflow "
            "'macro' mezőjében._)\n"
        )

    return f"### Politika/FED / Trump-napihír\n{m}\n"


# ---------------------------------------------------------------------------
# Hírek / bejelentések & fel/lemínősítések
# ---------------------------------------------------------------------------

@dataclass
class NewsItem:
    ticker: str
    source: str                      # pl. "Yahoo", "MarketBeat", "IR"
    when: datetime
    headline: str
    summary: str                     # 1–2 mondatos indoklás
    category: str                    # "rating", "guide", "deal", "results", "other"
    direction: Optional[str] = None  # "pozitív" / "negatív" / "semleges"
    rating_house: Optional[str] = None
    rating_action: Optional[str] = None   # "felminősítés" / "leminősítés" / "célár-emelés" / stb.
    rating_from: Optional[str] = None
    rating_to: Optional[str] = None
    target_old: Optional[float] = None
    target_new: Optional[float] = None
    importance: Optional[str] = None  # "high" / "medium" / "low"


def merge_news_sources(a: List[NewsItem], b: List[NewsItem]) -> List[NewsItem]:
    """
    Két hírlista összefésülése ticker + headline + idő alapján.
    Deduplikál, majd idő szerint (legfrissebb elöl) rendez.
    """
    seen = set()
    merged: List[NewsItem] = []

    for item in (a + b):
        key = (item.ticker, item.headline, item.when.replace(second=0, microsecond=0))
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)

    merged.sort(key=lambda x: x.when, reverse=True)
    return merged


def _format_news_line(item: NewsItem) -> str:
    t = item.ticker
    dt_str = item.when.strftime("%Y-%m-%d %H:%M")
    if item.category == "rating":
        # Elemzői fel/lemínősítés formátuma
        house = item.rating_house or item.source
        action = item.rating_action or ""
        new_rating = item.rating_to or ""
        tgt_part = ""
        if item.target_new is not None:
            if item.target_old is not None:
                tgt_part = f", célár: {item.target_old} → {item.target_new}"
            else:
                tgt_part = f", új célár: {item.target_new}"
        dir_part = f" [{item.direction}]" if item.direction else ""
        return (
            f"{t} — {house} {action} → {new_rating}{tgt_part}{dir_part} — "
            f"{item.summary} ({dt_str})"
        )
    else:
        dir_part = f" [{item.direction}]" if item.direction else ""
        return f"{t} — {item.headline}{dir_part} — {item.summary} ({dt_str})"


def build_news_block_1(all_news: List[NewsItem]) -> str:
    """
    #1 riport alá tartozó „Bejelentések & fel/lemínősítések” blokk.

    Ha nincs hír, üres stringet ad vissza.
    """
    if not all_news:
        return ""

    lines: List[str] = []
    lines.append("### Bejelentések & fel/lemínősítések\n")

    for item in all_news:
        lines.append("- " + _format_news_line(item))

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Közeli katalizátorok blokk
# ---------------------------------------------------------------------------

@dataclass
class UpcomingCatalyst:
    ticker: str
    date: datetime
    description: str   # pl. "Q3 gyorsjelentés", "Investor Day", "terméklaunch"


def build_catalyst_block_1(catalysts: List[UpcomingCatalyst]) -> str:
    """
    #1 riporthoz: „Közeli katalizátorok (pár napos táv)”.
    Ha nincs releváns katalizátor, üres stringet ad.
    """
    if not catalysts:
        return ""

    lines: List[str] = []
    lines.append("### Közeli katalizátorok (pár napos táv)\n")

    catalysts_sorted = sorted(catalysts, key=lambda c: c.date)
    for c in catalysts_sorted:
        d_str = c.date.strftime("%Y-%m-%d")
        lines.append(f"- {d_str} — {c.ticker} — {c.description}")

    return "\n".join(lines) + "\n"
