# -*- coding: utf-8 -*-
"""
report_1_helpers.py
--------------------

Segédfüggvények az #1 – After-hours & Premarket riporthoz.

Biblia-szerinti funkciók:
- Lefedettség blokk ticker-szintű "oka: ..." bontással
- Politika / FED / Makró blokk (--macro paraméterből + default szöveg)
- Bejelentések & fel/lemínősítések formázása (NewsItem)
- Közeli katalizátorok (UpcomingCatalyst)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Lefedettség
# ---------------------------------------------------------------------------


@dataclass
class TickerStatus:
    ok: bool
    reason: Optional[str] = None  # pl. "no_price_data", "fetch_error: ..."


def build_coverage_block(status_map: Dict[str, TickerStatus]) -> str:
    """
    Biblia:
    - TELJES
    - HIÁNYOS – nem elérhető ticker(ek): TICKER1 (oka: ...), TICKER2 (oka: ...)

    status_map: { "NVDA": TickerStatus(ok=True), "GOLD": TickerStatus(ok=False, reason="no_price_data") }
    """
    failed = {t: s for t, s in status_map.items() if not s.ok}

    if not failed:
        return "**Lefedettség:** TELJES\n"

    parts: List[str] = []
    for ticker in sorted(failed.keys()):
        st = failed[ticker]
        if st.reason:
            parts.append(f"{ticker} (oka: {st.reason})")
        else:
            parts.append(ticker)

    joined = ", ".join(parts)
    return f"**Lefedettség:** HIÁNYOS – nem elérhető ticker(ek): {joined}\n"


# ---------------------------------------------------------------------------
# Politika / FED / Makró blokk
# ---------------------------------------------------------------------------


def build_macro_block_1(macro: Optional[str]) -> str:
    """
    Politika/FED blokk az 1-es riport tetejére.

    Biblia:
    - Trump-napihír + 1–4 mondat makró / szektor hangulatról.
    - Ha nincs külön macro szöveg megadva, akkor is legyen egy rövid default blokk.
    """
    header = "## Politika / FED / Makró (röviden)\n"

    if macro and macro.strip():
        # A workflow-ból jövő szöveget közvetlenül beillesztjük a header alá.
        body = macro.strip()
    else:
        # Default, ha nincs macro paraméter megadva – inkább legyen minimális szöveg, mint semmi.
        body = (
            "Nincs kiemelkedő, új makró- vagy FED-hír a vizsgált AH/PM ablakban. "
            "A piaci hangulatot továbbra is az általános kamat–infláció narratíva "
            "és a szektor-rotációs flow-k határozzák meg."
        )

    return f"{header}{body}\n"


# ---------------------------------------------------------------------------
# Bejelentések & fel/lemínősítések
# ---------------------------------------------------------------------------


@dataclass
class NewsItem:
    """
    Egységes hírstruktúra az 1-es riporthoz.

    Két fő kategória:
    - 'rating'  -> elemzői lépés / célár
    - 'pr'      -> cégközlés / PR / 8-K / earnings / guidance

    A biblia szerinti formátumhoz szükséges mezők:
    - ticker (pl. "NVDA")
    - kind: "rating" vagy "pr"
    - house: elemzőház neve (ratingnél), vagy hírforrás (pr-nél)
    - action: ratingnél felminősítés / leminősítés / megerősítés; pr-nél pl. "Eredményjelentés", "Guidance", stb.
    - new_rating: új besorolás (pl. "Buy", "Overweight", "Neutral")
    - new_target: új célár (float, USD)
    - headline: rövid cím / bejelentés tárgya
    - impact: várható hatás (pl. "pozitív", "negatív", "semleges", vagy bővebben)
    - ts: hír időpontja (CEST-ben vagy ISO stringben)
    """

    ticker: str
    kind: str  # "rating" vagy "pr"
    house: Optional[str] = None
    action: Optional[str] = None
    new_rating: Optional[str] = None
    new_target: Optional[float] = None
    headline: Optional[str] = None
    impact: Optional[str] = None
    ts: Optional[datetime] = None  # ha nincs, lehet None

    def key(self) -> tuple:
        """Dedup kulcs merge-hez."""
        ts_str = self.ts.isoformat() if isinstance(self.ts, datetime) else str(self.ts)
        return (self.ticker, self.kind, (self.headline or "").strip(), ts_str or "")


def merge_news_sources(primary: List[NewsItem], extra: List[NewsItem]) -> List[NewsItem]:
    """
    Több forrás (Yahoo RSS + egyéb feed) deduplikálása.

    Simple policy:
    - primary listának prioritása van
    - extra elemek csak akkor kerülnek be, ha a .key() alapján nem szerepelnek már
    """
    combined: List[NewsItem] = []
    seen = set()

    for item in primary + extra:
        k = item.key()
        if k in seen:
            continue
        seen.add(k)
        combined.append(item)

    # Rendezés idő szerint, ha van, különben ticker szerint
    def sort_key(it: NewsItem):
        t = it.ts or datetime.min
        return (it.ticker, t)

    combined.sort(key=sort_key)
    return combined


def _format_ts(ts: Optional[datetime]) -> str:
    if isinstance(ts, datetime):
        # egyszerű, rövid formátum CET-ben – feltételezzük, hogy már helyi időre van konvertálva
        return ts.strftime("%Y-%m-%d %H:%M")
    return "idő: n/a"


def build_news_block_1(news: List[NewsItem]) -> str:
    """
    Biblia szerinti formázás:

    Rating (elemzői lépés):
        Ticker — Ház — Lépés (fel/lemínősítés / megerősítés) — Új besorolás/célár — rövid indok — idő

    Cégközlés / PR:
        Ticker — Bejelentés: CÍM — várható hatás: IMPACT — idő
    """
    header = "## Bejelentések & fel/lemínősítések (AH + Premarket)\n"

    if not news:
        return f"{header}(nincs releváns bejegyzés az ablakban)\n"

    lines: List[str] = [header]

    for item in news:
        when = _format_ts(item.ts)
        if item.kind == "rating":
            house = item.house or "ismeretlen ház"
            action = item.action or "rating változás"
            new_rating = item.new_rating or "n/a"
            if item.new_target is not None:
                target_str = f"{item.new_target:.1f} USD"
            else:
                target_str = "n/a"

            reason = item.impact or (item.headline or "").strip() or "rövid indok nincs megadva"

            lines.append(
                f"- {item.ticker} — {house} — {action} — új besorolás/célár: "
                f"{new_rating} / {target_str} — {reason} — {when}"
            )
        else:
            # PR / cégközlés / earnings / guidance
            headline = (item.headline or "").strip() or "Bejelentés"
            impact = item.impact or "várható hatás: nincs jelölve"
            lines.append(
                f"- {item.ticker} — Bejelentés: {headline} — várható hatás: {impact} — {when}"
            )

    lines.append("")  # záró üres sor
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Közeli katalizátorok
# ---------------------------------------------------------------------------


@dataclass
class UpcomingCatalyst:
    """
    Közeli (pár napos) katalizátor a bibliához:

    - ticker
    - date: dátum (CE(S)T)
    - kind: "earnings", "guidance", "investor_day", "adat", stb.
    - description: rövid szöveg (mi ez pontosan)
    - importance: "high", "medium", "low" (nem kötelező, de jó jelzés)
    """

    ticker: str
    date: date
    kind: str
    description: str
    importance: str = "medium"


def build_catalyst_block_1(catalysts: List[UpcomingCatalyst]) -> str:
    header = "## Közeli katalizátorok (pár nap)\n"

    if not catalysts:
        return header + "Nincs automatikusan azonosított katalizátor; ha van saját listád, egészítsd ki kézzel.\n"

    # rendezés dátum szerint
    catalysts_sorted = sorted(catalysts, key=lambda c: (c.date, c.ticker))

    lines: List[str] = [header]

    for c in catalysts_sorted:
        date_str = c.date.strftime("%Y-%m-%d")
        lines.append(
            f"- {date_str} — {c.ticker} — {c.kind} — {c.description} "
            f"(fontosság: {c.importance})"
        )

    lines.append("")
    return "\n".join(lines)
