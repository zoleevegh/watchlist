# report_1_helpers.txt
# KIEGÉSZÍTŐ MODUL AZ #1 LEKÉRDEZÉSHEZ (AH + PREMARKET)
#
# Ez a fájl a meglévő report_runner.py-hez ad kiegészítő funkciókat,
# hogy az 1-es riport (AH + PM) a „biblia” szerint működjön az alábbi területeken:
#
# 1) Lefedettség blokk – ticker-szintű okokkal
# 2) Politika/FED blokk – a --macro paraméterből
# 3) Bejelentések & fel/lemínősítések – formátum (rating vs cégközlés)
# 4) Több forrású hír-aggregálás – Yahoo + opcionális 2. forrás
# 5) Közeli katalizátorok blokk – formázás (a forrást te adod meg)
#
# FONTOS:
# - A konkrét adatlekérés (árak, hírek, katalizátorok) továbbra is a SAJÁT kódodban van.
# - Itt főleg struktúra + formázás van, amit könnyen be tudsz illeszteni.
# - Ahol „TODO” kommented van, ott tudsz egy második hírforrást bekötni
#   (pl. MarketBeat Ratings, PR Newswire, stb.).

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

BUDAPEST = ZoneInfo("Europe/Budapest")


# -------------------------------------------------------------------------
# 1) LEFEDETTSÉG BLOKK (#1-hez is, de általánosan használható)
# -------------------------------------------------------------------------

@dataclass
class TickerStatus:
    """
    Egy ticker lefedettségi állapota a riport futása után.

    ok:    True  -> minden szükséges adat megvan (ár, AH/PM history, hír, stb.).
           False -> a ticker valamelyik kritikus résznél hiányos volt.
    reason: rövid kód / magyarázat, pl. 'no_price_data', 'no_5m_prepost', 'fetch_error'.
    """

    ok: bool
    reason: Optional[str] = None


def build_coverage_block(statuses: Dict[str, TickerStatus]) -> str:
    """
    Biblia szerinti lefedettség blokk:

    - Ha minden ticker ok:
      "Lefedettség: TELJES"

    - Ha bármelyik hibás:
      "Lefedettség: HIÁNYOS – nem elérhető ticker(ek): NVDA – no_price_data; GOLD – no_5m_prepost"

    Használat az 1-es riport elején (de mehet 2/3-ban is):

        coverage_text = build_coverage_block(ticker_statuses)
        lines.append(coverage_text)
    """
    if not statuses:
        return "Lefedettség: HIÁNYOS – nem elérhető ticker(ek): (oka: nincs input)\n\n"

    missing = {t: s for t, s in statuses.items() if not s.ok}
    if not missing:
        return "Lefedettség: TELJES\n\n"

    parts: List[str] = []
    for ticker, st in sorted(missing.items()):
        reason = st.reason or "ismeretlen hiba"
        parts.append(f"{ticker} – {reason}")

    joined = "; ".join(parts)
    return f"Lefedettség: HIÁNYOS – nem elérhető ticker(ek): {joined}\n\n"


# -------------------------------------------------------------------------
# 2) POLITIKA / FED BLOKK – --macro PARAMÉTER ALAPJÁN
# -------------------------------------------------------------------------

def build_macro_block_1(macro_text: Optional[str]) -> str:
    """
    Politika/FED blokk az 1-es riport tetejére.

    A --macro paraméterben egy 1–4 mondatos szöveget adsz át, pl.:

    "Trump ma reggel ... A hozamgörbe ... A félvezető-szektort ..."

    A blokk formátuma:

    "Politika / FED / makró (röviden)
    Trump-napihír + piaci relevancia...
    "
    """
    if not macro_text:
        return ""

    macro_text = macro_text.strip()
    if not macro_text:
        return ""

    header = "Politika / FED / makró (röviden)\n"
    body = macro_text + "\n\n"
    return header + body


# -------------------------------------------------------------------------
# 3) BEJELENTÉSEK & FEL/LEMÍNŐSÍTÉSEK BLOKK – FORMÁZÁS
# -------------------------------------------------------------------------

@dataclass
class NewsItem:
    """
    Normalizált hír/elemzői lépés egy tickerhez.

    kind:
      - "rating"      -> elemzői fel/lemínősítés, célár-változás
      - "company"     -> hivatalos cégközlés (PR, 8-K, guidance, buyback, M&A, stb.)

    sentiment: "pozitív" / "negatív" / "semleges" (a biblia szerinti jelző az elején)

    A rating típusnál használt mezők:
      - analyst_house  -> pl. "Morgan Stanley"
      - rating_action  -> pl. "felminősítés", "leminősítés", "céláremelés"
      - new_rating     -> pl. "Overweight"
      - new_price      -> új célár (USD)

    A company típusnál használt mezők:
      - title          -> bejelentés tárgya (pl. "20 Mrd USD-s új buyback-program")
      - impact         -> 1 mondatos várható hatás (pl. "EPS-támogató, rövid távon pozitív")

    material / from_watchlist:
      - material=True  -> anyagilag lényeges (watchlist nevűeknél ez alapján szűrsz)
      - from_watchlist -> a ticker a watchlisten van-e (ha False, akkor darabszámos vagy egyéb).
    """

    ticker: str
    kind: str  # "rating" vagy "company"
    sentiment: str  # "pozitív" / "negatív" / "semleges"
    source: str  # pl. "Yahoo Finance", "MarketBeat", "PR Newswire"

    # közös
    time: datetime

    # rating-specifikus
    analyst_house: Optional[str] = None
    rating_action: Optional[str] = None
    new_rating: Optional[str] = None
    new_price: Optional[float] = None

    # company-specifikus
    title: Optional[str] = None
    impact: Optional[str] = None

    # meta
    material: bool = False
    from_watchlist: bool = False


def format_news_time(dt: datetime) -> str:
    """Időbélyeg formázása CET/CEST szerint (biblia: CEST/CET)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BUDAPEST)
    else:
        dt = dt.astimezone(BUDAPEST)
    return dt.strftime("%Y-%m-%d %H:%M CET")


def build_news_block_1(news_items: List[NewsItem]) -> str:
    """
    Bejelentések & fel/lemínősítések blokk az 1-es riport aljára (globális szabály szerint).

    LOGIKA A BIBLIA ALAPJÁN:
    - Darabszámos tickereknél: MINDEN friss hivatalos cégközlés + elemzői rating.
    - Watchlisten: CSAK ahol material == True (anyagilag lényeges) vagy a felette
      lévő logikád alapján a hír ≥±3% mozgást vált(ott) ki.

    FORMÁTUMOK:

    Rating:
      TICKER — HÁZ — LÉPÉS (fel/lemínősítés/célár-változás)
      — új besorolás/célár — rövid indok — idő

      Pl.:
      NVDA — Morgan Stanley — felminősítés — új besorolás: Overweight, új célár: 250 USD
      — rövid indok: AI pipeline erősödik, adatközpontos capex tartósan magas — 2025-11-04 10:15 CET

    Company PR:
      TICKER — Bejelentés: CÍM — várható hatás: IMPACT — idő

      Pl.:
      AAPL — Bejelentés: 20 Mrd USD-s új buyback-program
      — várható hatás: EPS-támogató, részvényárfolyamra rövid távon pozitív — 2025-11-04 10:20 CET
    """
    if not news_items:
        return ""  # nincs blokk, ha nincs hír

    # Darabszámosak elöl, watchlist később – de maga a sorrend a handed-in listán is múlhat.
    # Itt csak annyit teszünk, hogy a darabszámosakat (from_watchlist=False) előre rendezzük.
    ordered = sorted(news_items, key=lambda n: (n.from_watchlist, n.ticker, n.time))

    lines: List[str] = []
    lines.append("Bejelentések & fel/lemínősítések (AH + Premarket sávban / friss hírek)\n")

    for item in ordered:
        # Watchlist: csak anyagilag lényegesek
        if item.from_watchlist and not item.material:
            continue

        t = item.ticker
        ts = format_news_time(item.time)

        if item.kind == "rating":
            house = item.analyst_house or item.source
            step = item.rating_action or "rating-változás"
            parts: List[str] = []
            parts.append(f"{t} — {house} — {step}")

            details: List[str] = []
            if item.new_rating:
                details.append(f"új besorolás: {item.new_rating}")
            if item.new_price is not None:
                details.append(f"új célár: {item.new_price:.2f} USD")

            # rövid indok -> az impact mezőt használjuk ide
            if item.impact:
                details.append(f"rövid indok: {item.impact}")

            detail_str = ", ".join(details) if details else ""
            if detail_str:
                parts.append(" — " + detail_str)

            parts.append(f" — {ts}")

            # opcionálisan: [pozitív]/[negatív]/[semleges] prefix
            direction = item.sentiment.strip().lower() if item.sentiment else ""
            if direction:
                prefix = f"[{direction}] "
            else:
                prefix = ""

            lines.append(prefix + "".join(parts))

        elif item.kind == "company":
            title = item.title or "Ismeretlen bejelentés"
            impact = item.impact or "(várható hatás: nem részletezett)"

            direction = item.sentiment.strip().lower() if item.sentiment else ""
            if direction:
                prefix = f"[{direction}] "
            else:
                prefix = ""

            line = (
                f"{prefix}{t} — Bejelentés: {title} — "
                f"várható hatás: {impact} — {ts}"
            )
            lines.append(line)

        else:
            # ismeretlen típus – legyen egy biztonsági fallback
            direction = item.sentiment.strip().lower() if item.sentiment else ""
            if direction:
                prefix = f"[{direction}] "
            else:
                prefix = ""

            title = item.title or "Hír"
            line = f"{prefix}{t} — {title} — forrás: {item.source} — {ts}"
            lines.append(line)

    lines.append("")  # záró üres sor
    return "\n".join(lines)


# -------------------------------------------------------------------------
# 4) TÖBB FORRÁSÚ HÍR-AGGREGÁLÁS (#1-hez, de általánosítható)
# -------------------------------------------------------------------------

def merge_news_sources(
    yahoo_items: List[NewsItem],
    extra_items: Optional[List[NewsItem]] = None,
) -> List[NewsItem]:
    """
    Yahoo + 2. hírforrás összevonása.

    - yahoo_items: a meglévő Yahoo RSS-alapú hírek (amit most is használsz)
    - extra_items: opcionális 2. forrás (pl. MarketBeat Ratings / PR hírek)

    Duplikációkezelés:
    - egyszerű dedup ticker + cím + idő alapján (ha mindkét forrás ugyanazt hozza).
    """
    merged: List[NewsItem] = []
    seen_keys = set()

    def add_items(items: List[NewsItem]):
        for it in items:
            # kulcs: ticker + (title vagy rating_action) + idő (perc pontossággal)
            key_base = it.title or it.rating_action or ""
            ts_key = it.time.astimezone(BUDAPEST).strftime("%Y-%m-%d %H:%M")
            key = (it.ticker, key_base, ts_key)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged.append(it)

    add_items(yahoo_items)
    if extra_items:
        add_items(extra_items)

    return merged


# -------------------------------------------------------------------------
# 5) KÖZELI KATALIZÁTOROK BLOKK (#1 – pár napos távra)
# -------------------------------------------------------------------------

@dataclass
class UpcomingCatalyst:
    """
    Közelgő katalizátor (earnings / guidance / investor day / makró adat).

    Példák:
      - ticker="AMD", kind="earnings", date=2025-11-07, note="Q3 gyorsjelentés"
      - ticker="TSLA", kind="investor_day", date=2025-11-10, note="AI Day / Robotaxi update"
    """

    ticker: str
    date: datetime
    kind: str          # pl. "earnings", "guidance", "investor_day", "macro"
    note: str          # rövid magyarázat
    from_watchlist: bool = False


def build_catalyst_block_1(catalysts: List[UpcomingCatalyst]) -> str:
    """
    "Közeli katalizátorok (pár nap)" blokk az 1-es riport végére.

    Várható formátum:

    Közeli katalizátorok (3–5 napos távon)
    AMD — 2025-11-07 — earnings — Q3 gyorsjelentés (watchlist)
    NVDA — 2025-11-09 — investor_day — AI & Data Center fókusz
    """
    if not catalysts:
        return ""

    # Rendezés dátum szerint, majd ticker szerint
    ordered = sorted(catalysts, key=lambda c: (c.date, c.ticker))

    lines: List[str] = []
    lines.append("Közeli katalizátorok (3–12 napos távon)\n")

    for c in ordered:
        date_str = c.date.astimezone(BUDAPEST).strftime("%Y-%m-%d")
        wl_flag = " (watchlist)" if c.from_watchlist else ""
        line = f"{c.ticker} — {date_str} — {c.kind} — {c.note}{wl_flag}"
        lines.append(line)

    lines.append("")  # záró üres sor
    return "\n".join(lines)


# -------------------------------------------------------------------------
# 6) PÉLDA: HOGYAN ILLeszd BE EZT AZ 1-ES RIportba
# -------------------------------------------------------------------------
#
# A SAJÁT report_runner.py 1-es riport részében kb. így használd:
#
#   from report_1_helpers import (
#       TickerStatus, build_coverage_block,
#       build_macro_block_1,
#       NewsItem, merge_news_sources, build_news_block_1,
#       UpcomingCatalyst, build_catalyst_block_1,
#   )
#
#   def run_report_1(..., macro: str | None, ...):
#       lines: List[str] = []
#
#       # 1) Lefedettség – a saját státusz-tábládból
#       coverage_text = build_coverage_block(ticker_statuses)
#       lines.append(coverage_text)
#
#       # 2) Politika / FED blokk
#       macro_block = build_macro_block_1(macro)
#       if macro_block:
#           lines.append(macro_block)
#
#       # 3) Darabszámos AH/PM blokk (EZ MARAD A SAJÁT KÓDODBAN)
#       #    ...
#
#       # 4) Hírek: Yahoo + extra forrás összefésülése
#       #    (a yahoo_items-t már most is előállítod, csak NewsItem struktúrára kell mappelni)
#       all_news = merge_news_sources(yahoo_items, extra_items)
#       news_block = build_news_block_1(all_news)
#       if news_block:
#           lines.append(news_block)
#
#       # 5) Közeli katalizátorok blokk – ha elő tudod állítani a katalizátor-listát
#       catalyst_block = build_catalyst_block_1(catalysts)
#       if catalyst_block:
#           lines.append(catalyst_block)
#
#       report_text = "\n".join(lines)
#       return report_text
#
# A Yahoo + extra forrás konkrét lekérdezése ITT NINCS benne –
# azt a saját meglévő fetch-függvényeidből tudod felépíteni. A lényeg:
# - a biblia szerinti formázás és blokkszerkezet most már rendelkezésre áll az 1-es riporthoz.
# - a 2. hírforrást (MarketBeat, PR, stb.) a merge_news_sources() előtt tudod begyűjteni.
# -------------------------------------------------------------------------
