#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
report_runner.py
-----------------

Egységes runner script az 1/2/3-as riportokhoz – Yahoo nélkül.

Árforrások:
- Elsődleges: Google Finance (HTML scrape)
- Másodlagos: Investing.com (HTML scrape, nagyon best-effort)

Riportok:
#1: AH + PM (becsült) – prev_close → pre/post-market (ha elérhető)
#2: Tegnapi Open→Close (egyszerűsített, prev_close → last, mint „becsült” O→C)
#3: Ma Open→Most (approx: regularMarketOpen helyett nyitó-közeli ár → last, best-effort)

FIGYELEM: AH/PM és intraday adatok HTML-alapú forrásból, nem garantáltan pontosak.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python <3.9 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore

BUDAPEST = ZoneInfo("Europe/Budapest")


# ---------------------------------------------------------------------------
# CSV beolvasása
# ---------------------------------------------------------------------------

@dataclass
class TickerRow:
    ticker: str
    quantity: Optional[float]  # None = watchlist
    k_threshold: float         # min. abs % (3% default)
    is_position: bool          # True = darabszámos pozíció


def _parse_float(val: str) -> Optional[float]:
    if val is None:
        return None
    val = val.strip().replace(",", ".")
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def load_tickers_from_csv(path: str, default_k: float = 3.0) -> List[TickerRow]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV nem található: {path}")

    rows: List[TickerRow] = []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = {h.strip().lower(): h for h in reader.fieldnames or []}

        def get_col(*names: str) -> Optional[str]:
            for n in names:
                if n.lower() in headers:
                    return headers[n.lower()]
            return None

        ticker_col = get_col("ticker", "tiker", "symbol")
        qty_col = get_col("darabszám", "db", "qty", "quantity")
        k_col = get_col("k", "minmove", "minmovepct", "min_pct")

        if not ticker_col:
            raise ValueError("A CSV-ben nincs 'Ticker' (vagy ekvivalens) oszlop.")

        for r in reader:
            t = (r.get(ticker_col) or "").strip().upper()
            if not t:
                continue

            qty_val = _parse_float(r.get(qty_col, "")) if qty_col else None
            k_val = _parse_float(r.get(k_col, "")) if k_col else None
            if k_val is None or k_val <= 0:
                k_val = default_k

            is_position = qty_val is not None and qty_val > 0

            rows.append(
                TickerRow(
                    ticker=t,
                    quantity=qty_val,
                    k_threshold=k_val,
                    is_position=is_position,
                )
            )

    return rows


# ---------------------------------------------------------------------------
# Egyszerű segédek
# ---------------------------------------------------------------------------

@dataclass
class PriceSnapshot:
    prev_close: Optional[float]
    last: Optional[float]
    ah_last: Optional[float]  # After-hours / post-market
    pm_last: Optional[float]  # Pre-market (nyitás előtti)


def pct_change(base: Optional[float], new: Optional[float]) -> Optional[float]:
    if base is None or new is None or base == 0:
        return None
    return (new - base) / base * 100.0


@dataclass
class TickerStatus:
    ok: bool
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Makró / egyéb blokkok – egyszerűsített
# (ha van saját report_1_helpers.py-d, ezt össze lehet vele drótozni, de
#  ez a script önmagában is működőképes marad.)
# ---------------------------------------------------------------------------

def build_coverage_block(status_map: Dict[str, TickerStatus]) -> str:
    if not status_map:
        return "Lefedettség: NINCS ADAT (üres lista)\n"

    bad = {t: s for t, s in status_map.items() if not s.ok}
    if not bad:
        return "Lefedettség: TELJES – minden tickerhez sikerült árat olvasni.\n"

    parts = []
    for t, s in bad.items():
        reason = s.reason or "ismeretlen ok"
        parts.append(f"{t} (oka: {reason})")

    joined = ", ".join(parts)
    return f"Lefedettség: HIÁNYOS – nem elérhető ticker(ek): {joined}\n"


def build_macro_block_1(macro: Optional[str]) -> str:
    """
    #1 / #3 Politika/FED/makró blokk.
    - Ha macro == "off": nem írunk semmit.
    - Ha macro egy konkrét szöveg: azt tesszük be.
    - Ha macro None vagy "auto": placeholder szöveg (később összeköthető külső makrófeed-del).
    """
    if macro == "off":
        return ""

    if macro and macro not in ("auto", "strict"):
        return f"### Politika / FED / makró\n{macro.strip()}\n"

    # auto / strict -> minimális placeholder (kézzel felülírható a workflow inputtal)
    today = datetime.now(BUDAPEST).strftime("%Y-%m-%d")
    return (
        "### Politika / FED / makró (váz)\n"
        f"- Dátum: {today} (CEST)\n"
        "- Trump-napihír / USA belpolitika: ide írd be kézzel, ha van konkrét headline.\n"
        "- FED / hozamgörbe / infláció: röviden, 1–3 mondatban a hangulat.\n"
        "- Szektor/tematika: AI / félvezető / kripto-beta / bankok – mi mozgatja épp a napot.\n"
    )


# ---------------------------------------------------------------------------
# Top 6 vagyonkezelő & Top 6 elemzőház – strukturális blokkok
# (adatbetöltés most kézi / külön forrásból, itt csak a formátum)
# ---------------------------------------------------------------------------

@dataclass
class TopAMFlow:
    house: str
    scope: str         # pl. "NVDA", "US tech", "TSLA, AAPL"
    direction: str     # pl. "nettó vétel", "nettó eladás", "overweight felé"
    size_note: str     # pl. "~+0,8% súlynövelés", "jelentős trimming"
    reason: str        # rövid indok
    source: str        # pl. "13F", "ETF holdings", stb.
    time_str: str      # CEST szöveg


def build_top_am_block(flows: List[TopAMFlow]) -> str:
    lines: List[str] = []
    lines.append("### Top 6 vagyonkezelő – friss portfóliómozgások\n")

    if not flows:
        lines.append(
            "Nincs jelentős, publikus portfólióváltozás a figyelt Top 6 "
            "vagyonkezelőtől az elmúlt napokban (BlackRock, Vanguard, Fidelity, "
            "SSGA, Morgan Stanley IM, J.P. Morgan AM).\n"
        )
        return "\n".join(lines)

    for f in flows:
        lines.append(
            f"{f.house} — {f.scope} — {f.direction} ({f.size_note}) — "
            f"indok: {f.reason} — forrás: {f.source}, idő: {f.time_str}"
        )

    lines.append("")
    return "\n".join(lines)


@dataclass
class AnalystEvent:
    ticker: str
    house: str            # csak: GS, MS, JPM, BofA, UBS, Jefferies/Wedbush/Barclays
    action: str           # "felminősítés", "leminősítés", "célár ↑", "célár ↓"
    old_rating: Optional[str]
    new_rating: Optional[str]
    old_pt: Optional[float]
    new_pt: Optional[float]
    reason: str
    time_str: str         # CEST


def build_analyst_block(events: List[AnalystEvent]) -> str:
    lines: List[str] = []
    lines.append("### Top 6 elemzőház – friss fel/leminősítések és célár-változások\n")

    if not events:
        lines.append(
            "Ma nincs érdemi (rating vagy ≥±10%-os célár) változás a figyelt házaktól "
            "(Goldman Sachs, Morgan Stanley, J.P. Morgan, BofA, UBS, Jefferies/Wedbush/Barclays).\n"
        )
        return "\n".join(lines)

    for e in events:
        pt_part = ""
        if e.old_pt is not None or e.new_pt is not None:
            pt_part = f", célár: {e.old_pt} → {e.new_pt}"

        rating_part = ""
        if e.old_rating or e.new_rating:
            rating_part = f"{e.old_rating or '?'} → {e.new_rating or '?'}"

        lines.append(
            f"{e.ticker} — {e.house} — {e.action}"
            + (f" — {rating_part}" if rating_part else "")
            + pt_part
            + f" — indok: {e.reason} — idő: {e.time_str}"
        )

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Google Finance / Investing.com – HTML alapú árlekérdezés
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    )
}


def _guess_google_symbol(ticker: str) -> List[str]:
    """
    Nagyon egyszerű heuristika, hogy milyen Google Finance szimbólumokat próbáljunk:

    - US ticker-ek: TICKER:NASDAQ, TICKER:NYSE
    - .WA eset: külön szabály, de PKN.WA-t amúgy is tiltjuk máshol.
    """
    syms: List[str] = []
    if "." in ticker:
        # pl. PKN.WA – ezt amúgy is skipeljük később
        return syms

    syms.append(f"{ticker}:NASDAQ")
    syms.append(f"{ticker}:NYSE")
    return syms


def _parse_number(val: str) -> Optional[float]:
    try:
        val = val.replace(",", "").strip()
        return float(val)
    except Exception:
        return None


def fetch_from_google(ticker: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[str]]:
    """
    Google Finance HTML-ből próbálja kiolvasni:
    - last (utolsó ár)
    - previous close
    - pre-market / post-market (ha van)

    Visszaad: (prev_close, last, ah_last, pm_last, error_reason)
    error_reason None, ha volt legalább prev_close + last.
    """
    symbols = _guess_google_symbol(ticker)
    if not symbols:
        return None, None, None, None, "no_google_symbol_guess"

    for sym in symbols:
        url = f"https://www.google.com/finance/quote/{sym}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
        except Exception as e:
            return None, None, None, None, f"google_network_error: {e}"

        if resp.status_code == 404:
            continue
        if resp.status_code != 200:
            return None, None, None, None, f"google_http_{resp.status_code}"

        html = resp.text

        # last ár: próbáljuk a data-last-price attribútumot
        last = None
        m = re.search(r'data-last-price="([0-9\.,]+)"', html)
        if m:
            last = _parse_number(m.group(1))
        else:
            # fallback: fő ár-blokk (YMlKec fxKbKc)
            m2 = re.search(r'YMlKec fxKbKc">([0-9\.,]+)<', html)
            if m2:
                last = _parse_number(m2.group(1))

        # previous close: valahol a "Previous close" sorban
        prev_close = None
        m3 = re.search(
            r'Previous close.*?([0-9\.,]+)<',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if m3:
            prev_close = _parse_number(m3.group(1))

        # pre-market / after-hours – nagyon best-effort
        ah_last = None
        pm_last = None

        # After hours / Post-market
        m4 = re.search(
            r'(After hours|Post-market)[^0-9]*([0-9\.,]+)',
            html,
            flags=re.IGNORECASE,
        )
        if m4:
            ah_last = _parse_number(m4.group(2))

        # Pre-market
        m5 = re.search(
            r'(Pre-market)[^0-9]*([0-9\.,]+)',
            html,
            flags=re.IGNORECASE,
        )
        if m5:
            pm_last = _parse_number(m5.group(2))

        if prev_close is not None and last is not None:
            return prev_close, last, ah_last, pm_last, None

        # ha idáig jutottunk, de nincs prev_close+last, próbáljuk a következő sym-et
        # de ha ez volt az egyetlen, akkor hiba
        # (itt megyünk a következő symbol-ra)
    return None, None, None, None, "google_no_prev_or_last"


def fetch_from_investing(ticker: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Investing.com HTML-ből próbálja kiolvasni:
    - last
    - previous close

    Itt nincs stabil ticker->URL mapping, ezért ez nagyon „best-effort”.
    Egyelőre csak egyszerű keresést csinálunk a globális quote oldalra:

    https://www.investing.com/search/?q={ticker}

    és az első releváns találatot próbáljuk lekérni. Ez lassú és törékeny,
    ezért fallbackként kezeljük.

    Visszaad: (prev_close, last, error_reason)
    """
    try:
        search_url = f"https://www.investing.com/search/?q={ticker}"
        resp = requests.get(search_url, headers=HEADERS, timeout=10)
    except Exception as e:
        return None, None, f"investing_search_error: {e}"

    if resp.status_code != 200:
        return None, None, f"investing_search_http_{resp.status_code}"

    html = resp.text

    # nagyon egyszerű: keresünk egy első linket, amiben benne van a ticker
    m = re.search(r'href="(/equities/[^"]+)"[^>]*>\s*' + re.escape(ticker) + r'\b', html, flags=re.IGNORECASE)
    if not m:
        # nem találtunk olyan equities linket, amiben a ticker név szerint szerepel
        return None, None, "investing_no_equity_link"

    rel_url = m.group(1)
    quote_url = f"https://www.investing.com{rel_url}"

    try:
        resp2 = requests.get(quote_url, headers=HEADERS, timeout=10)
    except Exception as e:
        return None, None, f"investing_quote_error: {e}"

    if resp2.status_code != 200:
        return None, None, f"investing_quote_http_{resp2.status_code}"

    qhtml = resp2.text

    # current price – best effort (valamilyen "instrument-price_last" class)
    last = None
    m2 = re.search(r'class="text-2xl[^"]*"?[^>]*>([0-9\.,]+)<', qhtml)
    if m2:
        last = _parse_number(m2.group(1))

    # previous close – "Prev. Close" vagy "Previous Close"
    prev_close = None
    m3 = re.search(
        r'(Prev\. Close|Previous Close).*?([0-9\.,]+)<',
        qhtml,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m3:
        prev_close = _parse_number(m3.group(2))

    if prev_close is None and last is None:
        return None, None, "investing_no_prev_or_last"

    return prev_close, last, None


def fetch_price_snapshot(ticker: str) -> Tuple[PriceSnapshot, Optional[str]]:
    """
    Kombinált árlekérdezés:
    1) Google Finance (prev_close, last, AH/PM ha van)
    2) ha az nem megy: Investing.com (prev_close, last)
    """
    # Google
    prev_close, last, ah_last, pm_last, err = fetch_from_google(ticker)
    if err is None and prev_close is not None and last is not None:
        return PriceSnapshot(prev_close=prev_close, last=last, ah_last=ah_last, pm_last=pm_last), None

    # Investing fallback
    prev_close2, last2, err2 = fetch_from_investing(ticker)
    if prev_close2 is not None and last2 is not None:
        # AH/PM-t itt nem tudjuk, marad None
        reason_note = f"google_failed: {err}" if err else "google_failed"
        return (
            PriceSnapshot(prev_close=prev_close2, last=last2, ah_last=None, pm_last=None),
            reason_note  # coverage-ben látni fogod, hogy google nem ment
        )

    # teljes kudarc
    reason = err or err2 or "no_price_data"
    return PriceSnapshot(None, None, None, None), reason


# ---------------------------------------------------------------------------
# 1-es riport: AH + PM (Google/Investing alapú, best-effort)
# ---------------------------------------------------------------------------

def run_report_1(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    lines: List[str] = []

    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")
    prev_day = (now_local - timedelta(days=1)).strftime("%Y-%m-%d")

    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST\n")
    lines.append(
        f"Vizsgált ablakok (CEST): AH {prev_day} 22:00 → {today_str} 02:00, "
        f"PM {today_str} 10:00 → 15:30\n"
    )
    lines.append("_Árforrás: Elsődlegesen Google Finance (HTML), fallback: Investing.com (HTML). "
                 "AH/PM adatok csak akkor érhetők el, ha a forrás oldalon külön szerepelnek._\n")

    # PKN.WA-t sose jelentjük
    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    status_map: Dict[str, TickerStatus] = {}
    price_map: Dict[str, PriceSnapshot] = {}

    ok_count = 0
    for row in filtered:
        snap, reason = fetch_price_snapshot(row.ticker)
        price_map[row.ticker] = snap
        if snap.prev_close is not None and snap.last is not None:
            status_map[row.ticker] = TickerStatus(ok=True)
            ok_count += 1
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason or "no_price_data")

        # ne verjük szét az oldalakat – 1–2 tized mp pihenő
        time.sleep(0.3)

    lines.append(build_coverage_block(status_map))
    lines.append(build_macro_block_1(macro))

    # Top 6 vagyonkezelő blokk – most még kézzel töltendő
    top_flows: List[TopAMFlow] = []
    lines.append(build_top_am_block(top_flows))

    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – After-hours & Premarket mozgások (best-effort)\n")

    watch_lines: List[str] = []
    watch_lines.append("### Watchlist – After-hours & Premarket mozgások (csak ha ≥K, best-effort)\n")

    for row in filtered:
        snap = price_map[row.ticker]

        # AH/PM: ha van pre/post, azt használjuk, különben csak prev_close→last (becsült)
        ah_pct = pct_change(snap.prev_close, snap.ah_last)
        pm_pct = pct_change(snap.prev_close, snap.pm_last)
        # fallback, ha semmi extended nincs: treat last as PM (nyitás előtti/utáni becsült)
        if ah_pct is None and pm_pct is None and snap.prev_close is not None and snap.last is not None:
            pm_pct = pct_change(snap.prev_close, snap.last)

        def fmt_pct(label: str, value: Optional[float]) -> str:
            if value is None:
                return f"{label}: n/a"
            sign = "+" if value >= 0 else ""
            return f"{label} {sign}{value:.2f}%"

        has_signal = (
            (ah_pct is not None and abs(ah_pct) >= row.k_threshold) or
            (pm_pct is not None and abs(pm_pct) >= row.k_threshold)
        )

        ah_str = fmt_pct("AH", ah_pct)
        pm_str = fmt_pct("PM", pm_pct)
        src_str = "árforrás: Google Finance / Investing.com (HTML)"

        if row.is_position:
            if has_signal:
                reason = "Érdemi AH/PM (vagy prev_close→last) elmozdulás (≥K)."
            else:
                reason = "Egyelőre nincs küszöb feletti AH/PM (vagy prev_close→last) elmozdulás."
            pos_lines.append(f"{row.ticker} — {ah_str} | {pm_str} — {reason} ({src_str})")
        else:
            if not has_signal:
                continue
            reason = "Watchlisten is érdemi AH/PM (vagy prev_close→last) elmozdulás (≥K)."
            watch_lines.append(f"{row.ticker} — {ah_str} | {pm_str} — {reason} ({src_str})")

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

    # Top 6 elemzőház blokk – egyelőre üres lista (adatforrás kell hozzá)
    analyst_events: List[AnalystEvent] = []
    lines.append(build_analyst_block(analyst_events))

    # Közeli katalizátorok – itt most csak placeholder
    lines.append("### Közeli katalizátorok (earnings/guide)\n")
    lines.append(
        "Itt fognak szerepelni a pár napon belüli fontos gyorsjelentések / "
        "guidance-ek (kézi vagy külső naptár alapján).\n"
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 2-es riport: Tegnapi Open→Close (egyszerűsített, prev_close→last becslés)
# ---------------------------------------------------------------------------

def run_report_2(
    tickers: List[TickerRow],
) -> str:
    lines: List[str] = []
    lines.append("#2 – Tegnapi nyitástól zárásig (Open→Close) – egyszerűsített\n")
    lines.append(
        "_Árforrás: Google Finance / Investing.com – prev_close → last, "
        "mint becsült Open→Close mozgás (nem hivatalos intraday O/C)._ \n"
    )

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    pos_lines: List[str] = []
    pos_lines.append("Darabszámos tickerek – abs(prev_close→last) becsült mozgás (≥K)\n")

    watch_lines: List[str] = []
    watch_lines.append("Watchlist – abs(prev_close→last) becsült mozgás (≥K)\n")

    for row in filtered:
        snap, reason = fetch_price_snapshot(row.ticker)
        move = pct_change(snap.prev_close, snap.last)

        if move is None or abs(move) < row.k_threshold:
            continue

        sign = "+" if move >= 0 else ""
        src_str = "árforrás: Google Finance / Investing.com"
        line = f"{row.ticker} — Open→Close (becsült prev_close→last): {sign}{move:.2f}% ({src_str})"

        if row.is_position:
            pos_lines.append(line)
        else:
            watch_lines.append(line)

        time.sleep(0.3)

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 3-as riport: Ma Open→Most (best-effort, prev_close→last proxy + minden pozíció)
# ---------------------------------------------------------------------------

def run_report_3(
    tickers: List[TickerRow],
    macro: Optional[str],
) -> str:
    lines: List[str] = []

    now_local = datetime.now(BUDAPEST)
    today_str = now_local.strftime("%Y-%m-%d")

    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most) — CEST\n")
    lines.append(
        f"Vizsgált ablak (CEST): mai USA nyitás (15:30) → lekérdezés időpontja ({today_str})\n"
    )
    lines.append(
        "_Árforrás: Google Finance / Investing.com – prev_close → last mint intraday proxy "
        "(nem hivatalos O→Most, de jó közelítés a napközbeni irányra)._ \n"
    )

    filtered = [r for r in tickers if r.ticker != "PKN.WA"]
    if not filtered:
        lines.append("Nincs feldolgozható ticker.\n")
        return "\n".join(lines)

    status_map: Dict[str, TickerStatus] = {}
    price_map: Dict[str, PriceSnapshot] = {}
    ok_count = 0

    for row in filtered:
        snap, reason = fetch_price_snapshot(row.ticker)
        price_map[row.ticker] = snap
        if snap.prev_close is not None and snap.last is not None:
            status_map[row.ticker] = TickerStatus(ok=True)
            ok_count += 1
        else:
            status_map[row.ticker] = TickerStatus(ok=False, reason=reason or "no_prev_or_last")

        time.sleep(0.3)

    lines.append(build_coverage_block(status_map))

    if ok_count == 0:
        lines.append(
            "\nA 3-as intranapi riport ma **nem értelmezhető**, mert egyik forrásból sem sikerült "
            "prev_close→last adatot kiolvasni a figyelt tickerekre. Ilyenkor az Open→Most mozgásokra "
            "nincs megbízható adat a Google/Investing kombinációból.\n"
        )
        return "\n".join(lines)

    lines.append(build_macro_block_1(macro))

    pos_lines: List[str] = []
    pos_lines.append("### Darabszámos tickerek – Ma nyitástól mostanáig (Open→Most, prev_close→last proxy)\n")

    watch_lines: List[str] = []
    watch_lines.append("### Watchlist – Open→Most mozgások (csak ha ≥K, prev_close→last proxy)\n")

    for row in filtered:
        snap = price_map[row.ticker]
        open_most = pct_change(snap.prev_close, snap.last)

        def fmt_open_most(value: Optional[float]) -> str:
            if value is None:
                return "Open→Most (prev_close→last proxy): n/a"
            sign = "+" if value >= 0 else ""
            return f"Open→Most (prev_close→last proxy): {sign}{value:.2f}%"

        open_most_str = fmt_open_most(open_most)
        src_base = "árforrás: Google Finance / Investing.com"

        if row.is_position:
            if open_most is None:
                reason_str = "Hiányzó adat: nincs értelmezhető prev_close→last mozgás."
            elif abs(open_most) >= row.k_threshold:
                reason_str = "Érdemi intranapi elmozdulás (≥K) nyitáshoz képest (prev_close→last alapján)."
            else:
                reason_str = "Mérsékelt intranapi mozgás, egyelőre nincs küszöb feletti elmozdulás."

            pos_lines.append(f"{row.ticker} — {open_most_str} — {reason_str} ({src_base})")
        else:
            has_signal = open_most is not None and abs(open_most) >= row.k_threshold
            if not has_signal:
                continue
            reason_str = "Watchlisten is érdemi intranapi mozgás (≥K) nyitáshoz képest (prev_close→last alapján)."
            watch_lines.append(f"{row.ticker} — {open_most_str} — {reason_str} ({src_base})")

    if len(pos_lines) > 1:
        lines.append("\n".join(pos_lines) + "\n")
    if len(watch_lines) > 1:
        lines.append("\n".join(watch_lines) + "\n")

    # Top 6 elemzőház blokk – itt intraday hatás is megjelenhetne, de adat hiányában üres
    analyst_events: List[AnalystEvent] = []
    lines.append(build_analyst_block(analyst_events))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Részvény riport futtató (1/2/3) – Google/Investing verzió")
    parser.add_argument(
        "--report",
        choices=["1", "2", "3"],
        required=True,
        help="Riport típusa: 1 = AH/PM, 2 = Tegnapi O→C (becsült), 3 = Ma O→Most (proxy)",
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Ticker-lista CSV elérési útja",
    )
    parser.add_argument(
        "--summary",
        required=False,
        help="GitHub Actions GITHUB_STEP_SUMMARY elérési útja (ha megadod, oda írja ki a riportot)",
    )
    parser.add_argument(
        "--macro",
        required=False,
        help="Politika/FED/makró szöveg (Trump-napihír + 1–4 mondat, az 1-es/3-as riporthoz)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    tickers = load_tickers_from_csv(args.csv)

    if args.report == "1":
        report_text = run_report_1(tickers, macro=args.macro)
    elif args.report == "2":
        report_text = run_report_2(tickers)
    elif args.report == "3":
        report_text = run_report_3(tickers, macro=args.macro)
    else:
        raise ValueError(f"Ismeretlen riport: {args.report}")

    if args.summary:
        with open(args.summary, "w", encoding="utf-8") as f:
            f.write(report_text)
    else:
        print(report_text)


if __name__ == "__main__":
    main()
