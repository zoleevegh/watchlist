"""
biblia_helper.py

Kanonikus szabálykönyv a #1 / #2 / #3 jelentésekhez ("biblia").
[... TELJES hosszú leírás, időablakok, forrás-prioritás, K/L/M, Eladasi ar, high-conviction blokk ...]
"""

from __future__ import annotations
from typing import List


def get_report1_checklist() -> List[str]:
    """
    #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST
    Fő fókusz: előző napi RTH záróhoz képest az AH + mai premarket mozgások.
    """
    return [
        "Időzóna: Europe/Budapest (CET/CEST).",
        "After-hours 22:00–02:00, Premarket 10:00–15:30 CEST; Yahoo v8 2d/5m includePrePost.",
        "Bázis: első pre/post gyertya előtti utolsó RTH 5m záró (utolsó teljes RTH close).",
        "Lefedettség-blokk a jelentés elején: TELJES / HIÁNYOS.",
        "Hiányosnak csak valódi forráshiba számít (HTTP error, chart_error, no_result).",
        "Politika/FED / Trump-napihír blokk a lefedettség után (max ~4 mondat).",
        "AH% és PM% mindig a bázis RTH close-hoz képest, két tizedesre kerekítve.",
        "Ha nincs pre/post gyertya egy sávban, AH/PM = n/a (nem lefedettség hiba).",
        "Darabszámos pozíció: MASTER-ben quantity/darabszám/db > 0.",
        "Watchlist: ahol nincs quantity vagy az <= 0.",
        "K oszlop: ticker-specifikus % küszöb; üres/hibás: K=3.",
        "Eladasi ar diff_pct = (aktuális ár - Eladasi ar) / Eladasi ar * 100, ha van eladási ár.",
        "Darabszámos blokk mindig megjelenik, max(|AH|,|PM|) szerinti csökkenő sorrendben.",
        "Darabszámos sor-formátum: 'TICKER — AH +x.xx% | PM -y.yy% — komment / vagy: Egyelőre nincs küszöb feletti AH/PM elmozdulás.'",
        "Minden 3%+ mozgásnál kötelező pontos % és 1 mondatos ok.",
        "Watchlist-blokk: csak ahol max(|AH|,|PM|) ≥ K.",
        "Watchlist-sorrend: max(|AH|,|PM|) szerinti csökkenő.",
        "Watchlist sor-formátum: 'TICKER — AH +x.xx% | PM -y.yy% — Watchlisten is érdemi AH/PM elmozdulás (≥K=...) az utolsó RTH záróhoz képest.'",
        "Ha van közeli katalizátor (pár nap), külön blokkban jelezni.",
        "A végén (ha van jelölt): 'Listán kívüli, 3–12 hónapos high-conviction jelöltek' blokk, csak portfólión/watchlisten kívüli nevekkel.",
    ]


def get_report2_checklist() -> List[str]:
    """
    #2 – Tegnapi nyitástól zárásig (Open→Close) jelentés.
    """
    return [
        "Időablak: előző kereskedési nap US RTH, 15:30–22:00 CEST (Open→Close).",
        "Árforrás: Yahoo Finance OHLC / intraday chart; Open és Close elegendő.",
        "Open→Close% = (Close - Open) / Open * 100, két tizedesre.",
        "Lefedettség-blokk a jelentés elején: TELJES / HIÁNYOS.",
        "Makró/FED/politika blokk a lefedettség után (előző napra).",
        "Darabszámos & watchlist tickerek forrása: MASTER (ugyanaz, mint #1-nél).",
        "K oszlop: ticker-specifikus % mozgás küszöb; üres/hibás: K=3.",
        "L/M oszlop: vol és 52w high/low közelség küszöb – opcionális, ha a script használja.",
        "Darabszámosaknál minden |Open→Close%| ≥ K kötelezően jelentendő.",
        "Watchlisten |Open→Close%| ≥ K vagy anyagilag lényeges hír esetén kerül be.",
        "Minden 3%+ mozgásnál pontos % + 1 mondatos ok.",
        "Sor-formátum: 'TICKER — Open→Close +x.xx% — rövid indok (eredmény, guide, M&A, makró stb.)'.",
        "Opcionális: High/Open és Low/Open százalékok hozzáírása.",
        "Blokksorrend: Lefedettség → Makró/FED → Darabszámos → Watchlist → Bejelentések & fel/lemínősítések → Katalizátorok → High-conviction.",
        "Bejelentések & fel/lemínősítések: darabszámosaknál minden material event; watchlisten csak anyagilag lényeges.",
        "Eladasi ar (ha használjuk): diff_pct = (Close - Eladasi ar) / Eladasi ar * 100, kiegészítő infóként.",
    ]


def get_report3_checklist() -> List[str]:
    """
    #3 – Ma nyitástól mostanáig (Open→Most) jelentés.
    """
    return [
        "Időablak: aktuális nap US RTH nyitástól (15:30 CEST) a lekérdezés pillanatáig (Open→Most).",
        "Árforrás: Yahoo Finance intraday chart (1m/5m) vagy Open + utolsó elérhető ár.",
        "Open→Most% = (Last - Open) / Open * 100, két tizedesre.",
        "Opcionálisan High/Open és Low/Open % is számolható.",
        "Lefedettség-blokk a jelentés elején: TELJES / HIÁNYOS.",
        "Ha nap közben érkezett fontos makró/FED hír, a lefedettség után külön blokkban szerepel.",
        "Darabszámos és watchlist-tickerek forrása: MASTER; K/L/M és Eladasi ar szabályok ugyanazok, mint #1/#2.",
        "K: intraday % küszöb (alap 3%), ticker szinten felülírható.",
        "Eladasi ar diff_pct = (Last - Eladasi ar) / Eladasi ar * 100, ha van adat.",
        "Darabszámosaknál minden |Open→Most%| ≥ K kötelezően jelentendő.",
        "Watchlisten: |Open→Most%| ≥ K vagy anyagilag lényeges hír esetén kerül a riportba.",
        "Minden 3%+ mozgásnál pontos % + egy mondatos indok.",
        "Sor-formátum: 'TICKER — Open→Most +x.xx% (High/Open +y.yy%, Low/Open -z.zz%) — rövid indok.' (a zárójeles rész opcionális).",
        "Blokksorrend: Lefedettség → Makró/FED (intraday) → Darabszámos → Watchlist → Bejelentések & fel/lemínősítések → Közeli katalizátorok (pl. zárás utáni jelentés) → High-conviction.",
        "Intraday Bejelentések & fel/lemínősítések: minden friss, material event, ami látványos intraday mozgást okoz.",
    ]
