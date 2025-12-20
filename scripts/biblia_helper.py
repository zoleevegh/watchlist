"""
biblia_helper.py

Kanonikus szabálykönyv a #1 / #2 / #3 jelentésekhez ("biblia").

CÉL
----
Ha valaha elveszne a ChatGPT-beszélgetés vagy bármilyen külön dokumentáció,
EZ A FÁJL tartalmazza, hogy:

- mit kell csinálni a #1 / #2 / #3 jelentéseknél,
- milyen időablakokkal dolgozunk,
- milyen blokkok KÖTELEZŐK minden jelentésben,
- honnan vesszük az adatokat (árak, hírek, elemzői ratingek),
- hogyan számoljuk a százalékos mozgásokat,
- hogyan kezeljük a MASTER sheetet (K/L/M, darabszámos vs. watchlist),
- hogyan használjuk az „Eladasi ar” oszlopot,
- hogyan működik a „listán kívüli, 3–12 hónapos high-conviction jelöltek” blokk.

FIX GIST LINK – #1 JELENTÉS (AH+PM)
-----------------------------------
A legutóbbi #1-es jelentés (After-hours + Premarket) mindig itt érhető el RAW formában:

https://gist.githubusercontent.com/zoleevegh/5df443b8a46ef863cdc97aad62756510/raw/summary_report_1.md

Ezt a GitHub Action minden #1-es futás után automatikusan frissíti a
`reports/summary_report_1.md` tartalmával. Ha bármi elveszne, innen bármikor
lekérhető a legutolsó #1-es riport.

FONTOS ALAPELVEK (MINDEN JELENTÉSRE)
------------------------------------
- Időzóna: Europe/Budapest (CET/CEST).
- Csak US kereskedési napokon fut a #1/#2/#3 jelentés.
- Árforrás alapvetően: Yahoo Finance chart v8 (2d/5m, includePrePost=true) +
  szükség esetén intraday / quote API (Open, High, Low, Close, Last).
- Ticker-sorrend:
    1) darabszámos pozíciók (quantity > 0),
    2) utána watchlist-tickerek (csak ha a feltételek teljesülnek).
- Küszöb a jelentendő mozgásokra: alapértelmezett K = ±3,00%,
  ticker-szintű felülírás a MASTER „K” oszlopából.
- MASTER K/L/M oszlopok:
    K  = min. intraday % mozgás (abszolút értékben), default 3,
    L  = unusual volume szorzó (× 20 napos átlagforgalom), default 2,
    M  = max. távolság a 52 hetes csúcstól/mélyponttól (%), default 1.
  Üres / érvénytelen cella esetén a fenti defaultok lépnek életbe.
- Eladási ár (új oszlop): „Eladasi ar”
    - Ha egy tickerhez meg van adva „Eladasi ar”, akkor
      az aktuális árhoz képest számolunk egy %-os eltérést:
          diff_pct = (current_price - eladasi_ar) / eladasi_ar * 100
      Ez később re-entry / túlértékeltség / alulértékeltség jelzéshez használható.
    - Ha a cella üres vagy nem számszerű, a diff_pct nem kötelező,
      a jelentés többi része ettől függetlenül lefuthat.

HÍRFORRÁS-PRIORITÁS
-------------------
A) Gyors, megbízható hírdótok:
    1. Reuters (Top/Markets/Breakingviews) – elsődleges.
    2. AP – megerősítés.
    3. Bloomberg (Top/Markets) – ha elérhető.
    4. Dow Jones Newswires / The Fly / Benzinga Pro – intraday tape.

B) Hivatalos vállalati csatornák:
    - SEC EDGAR (8-K/6-K/10-Q/10-K),
    - vállalati IR/Newsroom oldalak,
    - Business Wire / PR Newswire / GlobeNewswire.

C) Elemzői lépések (rating / célár):
    - MarketBeat Ratings – elsődleges,
    - StreetInsider / The Fly (Analyst) – másodlagos,
    - TipRanks – kiegészítő.

D) Makró / FED / politika:
    - FederalReserve.gov, BLS, BEA, U.S. Treasury,
    - Reuters / AP political news,
    - White House hivatalos közlései (csak piaci relevancia esetén).

E) Szektor-specifikus:
    - Félvezetők: TSMC, ASML, LRCX, KLAC saját IR + iparági cikkek.
    - Kriptoérzékeny papírok (MSTR, COIN, BTDR, RGTI, IREN, SOUN stb.):
      Coindesk / The Block csak megerősítésként, nem önálló primer forrásként.

ANYAGI LÉNYEGESSÉG
------------------
Egy hír / esemény akkor kerül be a jelentésbe (akkor is, ha a % mozgás
3% alatt marad), ha legalább az alábbiak közül egy teljesül:

    - ~5%+ hatás a guidance / EPS / árbevétel várakozásokra,
    - M&A / stratégiai deal (felvásárlás, spin-off, nagy JV),
    - buyback / osztalék program indulása vagy jelentős bővítése,
    - CEO/CFO/Chair csere,
    - a core üzletet érdemben érintő szabályozási / jogi döntés.

Nem kerül be:
    - puszta pletyka (főleg Reddit/Stocktwits),
    - kis jelentőségű PR-hír, ami nem mozgat árfolyamot.

HIGH-CONVICTION BLOKK
----------------------
- Név: „Listán kívüli, 3–12 hónapos high-conviction jelöltek”.
- Csak olyan ticker kerülhet ide, amely NINCS a felhasználó
  portfóliójában ÉS NINCS a watchlisten (tehát „listán kívüli”).
- Forrás-prioritás: Yahoo Finance + MarketBeat.
- Szűrési feltételek – legalább kettő teljesüljön:
    1. 2–3+ friss felminősítés / céláremelés nagy házaktól,
    2. iránymutatás-emelés / pozitív guide,
    3. konszenzus EPS / árbevétel felfelé módosul,
    4. 3–12 hónapon belüli konkrét katalizátor,
    5. relatív erő / 52w high-közeli teljesítmény.
- A blokk csak akkor jelenik meg, ha van valóban erős jelölt.

LEFEDETTSÉG-BLOKK (MINDEN JELENTÉS ELEJÉN KÖTELEZŐ)
----------------------------------------------------
- Ha minden tickerre sikerült adatot húzni:
    „Lefedettség: TELJES”
- Ha bármelyik tickerhez nem volt adat (HTTP-hiba, no_result, stb.):
    „Lefedettség: HIÁNYOS – nem elérhető ticker(ek): AAA, BBB… (oka: lásd belső logot / forráshibát)”
- FONTOS: az, hogy egy tickernek nincs AH/PM gyertyája egy adott
  2d/5m charton, nem lefedettség-hiba (ilyenkor AH/PM = n/a).

IDŐABLAKOK – KIVONAT
--------------------
#1 – After-hours (22:00–02:00) + Premarket (10:00–15:30)
    - Bázis: az utolsó teljes RTH (Regular Trading Hours) záróár,
      az első pre/post gyertya előtti utolsó 5m candle close.
    - AH_zár: a 22:00–02:00 CEST ablakban elérhető utolsó pre/post
      5m gyertya záróára. Ha nincs ilyen, AH = n/a.
    - PM_zár: a 10:00–15:30 CEST ablakban elérhető utolsó pre/post
      5m gyertya záróára. Ha nincs ilyen, PM = n/a.
    - AH% = (AH_zár - RTH_zár) / RTH_zár * 100.
    - PM% = (PM_zár - RTH_zár) / RTH_zár * 100.
    - Jelentésben két tizedesre kerekítve jelenik meg.

#2 – Tegnapi nyitástól zárásig (Open→Close)
    - Időablak: előző kereskedési nap US RTH, 15:30–22:00 CEST.
    - Open→Close% = (Close - Open) / Open * 100.

#3 – Ma nyitástól mostanáig (Open→Most)
    - Időablak: aktuális nap US RTH 15:30 CEST → lekérdezés pillanata.
    - Open→Most% = (Last - Open) / Open * 100.
    - Opcionálisan: High/Open és Low/Open %.

MI FUT JELENLEG PYTHON-SCRIPTBŐL (#1, VERZIÓ ~2.2.3)
----------------------------------------------------
A #1-es jelentésben jelenleg a Python-script (report_runner.py) feladata:

- MASTER CSV beolvasása:
    - darabszámos pozíciók azonosítása (quantity > 0),
    - watchlist-tickerek azonosítása (nincs darabszám vagy <= 0),
    - K/L/M/Eladasi ar oszlopok numeric parse + defaultok.

- Árfolyamok lekérése Yahoo v8 2d/5m includePrePost alapján:
    - RTH_zár (utolsó RTH 5m candle close),
    - AH_zár (utolsó pre/post candle 22:00–02:00 között),
    - PM_zár (utolsó pre/post candle 10:00–15:30 között).

- Százalékos mozgások számítása:
    - AH% = (AH_zár - RTH_zár) / RTH_zár * 100 (ha van AH_zár, különben n/a),
    - PM% = (PM_zár - RTH_zár) / RTH_zár * 100 (ha van PM_zár, különben n/a),
    - két tizedesre kerekítve.

- Küszöbkezelés:
    - K-t a MASTER-ből olvassa; ha hiányzik: K=3,
    - darabszámos blokk: MINDEN pozíció szerepel, jelzi, ha van ≥K mozgás,
    - watchlist blokk: CSAK azok kerülnek be, ahol max(|AH%|, |PM%|) ≥ K.

- Lefedettség:
    - ha minden tickerre sikerült adatot húzni → „Lefedettség: TELJES”,
    - ha bármelyiknél forráshiba van (HTTP error, chart_error, no_result) → HIÁNYOS,
      és felsorolja az érintett tickereket.

- Jelentés struktúra:
    - Lefedettség-blokk,
    - „Politika/FED / Trump-napihír” címke + makró szöveg (ha a workflow-ból érkezik),
    - Darabszámos blokk,
    - Watchlist blokk (csak ≥K),
    - Időbélyeg: „Job summary generated at run-time (...)”.


Ezen felül a #1-es pipeline további Python moduljai:

- macro_fetcher.py
    - makró/FED/politikai hírblokkok automatikus kigyűjtése (Reuters/AP/Bloomberg/Dow Jones prioritás szerint),
    - az eredményt `reports/macro_news_1.json` formában adja át a postprocess modulnak.

- highconv_builder.py
    - a high-conviction jelöltekhez tartozó `reports/high_conv_1.json` felépítése,
    - a katalizátor-blokkhoz tartozó `reports/catalysts_1.json` felépítése (earnings, guidance, M&A, regulatory).

- analyst_feed_parser.py
    - az Apps Script analyst webapp (MarketBeat / StreetInsider / stb. aggregált feedje) lekérése,
    - az eredmény `reports/analyst_1.json`-ba mentése, amelyből a „Bejelentések & fel/lemínősítések” blokk épül.

- postprocess_report.py
    - a fenti JSON-okból egységes `summary_report_1.md` felépítése,
    - blokkok: Lefedettség, Makró/FED, Darabszámos, Watchlist, Bejelentések & fel/lemínősítések,
      Katalizátorok, Listán kívüli 3–12 hó high-conviction jelöltek.

PYTHONON KÍVÜL MARADÓ / RÉSZBEN MANUÁLIS ELEMEK
----------------------------------------------
A #1-es jelentés célállapota a biblia szerint ma már nagyrészt Python-scriptből fut
(makró, bejelentések, katalizátorok, high-conviction blokkok). Jelenleg kimondottan
„backlog” státuszban az alábbi elem maradt:

- „Eladott pozíciók – aktuális ár az eladási árhoz képest” dedikált blokk:
    - az „Eladasi ar” oszlop beolvasása már működik (diff_pct = (current_price - eladasi_ar) / eladasi_ar * 100),
    - de külön, önálló riportblokk (pl. „X% alá/fölé jött az exithez képest”) még NINCS generálva Pythonból,
      csak kiegészítő információként használható.

A KÖVETKEZŐ LÉPÉSEK #1-HEZ
---------------------------
A script fejlesztésének következő, biblia szerinti lépése #1-nél elsősorban az
„Eladasi ar” alapú, külön riportblokk leprogramozása (pl. „re-entry radar”, „túl drága a korábbi exithez képest”).
Ezen felül a meglévő blokkoknál (makró, bejelentések, katalizátorok, high-conviction)
inkább finomhangolásról, formátum-tisztításról és forrás-robosztusságról van szó, nem új logikai elemről.

---------------------------
Ha a script fejlesztése tovább halad, a fenti PYTHON- vs. MANUÁLIS-lista
szolgál kiindulópontként. A cél, hogy:

- a teljes #1-es biblia logika (makró, bejelentések, katalizátorok,
  high-conviction, Eladasi ar-blokk) fokozatosan átköltözzön a Python-scriptbe,
- a webes / manuális réteg legfeljebb finomhangolásra, magyarázatra kelljen.

"""

from __future__ import annotations
import os
import json
from typing import List, Optional, Dict, Any, Set

# Használható konstans, ha máshonnan is hivatkozni akarunk a RAW #1-es linkre
GIST_REPORT1_RAW_URL = "https://gist.githubusercontent.com/zoleevegh/5df443b8a46ef863cdc97aad62756510/raw/summary_report_1.md"


def get_report1_checklist() -> List[str]:
    """
    #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST

    Rövid ellenőrző lista ahhoz, hogy a script által generált #1-es jelentés
    megfelel-e a bibliának. A lista elemei végigzongorázhatók debug során.
    """
    return [
        # Időzóna és árforrás
        "Időzóna: Europe/Budapest (CET/CEST).",
        "Árforrás: Yahoo Finance v8 2d/5m, includePrePost=true, US trading day.",
        "Bázisár: RTH_zár = első pre/post gyertya előtti utolsó RTH 5m candle close.",
        # AH/PM számolás
        "AH_zár: 22:00–02:00 CEST közötti utolsó pre/post 5m candle záróára (ha nincs: AH=n/a).",
        "PM_zár: 10:00–15:30 CEST közötti utolsó pre/post 5m candle záróára (ha nincs: PM=n/a).",
        "AH% = (AH_zár - RTH_zár) / RTH_zár * 100, két tizedesre kerekítve.",
        "PM% = (PM_zár - RTH_zár) / RTH_zár * 100, két tizedesre kerekítve.",
        "Ha nincs releváns AH/PM candle, akkor a reportban 'AH n/a' vagy 'PM n/a' szerepel, ez nem lefedettség-hiba.",
        # Lefedettség, sorrend
        "Jelentés elején kötelező a Lefedettség-blokk: TELJES vagy HIÁNYOS + tickerek okaival.",
        "Darabszámos pozíció: MASTER-ben quantity > 0 → mindig megjelenik a darabszámos blokkban.",
        "Watchlist: ahol nincs quantity vagy az <= 0 → csak a watchlist-blokkban jelenik meg.",
        # Küszöbök
        "K oszlop: ticker-specifikus % küszöb; ha üres/hibás, implicit K=3.",
        "Darabszámos blokk: minden pozíció szerepel, de jelölve, ha max(|AH%|,|PM%|) ≥ K.",
        "Watchlist-blokk: CSAK azok a tickerek szerepelnek, ahol max(|AH%|,|PM%|) ≥ K.",
        # Sor-formátumok
        "Darabszámos sor-formátum: 'TICKER — AH +x.xx% | PM -y.yy% — komment / vagy: Egyelőre nincs küszöb feletti AH/PM elmozdulás.'.",
        "Watchlist sor-formátum: 'TICKER — AH +x.xx% | PM -y.yy% — Watchlisten is érdemi AH/PM elmozdulás (≥K=...) az utolsó RTH záróhoz képest.'.",
        # Eladasi ar
        "Eladasi ar diff_pct = (aktuális ár - Eladasi ar) / Eladasi ar * 100, ha van eladási ár – jelenleg opcionális, külön blokk még nincs implementálva.",
        # Híres blokkok – target állapot
        "Politika/FED / Trump-napihír blokk: jelenleg a workflow 'macro' paramétere adja a szöveget (nem automata scraping).",
        "Bejelentések & fel/lemínősítések blokk: biblia szerint MarketBeat/StreetInsider alapú, de Pythonban még TODO.",
        "Közeli katalizátorok blokk: earnings / események listázása – jelenleg manuális, Pythonban TODO.",
        "Listán kívüli, 3–12 hónapos high-conviction blokk: csak portfólión/watchlisten kívüli nevekkel – jelenleg manuális, Pythonban TODO.",
        # Időbélyeg
        "Jelentés végén kötelező az időbélyeg: 'Job summary generated at run-time (ISO8601 CEST)'.",
    ]


def get_report2_checklist() -> List[str]:
    """
    #2 – Tegnapi nyitástól zárásig (Open→Close) jelentés.
    """
    return [
        "Időablak: előző kereskedési nap US RTH, 15:30–22:00 CEST (Open→Close).",
        "Árforrás: Yahoo Finance OHLC / intraday chart; Open és Close elegendő.",
        "Open→Close% = (Close - Open) / Open * 100, két tizedesre.",
        "Lefedettség-blokk a jelentés elején: TELJES / HIÁNYOS a biblia szerint.",
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
        "Eladasi ar diff_pct (ha használjuk): (Close - Eladasi ar) / Eladasi ar * 100, kiegészítő infóként.",
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


# --- Makró / elemzői / katalizátor / high-conviction helper függvények ---

import time
import datetime as _dt
from typing import List, Optional, Dict, Any, Set


_SESSION = requests.Session()
_SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
)


def _safe_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 10) -> Optional[Dict[str, Any]]:
    """Biztonságos JSON-letöltés – hiba esetén None-t ad vissza."""
    try:
        resp = _SESSION.get(url, params=params or {}, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


# --- Politika / FED / piaci hangulat ---

# --- Politika / FED / piaci hangulat ---

    Visszatérési érték (siker esetén):
        {
          "ticker": ...,
          "current": float,
          "target_mean": float,
          "upside_pct": float,
          "num_analysts": int,
          "strongBuy": int,
          "buy": int,
          "hold": int,
          "sell": int,
          "strongSell": int,
        }
    """
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
    params = {"modules": "financialData,defaultKeyStatistics,recommendationTrend"}
    data = _safe_get_json(url, params=params)
    try:
        result = data["quoteSummary"]["result"][0]
    except Exception:
        return None

    try:
        fin = result.get("financialData", {})
        stats = result.get("defaultKeyStatistics", {})
        reco = result.get("recommendationTrend", {})

        current = fin.get("currentPrice", {}).get("raw")
        target_mean = fin.get("targetMeanPrice", {}).get("raw")
        num_analysts = stats.get("numberOfAnalystOpinions", {}).get("raw")

        trend = reco.get("trend", [])
        latest = trend[0] if trend else {}
        strong_buy = latest.get("strongBuy", 0) or 0
        buy = latest.get("buy", 0) or 0
        hold = latest.get("hold", 0) or 0
        sell = latest.get("sell", 0) or 0
        strong_sell = latest.get("strongSell", 0) or 0

        if not current or not target_mean or not num_analysts:
            return None

        upside_pct = (float(target_mean) - float(current)) / float(current) * 100.0

        return {
            "ticker": ticker,
            "current": float(current),
            "target_mean": float(target_mean),
            "upside_pct": float(upside_pct),
            "num_analysts": int(num_analysts),
            "strongBuy": int(strong_buy),
            "buy": int(buy),
            "hold": int(hold),
            "sell": int(sell),
            "strongSell": int(strong_sell),
        }
    except Exception:
        return None




def _load_banned_tickers_from_master(path: str = "reports/master.csv") -> Set[str]:
    """MASTER/watchlist CSV-ből betölti az összes saját tickert (pozíció + watchlist).

    Cél: a high-conviction blokkba soha ne kerüljön olyan név, ami szerepel a
    felhasználó portfóliójában vagy watchlistjén.
    """
    import csv

    banned: Set[str] = set()
    full = os.path.join(os.getcwd(), path)
    if not os.path.exists(full):
        return banned

    try:
        with open(full, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            if not fieldnames:
                return banned

            # Próbáljuk kitalálni a ticker oszlopot
            candidates = [
                "ticker",
                "symbol",
                "szimbólum",
                "szimbolum",
                "Ticker",
                "SZIMBOLUM",
            ]
            ticker_col: Optional[str] = None
            for cand in candidates:
                for col in fieldnames:
                    if col.strip().lower() == cand.lower():
                        ticker_col = col
                        break
                if ticker_col:
                    break

            # Ha nem találjuk, használjuk az első oszlopot fallbackként
            if ticker_col is None:
                ticker_col = fieldnames[0]

            for row in reader:
                sym = (row.get(ticker_col) or "").strip().upper()
                if sym:
                    banned.add(sym)
    except Exception:
        # Hiba esetén inkább csendben visszaadjuk, amit addig gyűjtöttünk
        return banned

    return banned


def _load_sp500_universe() -> List[str]:
    """S&P500 ticker-univerzum lekérése publikus forrásból.

    Ha a hálózati hívás sikertelen, üres listát ad vissza.
    """
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.json"
    data = _safe_get_json(url)
    if not data:
        return []
    tickers: List[str] = []
    for row in data:
        sym = str(row.get("Symbol", "")).strip().upper()
        if sym:
            tickers.append(sym)
    return sorted(set(tickers))


def _filter_highconv_candidates(
    tickers: List[str],
    min_upside_pct: float = 15.0,
    min_analysts: int = 10,
    min_buy_ratio: float = 0.70,
    hard_upside_boost: float = 25.0,
    max_count: int = 10,
) -> List[Dict[str, Any]]:
    """High-conviction jelöltek szűrése az univerzumon belül."""
    results: List[Dict[str, Any]] = []

    for sym in tickers:
        info = _yahoo_reco_and_price(sym)
        if not info:
            continue

        num = info["num_analysts"]
        if num < min_analysts:
            continue

        strong_buy = info["strongBuy"]
        buy = info["buy"]
        hold = info["hold"]
        sell = info["sell"]
        strong_sell = info["strongSell"]

        total_cov = strong_buy + buy + hold + sell + strong_sell
        if total_cov <= 0:
            continue

        buy_ratio = (strong_buy + buy) / float(total_cov)
        upside = info["upside_pct"]

        # Alap szűrők (Zoli-paraméterek)
        if upside < min_upside_pct and upside < hard_upside_boost:
            continue
        if buy_ratio < min_buy_ratio:
            continue

        info["buy_ratio"] = buy_ratio
        results.append(info)

        if len(results) >= max_count * 3:
            break

        time.sleep(0.2)

    results_sorted = sorted(results, key=lambda x: x.get("upside_pct", 0.0), reverse=True)
    return results_sorted[:max_count]


def generate_highconviction_json(
    path: str = "reports/high_conv_1.json",
    min_upside_pct: float = 15.0,
    min_analysts: int = 10,
    min_buy_ratio: float = 0.70,
    hard_upside_boost: float = 25.0,
    max_count: int = 10,
) -> List[Dict[str, Any]]:
    """Automatikus high-conviction lista generálása (S&P500-univerzumra,
    a saját portfólió + watchlist kizárásával)."""

    # 1) Alap univerzum: S&P500
    tickers = _load_sp500_universe()
    if not tickers:
        return []

    # 2) Saját tickerek kizárása (portfólió + watchlist a MASTER-ből)
    banned = _load_banned_tickers_from_master()
    if banned:
        tickers = [t for t in tickers if t not in banned]

    if not tickers:
        return []

    # 3) Szűrés a Yahoo recommendation + PT alapján
    candidates = _filter_highconv_candidates(
        tickers=tickers,
        min_upside_pct=min_upside_pct,
        min_analysts=min_analysts,
        min_buy_ratio=min_buy_ratio,
        hard_upside_boost=hard_upside_boost,
        max_count=max_count,
    )
    if not candidates:
        return []

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    payload: List[Dict[str, Any]] = []
    for c in candidates:
        ticker = c["ticker"]
        upside = c["upside_pct"]
        strong_buy = c["strongBuy"]
        buy = c["buy"]
        hold = c["hold"]
        sell = c["sell"]
        strong_sell = c["strongSell"]
        target_mean = c["target_mean"]
        current = c["current"]

        text = (
            f"{ticker} – {upside:.1f}% konszenzus upside a 12M célárhoz képest, "
            f"Strong Buy: {strong_buy}, Buy: {buy}, Hold: {hold}, Sell: {sell}, Strong Sell: {strong_sell}. "
            f"Célár (mean): {target_mean:.2f}, aktuális ár: {current:.2f}."
        )

        payload.append(
            {
                "ticker": ticker,
                "text": text,
                "upside_pct": upside,
                "strongBuy": strong_buy,
                "buy": buy,
                "hold": hold,
                "sell": sell,
                "strongSell": strong_sell,
                "target_mean": target_mean,
                "current": current,
            }
        )

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return payload


def fetch_highconviction_events(path: str = "reports/high_conv_1.json") -> List[str]:
    """
    High-conviction (3–12 hó) jelöltek betöltése.

    Minden híváskor megpróbál egy FRISS listát generálni
    (S&P500-univerzum, MASTER-beli tickerek kizárásával).
    A JSON fájlt csak logolási / debug célból írjuk ki.

    Visszatérési érték: display-ready lista, pl.:
        ["XYZ – 23.4% konszenzus upside ...", ...]
    """
    full = os.path.join(os.getcwd(), path)

    # 1) Első próbálkozás: generáljunk friss listát
    candidates = generate_highconviction_json(path=full)

    events: List[str] = []
    for c in candidates:
        ticker = str(c.get("ticker", "")).strip().upper()
        text = str(c.get("text", "")).strip()
        if ticker and text:
            events.append(f"{ticker} – {text}")
        elif text:
            events.append(text)

    if events:
        return events

    # 2) Ha valamiért nem sikerült generálni, próbáljuk meg
    # fallbackként beolvasni a meglévő JSON-t (ha van)
    if not os.path.exists(full):
        return []

    try:
        with open(full, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []

    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                txt = item.strip()
                if txt:
                    events.append(txt)
            elif isinstance(item, dict):
                ticker = str(item.get("ticker", "")).strip().upper()
                text = str(item.get("text", "")).strip()
                if ticker and text:
                    events.append(f"{ticker} – {text}")
                elif text:
                    events.append(text)

    return events


def format_highconviction_block(events: List[str]) -> str:
    if not events:
        return ""
    lines = ["**Listán kívüli, 3–12 hónapos high-conviction jelöltek**"]
    for e in events:
        e = str(e).strip()
        if not e:
            continue
        lines.append(f"- {e}")
    return "\n".join(lines)

# -*- coding: utf-8 -*-
"""
BIBLIA HELPER – 1/2/3-as jelentések kanonikus leírása

Ez a helper NEM technikai doksi (nincsenek benne path-ok, URL-ek, kódhívások),
hanem a három jelentés (1/2/3) működési „bibliája”.
Minden scriptnek és minden manuális átnézésnek ehhez kell igazodnia.
"""

BIBLIA_HELP_TEXT = r"""
A: ÁLTALÁNOS ELVEK
------------------
- Fókusz: árfolyamot érdemben mozgató információk (anyagi lényegesség).
- Csak megerősített, hiteles forrás: Reuters/AP/Bloomberg, hivatalos IR/SEC,
  MarketBeat / elemzői feedek, nagy házak kommentjei, szektor-specifikus portálok.
- Pletyka, Reddit/Stocktwits, random blog NEM kerül be.
- Minden ±3,00% vagy nagyobb ármozgásnál kötelező a pontos % (két tizedes)
  és az ok 1 mondatban.
- A darabszámos tickerek MINDIG előre kerülnek, utána jön a watchlist – csak
  ha ott teljesülnek a küszöbök.
- A riport elején kötelező a „Lefedettség” blokk:
    • „Lefedettség: TELJES” – ha az összes tickerre sikerült adatot szerezni.
    • „Lefedettség: HIÁNYOS – nem elérhető ticker(ek): … (oka: …)” – ha bármi hiányzik.
- Ha egy blokk logikailag üres (pl. nincs high-conv jelölt), azt vagy ki kell hagyni,
  vagy egyértelmű, rövid mondattal jelezni, hogy „nincs releváns tétel”.

Forrás-prioritások (A–E)
------------------------
A) Gyors, megbízható hírfeed:
   - Reuters (Top, Markets, Breakingviews)
   - AP News
   - Bloomberg Top/Markets (ha elérhető)
   - Dow Jones Newswires / The Fly / Benzinga Pro tape

B) Hivatalos vállalati csatornák:
   - SEC EDGAR (8-K, 6-K, 10-Q, 10-K)
   - Cég IR / Newsroom
   - Business Wire, PR Newswire, GlobeNewswire

C) Elemzői fel/lemínősítések, célárak:
   - MarketBeat Ratings (elsődleges)
   - StreetInsider / The Fly (Analyst)
   - TipRanks (kiegészítő)

D) Makró / FED / politika:
   - FederalReserve.gov
   - BLS, BEA
   - US Treasury
   - Reuters/AP politikai / makró hírei

E) Szektor-specifikus feedek:
   - Félvezetők, AI, kripto, biotech, stb. – csak megerősített, több forrásból.

Anyagi lényegesség – mik kerülnek be?
-------------------------------------
- Guidance-emelés vagy -vágás (bevétel, EPS, margin, FCF).
- Jelentős EPS/árbevétel meglepetés (pozitív vagy negatív).
- M&A, nagy stratégiai deal, partnerkapcsolat, licence.
- Buyback-program, osztalék bejelentés vagy érdemi módosítás.
- CEO/CFO/Chair váltás, komoly vezetői változás.
- Szabályozási / jogi döntés, ami a core üzletet érinti.
- Több, egymást erősítő elemzői felminősítés / céláremelés.
Ami NEM kerül be: apró PR, marketing-cikk, gyenge pletyka, egyforrásos zaj.


I. #1 JELENTÉS – „TEGNAPI ZÁRÁSTÓL MOSTANÁIG” (AH + PM)
-------------------------------------------------------
Időablakok (CET/CEST):
- After-hours (AH): előző kereskedési nap 22:00–02:00.
- Premarket (PM): aktuális nap 10:00–15:30.
A #1-es riport CSAK ezekkel az idősávokkal foglalkozik; intraday Open→Close
mozgás NEM része.

Árforrás és számolás:
- Yahoo Finance chart (2 nap / 5 perces gyertya, includePrePost).
- Bázisár: az utolsó rendes kereskedési (RTH) záróár.
- AH/PM %: (AH/PM utolsó ár – előző RTH záró) / záró × 100.
- Minden ±3,00% vagy nagyobb mozgásnál kötelező:
    • pontos %-érték (két tizedre),
    • megnevezni, hogy AH vagy PM sáv,
    • 1 mondatban az ok.

Blokksorrend:
1) Lefedettség blokk (TELJES / HIÁNYOS + hiányzó tickerek).
2) Makró / FED / piaci hangulat blokk.
3) Darabszámos tickerek AH/PM mozgásai.
4) Watchlist tickerek AH/PM mozgásai (csak ha van releváns mozgás/hír).
5) Bejelentések & fel/lemínősítések.
6) Közeli katalizátorok (3–10 nap).
7) Listán kívüli high-conviction jelöltek (ha vannak).

Makró / FED / piaci hangulat blokk (#1)
---------------------------------------
Cél: 3–5 soros, Bloomberg-szerű összefoglaló az AH/PM sáv legfontosabb
makró, FED és általános piaci hangulat híreiről.

Tartalom:
- Rövid, „headline-stílusú” mondatok:
    • kulcs makró-adatok (infláció, munkaerőpiac, GDP, PMI, stb.),
    • Fed-kommentek (rate cut / hike várakozás, dot plot, stb.),
    • globális kockázati étvágy (futures, hozamok, dollár, olaj, kriptó).
- Utána 1–2 mondat arról, hogy ez várhatóan hogyan hat:
    • risk-on vs. risk-off hangulat,
    • growth/tech vs. value/defenzív szektorok.

Üres eset:
- Ha az adott sávban nincs piaci szintű, lényeges makró/FED hír:
  „Az AH/PM sávban nem érkezett a piac egészét érintő, anyagilag lényeges makró vagy FED hír.”

Darabszámos tickerek – AH/PM (#1)
---------------------------------
Cél: a portfólióban darabszámmal szereplő papírok AH/PM viselkedésének rövid, indokolt bemutatása.

Szabályok:
- Csak darabszámos ticker kerül ide.
- Küszöb:
    • ±3,00% vagy nagyobb AH/PM mozgás → RÉSZLETES sor.
    • ±3,00% alatt → elegendő egy összefoglaló blokk („küszöb alatti”).

Részletes sor tartalma:
- Ticker.
- AH változás % (két tized, ha van releváns AH forgalom).
- PM változás % (két tized, ha van releváns PM forgalom).
- Rövid ok (1 mondat), a forrás-prioritás szerint:
    • earnings / guidance,
    • makró / szektorszintű hatás,
    • elemzői lépés,
    • egyéb lényeges hír.
- „Várható hatás nyitáskor” – 1 mondatban: gap-up/gap-down, volatilitás, stb.

Küszöb alatti mozgás:
- „[Ticker]: Küszöb alatti AH/PM elmozdulás (<3%), nincs új lényeges hír.”

Watchlist – AH/PM (#1)
----------------------
Cél: a figyelőlistán lévő papírok közül csak azokat kiemelni, ahol VALÓDI történés van.

Szabályok:
- Watchlist ticker csak akkor kerül be, ha:
    • AH/PM sávban legalább ±3,00% a mozgás, VAGY
    • hírfolyamban anyagilag lényeges esemény (earnings, guidance,
      nagy deal, szabályozási döntés, elemzői fel/lemínősítés).
- Formátum ugyanaz, mint a darabszámos tickereknél.

Bejelentések & fel/lemínősítések (#1)
-------------------------------------
Cél: összefoglalni minden, a portfóliót vagy watchlistet érintő, anyagilag lényeges vállalati bejelentést és elemzői lépést az AH/PM sávban.

Közeli katalizátorok (#1)
-------------------------
- Earnings-dátumok, Fed-meeting, fontos makrók, product launch, investor day, PDUFA, stb.
- Formátum: „[Ticker] – [Esemény típusa] – [Dátum/idősáv] – 1 mondatos jelentőség.”

Listán kívüli, 3–12 hónapos high-conviction jelöltek (#1)
---------------------------------------------------------
Kritériumok (legalább 2 teljesüljön az 5-ből):
1) Több friss felminősítés / céláremelés nagy házaktól.
2) Guidance-emelés, pozitív vállalati iránymutatás.
3) Konszenzus EPS/árbevétel felfelé módosul.
4) 3–12 hónapon belüli konkrét katalizátor.
5) Erős relatív erő, 52 hetes csúcs közeli teljesítmény.

Szabályok:
- A blokkban SOHA nem szerepelhet olyan ticker, ami portfólióban vagy watchlisten van.
- Csak akkor jelenik meg a blokk, ha van ténylegesen erős jelölt.
- Minden jelöltnél: ticker + szektor, 2–3 mondatos „thesis”, felsorolva a teljesülő kritériumokat.


II. #2 JELENTÉS – „TEGNAPI NYITÁSTÓL ZÁRÁSIG” (OPEN→CLOSE)
-----------------------------------------------------------
Időablak:
- Előző kereskedési nap 15:30–22:00 (CET/CEST).

Fókusz:
- Előző nap NAPI mozgásai, különösen a ±3,00% feletti Open→Close elmozdulások indoklással.
- Intraday hírek, amelyek ezeket okozták.

Árforrás és számolás:
- Open→Close %: (záró – nyitó) / nyitó × 100.
- Küszöb: ±3,00% vagy nagyobb elmozdulásnál kötelező részletes magyarázat.

Blokksorrend:
1) Lefedettség blokk.
2) Makró / FED / piaci hangulat – előző nap intraday eseményei.
3) Darabszámos tickerek Open→Close mozgásai (≥3% fókusz).
4) Watchlist tickerek Open→Close mozgásai (csak ha hír vagy ≥3%).
5) Bejelentések & fel/lemínősítések (nap közben).
6) Közeli katalizátorok.
7) High-conv jelöltek (ha a nap során jön hozzájuk új jel).


III. #3 JELENTÉS – „MA NYITÁSTÓL MOSTANÁIG” (OPEN→MOST)
--------------------------------------------------------
Időablak:
- Aktuális kereskedési nap 15:30–lekérdezés pillanata (CET/CEST).

Fókusz:
- Aktuális intraday mozgások:
    • Open→Most %,
    • opcionálisan High/Open és Low/Open,
    • nap közben érkező hírek, makrók, Fed-kommentek.

Árforrás és számolás:
- Open→Most %: (aktuális ár – nyitóár) / nyitóár × 100.
- High/Open %: (intraday csúcs – nyitó) / nyitó × 100.
- Low/Open %: (intraday mélypont – nyitó) / nyitó × 100.
- Küszöb: ±3,00% vagy nagyobb Open→Most mozgásnál kötelező indoklás.

Blokksorrend:
1) Lefedettség blokk.
2) Makró / FED / piaci hangulat – aznapi intraday fejlemények.
3) Darabszámos tickerek Open→Most (és ha kérve: High/Open, Low/Open).
4) Watchlist tickerek (csak ha hír vagy ≥3%).
5) Bejelentések & fel/lemínősítések (nap közben).
6) Közeli katalizátorok (ha aznapi hír rájuk utal).
7) High-conv jelöltek (ha ma erősödik a case).


IV. LEFEDETTSÉG ÉS FALLBACK LOGIKA – ÖSSZEFOGLALÓ
--------------------------------------------------
- Minden jelentés elején:
    • Ha az összes tickerre sikerül árat + hírt hozni:
        „Lefedettség: TELJES”.
    • Ha bármelyik tickerre nincs friss ár/hír:
        „Lefedettség: HIÁNYOS – nem elérhető ticker(ek): [lista]
         (oka: pl. nincs friss adat / forráshiba / késik feed)”.
- Ha egy ticker árfolyamadata nem elérhető, de releváns hír van róla:
    • a hírt akkor is lehet röviden leírni, zárójellel jelezve, hogy
      az árfolyamadat hiányzik.

A fenti szabályok adják a három riport (1/2/3) kanonikus, hosszú távra érvényes
működési keretét. Kódoldali módosításnál mindig ez legyen az igazodási pont.
"""

def get_biblia_text() -> str:
    """Helper, ha máshonnan akarod beolvasni a teljes bibliát."""
    return BIBLIA_HELP_TEXT



# =========================
# HOWTO – PIPELINE OVERVIEW
# =========================
# 1) macro_news_{r}.json
# 2) earnings_{r}.json
# 3) analyst_{r}.json / catalysts_{r}.json
# 4) high_conv_{r}.json
# 5) report_runner.py
# 6) postprocess_report.py (macro + reflow)
# 7) validate_run.py (guard)
# 8) publish (gist)

# =========================
# REPORT-SPECIFIC FILES
# =========================
# #1 -> high_conv_1.json
# #2 -> high_conv_2.json
# #3 -> high_conv_3.json

# =========================
# TROUBLESHOOTING
# =========================
# - Lapított report -> postprocess reflow
# - Hiányzó adat -> Lefedettség: HIÁNYOS (WARN)
# - MarketBeat 403 -> analyst feed üres, placeholder OK
