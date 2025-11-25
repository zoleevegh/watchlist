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

A #2-es és #3-as jelentések fix, kanonikus RAW linkjei:

#2 – Tegnapi nyitástól zárásig (Open→Close) jelentés:
https://gist.githubusercontent.com/zoleevegh/31b8daf60f983fbdfe37e3d0f2251fc7/raw/summary_report_2.md

#3 – Mai nyitástól mostanáig (Open→Most) jelentés:
https://gist.githubusercontent.com/zoleevegh/8f77c3b0ec040030d492859095686030/raw/summary_report_3.md

GIST RAW LINK HASZNÁLAT – CACHE-BONTÁS
--------------------------------------
A GitHub Gist RAW linkeknél erős cache-elés lehet (CDN/böngésző). Ezért:
- Ha a legfrissebb #1/#2/#3 riportot akarjuk lekérdezni, a fenti fix RAW
  URL-ek mögé MINDIG tegyünk egy egyedi query paramétert, pl.:
  `?run=<GitHub run_id>` vagy `?ts=<timestamp>`.
- A GitHub Action summary-ben is ilyen cache-bontó paraméterrel jelenjen meg
  a link (pl. `...?run=${{ github.run_id }}`), hogy se a böngésző, se a CDN
  ne adja vissza a korábbi verziót.
- ChatGPT oldalon is alapértelmezett szabály, hogy a fix gist linket
  mindig egyedi query paraméterrel kérdezzük le, ha a „legutóbbi” jelentést
  kell látni.

MAKRÓ / FED / POLITIKA BLOKK KEZELÉSE
-------------------------------------
A makró / FED / piaci hangulat blokkot NEM a script állítja elő automatikusan.
A #1/#2/#3 futások csak a ticker-szintű adatokat, mozgásokat, elemzői /
katalizátor / high‑conviction eseményeket írják ki a summary_report_*.md
fájlokba.
Amikor a felhasználó teljes #1/#2/#3 jelentést kér, ChatGPT a friss
hírforrások (Reuters, Bloomberg, AP, Yahoo Finance, Investing, MarketBeat stb.)
alapján KÉZZEL ír egy strukturált makróblokkot (Fed/kamatvárakozások,
indexek, piaci hangulat, politika), és ezt a script által generált riport
elé illeszti a válaszban.


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

MI NEM FUT MÉG AUTOMATÁN, CSAK WEBES / MANUÁLIS RÉTEGBEN
--------------------------------------------------------
A #1-es jelentés célállapota a biblia szerint, de JELENLEG (2.2.3 környékén)
ezeket még nem a Python-script intézi:

- Politika/FED / „Trump-napihír” tartalmi kitöltése:
    - most a workflow `macro` paraméteréből jön egy szöveg,
      amit ember ír (Reuters/AP/FED hírek alapján).

- „Bejelentések & fel/lemínősítések” blokk:
    - MarketBeat / StreetInsider / TipRanks alapján,
    - automata összefűzés még nincs leprogramozva.

- „Közeli katalizátorok” (earnings, guidemódosítás, események):
    - earnings-calendar alapú automata kigyűjtés még nincs scriptben.

- „Listán kívüli, 3–12 hó high-conviction jelöltek”:
    - Yahoo + MarketBeat kombó alapján,
    - jelenleg manuális összeállítás, scriptben TODO.

- „Eladott pozíciók – aktuális ár az eladási árhoz képest” blokk:
    - az Eladasi ar oszlop beolvasása már elképzelhető,
      de dedikált riportblokk (pl. „X% alá/fölé jött az exithez képest”)
      még NINCS generálva Pythonból.

A KÖVETKEZŐ LÉPÉSEK #1-HEZ
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
from typing import List, Optional, Dict, Any

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
from typing import List, Optional, Dict, Any

import requests

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

def fetch_yahoo_macro_news(report_type: int = 1, now_cet: Optional[object] = None) -> List[str]:
    """
    Politika / FED / piaci hangulat hírek – JSON-alapú feed a makróblokkhoz.

    A tényleges webes hírfetch (Yahoo Finance / CNBC / Bloomberg) külön
    workflow-ban vagy scriptben fut, és az eredményt JSON-ben menti a
    ``reports/macro_news_{report_type}.json`` fájlba.

    Várt JSON-struktúra (UTF-8):

    {
        "generated_at": "2025-11-24T09:35:00+01:00",
        "report_type": 1,
        "items": [
            "Yahoo Finance: Stocks edge higher as investors await Fed minutes",
            "Bloomberg: Treasury yields fall after dovish Fed comments",
            "CNBC: Investors eye key inflation data later this week"
        ]
    }

    IDŐABLAKOK (hírekre, CEST – a JSON-ban már szűrve, itt csak dokumentáció):

    - report_type 1 vagy 2 esetén: előző kereskedési nap 15:30 → now_cet
    - report_type 3 esetén: előző piaczárás 22:00 → now_cet

    A gyakorlatban:

    - #1 és #2: az előző US RTH nyitástól (15:30) számítva minden makró/FED/politikai
      hír, ami a mostani lekérdezésig kijött (beleértve a hétvégét is, ha hétfőn fut),
    - #3: az előző zárás (22:00) után érkező hírek a lekérdezés pillanatáig.

    Jelenlegi implementáció:

    - megpróbálja beolvasni a ``reports/macro_news_{report_type}.json`` fájlt;
    - ha nincs ilyen fájl, vagy hibás a formátum → üres listát ad vissza;
    - legfeljebb 8 sztringet ad vissza, felesleges whitespace nélkül.
    """
    filename = f"macro_news_{report_type}.json"
    candidates = [
        os.path.join("reports", filename),
        filename,
    ]

    for path in candidates:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue

            items = data.get("items") if isinstance(data, dict) else data
            if not isinstance(items, list):
                return []

            cleaned: List[str] = []
            for item in items:
                if not isinstance(item, str):
                    continue
                s = item.strip()
                if s:
                    cleaned.append(s)
            # Maximum 8 headline – a formázó úgyis tovább szűri.
            return cleaned[:8]

    # Ha semmilyen JSON-t nem találunk, üres listát adunk vissza.
    return []


def summarize_macro_items(items: List[str], max_lines: int = 4) -> List[str]:
    """Nyers Yahoo/CNBC/Bloomberg headline-okból magyar mondatokat gyárt.

    - Forrásprefixel dolgozunk: "Yahoo Finance:", "CNBC:", "Bloomberg:" stb.
    - max_lines sor erejéig alakítjuk át őket magyar mondatokká.
    """
    summary: List[str] = []

    for raw in items:
        s = (raw or "").strip()
        if not s:
            continue

        core = s
        prefix = ""
        if s.startswith("Yahoo Finance:"):
            prefix = "A Yahoo Finance szerint"
            core = s.split(":", 1)[1].strip() or core
        elif s.startswith("CNBC:"):
            prefix = "A CNBC beszámolója szerint"
            core = s.split(":", 1)[1].strip() or core
        elif s.startswith("Bloomberg:"):
            prefix = "A Bloomberg kiemeli, hogy"
            core = s.split(":", 1)[1].strip() or core

        if prefix:
            sentence = f"{prefix}: {core}"
        else:
            sentence = f"Piaci hír: {s}"

        summary.append(sentence)
        if len(summary) >= max_lines:
            break

    return summary


def format_macro_block(macro_text: Optional[str], yahoo_news: List[str]) -> str:
    """
    Politika / FED / piaci hangulat blokk formázása.

    - Ha van manuális macro_text → azt mindig megjelenítjük (első sorok).
    - Ha vannak hírek → ezekből készül 3–4 magyar mondat a summarize_macro_items
      segítségével, bulletpontosan.
    - Ha semmi sincs → generikus fallback szöveget adunk vissza.
    """
    lines: List[str] = []
    title = "**Politika / FED / piaci hangulat**"

    text = (macro_text or "").strip()
    # Ha valaha "auto" kerülne ide, kezeljük üresként, ne jelenjen meg a szó.
    if text.lower() == "auto":
        text = ""

    summarized = summarize_macro_items(yahoo_news or [])

    if text and summarized:
        lines.append(title)
        lines.append(text)
        lines.extend(f"- {item}" for item in summarized)
    elif text:
        lines.append(title)
        lines.append(text)
    elif summarized:
        lines.append(title)
        lines.extend(f"- {item}" for item in summarized)
    else:
        # Fallback: nincs releváns hír, de a blokk akkor is jelenjen meg.
        lines.append(title)
        lines.append(
            "Ma nem érkezett érdemi politikai vagy FED-bejelentés; "
            "a piacok elsősorban a vállalati hírekre és makroadatokra fókuszálnak."
        )

    return "\n".join(lines)


def _load_json_list(path: str) -> List[Any]:
    """Egyszerű JSON-lista betöltő helper – hiba esetén üres listát ad."""
    try:
        full = os.path.join(os.getcwd(), path)
        if not os.path.exists(full):
            return []
        with open(full, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except Exception:
        return []
    return []


def fetch_analyst_events(path: str = "reports/analyst_1.json") -> List[str]:
    """Elemzői fel/lemínősítések eseményeinek betöltése opcionális JSON-fájlból."""
    raw = _load_json_list(path)
    events: List[str] = []
    for item in raw:
        if isinstance(item, str):
            txt = item.strip()
        elif isinstance(item, dict):
            txt = str(item.get("text", "")).strip()
        else:
            txt = str(item).strip()
        if txt:
            events.append(txt)
    return events


def format_analyst_block(events: List[str]) -> str:
    if not events:
        return ""
    lines = ["**Bejelentések & elemzői fel/lemínősítések**"]
    for e in events:
        e = str(e).strip()
        if not e:
            continue
        lines.append(f"- {e}")
    return "\n".join(lines)


# --- Közeli katalizátorok ---

def fetch_catalyst_events(path: str = "reports/catalysts_1.json") -> List[str]:
    """Közeli (3–12 hónapos) katalizátorok betöltése opcionális JSON-fájlból."""
    raw = _load_json_list(path)
    events: List[str] = []
    for item in raw:
        if isinstance(item, str):
            txt = item.strip()
        elif isinstance(item, dict):
            txt = str(item.get("text", "")).strip()
        else:
            txt = str(item).strip()
        if txt:
            events.append(txt)
    return events


def format_catalyst_block(events: List[str]) -> str:
    if not events:
        return ""
    lines = ["**Közeli katalizátorok (3–12 hónap)**"]
    for e in events:
        e = str(e).strip()
        if not e:
            continue
        lines.append(f"- {e}")
    return "\n".join(lines)


# --- High-conviction (3–12 hónapos, listán kívüli jelöltek) ---

def _yahoo_reco_and_price(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Yahoo Finance quoteSummary lekérése egy tickerre – recommendation + PT + ár.

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
    path: str = "reports/highconviction_1.json",
    min_upside_pct: float = 15.0,
    min_analysts: int = 10,
    min_buy_ratio: float = 0.70,
    hard_upside_boost: float = 25.0,
    max_count: int = 10,
) -> List[Dict[str, Any]]:
    """Automatikus high-conviction lista generálása (S&P500-univerzumra)."""
    tickers = _load_sp500_universe()
    if not tickers:
        return []

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


def fetch_highconviction_events(path: str = "reports/highconviction_1.json") -> List[str]:
    """
    High-conviction (3–12 hó) jelöltek betöltése.

    Ha a fájl nem létezik vagy üres, megpróbál automatikusan egy új JSON-t generálni
    a fenti szabályok alapján (S&P500-univerzumra).

    A visszatérési érték egy már display-ready lista:
        ["SMCI – ...", "NVO – ...", ...]
    """
    full = os.path.join(os.getcwd(), path)

    if not os.path.exists(full):
        candidates = generate_highconviction_json(path=full)
        return [f"{c['ticker']} – {c['text'].split('–', 1)[-1].strip()}" for c in candidates]

    try:
        with open(full, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        candidates = generate_highconviction_json(path=full)
        return [f"{c['ticker']} – {c['text'].split('–', 1)[-1].strip()}" for c in candidates]

    events: List[str] = []

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

    if not events:
        candidates = generate_highconviction_json(path=full)
        return [f"{c['ticker']} – {c['text'].split('–', 1)[-1].strip()}" for c in candidates]

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

