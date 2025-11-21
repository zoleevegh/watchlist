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
- hogyan használjuk az „Eladasi ar” oszlopot (exit árhoz viszonyított % eltérés),
- hogyan működik a „listán kívüli, 3–12 hónapos high-conviction jelöltek” blokk.

FIX GIST LINK – #1 JELENTÉS (AH+PM)
-----------------------------------
A legutóbbi #1-es jelentés (After-hours + Premarket) mindig itt érhető el RAW formában:

https://gist.githubusercontent.com/zoleevegh/5df443b8a46ef863cdc97aad62756510/raw/summary_report_1.md

Ezt a GitHub Action minden #1-es futás után automatikusan frissíti a
`reports/summary_report_1.md` tartalmával. Ha bármi elveszne, innen bármikor
lekérhető a legutolsó #1-es riport.

FONTOS ALAPELVEK
----------------
- Időzóna: Europe/Budapest (CET/CEST).
- Csak US kereskedési napokon fut a #1/#2/#3 jelentés.
- Árforrás: Yahoo Finance chart v8 (2d/5m, includePrePost=true) és szükség esetén
  intraday / quote API (Open, High, Low, Close, Last).
- Ticker-sorrend minden jelentésben:
    1) darabszámos pozíciók (quantity > 0),
    2) utána watchlist-tickerek (csak ha a feltételek teljesülnek).
- Küszöb a jelentendő mozgásokra: alapértelmezett K = ±3,00%,
  ticker-szintű felülírás a MASTER „K” oszlopából.
- K/L/M oszlopok a MASTER-ben:
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

- PKN.WA: nincs speciális szabály – ha valaha visszakerülne a
  MASTER-be, normál tickerként kezelendő (nincs automatikus kihagyás).

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
    - AH% = (AH utolsó ár - RTH close) / RTH close * 100.
    - PM% = (PM utolsó ár - RTH close) / RTH close * 100.
    - Ha nincs releváns pre/post gyertya: AH/PM = n/a.

#2 – Tegnapi nyitástól zárásig (Open→Close)
    - Időablak: előző kereskedési nap US RTH, 15:30–22:00 CEST.
    - Open→Close% = (Close - Open) / Open * 100.

#3 – Ma nyitástól mostanáig (Open→Most)
    - Időablak: aktuális nap US RTH 15:30 CEST → lekérdezés pillanata.
    - Open→Most% = (Last - Open) / Open * 100.
    - Opcionálisan: High/Open és Low/Open %.

CHECKLIST-FÜGGVÉNYEK
--------------------
Az alábbi get_report*_checklist() függvények rövid, gép- és ember-olvasható
checklistákat adnak vissza. Ezeket:

- használhatja a script belső „önellenőrzésre” (minden blokk megvan-e),
- vagy hibakeresésnél / debug során össze lehet vetni az aktuális,
  legenerált jelentéssel.

"""

from __future__ import annotations
from typing import List

# Használható konstans, ha máshonnan is hivatkozni akarunk a RAW #1-es linkre
GIST_REPORT1_RAW_URL = "https://gist.githubusercontent.com/zoleevegh/5df443b8a46ef863cdc97aad62756510/raw/summary_report_1.md"


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
        "Watchlist sor-formátum: 'TICKER — AH +x.xx% | PM -y.yy% — Watchlisten is érdemi AH/PM elmozdulás (≥K=...) az utolsó RTH záróhoz képest.'.",
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
