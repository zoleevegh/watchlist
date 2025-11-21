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
from typing import List

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


# --- AUTO-ADDED: Yahoo macro news + macro block formatter (UPDATED2) ---

def fetch_yahoo_macro_news():
    import requests
    url = "https://query1.finance.yahoo.com/v1/finance/search?q=markets"
    try:
        r = requests.get(url, timeout=8)
        data = r.json()
        news = []
        for item in data.get("news", []):
            title = item.get("title", "")
            if any(kw in title.lower() for kw in [
                "fed","rate","inflation","treasury","yield","futures","stocks",
                "market","jobs","opec","oil"
            ]):
                news.append({
                    "title": item.get("title",""),
                    "summary": item.get("summary","")
                })
        return news[:3]
    except:
        return []


def format_macro_block(macro_text, yahoo_news):
    block = ["**Politika / FED / Piaci hangulat**"]
    if macro_text:
        block.append(macro_text.strip())
    if yahoo_news:
        for item in yahoo_news[:3]:
            t = item.get("title","")
            if t:
                block.append(f"- {t}")
    return "\n".join(block)


# --- AUTO-ADDED: Analyst steps block (stub) ---

def fetch_analyst_events():
    # Placeholder: to be replaced with real API calls in GH Actions environment.
    try:
        import requests
        url = "https://api.marketbeat.com/v1/ratings/recent"  # placeholder; may require key
        r = requests.get(url, timeout=8)
        if r.status_code==200:
            data=r.json()
            events=[]
            for item in data.get("ratings",[])[:10]:
                events.append(f"{item.get('symbol')} – {item.get('action')} – {item.get('price_target','')}")
            return events
    except:
        return []
    return []


def format_analyst_block(events):
    if not events:
        return ""
    out=["**Elemzői lépések / fel-lemínősítések**"]
    for e in events[:10]:
        out.append(f"- {e}")
    return "\n".join(out)
