# Részvények Projekt -- BIBLIA LEÍRÁS (MD)

## Fájlszerkezet, szerepek és riportlogika

## 1. Könyvtárszerkezet

    repo/
    ├─ scripts/
    │   ├─ report_runner.py
    │   ├─ biblia_helper.py
    │   ├─ macro_highconv_helpers_v2.py
    │   ├─ highconv_builder.py
    │   ├─ macro_fetcher.py
    │   └─ analyst_feed_parser.py
    ├─ reports/
    │   ├─ 1/
    │   │   ├─ summary_report_1.md
    │   │   ├─ latest_1.md
    │   │   ├─ macro_news_1.json
    │   │   └─ high_conv_1.json
    │   ├─ 2/
    │   └─ 3/
    └─ data/
        ├─ master.csv
        └─ universe/

## 2. Fájlok szerepköre

### scripts/biblia_helper.py

Segédfüggvények: - #1/#2/#3 riport logika ellenőrző listái - makró és
hírblokkok formázása - high-conv kritérium-értékelés - ticker-szűrés a
MASTER alapján

### scripts/highconv_builder.py

-   analyst feed → high-conv JSON generálás
-   kizárja a portfólió + watchlist tickereket
-   5 biblia-kritérium alapján pontoz

### scripts/macro_highconv_helpers_v2.py

-   makró blokk és high-conv blokk beszúrása a summary_report_x.md-be
-   latest_x.md frissítése

### scripts/report_runner.py

-   #1/#2/#3 futtatások logikája
-   input → helper → output feldolgozás

## 3. Jelentések logikája

### #1 After-hours + Premarket

-   időablakok: 22:00--02:00 és 10:00--15:30
-   makróblokkal indul
-   darabszámos tickerek előre
-   watchlist: csak ha hír vagy ≥ ±3%
-   bejelentések
-   katalizátorok
-   high-conv

### #2 Open→Close (előző nap)

-   napi intraday mozgások
-   ±3% fókusz
-   intraday hírek + makró

### #3 Open→Most (mai nap)

-   aktuális intraday mozgások
-   High/Open és Low/Open opcionális
-   nap közbeni breaking hírek

## 4. Adatfolyam

1.  Yahoo → ármozgás
2.  Apps Script → analyst feed
3.  macro_fetcher → makróhírek
4.  highconv_builder → high_conv_1.json
5.  macro_highconv_helpers_v2 → summary_report_1.md véglegesítése

## 5. Fallback logika

-   Yahoo elsődleges, Google / Investing second/third fallback
-   ha ár nem elérhető: hír akkor is mehet (jelölve)

## 6. High-conv kritériumok

Legalább 2 teljesüljön: - elemzői felminősítések - guide emelés -
konszenzus-felhúzás - 3--12 hónapos katalizátor - relatív erő / 52w high
közeli

## Technikai működés és debug (migrálva a biblia_helper.py-ből)

"Ha nincs releváns AH/PM candle, akkor a reportban 'AH n/a' vagy 'PM n/a' szerepel, ez nem lefedettség-hiba.",

---

        "Jelentés elején kötelező a Lefedettség-blokk: TELJES vagy HIÁNYOS + tickerek okaival.",
        "Darabszámos pozíció: MASTER-ben quantity > 0 → mindig megjelenik a darabszámos blokkban.",

---

        "Darabszámos sor-formátum: 'TICKER — AH +x.xx% | PM -y.yy% — komment / vagy: Egyelőre nincs küszöb feletti AH/PM elmozdulás.'.",

---

        "Bejelentések & fel/lemínősítések blokk: biblia szerint MarketBeat/StreetInsider alapú, de Pythonban még TODO.",
        "Közeli katalizátorok blokk: earnings / események listázása – jelenleg manuális, Pythonban TODO.",
        "Listán kívüli, 3–12 hónapos high-conviction blokk: csak portfólión/watchlisten kívüli nevekkel – jelenleg manuális, Pythonban TODO.",

---

        "Árforrás: Yahoo Finance OHLC / intraday chart; Open és Close elegendő.",

---

        "Lefedettség-blokk a jelentés elején: TELJES / HIÁNYOS a biblia szerint.",

---

        "L/M oszlop: vol és 52w high/low közelség küszöb – opcionális, ha a script használja.",
        "Darabszámosaknál minden |Open→Close%| ≥ K kötelezően jelentendő.",
        "Watchlisten |Open→Close%| ≥ K vagy anyagilag lényeges hír esetén kerül be.",
        "Minden 3%+ mozgásnál pontos % + 1 mondatos ok.",

---

        "Opcionális: High/Open és Low/Open százalékok hozzáírása.",
        "Blokksorrend: Lefedettség → Makró/FED → Darabszámos → Watchlist → Bejelentések & fel/lemínősítések → Katalizátorok → High-conviction.",
        "Bejelentések & fel/lemínősítések: darabszámosaknál minden material event; watchlisten csak anyagilag lényeges.",

---

        "Opcionálisan High/Open és Low/Open % is számolható.",
        "Lefedettség-blokk a jelentés elején: TELJES / HIÁNYOS.",
        "Ha nap közben érkezett fontos makró/FED hír, a lefedettség után külön blokkban szerepel.",
        "Darabszámos és watchlist-tickerek forrása: MASTER; K/L/M és Eladasi ar szabályok ugyanazok, mint #1/#2.",

---

        "Darabszámosaknál minden |Open→Most%| ≥ K kötelezően jelentendő.",
        "Watchlisten: |Open→Most%| ≥ K vagy anyagilag lényeges hír esetén kerül a riportba.",
        "Minden 3%+ mozgásnál pontos % + egy mondatos indok.",

---

        "Intraday Bejelentések & fel/lemínősítések: minden friss, material event, ami látványos intraday mozgást okoz.",

---

    Cél: a high-conviction blokkba soha ne kerüljön olyan név, ami szerepel a
    felhasználó portfóliójában vagy watchlistjén.

---

    Ha a hálózati hívás sikertelen, üres listát ad vissza.

---

    A JSON fájlt csak logolási / debug célból írjuk ki.

---

    Visszatérési érték: display-ready lista, pl.:

---

Minden scriptnek és minden manuális átnézésnek ehhez kell igazodnia.

---

A: ÁLTALÁNOS ELVEK

---

- Csak megerősített, hiteles forrás: Reuters/AP/Bloomberg, hivatalos IR/SEC,
  MarketBeat / elemzői feedek, nagy házak kommentjei, szektor-specifikus portálok.
- Pletyka, Reddit/Stocktwits, random blog NEM kerül be.

---

  és az ok 1 mondatban.
- A darabszámos tickerek MINDIG előre kerülnek, utána jön a watchlist – csak
  ha ott teljesülnek a küszöbök.
- A riport elején kötelező a „Lefedettség” blokk:
    • „Lefedettség: TELJES” – ha az összes tickerre sikerült adatot szerezni.

---

  vagy egyértelmű, rövid mondattal jelezni, hogy „nincs releváns tétel”.

---

   - BLS, BEA

---

   - Félvezetők, AI, kripto, biotech, stb. – csak megerősített, több forrásból.

---

- M&A, nagy stratégiai deal, partnerkapcsolat, licence.
- Buyback-program, osztalék bejelentés vagy érdemi módosítás.
- CEO/CFO/Chair váltás, komoly vezetői változás.
- Szabályozási / jogi döntés, ami a core üzletet érinti.
- Több, egymást erősítő elemzői felminősítés / céláremelés.
Ami NEM kerül be: apró PR, marketing-cikk, gyenge pletyka, egyforrásos zaj.

---

mozgás NEM része.

---

Árforrás és számolás:

---

- Minden ±3,00% vagy nagyobb mozgásnál kötelező:

---

    • megnevezni, hogy AH vagy PM sáv,

---

Cél: 3–5 soros, Bloomberg-szerű összefoglaló az AH/PM sáv legfontosabb
makró, FED és általános piaci hangulat híreiről.

---

- Rövid, „headline-stílusú” mondatok:

---

- Utána 1–2 mondat arról, hogy ez várhatóan hogyan hat:

---

    • growth/tech vs. value/defenzív szektorok.

---

Üres eset:
- Ha az adott sávban nincs piaci szintű, lényeges makró/FED hír:
  „Az AH/PM sávban nem érkezett a piac egészét érintő, anyagilag lényeges makró vagy FED hír.”

---

Cél: a portfólióban darabszámmal szereplő papírok AH/PM viselkedésének rövid, indokolt bemutatása.

---

Szabályok:
- Csak darabszámos ticker kerül ide.
- Küszöb:
    • ±3,00% vagy nagyobb AH/PM mozgás → RÉSZLETES sor.

---

Részletes sor tartalma:

---

    • makró / szektorszintű hatás,
    • elemzői lépés,
    • egyéb lényeges hír.
- „Várható hatás nyitáskor” – 1 mondatban: gap-up/gap-down, volatilitás, stb.

---

Küszöb alatti mozgás:

---

Cél: a figyelőlistán lévő papírok közül csak azokat kiemelni, ahol VALÓDI történés van.

---

Szabályok:
- Watchlist ticker csak akkor kerül be, ha:
    • AH/PM sávban legalább ±3,00% a mozgás, VAGY

---

- Formátum ugyanaz, mint a darabszámos tickereknél.

---

Cél: összefoglalni minden, a portfóliót vagy watchlistet érintő, anyagilag lényeges vállalati bejelentést és elemzői lépést az AH/PM sávban.

---

- Earnings-dátumok, Fed-meeting, fontos makrók, product launch, investor day, PDUFA, stb.

---

Szabályok:
- A blokkban SOHA nem szerepelhet olyan ticker, ami portfólióban vagy watchlisten van.
- Csak akkor jelenik meg a blokk, ha van ténylegesen erős jelölt.
- Minden jelöltnél: ticker + szektor, 2–3 mondatos „thesis”, felsorolva a teljesülő kritériumokat.

---

Időablak:

---

Fókusz:
- Előző nap NAPI mozgásai, különösen a ±3,00% feletti Open→Close elmozdulások indoklással.
- Intraday hírek, amelyek ezeket okozták.

---

Árforrás és számolás:

---

- Küszöb: ±3,00% vagy nagyobb elmozdulásnál kötelező részletes magyarázat.

---

Időablak:

---

Fókusz:
- Aktuális intraday mozgások:

---

    • opcionálisan High/Open és Low/Open,
    • nap közben érkező hírek, makrók, Fed-kommentek.

---

Árforrás és számolás:

---

- Küszöb: ±3,00% vagy nagyobb Open→Most mozgásnál kötelező indoklás.

---

IV. LEFEDETTSÉG ÉS FALLBACK LOGIKA – ÖSSZEFOGLALÓ

---

- Minden jelentés elején:
    • Ha az összes tickerre sikerül árat + hírt hozni:
        „Lefedettség: TELJES”.
    • Ha bármelyik tickerre nincs friss ár/hír:

---

- Ha egy ticker árfolyamadata nem elérhető, de releváns hír van róla:
    • a hírt akkor is lehet röviden leírni, zárójellel jelezve, hogy
      az árfolyamadat hiányzik.

---

működési keretét. Kódoldali módosításnál mindig ez legyen az igazodási pont.
