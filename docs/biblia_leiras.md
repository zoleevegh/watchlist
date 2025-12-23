# Részvényjelentés Automata – BIBLIA leírás  

## Alapelvek és hatáskör

## Könyvtárszerkezet (FRISSÍTETT)

```
repo/
├─ README.md
├─ .github/
│  └─ workflows/
│     └─ run_report.yml
├─ docs/
│  └─ biblia_leiras.md
├─ reports/
│  ├─ master.csv
│  ├─ macro_news_1.json
│  ├─ macro_news_2.json
│  ├─ macro_news_3.json
│  ├─ raw_analyst_1.json
│  ├─ raw_catalysts_1.json
│  ├─ raw_analyst_2.json
│  ├─ raw_catalysts_2.json
│  ├─ raw_analyst_3.json
│  ├─ raw_catalysts_3.json
│  ├─ analyst_1.json
│  ├─ analyst_2.json
│  ├─ analyst_3.json
│  ├─ catalysts_1.json
│  ├─ catalysts_2.json
│  ├─ catalysts_3.json
│  ├─ health_analyst_1.json
│  ├─ health_analyst_2.json
│  ├─ health_analyst_3.json
│  ├─ latest_1.json
│  ├─ latest_2.json
│  ├─ latest_3.json
│  ├─ latest_1.md
│  ├─ latest_2.md
│  ├─ latest_3.md
│  ├─ summary_report_1.md
│  ├─ summary_report_2.md
│  └─ summary_report_3.md
└─ scripts/
   ├─ macro_fetcher.py
   ├─ sec_edgar_fetcher.py
   ├─ crawler_analyst_catalyst.py
   ├─ events_fetcher.py
   ├─ yahoo_analyst_events_fetcher.py
   ├─ earnings_fetcher.py
   ├─ report_runner.py
   ├─ report_runner_3.py
   ├─ postprocess_report.py
   ├─ postprocess_report_2.py
   ├─ postprocess_report_3.py
   ├─ blocks_events_3.py
   ├─ blocks_intraday_3.py
   ├─ validate_run.py
   └─ biblia_helper.py
```

A fenti lista **előírás**: ami itt szerepel, annak léteznie és futnia kell. Ami nem szerepel, az nem része a rendszernek.

Ez a dokumentum a **Részvények projekt egyetlen kanonikus szabálykönyve**.

Ha bármely workflow, script, README vagy komment ellentmond ennek a dokumentumnak,
akkor **EZ A DOKUMENTUM AZ IRÁNYADÓ**, és az eltérést hibának kell tekinteni.


**Verzió:** v3.7.4  




## 2. Scripts – szerepek és felelősségek

### 2.1 biblia_helper.py

Segédfüggvények a BIBLIA szerinti logika implementálásához:

- riporttípusok (#1/#2/#3) metaadatainak kódolása,
- formázási segédfüggvények (pl. AH/PM százalékok, blokkok címei),
- ticker-szűrések (darabszámos vs. watchlist vs. listán kívüli),
- high-conv kritériumok kódoldali tükrözése.

### 2.2 report_runner.py

A fő „runner” script, amely:

- a MASTER CSV (master.csv) alapján felveszi a ticker-univerzumot,
- árakat tölt le (Yahoo v8 chart API, includePrePost),
- számolja a #1/#2/#3-hoz tartozó százalékos mozgásokat,
- felépíti a nyers riport-struktúrát,
- legenerálja az elsődleges `summary_report_X.md`-et (#1/#2/#3).

A konkrét logika:

- `--report 1` → After-hours + Premarket (AH/PM) (#1 riport),
- `--report 2` → Open→Close (előző napi intraday) (#2 riport),
- `--report 3` → Open→Most (mai intraday, #3 jelentés előkészítése).

### 2.3 macro_fetcher.py

- MACRO_FEED_URL_1/2/3 Apps Script webapp hívása,
- a webapp által visszaadott makró / FED / piaci hangulat blokkok letöltése,
- eredmény: `macro_news_1.json`, `macro_news_2.json`, `macro_news_3.json`.

Ezeket később a postprocess lépés fűzi be a summary_report_X.md elejére.

### 2.4 analyst_feed_parser.py

- ANALYST_FEED_URL_1/2/3 és CATALYST_FEED_URL_1/2/3 hívása (Apps Script webapp),
- a kapott JSON feedek feldolgozása egységes formára,
- fájlok:

  - `reports/analyst_1.json`, `reports/catalysts_1.json` (#1),
  - `reports/analyst_2.json`, `reports/catalysts_2.json` (#2),
  - `reports/analyst_3.json`, `reports/catalysts_3.json` (#3 – opcionális).

### 2.5 highconv_builder.py

- A globalis high-conv jelöltek gyártója,
- bemenet: analyst/catalyst feedek + piaci teljesítmény,
- kimenet: `reports/high_conv_1.json`,
- a BIBLIA szerinti kritériumok alapján pontoz:

  - 2–3+ friss felminősítés / céláremelés,
  - iránymutatás-emelés / pozitív guide,
  - konszenzus EPS/árbevétel felfelé módosul,
  - 3–12 hónapos konkrét katalizátor,
  - relatív erő (52w high közeli árfolyam).

Kritikus: **portfólió- és watchlist-tickerek kizárása** – a high-conv blokk „listán kívüli” jelölteket tartalmaz.

### 2.6 analyst_block_builder.py

- Az analyst/catalyst feed alapú „Bejelentések & fel/lemínősítések” blokk építője (#1/#2),
- bemenet: `analyst_X.json`, `catalysts_X.json`, MASTER-univerzum,
- szűrés:

  - darabszámos tickerek → mindig bekerüljenek, ha van anyagilag lényeges esemény,
  - watchlist-tickerek → csak lényeges hír vagy >= ±3% árhatás esetén,
  - listán kívüli tickerek → csak ha high-conv jellegű.

### 2.7 highconv_block_builder.py

- A high-conv jelöltek markdown blokkja (#1/#2),
- bemenet: `high_conv_1.json`,
- kimenet: „Listán kívüli, 3–12 hónapos high-conviction jelöltek” blokk,
- a blokk **soha nem tartalmazhat portfólió- vagy watchlist-tickereket**.

### 2.8 macro_highconv_helpers_v2.py

- kiegészítő helper a macro + high-conv blokkok beszúrásához,
- segédfüggvények a `postprocess_report.py` számára,
- biztosítja, hogy a makró és a high-conv blokkok BIBLIA-kompatibilis sorrendben jelenjenek meg.

### 2.9 postprocess_report.py

A #1 és #2 jelentések központi utófeldolgozója:

- a nyers `summary_report_X.md` törzsét kiegészíti:

  - makró blokkal (`macro_news_X.json`),
  - analyst & catalyst blokkal (`analyst_X.json`, `catalysts_X.json`),
  - high-conv blokkal (`high_conv_1.json`),

- gondoskodik róla, hogy a GITHUB_STEP_SUMMARY végére bekerüljön a „Raw URL for this run” info,
- eltávolítja a belső debug / job summary sorokat a végső summary-ból.

### 2.10 postprocess_report_2.py

- **kifejezetten a #2-es riporthoz tartozó wrapper**, amely:

  - fixen a `reports/summary_report_2.md`-et használja bemenetként,
  - `--report 2` paraméterrel hívja a fő `postprocess_report.py`-t,
  - biztosítja, hogy a #2 riportunak is legyen dedikált postprocess scriptje (logokban, struktúrában szépen elkülönül).

### 2.11 postprocess_report_3.py

- a #3-as intraday riport saját utófeldolgozója,
- a `blocks_events_3.py` és `blocks_intraday_3.py` által gyártott részblokkokat fűzi össze egyetlen `summary_report_3.md` fájlba,
- figyelembe veszi a BIBLIA #3-specifikus szabályait (Open→Most, High/Open, Low/Open, intraday hírek).

### 2.12 blocks_events_3.py és blocks_intraday_3.py

- #3 intraday logika:

  - `blocks_events_3.py` → esemény-blokkok (gyorsjelentés, deal, SEC, stb.),
  - `blocks_intraday_3.py` → tisztán ármozgás alapú intraday blokkok (Open→Most, csúcsok, mélypontok).

---

## 3. Jelentések logikája (BIBLIA szerint)

### 3.1 #1 – After-hours & Premarket (AH/PM) riport

**Időablakok (CE(S)T):**

- After-hours (AH): 22:00–02:00,
- Premarket (PM): 10:00–15:30.

**Fókusz:**

- AH és PM sávban történt minden lényeges ármozgás,
- vállalati hírek, elemzői lépések, makró események, amelyek a nyitást mozgatni tudják.

**Sorrend (blokkszint):**

1. Lefedettség-ellenőrzés (#1 – TELJES / HIÁNYOS, hiányzó tickerek felsorolásával),
2. Makró / Politika / FED / „Trump-napihír” blokk (macro_news_1.json),
3. Darabszámos tickerek AH/PM mozgásai,
4. Watchlist tickerek (csak ha >= ±3,00% vagy lényeges hír),
5. „Bejelentések & fel/lemínősítések” – analyst_1.json + catalysts_1.json alapján,
6. Közelgő katalizátorok,
7. „Listán kívüli, 3–12 hónapos high-conviction jelöltek” – high_conv_1.json alapján.

### 3.2 #2 – Open→Close (előző nap) riport

**Időablak (CE(S)T):**

- Előző kereskedési nap nyitástól zárásig (15:30–22:00).

**Fókusz:**

- nap közbeni ármozgások (Open→Close),
- ±3,00% vagy nagyobb mozgások részletes magyarázattal,
- intraday vállalati hírek, elemzői lépések, makró események.

**Blokkok:**

1. Lefedettség (#2 – TELJES / HIÁNYOS),
2. Makró blokk (macro_news_2.json),
3. Darabszámos tickerek Open→Close mozgásai,
4. Watchlist tickerek (ha hír vagy >= ±3,00% mozgás),
5. Elemzői lépések, katalizátorok (analyst_2.json, catalysts_2.json),
6. High-conv jelöltek (high_conv_1.json – listán kívüli tickerek).

**Implementáció:**

- a nyers `summary_report_2.md`-et a `report_runner.py` állítja elő,
- a végleges szerkezetet a `postprocess_report_2.py` + `postprocess_report.py` páros rakja össze.

### 3.3 #3 – Open→Most (mai nap) riport

**Időablak (CE(S)T):**

- Az aktuális kereskedési nap nyitástól (15:30) a lekérdezésig.

**Fókusz:**

- aktuális intraday mozgások,
- jelentősebb események (hír, gyorsjelentés, deal) az adott napon,
- azonnali piaci reakciók.

**Blokkok:**

- Lefedettség (#3),
- Darabszámos intraday mozgások (Open→Most),
- Watchlist intraday mozgások,
- Eseményblokkok (gyorsjelentések, deal-ek, SEC hírek),
- Opcionálisan High/Open és Low/Open viszonyok,
- High-conv / katalizátor-blokkok a #3-ra szabott logika szerint.

Megvalósítás: `blocks_events_3.py`, `blocks_intraday_3.py`, `postprocess_report_3.py`.

---

## 4. Adatfolyam

1. **MASTER CSV (Google Sheets → Publish to web → CSV)**  
   - letöltés: `run_report.yml` → `Fetch MASTER CSV` lépés,  
   - kimenet: `reports/master.csv`.

2. **Makró feed (Apps Script MACRO webapp)**  
   - `macro_fetcher.py` hívja a MACRO_FEED_URL_1/2/3 URL-eket,  
   - kimenet: `macro_news_1.json`, `macro_news_2.json`, `macro_news_3.json`.

3. **Analyst / Catalyst feed (Apps Script webapp)**  
   - `analyst_feed_parser.py` hívja az ANALYST_FEED_URL_X és CATALYST_FEED_URL_X URL-eket,  
   - kimenet: `analyst_X.json`, `catalysts_X.json`.

4. **High-conv builder**  
   - `highconv_builder.py` → `high_conv_1.json`.

5. **Report runner**  
   - `report_runner.py` → `summary_report_1.md`, `summary_report_2.md`, `summary_report_3.md` (nyers váz).

6. **Postprocess**  
   - #1: `postprocess_report.py` → végleges `summary_report_1.md`,
   - #2: `postprocess_report_2.py` → `postprocess_report.py` → végleges `summary_report_2.md`,
   - #3: `postprocess_report_3.py` → végleges `summary_report_3.md`.

7. **Gist frissítés**  
   - `run_report.yml` a három fix Gist-ID-re frissíti a  
     `summary_report_1.md`, `summary_report_2.md`, `summary_report_3.md` tartalmát.

---

## 5. Fallback logika

- Árforrás:

  - Yahoo Finance az elsődleges (v8 chart API, includePrePost),
  - Google Finance és Investing.com a másodlagos / harmadlagos források,
  - ha minden árfolyamforrás elérhetetlen egy tickerre:
    - a hírrész akkor is mehet,
    - a lefedettség-blokkban a ticker külön jelölve: „adat nem elérhető (kihagyva)”.

- Jelentések BIBLIA fallback:

  - ha a #1-es jelentést a pipeline valamiért nem tudja BIBLIA szerint legenerálni,
  - ChatGPT a fix #1 Gist RAW tartalmából készíthet teljes #1-es jelentést a BIBLIA szabályai szerint.

---

## 6. High-conv kritériumok (összefoglaló)

Legalább **kettőnek** teljesülnie kell:

- 2–3+ friss felminősítés / céláremelés nagy házaktól,
- iránymutatás-emelés / pozitív guide,
- konszenzus EPS/árbevétel felfelé módosul,
- 3–12 hónapos konkrét katalizátor,
- relatív erő (52w high közeli teljesítmény).

További szabályok:

- high-conv blokkba **soha nem kerülhet** olyan ticker, amely a felhasználó portfólió- vagy watchlistjén szerepel,
- a blokk csak akkor jelenik meg, ha valóban van 3–12 hónapos távon is értelmezhető, erős jelzés (nem egyetlen random upgrade).

---

# Fallback – ChatGPT által generált #1 BIBLIA jelentés

Hivatalosan támogatott, hogy ha a #1-es jelentést a GitHub Actions pipeline valamilyen hiba miatt
nem hozza létre BIBLIA szerint, akkor ChatGPT a fix #1-es Gist RAW tartalmából generálja a
teljes, formázott #1 jelentést.

Ebben az esetben a felhasználó így kérheti:

> „Kérek egy teljes #1 bibliás jelentést erre a gistre: `<RAW gist link>`”

A válaszban ChatGPT köteles:

- a BIBLIA szerinti blokkstruktúrát követni,
- a +3,00% vagy nagyobb mozgásokat pontos százalékértékkel és 1 mondatos indoklással jelezni,
- a forrás-prioritást (Reuters/SEC/MarketBeat/Yahoo) a háttérben betartani.

---

## Cache-kezelés és „legfrissebb futás” ellenőrzése (KÖTELEZŐ)

A GitHub Gist **RAW URL cache-elt** (GitHub CDN / Cloudflare).
Ezért **TILOS** cache-bontás nélküli RAW URL alapján megállapítani,
hogy egy jelentés a legutóbbi futásból származik-e.

### Kötelező ellenőrzési szabály
A jelentést **csak akkor tekintsd aktuálisnak**, ha:

1. A RAW URL végén szerepel a cache-bontó paraméter:
   ```
   ?run=GITHUB_RUN_ID
   ```
2. A jelentés fejlécében lévő időpont **megegyezik** a GitHub Actions
   futás idejével (Job summary generated at run-time).
3. A `reports/summary_report_X.md` fájl **megegyezik** a bundle ZIP tartalmával.

Ha a fenti három feltétel közül **bármelyik hiányzik**:
→ a jelentés **NEM tekinthető frissnek**.

### Tiltott gyakorlat
- cache-bontás nélküli RAW URL használata
- UI preview alapján történő ellenőrzés
- régebbi, lokálisan feltöltött `summary_report_X.md` vizsgálata

Ez a szabály **minden #1 / #2 / #3 jelentésre kötelező**.

---

## 🔒 KANONIKUS SZABÁLY – #1 JELENTÉS WEBES HÍREKKEL

A felhasználónak **NEM kell külön kérnie**, hogy a #1 jelentés „webes hírekkel” készüljön.

👉 **„#1 jelentés” = AUTOMATIKUSAN webes hírekkel készül.**

### Időkapu (kizárólag):
- After-hours (AH): 22:00–02:00 CEST
- Premarket (PM): 10:00–15:30 CEST

### Blokkonkénti forrásszabály:
- **Makró / Politika / FED:** WEBES (Reuters / AP), mindig
- **Darabszámos tickerek:** ár/% script, indok webes ha van
- **Watchlist ≥ ±3%:** trigger script, indok webes ha van
- **Bejelentések & fel/lemínősítések:** WEBES, mindig
- **Közelgő katalizátorok:** WEBES, csak konkrét esemény esetén
- **Listán kívüli 3–12 hó high‑conviction:** nem automatikus
- **Job summary:** technikai blokk, nem webes

Ha egy blokkban nincs releváns hír, azt **explicit módon jelezni kell**.

---

## ✅ CI/Workflow Guard – kötelező végellenőrzés (validate_run.py)

A pipeline akkor tekinthető sikeresnek, ha a végső Markdown kimenet **BIBLIA-követő**.

### Új fájl
- `scripts/validate_run.py`

### Funkció
A `validate_run.py` a workflow-ban **postprocess után, Gist frissítés előtt** fut, és **fail-fast** módon megállítja a futást, ha:

- hiányzik vagy üres a `reports/summary_report_{N}.md`
- #1 jelentésnél hiányzik bármely kötelező blokk/token:
  - `Lefedettség:`
  - `### Makró / Politika / FED`
  - `### Bejelentések & fel/lemínősítések`
  - `### Közeli katalizátorok`
  - `### Listán kívüli, 3–12 hónapos high-conviction jelöltek`
  - `Job summary generated at run-time`
- a jelentés „összelapított” (túl kevés sor → gyanús)

### Következmény
- A workflow **nem lehet zöld**, ha a jelentés hibás.
- Ez megszünteti a „kamu siker” állapotot.

### Verzió

- **v3.8.0** – Makró adatút tisztázva, `macro_fetcher.py` deprecated.
zás
A `validate_run.py` és a workflow módosításai is a kötelező verziófolytatás hatálya alá esnek.

---

## 📁 Könyvtárszerkezet – kiegészítés

Új fájl a guard ellenőrzéshez:

```
scripts/
  validate_run.py        # CI guard: a futás csak akkor zöld, ha a report szerkezetileg valid
```

---



- scripts/analyst_catalyst_builder.py – MarketBeat/MarketWatch analyst+catalyst builder (jina-md parser)


### analyst_catalyst_builder.py (ÚJ – analyst/catalyst feed építő, Apps Script kiváltása)

**Miért kellett:**
- MarketBeat / MarketWatch / 247WallSt jellegű scrape Apps Scriptből gyakran **401/403** (blokkolás).
- Emiatt az analyst + catalyst események előállítása **Python builder** oldalra került.

**Feladat:**
- Webes forrásokból (Python környezetből) összegyűjti és egységesíti az analyst/catalyst eseményeket.
- Deduplikál és egységes JSON struktúrát ír.

**Kimenetek:**
- `reports/{N}/analyst_{N}.json`
- `reports/{N}/catalysts_{N}.json`
- (opcionális) `reports/{N}/health_analyst_{N}.json` – forrás-szintű diagnosztika (`ok/count/httpStatus/error/ms`).

**Helye a pipeline-ban:**
- `run_report.yml` futtatja postprocess előtt, hogy a #1/#2/#3 riportok „Bejelentések & fel/lemínősítések” és „Közeli katalizátorok” blokkjai ne maradjanak üresek forrás-blokkolás miatt.


### Apps Script webapp szerepe (pontosítás)

- Apps Script webapp **CSAK** a **Makró / FED / piaci hangulat** feedet szolgáltatja.
- Analyst/Catalyst jellegű scrape **nem** Apps Scriptből megy (401/403 blokkolás miatt), hanem a `analyst_catalyst_builder.py` építi.
- A macro webapp-hoz van `?type=health` endpoint, ami forrásonként jelzi, ha valamelyik feed nem ad adatot.

#### Makró blokk – üres adat kezelése (pontosítás)

A Makró / Politika / FED blokk a #1 jelentésben **strukturálisan kötelező**.

Amennyiben a `macro_news_1.json` az adott futás során üres,
vagy nem tartalmaz piaci relevanciájú makró / FED / politikai eseményt,
a `postprocess_report.py` **kötelezően beszúr egy üres (placeholder) makró blokkot**
a jelentés elejére.

Ez biztosítja, hogy:
- a #1 jelentés blokkstruktúrája minden futásnál azonos maradjon,
- az „adat = 0” állapot **nem minősül hibának**,
- a `validate_run.py` guard BIBLIA-konform módon működjön.

### Artifact-szabály: minden jelentés külön fájlokat használ (kötelező)

A #1 / #2 / #3 jelentések **nem osztoznak** artifact fájlokon: minden jelentéstípus a saját, azonos sorszámú JSON-okat használja.

**High‑conviction (listán kívüli) jelöltek:**
- #1 → `high_conv_1.json`
- #2 → `high_conv_2.json`
- #3 → `high_conv_3.json`

**Egyéb feed artifactok (példa):**
- `macro_news_1.json`, `macro_news_2.json`, `macro_news_3.json`
- `earnings_1.json`, `earnings_2.json`, `earnings_3.json`
- `analyst_1.json`, `analyst_2.json`, `analyst_3.json`
- `catalysts_1.json`, `catalysts_2.json`, `catalysts_3.json`

### Lefedettség és validálás (pontosítás)

- **Lefedettség: HIÁNYOS** akkor jelenik meg, ha bármely tickerre az adott ablakban nincs értelmezhető adat.
- A **HIÁNYOS lefedettség nem hibakilépés**: a `validate_run.py` ezt **WARN**-ként kezeli.
- A validátor **csak szerkezeti hibára** (hiányzó kötelező blokk-header, hibás formátum, szintaktikai hiba) állítsa PIROSRA a futást.

### #1 időablak – hírek/indoklások időkapuja (egységesítés)

A #1 jelentésben a hírek/indoklások **időkapuja megegyezik** az ármozgás sávokkal:
- **After‑hours:** 22:00–02:00 (CET/CEST)
- **Premarket:** 10:00–15:30 (CET/CEST)

A #1-ben **nem** kerülhet be olyan hír/indoklás, ami ezen sávokon kívül történt, kivéve ha a Biblia külön kivételként rögzíti (pl. hivatalos SEC filing, ami közvetlenül a sávhatár előtt/után jelent meg).

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



## Adatfolyam (kanonikus, a futó workflow szerint)

### 0) Bemenet
- `reports/master.csv` – MASTER ticker-univerzum (robust letöltés után mindig ez az alap)

### 1) Makró feed (Apps Script webapp)
- A workflow **curl-lel** tölti le:
  - `reports/macro_news_{report}.json`
- Ha nincs `MACRO_FEED_URL_{report}`, a fájl üres `[]`.

### 2) SEC EDGAR filing réteg (tény-alapú catalyst)
- `scripts/sec_edgar_fetcher.py`
- Kimenet: javasolt külön artifact `reports/sec_filings_{report}.json` **vagy** beolvasztás a catalysts JSON-ba (projekt-döntés).
- Kötelező env: `SEC_USER_AGENT`

### 3) Nyers analyst & catalyst események
- `scripts/crawler_analyst_catalyst.py`
- Kimenetek (nyers):
  - `reports/raw_analyst_{report}.json`
  - `reports/raw_catalysts_{report}.json`

### 4) Normalizált analyst & catalyst JSON
- `scripts/events_fetcher.py`
- Kimenetek:
  - `reports/analyst_{report}.json`
  - `reports/catalysts_{report}.json`
  - opcionális health:
    - `reports/health_analyst_{report}.json`

### 5) Report generálás
- `scripts/report_runner.py` – #1/#2/#3 generálás a biblia szerinti blokkokkal
- #3 esetén, ha létezik:
  - `scripts/report_runner_3.py` (külön intraday/postprocess)

### 6) Earnings fetch (kiegészítő)
- `scripts/earnings_fetcher.py` – earnings/earnings-katalizátor kiegészítés (ha be van kötve a workflow-ba)

### 7) Yahoo analyst events (kiegészítő)
- `scripts/yahoo_analyst_events_fetcher.py` – jelenleg **aktív step** a workflow-ban

### 8) Postprocess
- `scripts/postprocess_report.py` / `_2.py` / `_3.py`

### 9) Validáció
- `scripts/validate_run.py`

### 10) Publikálás (artifact / fixed gist)
- A workflow a `reports/**` kimeneteket artifactként feltölti, majd frissíti a fix Gist(ek)et.


## Nem használt fájlok (törölhetőek a repo-ból)

Az alábbi fájlok **nincsenek hívva** a `run_report.yml` jelenlegi futásában, és a jelenlegi pipeline-hoz nem szükségesek:

- `scripts/analyst_feed_parser.py`
- `scripts/macro_fetcher.py`
- `scripts/macro_news_fetcher.py`
- `scripts/export_biblia_md.py` *(csak akkor kell, ha külön “update_biblia_docs.yml” workflow használja)*

**Megjegyzés:** ha később bevezeted az `update_biblia_docs.yml` automatát, akkor az `export_biblia_md.py` visszakerülhet.



## Makró / FED / Politika – KANONIKUS ADATÚT (AKTÍV)

1. **Google Apps Script macro webapp**
   - Endpoint: `/exec?type=macro&report=1`
   - Források: Reuters / AP (Biblia szerinti market-moving szűrés)
   - Kimenet: JSON

2. **Workflow (run_report.yml)**
   - Letölti a JSON-t ide: `reports/macro_news_1.json`

3. **report_runner.py**
   - Beolvassa a `macro_news_1.json` fájlt
   - Ha üres → korrekt „nem érkezett érdemi hír” szöveg
   - Soha nem talál ki hírt

**Megjegyzés:** A `macro_fetcher.py` Python script **DEPRECATED**, nincs használatban.
