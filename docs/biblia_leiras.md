# Részvényjelentés Automata – BIBLIA leírás  
**Verzió:** v3.6.0  

Ez a dokumentum írja le a teljes #1 / #2 / #3 riport-pipeline architektúráját, a
fájlszerkezetet, a fő scripteket és a BIBLIA szerinti logikát.

---

## 1. Könyvtárszerkezet

```text
repo/
├─ README.md
├─ .github/
│   └─ workflows/
│       ├─ run_report.yml
│       └─ update_biblia_docs.yml
├─ docs/
│   ├─ CHANGELOG.md
│   └─ biblia_leiras.md
├─ reports/
│   ├─ 1/
│   │   ├─ analyst_1.json
│   │   ├─ catalysts_1.json
│   │   ├─ macro_news_1.json
│   │   ├─ high_conv_1.json
│   │   └─ summary_report_1.md
│   ├─ 2/
│   │   ├─ analyst_2.json
│   │   ├─ catalysts_2.json
│   │   ├─ macro_news_2.json
│   │   └─ summary_report_2.md
│   └─ 3/
│       ├─ latest_3.json
│       ├─ latest_3.md
│       ├─ macro_news_3.json
│       ├─ summary_report_3.md
│       ├─ raw_analyst_1.json
│       └─ raw_catalysts_1.json
└─ scripts/
    ├─ analyst_block_builder.py
    ├─ analyst_feed_parser.py
    ├─ biblia_helper.py
    ├─ blocks_events_3.py
    ├─ blocks_intraday_3.py
    ├─ export_biblia_md.py
    ├─ highconv_block_builder.py
    ├─ highconv_builder.py
    ├─ macro_fetcher.py
    ├─ macro_highconv_helpers_v2.py
    ├─ postprocess_report.py
    ├─ postprocess_report_2.py
    ├─ postprocess_report_3.py
    ├─ report_runner.py
    └─ (egyéb segédscriptek a projekt korábbi verzióiból)
```

---

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

### Verziózás
A `validate_run.py` és a workflow módosításai is a kötelező verziófolytatás hatálya alá esnek.

---

## 📁 Könyvtárszerkezet – kiegészítés

Új fájl a guard ellenőrzéshez:

```
scripts/
  validate_run.py        # CI guard: a futás csak akkor zöld, ha a report szerkezetileg valid
```

