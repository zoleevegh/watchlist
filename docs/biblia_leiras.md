# Részvények Projekt -- BIBLIA LEÍRÁS (MD)
BIBLIA VERZIÓ: v3.3.0


## Fájlszerkezet, szerepek és riportlogika

## 1. Könyvtárszerkezet

    repo/
    ├─ scripts/
    │   ├─ report_runner.py
    │   ├─ biblia_helper.py
    │   ├─ macro_highconv_helpers_v2.py
    │   ├─ highconv_builder.py
    │   ├─ macro_fetcher.py
    │   ├─ analyst_feed_parser.py
    │   ├─ analyst_block_builder.py
    │   ├─ highconv_block_builder.py
    │   └─ postprocess_report.py
    ├─ reports/
    │   ├─ 1/
    │   │   ├─ summary_report_1.md
    │   │   ├─ latest_1.md
    │   │   ├─ macro_news_1.json
    │   │   ├─ analyst_1.json
    │   │   ├─ catalysts_1.json
    │   │   └─ high_conv_1.json
    │   ├─ 2/
    │   └─ 3/
    └─ data/
        ├─ master.csv
        └─ universe/

## 1.3 GitHub Actions workflow-k (.github/workflows)

.github/
└─ workflows/
   ├─ run_report.yml
   │   - Napi automata #1/#2/#3 report pipeline
   │   - Futtatja a Python modulokat sorrendben:
   │       1) report_runner.py
   │       2) macro_fetcher.py
   │       3) highconv_builder.py
   │       4) analyst_feed_parser.py
   │       5) postprocess_report.py
   │   - Feltölti gist-re a summary_report_1.md végleges verzióját
   │
   └─ update_biblia_docs.yml
       - Dokumentáció automatikus frissítése (biblia, README, changelog)
       - A változásokat commitolja a repo-ba



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


### scripts/analyst_feed_parser.py – elemzői/catalyst feed (ÚJ)

- Feladat: az Apps Script webapp által szolgáltatott egységes JSON feed (`?type=analyst`, `?type=catalyst`) feldolgozása.
- Környezeti változók:
  - `ANALYST_FEED_URL`   – analyst feed endpoint (JSON)
  - `CATALYST_FEED_URL`  – catalyst feed endpoint (JSON)
- Kimenetek:
  - `reports/analyst_1.json`   – elemzői események (#1/#2/#3 „Bejelentések & fel/lemínősítések” blokkhoz)
  - `reports/catalysts_1.json` – katalizátorok (#1/#2/#3 „Közelgő katalizátorok” blokkhoz)
- Tickerenként csoportosít, és *forrás-prioritást* rendel minden eseményhez.
- A legjobb forrásból származó eseményt jelöli `is_primary = true` flaggel, amelyet a
  `analyst_block_builder` / `highconv_block_builder` használhat a 1 mondatos indokhoz.

#### Elemzői feed – forrás-prioritás (#1/#2/#3)

Ha ugyanarra a tickerre / eseményre több hírforrás is ad információt, az 1 mondatos
magyarázat és a „fő” event kiválasztásánál a sorrend *mindig* ez:

1. **Yahoo Finance**
2. **Bloomberg**
3. **MarketBeat**
4. **Reuters / AP / hivatalos IR** (Investor Relations)
5. Egyéb források (csak ha a fenti négy közül egyik sincs)

A priorizálás technikailag a `source` mező best-effort normalizálásával és egy
`source_rank` mezővel valósul meg (`1` a legjobb, `9` az ismeretlen).

### scripts/analyst_block_builder.py

- `reports/1/analyst_1.json` → „Bejelentések & fel/lemínősítések” markdown blokk építése
- ticker / dátum / PT / rating / megjegyzés mezők összefésülése egy-egy listaponttá
- csak formáz, nem számol újra; a nyers adatot az Apps Script feed + analyst_feed_parser.py adja

### scripts/highconv_block_builder.py

- `reports/1/high_conv_1.json` és `reports/1/catalysts_1.json` → két blokk:
    - „Közelgő katalizátorok”
    - „Listán kívüli, 3–12 hónapos high-conv jelöltek”
- a highconv_builder.py által számolt pontszámot és leírást formázza markdownná

### scripts/postprocess_report.py

- bemenet: `reports/1/summary_report_1.md` + `reports/1/macro_news_1.json` + `analyst_1.json` + `catalysts_1.json` + `high_conv_1.json`
- lefut a #1 report_runner.py után
- lépések:
    1. „Politika / FED / Makró” blokk beszúrása a „Lefedettség:” sor után
    2. „Job summary generated at run-time …” sor(ok) eltávolítása
    3. analista blokk + katalizátor blokk + high-conv blokk hozzáfűzése a jelentés végére
- ez adja a gist-re kikerülő, végleges #1-es markdown jelentést
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
5.  analyst_feed_parser → analyst_1.json / catalysts_1.json
6.  postprocess_report + analyst_block_builder + highconv_block_builder → summary_report_1.md véglegesítése

## 5. Fallback logika

-   Yahoo elsődleges, Google / Investing second/third fallback
-   ha ár nem elérhető: hír akkor is mehet (jelölve)

## 6. High-conv kritériumok

Legalább 2 teljesüljön: - elemzői felminősítések - guide emelés -
konszenzus-felhúzás - 3--12 hónapos katalizátor - relatív erő / 52w high
közeli


---

## 7. End-to-end folyamat áttekintése (példa)

### 7.1 Apps Script → Python pipeline (konkrét adatút)

1) **Apps Script webapp**
   - Endpoint: `...?type=analyst` és `...?type=catalyst`
   - Összegyűjti a híreket Yahoo Finance / Bloomberg / MarketBeat / Reuters / AP / IR forrásokból.
   - Normalizált JSON-t ad vissza.

2) **analyst_feed_parser.py**
   - Letölti az Apps Script JSON-t.
   - Ticker szerint csoportosít.
   - Meghatározza a `source_rank` értéket (1–9).
   - Kijelöli a `is_primary = true` eseményt.
   - Kimenetek:
     * `reports/analyst_1.json`
     * `reports/catalysts_1.json`

3) **report_runner.py (mode=1)**
   - Lekéri az árakat (AH/PM ablak).
   - Felépíti a #1 riport törzsét (Lefedettség, tickerlisták, mozgások).

4) **postprocess_report.py**
   - Makró-blokk beillesztése.
   - Analyst blokkok → végére.
   - Katalizátor blokkok → végére.
   - High-conv blokkok → végére.
   - Tisztítja a „Job summary generated…” sort.

### 7.2 Példa: NVDA esemény átfolyása

- Apps Script több hírforrást talál:
  - Yahoo: „NVDA shares rise after AI server demand beats expectations”
  - Bloomberg: „Nvidia climbs on improving hyperscale demand”
  - MarketBeat: „Analyst raises price target to $150”
  - Reuters: „Nvidia says it expects strong Q4 revenue”

- analyst_feed_parser:
  - Yahoo → `source_rank = 1`
  - Bloomberg → `source_rank = 2`
  - MarketBeat → `source_rank = 3`
  - Reuters → `source_rank = 4`
  - Első esemény lesz a primary (Yahoo)

- analyst_1.json:
  - NVDA elemnél:
    * első entry: `is_primary = true`
    * többi: `is_primary = false`

- analyst_block_builder:
  - A primary eventből írja a fő sort.
  - Ha több event van, beteszi alálőve.

- Kész riportban:
  ```
  ### Bejelentések & fel/lemínősítések
  - NVDA
    - 2025-11-18 – Yahoo Finance – Upgrade – rating: EW → OW – PT: 130 → 150 USD
  ```

---

## 8. JSON minimál layout követelmények

A feed-parser és a bloképítők toleránsak, de a minimum elvárások:

### 8.1 analyst feed JSON

Bármelyik szerkezet elfogadott:

```json
[
  {
    "ticker": "NVDA",
    "source": "Yahoo Finance",
    "headline": "...",
    "summary": "...",
    "datetime": "2025-11-18T13:52:00Z"
  }
]
```

Vagy:

```json
{
  "items": [
    { ... }
  ]
}
```

### 8.2 catalyst feed JSON

```json
[
  {
    "ticker": "NVDA",
    "event_date": "2026-02-15",
    "type": "earnings",
    "description": "Q4 FY25 report"
  }
]
```

### 8.3 analyst_1.json (feed-parser után)

Már tartalmaz:

- `source_rank`
- `source_normalized`
- `datetime_norm`
- `is_primary`

### 8.4 Katalizátor és high-conv JSON-ok

- `catalysts_1.json` listát tartalmaz.
- `high_conv_1.json` listát tartalmaz (Apps Script tölti).

---

## 9. Modulok szerepe rövid táblázatban

| Modul | Feladat | Bemenet | Kimenet |
|-------|---------|---------|---------|
| report_runner.py | #1/#2/#3 alapjelentés | Yahoo árak, tickerlista | raw summary_report_X.md |
| macro_fetcher.py | makróhírek | Reuters/Yahoo API | macro_news_1.json |
| analyst_feed_parser.py | elemzői/katalizátor feed tisztítása, priorizálása | Apps Script JSON | analyst_1.json, catalysts_1.json |
| analyst_block_builder.py | analyst blokk | analyst_1.json | markdown blokk |
| highconv_block_builder.py | katalizátor + high-conv blokkok | catalysts_1.json, high_conv_1.json | markdown blokkok |
| postprocess_report.py | végső #1/#2/#3 felépítés | summary_report_1.md + blokkok | kész jelentés |
| biblia_helper.py | formázási utilok | n/a | belső használat |
| macro_highconv_helpers_v2.py | high-conv és makró segédlogika | JSON feedek | szűrt adatok |

---

