# Részvények Projekt -- BIBLIA LEÍRÁS (MD)
BIBLIA VERZIÓ: v3.5.0


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


### scripts/analyst_block_builder.py

- `reports/1/analyst_1.json` → „Bejelentések & fel/lemínősítések” markdown blokk építése
- ticker / dátum / PT / rating / megjegyzés mezők összefésülése egy-egy listaponttá
- csak formáz, nem számol újra; a nyers adatot az Apps Script feed + analyst_feed_parser.py adja

### scripts/analyst_feed_parser.py – elemzői/catalyst feed (ÚJ)

- Feladat: az Apps Script webapp által szolgáltatott egységes JSON feed (`?type=analyst`, `?type=catalyst`) feldolgozása.
- Környezeti változók (GitHub Secrets → env):
  - `ANALYST_FEED_URL_1/2/3`   – #1/#2/#3 analyst feed endpoint
  - `CATALYST_FEED_URL_1/2/3`  – #1/#2/#3 catalyst feed endpoint
- Kimenetek:
  - `reports/1/analyst_1.json`, `reports/2/analyst_2.json`, `reports/3/analyst_3.json`
  - `reports/1/catalysts_1.json`, `reports/2/catalysts_2.json`, `reports/3/catalysts_3.json`
- Tickerenként csoportosít, és *forrás-prioritást* rendel minden eseményhez.
- A legjobb forrásból származó eseményt jelöli `is_primary = true` flaggel, amelyet a
  `analyst_block_builder.py` / `highconv_block_builder.py` használ az 1 mondatos okhoz.

#### Elemzői hírek forrás-prioritása (#1/#2/#3)

Azonos tickerhez / eseményhez tartozó több forrás esetén mindig az alábbi sorrend dönt:

1. **Yahoo Finance**
2. **Bloomberg**
3. **MarketBeat**
4. **Reuters / AP / hivatalos IR**
5. Egyéb források (csak ha a fenti négy között nincs találat)

A priorizálás technikailag a `source` mező normalizálásával és egy `source_rank` mezővel
valósul meg (`1` a legjobb, `9` az ismeretlen), és az `is_primary` jelölés kerül fel a
legjobb (prioritás + időbélyeg) eseményre.

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

## 7. End-to-end folyamat áttekintése (példával)

### 7.1 Apps Script → Python pipeline

1. **Apps Script webapp**
   - Endpoint: `...?type=analyst` és `...?type=catalyst`
   - Összegyűjti a híreket Yahoo Finance / Bloomberg / MarketBeat / Reuters / AP / IR forrásokból.
   - Normalizált JSON-t ad vissza (#1/#2/#3 ablakokra bontva).

2. **scripts/analyst_feed_parser.py**
   - Letölti az Apps Script JSON-t.
   - Tickerenként csoportosítja az eseményeket.
   - Forrás-prioritást számol (`source_rank`), kijelöli az `is_primary = true` eventet.
   - Kiírja az egységesített feedet:
     - `reports/{report}/analyst_{report}.json`
     - `reports/{report}/catalysts_{report}.json`

3. **scripts/report_runner.py**
   - Megcsinálja a #1 / #2 / #3 alap riportot (árak, lefedettség, mozgások).
   - Kimenet: `reports/{report}/summary_report_{report}.md` (nyers).

4. **scripts/macro_fetcher.py + highconv_builder.py**
   - Makró-hírek, high-conv pontszámok, katalizátorok JSON-okba (`macro_news_*.json`, `high_conv_*.json`, `catalysts_*.json`).

5. **scripts/postprocess_report.py**
   - Beolvassa a nyers markdown-t + a fenti JSON-okat.
   - Beilleszti a makró blokkot, az analyst blokkot, a katalizátor blokkot és a high-conv blokkot.
   - Törli a „Job summary generated at run-time…” sort.
   - Kimenet: végleges, biblia szerinti `summary_report_{report}.md`.

6. **GitHub Actions – gist PATCH**
   - A `run_report.yml` a végleges markdown-t feltölti a fix gist ID-re (#1/#2/#3),
   - a step summary-ben kiírja a raw URL-t (amit te nézel).

### 7.2 Példa: NVDA esemény útja

- Apps Script több forrást talál NVDA-ra (Yahoo, Bloomberg, MarketBeat, Reuters).
- `analyst_feed_parser.py`:
  - Yahoo → `source_rank = 1`
  - Bloomberg → `source_rank = 2`
  - MarketBeat → `source_rank = 3`
  - Reuters → `source_rank = 4`
  - A legjobb event kapja az `is_primary = true` jelölést.
- `analyst_1.json`-ban az NVDA-hoz tartozó első (primary) eventből lesz a fő sor
  a „Bejelentések & fel/lemínősítések” blokkban, a többi csak kiegészítő információ.

---

## 8. JSON minimál layout követelmények

A feed-parser és a bloképítők *toleránsak*, de az alábbi minimumokat várják.

### 8.1 analyst feed JSON (Apps Script output)

Elfogadott példák:

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

vagy

```json
{
  "items": [
    {
      "ticker": "NVDA",
      "source": "Yahoo Finance",
      "headline": "..."
    }
  ]
}
```

### 8.2 catalyst feed JSON (Apps Script output)

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

### 8.3 analyst_{report}.json (feed-parser után)

A parser már hozzáadja az alábbi mezőket (a meglévők mellé):

- `source_rank` (1–9)
- `source_normalized`
- `datetime_norm`
- `is_primary` (true/false)
- `feed_type` (analyst/catalyst)

### 8.4 catalysts_{report}.json és high_conv_{report}.json

- `catalysts_{report}.json` egyszerű lista az eseményekről (ticker, dátum, típus, leírás).
- `high_conv_{report}.json` a high-conv pontszámokat és rövid indokokat tartalmazza.
- A builderek (highconv_block_builder) csak néhány kulcsra támaszkodnak; hiányzó mezők
  esetén a blokk egyszerűen rövidebb lesz.

---

## 9. Modulok szerepe – gyors áttekintő táblázat

| Modul                         | Feladat                                                     | Bemenet                               | Kimenet                                      |
|------------------------------|-------------------------------------------------------------|---------------------------------------|----------------------------------------------|
| `scripts/report_runner.py`   | #1/#2/#3 alap riport (árak, lefedettség, mozgások)          | Yahoo árak, tickerlista               | `summary_report_{report}.md` (nyers)         |
| `scripts/macro_fetcher.py`   | Makró / FED / politika hírek lekérése                       | Reuters / Yahoo / egyéb makró forrás | `macro_news_{report}.json`                   |
| `scripts/highconv_builder.py`| High-conv pontszámok, katalizátor-ok számítása              | Apps Script / külső feedek            | `high_conv_{report}.json`, `catalysts_*.json`|
| `scripts/analyst_feed_parser.py` | Elemzői / catalyst feed tisztítása, priorizálása      | Apps Script JSON feed                 | `analyst_{report}.json`, `catalysts_{report}.json` |
| `scripts/analyst_block_builder.py` | Bejelentések & fel/lemínősítések blokk építése      | `analyst_{report}.json`               | markdown blokk (#1/#2/#3 végére)            |
| `scripts/highconv_block_builder.py` | Katalizátor + high-conv blokkok építése            | `high_conv_{report}.json`, `catalysts_{report}.json` | 2 markdown blokk (#1/#2/#3 végére) |
| `scripts/postprocess_report.py` | Kész #1/#2/#3 jelentés összeállítása, takarítás        | nyers markdown + JSON-ok              | végleges `summary_report_{report}.md`        |
| `scripts/biblia_helper.py`   | Formázási / közös segédfüggvények                          | –                                     | csak belső használat                         |
| `scripts/macro_highconv_helpers_v2.py` | Makró + high-conv segédfüggvények              | –                                     | csak belső használat                         |

