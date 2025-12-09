1. Könyvtárszerkezet

```text
repo/
├─ 00_README_START_HERE.md
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
│   │   ├─ report.json
│   │   └─ report_summary.md
│   └─ 3/
│       ├─ summary_report_3.md
│       ├─ analyst_1.json
│       ├─ catalysts_1.json
│       ├─ latest_1.json
│       ├─ latest_1.md
│       ├─ latest_3.json
│       ├─ latest_3.md
│       ├─ raw_analyst_1.json
│       └─ raw_catalysts_1.json
├─ scripts/
│   ├─ analyst_block_builder.py
│   ├─ analyst_feed_parser.py
│   ├─ biblia_helper.py
│   ├─ blocks_events_3.py
│   ├─ blocks_intraday_3.py
│   ├─ crawler_analyst_catalyst.py
│   ├─ events_fetcher.py
│   ├─ export_biblia_md.py
│   ├─ highconv_block_builder.py
│   ├─ highconv_builder.py
│   ├─ macro_fetcher.py
│   ├─ macro_highconv_helpers_v2.py
│   ├─ macro_news_fetcher.py
│   ├─ postprocess_report.py
│   ├─ postprocess_report_3.py
│   ├─ report_runner.py
│   └─ report_runner_Yahoo.py   (archív, nem használt)

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


# Fallback logika – ChatGPT által generált #1 BIBLIA jelentés

A rendszerben hivatalosan támogatott, hogy ha a #1-es jelentést a pipeline nem hozza létre
BIBLIA szerint, akkor ChatGPT a Gist RAW tartalmából generálja a teljes formátumot.
