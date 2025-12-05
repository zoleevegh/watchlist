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


### scripts/analyst_block_builder.py

- `reports/1/analyst_1.json` → „Bejelentések & fel/lemínősítések” markdown blokk építése
- események ticker szerint csoportosítva; ticker alatt időrendben (legfrissebb elöl) több sor is lehet
- ticker / dátum / PT / rating / megjegyzés mezők összefésülése egy-egy jól olvasható listaponttá
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
