# Részvények Projekt -- BIBLIA LEÍRÁS (MD)

## Fájlszerkezet, szerepek és riportlogika

1. Könyvtárszerkezet

repo/
├─ scripts/
│   ├─ report_runner.py
│   ├─ biblia_helper.py
│   ├─ macro_highconv_helpers_v2.py
│   ├─ highconv_builder.py
│   ├─ macro_fetcher.py
│   ├─ analyst_feed_parser.py
│   ├─ analyst_block_builder.py       # ÚJ – analyst_1.json → „Bejelentések & fel/lemínősítések”
│   ├─ highconv_block_builder.py      # ÚJ – high_conv_1.json / catalysts_1.json → blokkok
│   └─ postprocess_report.py          # ÚJ – makró + analyst + catalyst + high-conv összefűzés
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

## Analyst / high-conv blokkok – Python oldali modulok (2025-12-04)

Ezek a modulok a #1-es jelentés végső markdown-ját egészítik ki. Az Apps Script csak a JSON feedeket adja, minden formázás Pythonban történik:

- `analyst_block_builder.py`  
  - Bemenet: `reports/1/analyst_1.json` (Apps Script AnalystFeed alapján).  
  - Kimenet: „### Bejelentések & fel/lemínősítések” blokk (#1 jelentés vége).

- `highconv_block_builder.py`  
  - Bemenet: `reports/1/high_conv_1.json` és `reports/1/catalysts_1.json`.  
  - Kimenet:  
    - „### Közelgő katalizátorok” blokk,  
    - „### Listán kívüli, 3–12 hónapos high-conv jelöltek” blokk.

- `postprocess_report.py`  
  - Bemenet: a nyers `reports/summary_report_1.md` + a fenti JSON fájlok (`macro_news_1.json`, `analyst_1.json`, `catalysts_1.json`, `high_conv_1.json`).  
  - Lépések:  
    1. Makró blokk beszúrása a „Lefedettség:” sor után (Politika / FED / Makró).  
    2. „Job summary generated at run-time …” sor(ok) eltávolítása.  
    3. Analyst + katalizátor + high-conv blokkok hozzáfűzése a jelentés végére, biblia szerinti sorrendben.
