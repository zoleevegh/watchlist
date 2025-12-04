# -*- coding: utf-8 -*-
"""
Exportálja a biblia helperben tárolt MD-specet a docs/biblia_leiras.md fájlba.

Ha frissíted a BIBLIA_SPEC_MD szöveget ebben a fájlban, futtasd:
    python scripts/export_biblia_md.py
és a docs/biblia_leiras.md automatikusan frissül.
"""

from pathlib import Path

BIBLIA_SPEC_MD = r"""# Részvények Projekt -- BIBLIA LEÍRÁS (MD)

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
"""

def main() -> None:
    docs_dir = Path("docs")
    docs_dir.mkdir(exist_ok=True)
    out_path = docs_dir / "biblia_leiras.md"
    out_path.write_text(BIBLIA_SPEC_MD, encoding="utf-8")
    print(f"biblia_leiras.md frissítve: {out_path}")

if __name__ == "__main__":
    main()
