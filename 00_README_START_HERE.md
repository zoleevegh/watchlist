# Részvényjelentés Automata  
### #1 / #2 / #3 Market Report Engine

Automatizált After-hours, Premarket, Open→Close és Intraday jelentésgenerálás  
Yahoo Finance, MarketBeat, Reuters és SEC alapokon.

A rendszer célja:  
• a teljes amerikai részvénypiac figyelése  
• automatikus napi riportok készítése  
• három külön futási móddal (#1/#2/#3)  
• GitHub Actions alapú automatizálás  
• a BIBLIA szabályrendszerének követése  
• a jelentések GIST-re publikálása  

---

## Hogyan működik a rendszer?

A futások a GitHub Actions-ből indíthatók és három mód közül választhatsz:

1 = #1 AH/PM riport  
2 = #2 Open→Close riport  
3 = #3 Intraday Open→Most riport  

A workflow a megfelelő JSON-eket és MD fájlokat generálja, majd a summary-t feltölti a gist-re.

---

## A három riport rövid összefoglalója

#1 – After-hours & Premarket (AH/PM)  
Időablakok:  
• AH = 22:00–02:00  
• PM = 10:00–15:30  

Blokkok:  
• Lefedettség  
• Makró / Politika / FED / Trump-napihír  
• Darabszámos tickerek AH/PM mozgásai  
• Watchlist (≥3% AH/PM)  
• Elemzői lépések  
• Közeli katalizátorok  
• High-conviction jelöltek (csak listán kívüliek)

---

#2 – Előző napi Open→Close riport  
• előző kereskedési nap teljes intraday elemzése  
• ≥3% mozgások  
• vállalati hírek, bejelentések  
• elemzői lépések  
• szektorhatások  

---

#3 – Intraday Open→Most riport  
Blokkok:  
• Lefedettség  
• Darabszámos intraday mozgások  
• Watchlist intraday mozgások  
• Events & News (üzleti közlések, gyorsjelentések, SEC hírek)  
• High-conv (#3 specifikus logika)  

A #3 futást a következő modulok valósítják meg:  
• blocks_events_3.py  
• blocks_intraday_3.py  
• postprocess_report_3.py  

---

## A rendszer folyamata (workflow)

GitHub Action (run_report.yml)  
   ↓  
report_runner.py (1 / 2 / 3 futás)  
   ↓ JSON fájlok  
postprocess_report / postprocess_report_3  
   ↓  
summary_report_X.md  
   ↓  
Gist-re tölti az Action  

---

## Könyvtárszerkezet (egyszerűsített)

repo/
- README.md  ← ez a dokumentum
- .github/workflows/
    - run_report.yml
    - update_biblia_docs.yml
- docs/
    - CHANGELOG.md
    - biblia_leiras.md (a teljes módszertan)
- reports/
    - 1/ (AH/PM JSON + summary)
    - 3/ (Intraday JSON + summary)
- scripts/
    - report_runner.py
    - postprocess_report.py
    - postprocess_report_3.py
    - blocks_events_3.py
    - blocks_intraday_3.py
    - analyst_feed_parser.py
    - analyst_block_builder.py
    - highconv_builder.py
    - highconv_block_builder.py
    - macro_fetcher.py
    - macro_highconv_helpers_v2.py
    - export_biblia_md.py
    - biblia_helper.py

---

## Fallback: hibás #1 futás esetén

Ha a #1 riport nem generálódik le a GitHub Action-ben,  
kérhetsz **BIBLIA-kompatibilis riportot közvetlenül ChatGPT-től**:

"Kérek egy teljes #1 bibliás jelentést erre a gistre: <RAW gist link>"

ChatGPT ez alapján legenerálja a teljes #1 riportot.

---

## Dokumentációk

BIBLIA (összes szabály): docs/biblia_leiras.md  
CHANGELOG: docs/CHANGELOG.md  
Minden további részlet: scripts/ modulok  

---

Minden készen áll.  
Irány a piac.
