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

`report = 1` → #1 AH/PM riport  
`report = 2` → #2 Open→Close riport  
`report = 3` → #3 Intraday Open→Most riport  

A workflow a megfelelő JSON-eket és MD fájlokat generálja, majd a summary-t feltölti a Gist-re.

---

## A három riport rövid összefoglalója

### #1 – After-hours & Premarket (AH/PM)  

Időablakok (CE(S)T):  
• AH = 22:00–02:00  
• PM = 10:00–15:30  

Blokkok:  
• Lefedettség  
• Makró / Politika / FED / Trump-napihír  
• Darabszámos tickerek AH/PM mozgásai  
• Watchlist (≥3% AH/PM)  
• Elemzői lépések + katalizátorok (analyst_1.json / catalysts_1.json)  
• High-conviction jelöltek (csak listán kívüli tickerek, high_conv_1.json alapján)

---

### #2 – Előző napi Open→Close riport  

• előző kereskedési nap teljes intraday elemzése (Open→Close)  
• ±3% mozgások kiemelve  
• nap közbeni vállalati hírek, bejelentések  
• elemzői lépések, szektorhatások  
• makróblokkok (macro_news_2.json)  
• analyst & catalyst blokkok (analyst_2.json, catalysts_2.json)  
• high-conv blokkok ugyanabból a high_conv_1.json-ből  

A #2-es riport végső összefűzését a `postprocess_report_2.py` wrapper végzi, a fő `postprocess_report.py` motorra támaszkodva.

---

### #3 – Intraday Open→Most riport  

Blokkok:  
• Lefedettség  
• Darabszámos intraday mozgások (Open→Most)  
• Watchlist intraday mozgások  
• Events & News (üzleti közlések, gyorsjelentések, SEC hírek)  
• High-conv (#3 specifikus logika)  

A #3 futást a következő modulok valósítják meg:  
• `blocks_events_3.py`  
• `blocks_intraday_3.py`  
• `postprocess_report_3.py`  

---

## A rendszer folyamata (workflow)

GitHub Action: `.github/workflows/run_report.yml`  

1. `report_runner.py` (#1/#2/#3 futás – árak, AH/PM, Open→Close, Intraday)  
2. `macro_fetcher.py` (macro_news_X.json)  
3. `analyst_feed_parser.py` (analyst_X.json, catalysts_X.json)  
4. `highconv_builder.py` (high_conv_1.json)  
5. `postprocess_report.py` / `postprocess_report_2.py` / `postprocess_report_3.py`  
6. `summary_report_X.md`  
7. Gist frissítés (#1/#2/#3 fix linkek)  

---

## Könyvtárszerkezet (egyszerűsített)

```text
repo/
- README.md  ← ez a dokumentum
- 00_README_START_HERE.md
- .github/workflows/
    - run_report.yml
    - update_biblia_docs.yml
- docs/
    - CHANGELOG.md
    - biblia_leiras.md (a teljes módszertan)
- reports/
    - 1/ (AH/PM JSON + summary_report_1.md)
    - 2/ (Open→Close JSON + summary_report_2.md)
    - 3/ (Intraday JSON + summary_report_3.md)
- scripts/
    - report_runner.py
    - postprocess_report.py
    - postprocess_report_2.py
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
