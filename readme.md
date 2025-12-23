
### Makró / FED / Politika – A-mód (kanonikus)

A makró blokk konzervatív (A-mód) szerint működik:
- nem ad piaci iránymutatást,
- nem értelmez kamatpályát,
- kizárólag jelzi, hogy érkezett-e a piac egészét érintő makró/FED/politikai esemény.

Ha nincs érdemi hír, kötelezően az alábbi mondat jelenik meg:
„Az előző piaczárás óta nem érkezett a piac egészét érdemben befolyásoló makró, Fed vagy politikai hír.”


# Részvények – #1/#2/#3 riport pipeline (rövid)

Igazságforrások:
- **Szabály (kanonikus):** `docs/biblia_leiras.md`
- **Technikai howto:** `biblia_helper.py`

Artifact-szabály (kötelező): minden riport **külön** JSON-okkal dolgozik.
- #1 → `high_conv_1.json`
- #2 → `high_conv_2.json`
- #3 → `high_conv_3.json`

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
- .github/workflows/
    - run_report.yml
- docs/
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
    - macro_highconv_helpers_v2.py
    - export_biblia_md.py
    - biblia_helper.py
    - validate_run.py

---

## ⚠️ #1 JELENTÉS – AUTOMATIKUS WEBES HÍREK (KANONIKUS)

A felhasználónak **NEM kell külön jeleznie**, hogy „hírekkel”.

👉 **„#1 jelentés” = AUTOMATIKUSAN webes hírekkel készül.**

**Időkapu (szigorú):**
- After-hours (AH): 22:00–02:00 CEST
- Premarket (PM): 10:00–15:30 CEST

A #1 jelentésben szereplő hírek és indoklások **kizárólag** ebből az időkapuból származhatnak.
Más idősávból származó információ nem kerülhet be.

---

## ✅ ÚJ: Futás-ellenőrzés (Validate guard)

A workflow mostantól tartalmaz egy kötelező ellenőrző lépést, ami **PIROSRA** teszi a futást,
ha a jelentés „látszólag” elkészült, de valójában **hibás / hiányos / összelapított**.

### Fájl
- `scripts/validate_run.py`

### Mit ellenőriz?
- Létezik és nem üres: `reports/summary_report_{1|2|3}.md`
- #1-nél kötelező tokenek/blokkok megvannak (pl. Makró/FED, Bejelentések, Katalizátorok, High-conv)
- Megvan a `Lefedettség:` sor
- Megvan a `Job summary generated at run-time`
- „Lapítás” detektálás: minimum sorszám (görbe egy-soros output kiszűrése)

### Workflow sorrend
`Postprocess` → **Validate** → `Update fixed Gist`

---

## 📁 Könyvtárszerkezet – kiegészítés

Új fájl a guard ellenőrzéshez:

```
scripts/
  validate_run.py        # CI guard: a futás csak akkor zöld, ha a report szerkezetileg valid
```



⚠️ #1 JELENTÉS – IDŐABLAK PONTOSÍTÁS (KANONIKUS)

- AH/PM idősáv (22:00–02:00, 10:00–15:30) kizárólag az ÁRFOLYAMMOZGÁSOKRA vonatkozik.
- Hírek, elemzői lépések, makró, katalizátorok időablaka:
  → előző napi piaczárás (22:00 CEST)
  → a lekérdezés pillanatáig
  → de legkésőbb az aktuális napi nyitásig.

Ez biztosítja, hogy a nyitás előtti releváns információk ne essenek ki.


scripts/
├── analyst_feed_parser.py
├── analyst_block_builder.py
├── analyst_catalyst_builder.py   # NEW – MarketBeat / MarketWatch analyst & catalyst parser (jina-md)
├── highconv_block_builder.py
├── highconv_builder.py

## Validate guard – pontosítás

A validate guard célja nem az, hogy „üres napokon” megállítsa a futást,
hanem hogy **szerkezetileg hibás riport ne kerülhessen publikálásra**.

A makró, analyst, katalizátor és high‑conv blokkok
**strukturálisan kötelezőek**, de üres adat esetén
a postprocess lépés placeholder blokkokat szúr be.

A workflow csak akkor áll PIROSRA, ha:
- a riport szerkezetileg sérült,
- kötelező blokk‑header hiányzik,
- vagy a jelentés nem felel meg a BIBLIA blokk‑sorrendjének.



---
README frissítve: v1.3.0 – 2025-12-23
