# Részvényjelentés Automata – BIBLIA leírás  
**Verzió:** v3.6.2

Ez a dokumentum írja le a teljes #1 / #2 / #3 riport‑pipeline architektúráját, a fájlszerkezetet, a fő scripteket és a BIBLIA szerinti logikát.

**KÖTELEZŐ SZABÁLY:** bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

---

## 1. Könyvtárszerkezet
```text
repo/
├─ README.md
├─ .github/
│  └─ workflows/
│     ├─ run_report.yml
│     └─ update_biblia_docs.yml
├─ docs/
│  └─ biblia_leiras.md
├─ reports/
│  ├─ 1/
│  │  ├─ analyst_1.json
│  │  ├─ catalysts_1.json
│  │  ├─ macro_news_1.json
│  │  ├─ high_conv_1.json
│  │  └─ summary_report_1.md
│  ├─ 2/
│  └─ 3/
└─ scripts/
   ├─ report_runner.py
   ├─ postprocess_report.py
   ├─ macro_fetcher.py
   ├─ analyst_block_builder.py
   ├─ analyst_feed_parser.py
   ├─ analyst_catalyst_builder.py
   ├─ highconv_block_builder.py
   └─ highconv_builder.py
```

---

## 2. Makró/FED feed (Apps Script webapp)
**Miért külön:** A gyors makró/FED hírekhez egy könnyű webapp feedet használunk.

**Endpoint:**
- `?type=macro&report=1|2|3` → makró hírek
- `?type=health` → forrás‑szintű státusz + darabszám

**Források:**
- Federal Reserve – RSS feedek (Press Releases / Speeches / FOMC). citeturn0search0

**Megjegyzés:**
- MarketBeat/MarketWatch/247 jellegű scrape **nem** fut Apps Scriptből, mert 401/403 blokkolás jellemző.

---

## 3. Analyst/Catalyst feed (Python builder)
**Miért:** Az Apps Scriptből jövő scrape a legtöbb “ratings/targets/initiations” oldalon blokkolódik.

**Felelős script:** `scripts/analyst_catalyst_builder.py`  
- lekéri és összevonja az analyst és catalyst eseményeket,
- kimenet: `reports/{N}/analyst_{N}.json` + `reports/{N}/catalysts_{N}.json`
- a `postprocess_report.py` ebből generálja a riport “Bejelentések & fel/lemínősítések” és “Közeli katalizátorok” blokkjait.

---

## 4. Ellenőrzés (coverage / health)
- A riportok ELEJÉN “Lefedettség: TELJES/HIÁNYOS” kötelező.
- A feedeknél külön `health_*.json` készülhet, amelyben forrásonként látszik:
  - `ok`, `count`, `httpStatus`, `error`, `ms`.
