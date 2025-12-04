# Részvények Projekt – BIBLIA LEÍRÁS v3.0.0 (2025-12-04)

## 1. Könyvtárszerkezet

repo/
├─ scripts/
│   ├─ report_runner.py              # fő futtató
│   ├─ postprocess_report.py         # #1 jelentés makró/analyst/katalizátor/high‑conv beépítése
│   ├─ macro_fetcher.py              # makróhírek lekérése (Apps Script webapp)
│   ├─ highconv_builder.py           # high‑conv JSON generátor
│   ├─ biblia_helper.py              # formázások, blokkok ellenőrzése
│   └─ analyst_feed_parser.py        # elemzői feed feldolgozása
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

### scripts/macro_fetcher.py
- A makró szöveg lekérése az **Apps Script WebApp** → `MACRO_FEED_URL_1/2/3`
- A raw makró szöveg kiírása: `reports/macro_1.txt`
- Bemenet: report szám
- Kimenet: makró szöveg (string)

### scripts/postprocess_report.py
- A runner után fut (csak #1 esetén)
- Beszúrja:
  - Politika / FED / Makró blokk
  - Bejelentések & fel/lemínősítések blokk
  - Közelgő katalizátorok blokk
  - High‑conv blokk
- Forrás:  
  - `macro_news_1.json`  
  - `analyst_1.json`  
  - `catalysts_1.json`  
  - `high_conv_1.json`
- Eltávolítja: „Job summary generated at run‑time …” sort

### scripts/highconv_builder.py
- analyst feed → `high_conv_1.json`
- watchlist és portfólió kizárva
- 5 biblia‑kritérium alapján pontozás

### scripts/report_runner.py
- #1/#2/#3 riport futtatás
- #1 esetén makró text lekérése → makró blokk
- Hívja: postprocess_report.py a végleges #1 reporthoz


## 3. #1 jelentés teljes logikája (AFTER‑HOURS + PREMARKET)

1. Lefedettség blokk  
   - TELJES vagy HIÁNYOS

2. **Makró / FED / Piaci hangulat**  
   - Reuters → AP → Bloomberg elsődleges  
   - 3–5 sor headline + snippet + source + timestamp  
   - forrás: macro_fetcher (Apps Script) + postprocess JSON integráció

3. **Darabszámos tickerek**  
   - mindig megjelenik  
   - AH (% + ok)  
   - PM (% + ok)  
   - várható nyitási hatás  

4. **Watchlist tickerek (feltételes)**  
   - csak ha hír vagy ≥ ±3% AH/PM

5. **Bejelentések & fel/lemínősítések blokk**  
   - MarketBeat → StreetInsider/TheFly → TipRanks  
   - minden darabszámos ticker  
   - watchlist: csak ha releváns

6. **Közelgő katalizátorok blokk**  
   - earnings, product launch, guidance események  
   - 3–12 napos ablak

7. **High‑conv blokk (listán kívüli)**  
   - Yahoo Finance + MarketBeat  
   - legalább 2 kritérium teljesül  
   - szigorúan NEM lehet portfólió/watchlist ticker

8. Jelentés vége:  
   - nincs „job summary”  
   - végleges formátum: summary_report_1.md + latest_1.md


## 4. Adatfolyam

1. Yahoo → ármozgások
2. Apps Script → analyst feed (`analyst_1.json`)
3. Apps Script makró feed → macro_fetcher → raw makró text
4. macro_news JSON → postprocess_report → makró blokk
5. highconv_builder → `high_conv_1.json`
6. report_runner + postprocess → végleges jelentés

## 5. Hírforrás-prioritás

### Makró:
1. Reuters (TOP)
2. AP
3. Bloomberg
4. Dow Jones Newswire
5. Benzinga Pro / The Fly

### Elemzői:
1. MarketBeat Ratings
2. StreetInsider / TheFly Analyst
3. TipRanks

### Hivatalos cég:
- SEC (8‑K, 6‑K, 10‑Q, 10‑K)
- IR newsroom
- PR Newswire / GlobeNewswire

## 6. High‑conv kritériumok (legalább 2 teljesül)

- 2–3 nagyházas felminősítés / PT emelés  
- pozitív guide / előrejelzés  
- konszenzus EPS/Revenue felfelé módosul  
- 3–12 hónapos katalizátor  
- relatív erő, 52w csúcs közeli árfolyam
