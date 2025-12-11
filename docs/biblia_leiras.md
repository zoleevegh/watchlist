# 1. Könyvtárszerkezet

```text
repo/
├─ 00_README_START_HERE.md
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
│   │   ├─ macro_news_1.json
│   │   ├─ high_conv_1.json
│   │   └─ summary_report_1.md
│   ├─ 2/
│   │   ├─ analyst_2.json
│   │   ├─ catalysts_2.json
│   │   ├─ macro_news_2.json
│   │   └─ summary_report_2.md
│   └─ 3/
│       ├─ summary_report_3.md
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
│   ├─ export_biblia_md.py
│   ├─ highconv_block_builder.py
│   ├─ highconv_builder.py
│   ├─ macro_fetcher.py
│   ├─ macro_highconv_helpers_v2.py
│   ├─ postprocess_report.py
│   ├─ postprocess_report_2.py
│   ├─ postprocess_report_3.py
│   └─ report_runner.py
```

2. Scripts – szerepek
scripts/biblia_helper.py

Segédfüggvények:

    #1/#2/#3 riport logika ellenőrző listái

    makró és hírblokkok formázása

    high-conv kritérium-értékelés

    ticker-szűrés a MASTER alapján

scripts/highconv_builder.py

    analyst feed → high-conv JSON generálás

    kizárja a portfólió + watchlist tickereket

    5 biblia-kritérium alapján pontoz

scripts/macro_highconv_helpers_v2.py

    makró blokk és high-conv blokk beszúrása a summary_report_1.md-be

    latest_1.md / latest_3.md frissítése

scripts/report_runner.py

    #1/#2/#3 futtatások logikája

    input → helper → output feldolgozás

scripts/postprocess_report.py

    #1/#2 (AH/PM és Open→Close) jelentések utófeldolgozása

    makróblokk, analyst & catalyst blokkok, high-conv blokk beillesztése

    a job summary sorainak törlése a végső summary_report_1.md / summary_report_2.md-ből

scripts/postprocess_report_2.py

    vékony wrapper a fő postprocess_report.py körül kifejezetten a #2-es jelentéshez

    fixen a reports/summary_report_2.md-vel dolgozik

    biztosítja, hogy a #2-es futásnak is külön, név szerint azonosítható postprocess scriptje legyen

scripts/postprocess_report_3.py

    intraday saját logika a #3-as jelentéshez (blocks_events_3 + blocks_intraday_3 output alapján)

scripts/blocks_events_3.py, scripts/blocks_intraday_3.py

    #3-as lekérdezés ticker-szintű intraday blokkjai (Open→Most, High/Open, Low/Open, eseményblokkok)

scripts/macro_fetcher.py

    MACRO_FEED_URL_1/2/3 hívása (Apps Script webapp)

    macro_news_1.json / macro_news_2.json / macro_news_3.json generálása

scripts/analyst_feed_parser.py

    Apps Script analyst/catalyst feed hívása (ANALYST_FEED_URL_X, CATALYST_FEED_URL_X)

    analyst_X.json / catalysts_X.json generálás (#1/#2/#3)

scripts/analyst_block_builder.py

    #1/#2-es jelentés „Bejelentések & fel/lemínősítések” blokkja

    az analyst_X.json / catalysts_X.json feldolgozása

scripts/highconv_block_builder.py

    „Közelgő katalizátorok” és „High-conv jelöltek” blokkok (#1/#2)

    high_conv_1.json alapján (listán kívüli jelöltek, 3–12 hónapos távon)

3. Jelentések logikája
#1 After-hours + Premarket

    időablakok: 22:00–02:00 és 10:00–15:30 (CE(S)T)

    makróblokkal indul (macro_news_1.json)

    darabszámos tickerek AH/PM mozgásai (±3,00% fölött kötelező részletezni)

    watchlist: csak ha hír vagy ≥ ±3,00%

    bejelentések & fel/lemínősítések (analyst_1.json + catalysts_1.json)

    közelgő katalizátorok

    high-conv jelöltek (high_conv_1.json – csak listán kívüli tickerek)

#2 Open→Close (előző nap)

    előző kereskedési nap teljes intraday mozgása (Open→Close)

    ±3,00% fókusz

    nap közbeni vállalati hírek, bejelentések, makró események

    elemzői lépések / katalizátorok (analyst_2.json + catalysts_2.json)

    high-conv blokkok ugyanabból a high_conv_1.json-ből

    a végső summary_report_2.md-et a postprocess_report_2.py + postprocess_report.py páros állítja össze

#3 Open→Most (mai nap)

    aktuális intraday mozgások (Open→Most)

    opcionálisan High/Open és Low/Open

    nap közbeni breaking hírek

    #3-specifikus intraday blokkok (blocks_events_3.py, blocks_intraday_3.py, postprocess_report_3.py)

4. Adatfolyam

    Yahoo Finance (v8 chart + includePrePost) → ármozgás (#1/#2/#3)

    Apps Script → analyst/catalyst feed (#1/#2/#3)

    macro_fetcher.py → makróhírek (macro_news_X.json)

    highconv_builder.py → high_conv_1.json

    postprocess_report.py / postprocess_report_2.py / postprocess_report_3.py → summary_report_X.md véglegesítése

    run_report.yml → Gist frissítés (#1/#2/#3 fix linkekre)

5. Fallback logika

    Yahoo az elsődleges árforrás, Google / Investing a másodlagos/harmadlagos

    ha ár nem elérhető: a hír akkor is mehet (jelölve a lefedettség-blokkban)

    #1-es jelentés fallback: ha a pipeline nem hozza létre a BIBLIA szerinti riportot, ChatGPT a fix #1 Gist RAW alapján generálhat teljes #1-es jelentést

6. High-conv kritériumok

Legalább 2 teljesül:

    2–3+ friss felminősítés / céláremelés nagy házaktól

    iránymutatás-emelés / pozitív guide

    konszenzus EPS/árbevétel felfelé módosul

    3–12 hónapos konkrét katalizátor

    relatív erő / 52w csúcshoz közeli teljesítmény

A high_conv_1.json soha nem tartalmazhat portfólió- vagy watchlist-tickersort (csak listán kívüli neveket).
