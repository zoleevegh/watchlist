## v3.0.0 - Biblia és makró/postprocess frissítés
- Biblia_leiras átdolgozva teljes pipeline leírással (#1/#2/#3, makró, high-conv, forrás-prioritás).
- macro_fetcher.py visszaállítva: Apps Script MACRO_FEED_URL_1/2/3 webapp hívása fetch_macro_text-tel.
- postprocess_report.py integrálása: makró, bejelentések, katalizátorok, high-conv blokkok + job summary sor eltávolítása.
- Fájlszerkezet pontosítása: reports/1/ JSON-ok (macro_news_1.json, analyst_1.json, catalysts_1.json, high_conv_1.json).

## v3.1.0 – 2025-12-04

- Hozzáadva: `analyst_block_builder.py` – a #1 jelentés „Bejelentések & fel/lemínősítések” blokkjának generálásához (`analyst_1.json` alapján).
- Hozzáadva: `highconv_block_builder.py` – a #1 jelentés „Közelgő katalizátorok” és „Listán kívüli, 3–12 hónapos high-conv jelöltek” blokkjához (`catalysts_1.json`, `high_conv_1.json`).
- Frissítve: `postprocess_report.py` – makró + analyst + katalizátor + high-conv blokkok beillesztése, valamint a „Job summary generated at run-time …” sor eltávolítása.
- Frissítve: `docs/biblia_leiras.md` és `00_README_START_HERE.md` az új pipeline-architektúra leírásával.
