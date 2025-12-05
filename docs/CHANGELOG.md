## v3.0.0 - Biblia és makró/postprocess frissítés
DOKUMENTUM VERZIÓ: v3.4.0

- Biblia_leiras átdolgozva teljes pipeline leírással (#1/#2/#3, makró, high-conv, forrás-prioritás).
- macro_fetcher.py visszaállítva: Apps Script MACRO_FEED_URL_1/2/3 webapp hívása fetch_macro_text-tel.
- postprocess_report.py integrálása: makró, bejelentések, katalizátorok, high-conv blokkok + job summary sor eltávolítása.
- Fájlszerkezet pontosítása: reports/1/ JSON-ok (macro_news_1.json, analyst_1.json, catalysts_1.json, high_conv_1.json).


## v3.1.0 – 2025-12-04

- Hozzáadva: `scripts/analyst_block_builder.py` a #1-es jelentés „Bejelentések & fel/lemínősítések” blokkjához.
- Hozzáadva: `scripts/highconv_block_builder.py` a „Közelgő katalizátorok” és „High-conv jelöltek” blokkokhoz.
- Hozzáadva: `scripts/postprocess_report.py` a makró + analyst + katalizátor + high-conv blokkok végső összefűzésére.
- Frissítve: `docs/biblia_leiras.md` és `00_README_START_HERE.md` az új pipeline-architektúrával.

## v3.4.0 – 2025-12-05

- Frissítve: `docs/biblia_leiras.md` BIBLIA VERZIÓ: v3.4.0 – pontosítva a scripts/fájlok szerepkörének leírása, a #1/#2/#3 pipeline és a block builderek közötti adatfolyam.
- Frissítve: `00_README_START_HERE.md` DOKUMENTUM VERZIÓ: v3.4.0, Verzió: 1.1.0 – a belépési pont szövege összhangba hozva a jelenlegi makró / analyst / catalyst / high-conv / postprocess pipeline-nal.
- Rögzítve: az analyst / high-conv / catalyst blokkok már teljesen integrált, automata #1-es workflow-elemek; a korábbi „TODO” jellegű megjegyzések kivezetve a dokumentációból.
