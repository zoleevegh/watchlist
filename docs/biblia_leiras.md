# -*- coding: utf-8 -*-
"""
BIBLIA HELPER – 1/2/3-as jelentések kanonikus leírása

Ez a helper NEM technikai doksi (nincsenek benne path-ok, URL-ek, kódhívások),
hanem a három jelentés (1/2/3) működési „bibliája”.
Minden scriptnek és minden manuális átnézésnek ehhez kell igazodnia.
"""

BIBLIA_HELP_TEXT = r"""
A: ÁLTALÁNOS ELVEK
------------------
- Fókusz: árfolyamot érdemben mozgató információk (anyagi lényegesség).
- Csak megerősített, hiteles forrás: Reuters/AP/Bloomberg, hivatalos IR/SEC,
  MarketBeat / elemzői feedek, nagy házak kommentjei, szektor-specifikus portálok.
- Pletyka, Reddit/Stocktwits, random blog NEM kerül be.
- Minden ±3,00% vagy nagyobb ármozgásnál kötelező a pontos % (két tizedes)
  és az ok 1 mondatban.
- A darabszámos tickerek MINDIG előre kerülnek, utána jön a watchlist – csak
  ha ott teljesülnek a küszöbök.
- A riport elején kötelező a „Lefedettség” blokk:
    • „Lefedettség: TELJES” – ha az összes tickerre sikerült adatot szerezni.
    • „Lefedettség: HIÁNYOS – nem elérhető ticker(ek): … (oka: …)” – ha bármi hiányzik.
- Ha egy blokk logikailag üres (pl. nincs high-conv jelölt), azt vagy ki kell hagyni,
  vagy egyértelmű, rövid mondattal jelezni, hogy „nincs releváns tétel”.

Forrás-prioritások (A–E)
------------------------
A) Gyors, megbízható hírfeed:
   - Reuters (Top, Markets, Breakingviews)
   - AP News
   - Bloomberg Top/Markets (ha elérhető)
   - Dow Jones Newswires / The Fly / Benzinga Pro tape

B) Hivatalos vállalati csatornák:
   - SEC EDGAR (8-K, 6-K, 10-Q, 10-K)
   - Cég IR / Newsroom
   - Business Wire, PR Newswire, GlobeNewswire

C) Elemzői fel/lemínősítések, célárak:
   - MarketBeat Ratings (elsődleges)
   - StreetInsider / The Fly (Analyst)
   - TipRanks (kiegészítő)

D) Makró / FED / politika:
   - FederalReserve.gov
   - BLS, BEA
   - US Treasury
   - Reuters/AP politikai / makró hírei

E) Szektor-specifikus feedek:
   - Félvezetők, AI, kripto, biotech, stb. – csak megerősített, több forrásból.

Anyagi lényegesség – mik kerülnek be?
-------------------------------------
- Guidance-emelés vagy -vágás (bevétel, EPS, margin, FCF).
- Jelentős EPS/árbevétel meglepetés (pozitív vagy negatív).
- M&A, nagy stratégiai deal, partnerkapcsolat, licence.
- Buyback-program, osztalék bejelentés vagy érdemi módosítás.
- CEO/CFO/Chair váltás, komoly vezetői változás.
- Szabályozási / jogi döntés, ami a core üzletet érinti.
- Több, egymást erősítő elemzői felminősítés / céláremelés.
Ami NEM kerül be: apró PR, marketing-cikk, gyenge pletyka, egyforrásos zaj.


I. #1 JELENTÉS – „TEGNAPI ZÁRÁSTÓL MOSTANÁIG” (AH + PM)
-------------------------------------------------------
Időablakok (CET/CEST):
- After-hours (AH): előző kereskedési nap 22:00–02:00.
- Premarket (PM): aktuális nap 10:00–15:30.
A #1-es riport CSAK ezekkel az idősávokkal foglalkozik; intraday Open→Close
mozgás NEM része.

Árforrás és számolás:
- Yahoo Finance chart (2 nap / 5 perces gyertya, includePrePost).
- Bázisár: az utolsó rendes kereskedési (RTH) záróár.
- AH/PM %: (AH/PM utolsó ár – előző RTH záró) / záró × 100.
- Minden ±3,00% vagy nagyobb mozgásnál kötelező:
    • pontos %-érték (két tizedre),
    • megnevezni, hogy AH vagy PM sáv,
    • 1 mondatban az ok.

Blokksorrend:
1) Lefedettség blokk (TELJES / HIÁNYOS + hiányzó tickerek).
2) Makró / FED / piaci hangulat blokk.
3) Darabszámos tickerek AH/PM mozgásai.
4) Watchlist tickerek AH/PM mozgásai (csak ha van releváns mozgás/hír).
5) Bejelentések & fel/lemínősítések.
6) Közeli katalizátorok (3–10 nap).
7) Listán kívüli high-conviction jelöltek (ha vannak).

Makró / FED / piaci hangulat blokk (#1)
---------------------------------------
Cél: 3–5 soros, Bloomberg-szerű összefoglaló az AH/PM sáv legfontosabb
makró, FED és általános piaci hangulat híreiről.

Tartalom:
- Rövid, „headline-stílusú” mondatok:
    • kulcs makró-adatok (infláció, munkaerőpiac, GDP, PMI, stb.),
    • Fed-kommentek (rate cut / hike várakozás, dot plot, stb.),
    • globális kockázati étvágy (futures, hozamok, dollár, olaj, kriptó).
- Utána 1–2 mondat arról, hogy ez várhatóan hogyan hat:
    • risk-on vs. risk-off hangulat,
    • growth/tech vs. value/defenzív szektorok.

Üres eset:
- Ha az adott sávban nincs piaci szintű, lényeges makró/FED hír:
  „Az AH/PM sávban nem érkezett a piac egészét érintő, anyagilag lényeges makró vagy FED hír.”

Darabszámos tickerek – AH/PM (#1)
---------------------------------
Cél: a portfólióban darabszámmal szereplő papírok AH/PM viselkedésének rövid, indokolt bemutatása.

Szabályok:
- Csak darabszámos ticker kerül ide.
- Küszöb:
    • ±3,00% vagy nagyobb AH/PM mozgás → RÉSZLETES sor.
    • ±3,00% alatt → elegendő egy összefoglaló blokk („küszöb alatti”).

Részletes sor tartalma:
- Ticker.
- AH változás % (két tized, ha van releváns AH forgalom).
- PM változás % (két tized, ha van releváns PM forgalom).
- Rövid ok (1 mondat), a forrás-prioritás szerint:
    • earnings / guidance,
    • makró / szektorszintű hatás,
    • elemzői lépés,
    • egyéb lényeges hír.
- „Várható hatás nyitáskor” – 1 mondatban: gap-up/gap-down, volatilitás, stb.

Küszöb alatti mozgás:
- „[Ticker]: Küszöb alatti AH/PM elmozdulás (<3%), nincs új lényeges hír.”

Watchlist – AH/PM (#1)
----------------------
Cél: a figyelőlistán lévő papírok közül csak azokat kiemelni, ahol VALÓDI történés van.

Szabályok:
- Watchlist ticker csak akkor kerül be, ha:
    • AH/PM sávban legalább ±3,00% a mozgás, VAGY
    • hírfolyamban anyagilag lényeges esemény (earnings, guidance,
      nagy deal, szabályozási döntés, elemzői fel/lemínősítés).
- Formátum ugyanaz, mint a darabszámos tickereknél.

Bejelentések & fel/lemínősítések (#1)
-------------------------------------
Cél: összefoglalni minden, a portfóliót vagy watchlistet érintő, anyagilag lényeges vállalati bejelentést és elemzői lépést az AH/PM sávban.

Közeli katalizátorok (#1)
-------------------------
- Earnings-dátumok, Fed-meeting, fontos makrók, product launch, investor day, PDUFA, stb.
- Formátum: „[Ticker] – [Esemény típusa] – [Dátum/idősáv] – 1 mondatos jelentőség.”

Listán kívüli, 3–12 hónapos high-conviction jelöltek (#1)
---------------------------------------------------------
Kritériumok (legalább 2 teljesüljön az 5-ből):
1) Több friss felminősítés / céláremelés nagy házaktól.
2) Guidance-emelés, pozitív vállalati iránymutatás.
3) Konszenzus EPS/árbevétel felfelé módosul.
4) 3–12 hónapon belüli konkrét katalizátor.
5) Erős relatív erő, 52 hetes csúcs közeli teljesítmény.

Szabályok:
- A blokkban SOHA nem szerepelhet olyan ticker, ami portfólióban vagy watchlisten van.
- Csak akkor jelenik meg a blokk, ha van ténylegesen erős jelölt.
- Minden jelöltnél: ticker + szektor, 2–3 mondatos „thesis”, felsorolva a teljesülő kritériumokat.


II. #2 JELENTÉS – „TEGNAPI NYITÁSTÓL ZÁRÁSIG” (OPEN→CLOSE)
-----------------------------------------------------------
Időablak:
- Előző kereskedési nap 15:30–22:00 (CET/CEST).

Fókusz:
- Előző nap NAPI mozgásai, különösen a ±3,00% feletti Open→Close elmozdulások indoklással.
- Intraday hírek, amelyek ezeket okozták.

Árforrás és számolás:
- Open→Close %: (záró – nyitó) / nyitó × 100.
- Küszöb: ±3,00% vagy nagyobb elmozdulásnál kötelező részletes magyarázat.

Blokksorrend:
1) Lefedettség blokk.
2) Makró / FED / piaci hangulat – előző nap intraday eseményei.
3) Darabszámos tickerek Open→Close mozgásai (≥3% fókusz).
4) Watchlist tickerek Open→Close mozgásai (csak ha hír vagy ≥3%).
5) Bejelentések & fel/lemínősítések (nap közben).
6) Közeli katalizátorok.
7) High-conv jelöltek (ha a nap során jön hozzájuk új jel).


III. #3 JELENTÉS – „MA NYITÁSTÓL MOSTANÁIG” (OPEN→MOST)
--------------------------------------------------------
Időablak:
- Aktuális kereskedési nap 15:30–lekérdezés pillanata (CET/CEST).

Fókusz:
- Aktuális intraday mozgások:
    • Open→Most %,
    • opcionálisan High/Open és Low/Open,
    • nap közben érkező hírek, makrók, Fed-kommentek.

Árforrás és számolás:
- Open→Most %: (aktuális ár – nyitóár) / nyitóár × 100.
- High/Open %: (intraday csúcs – nyitó) / nyitó × 100.
- Low/Open %: (intraday mélypont – nyitó) / nyitó × 100.
- Küszöb: ±3,00% vagy nagyobb Open→Most mozgásnál kötelező indoklás.

Blokksorrend:
1) Lefedettség blokk.
2) Makró / FED / piaci hangulat – aznapi intraday fejlemények.
3) Darabszámos tickerek Open→Most (és ha kérve: High/Open, Low/Open).
4) Watchlist tickerek (csak ha hír vagy ≥3%).
5) Bejelentések & fel/lemínősítések (nap közben).
6) Közeli katalizátorok (ha aznapi hír rájuk utal).
7) High-conv jelöltek (ha ma erősödik a case).


IV. LEFEDETTSÉG ÉS FALLBACK LOGIKA – ÖSSZEFOGLALÓ
--------------------------------------------------
- Minden jelentés elején:
    • Ha az összes tickerre sikerül árat + hírt hozni:
        „Lefedettség: TELJES”.
    • Ha bármelyik tickerre nincs friss ár/hír:
        „Lefedettség: HIÁNYOS – nem elérhető ticker(ek): [lista]
         (oka: pl. nincs friss adat / forráshiba / késik feed)”.
- Ha egy ticker árfolyamadata nem elérhető, de releváns hír van róla:
    • a hírt akkor is lehet röviden leírni, zárójellel jelezve, hogy
      az árfolyamadat hiányzik.

A fenti szabályok adják a három riport (1/2/3) kanonikus, hosszú távra érvényes
működési keretét. Kódoldali módosításnál mindig ez legyen az igazodási pont.
"""

def get_biblia_text() -> str:
    """Helper, ha máshonnan akarod beolvasni a teljes bibliát."""
    return BIBLIA_HELP_TEXT
