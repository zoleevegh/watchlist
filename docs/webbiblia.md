# WEBBIBLIA
Automatizált #1 / #2 / #3 részvényjelentések – KANONIKUS SPECIFIKÁCIÓ

KANONIKUS ELÉRÉS (RAW):
https://raw.githubusercontent.com/zoleevegh/watchlist/main/docs/webbiblia.md

---

## 0. Alapelv (normatív)
Ez a dokumentum normatív specifikáció.
Értelmezésre nyitott, „puha” megfogalmazás nem szerepelhet benne.
Ami itt nincs definiálva, az nem létezik szabályként.

---

## 1. Jelentéstípusok

#1 jelentés – After-hours + Premarket fókusz  
#2 jelentés – Előző kereskedési nap (Open → Close)  
#3 jelentés – Aktuális kereskedési nap (Open → Most)

---

## 2. Időablakok (KANONIKUS)

### Ármozgásokra vonatkozó időablakok
- After-hours: előző kereskedési nap 22:00 – 02:00 CET/CEST
- Premarket: aktuális nap 10:00 – 15:30 CET/CEST
- Rendes kereskedés: 15:30 – 22:00 CET/CEST

### Hírek / események
- Időtartam: előző napi zárástól a lekérdezés pillanatáig
- Nem kötöttek az ármozgási időablakokhoz

---

## 3. Jelentési sorrend (KANONIKUS)

1. Lefedettség-ellenőrzés
2. Makró / FED / kormányzati események (csak piaci relevancia esetén)
3. Darabszámos tickerek (pozíciók)
4. Watchlist tickerek (csak releváns hír vagy ≥±3% ármozgás esetén)
5. Bejelentések és fel-/lemínősítések
6. Közeli katalizátorok
7. Listán kívüli high-conviction jelöltek (1–3 hónap, 3–6 hónap)

---

## 4. Forráskezelés (KANONIKUS)

### Tier A – Elsődleges
- Reuters (Top / Markets / Breakingviews)
- AP
- Bloomberg (ha publikus tartalom elérhető)

### Tier B – Hivatalos vállalati csatornák
- SEC EDGAR (8-K, 6-K, 10-Q, 10-K)
- Vállalati IR / Newsroom
- Business Wire / PR Newswire / GlobeNewswire

### Tier C – Elemzői források
- MarketBeat (elsődleges)
- The Fly (headline szint)
- StreetInsider (headline szint)
- TipRanks (kiegészítő)

### Tier D – Makró
- FederalReserve.gov
- BLS / BEA / U.S. Treasury
- Reuters / AP politikai hírek

Csak publikus, webesen ellenőrizhető forrás használható.

---

## 5. Deduplikáció és fallback

- Egy esemény egyszer szerepelhet.
- Prioritás: Tier B > Tier A > Tier C.
- Árfolyam fallback: Yahoo Finance → Google Finance → Investing.com.
- Hír akkor is jelenthető, ha árfolyamadat nem elérhető.

---

## 6. Piaci relevancia (KANONIKUS)

Makró / kormányzati esemény piaci releváns, ha legalább egy teljesül:
- US index futures ≥ ±0,5%
- US10Y hozam ≥ ±5 bp
- DXY ≥ ±0,4%
- FED-kamat / likviditás döntés
- Konkrét, végrehajtható elnöki / kormányzati intézkedés

Nyilatkozat vagy kampányretorika önmagában nem releváns.

---

## 7. Anyagi lényegesség

Egy vállalati hír anyagilag lényeges, ha legalább egy teljesül:
1. EPS vagy bevétel guidance ≥ ±5%
2. Célárváltozás ≥ ±10%
3. Core üzletágat érintő M&A
4. Új vagy jelentősen bővített buyback / osztalék
5. CEO / CFO váltás
6. Core üzletágat érintő jogi / szabályozási döntés

---

## 8. Várható hatás nyitáskor

A blokk csak akkor jelenik meg, ha:
- After-hours vagy Premarket ármozgás ≥ ±3%
ÉS
- Konkrét, azonosítható hír oka van (Tier A–C)

---

## 9. Forrás-megerősítés

Egy hír megerősítettnek tekinthető, ha:
- legalább két független forrásból származik,
- amelyek közül legalább egy Tier A vagy Tier B.
Hivatalos IR / EDGAR közlés önmagában is elegendő.

---

## 10. High-conviction jelöltek

- Csak listán kívüli tickerek
- Időtáv: 1–3 hónap vagy 3–6 hónap
- Legalább két feltétel teljesül:
  - Több elemzői felminősítés / céláremelés
  - Pozitív guidance
  - Konszenzus EPS / árbevétel emelés
  - Konkrét katalizátor
  - Relatív erő / 52w csúcsközeli ár

---

## 11. Zárás

Ez a webbiblia lezárt, kanonikus állapot.
Módosítás kizárólag verzióváltással történhet.
