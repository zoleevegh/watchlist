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


### Forráslefedés – ellenőrzött (KANONIKUS KOMMENT)

Az alábbi eseménytípusok mindegyike biztosan elérhető legalább egy,
a webbibliában megadott publikus webes forrásból:

- Elemzői fel-/leminősítések, célár-változások:
  MarketBeat (elsődleges), The Fly / StreetInsider (headline)
- Coverage indítás / megszüntetés:
  The Fly / StreetInsider (headline), MarketBeat (késleltetve)
- Earnings release és guidance:
  SEC EDGAR (8-K), vállalati IR / Newsroom
- M&A, stratégiai lépések:
  SEC 8-K, IR / Press Release, Reuters / AP
- Buyback / osztalék:
  IR / EDGAR, Business Wire / PR Newswire
- CEO / CFO váltás:
  SEC 8-K (kötelező), IR / Reuters

A fenti események egyike sem igényel előfizetéses terminált;
mind publikus, webesen ellenőrizhető forrásból származik.


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

## 10. Közeli katalizátorok (KANONIKUS)

### 10.1 Definíció
Közeli katalizátor = konkrét dátumhoz/időponthoz köthető esemény, amely rövid távon (napok–hetek) érdemi árfolyamhatást válthat ki.

**Beletartozik:**
- Earnings (jelentési dátum)
- Hatósági döntés (pl. FDA), bírósági ítélet
- Investor Day / Capital Markets Day
- Lock-up expiry
- Makró esemény (CPI, FOMC) – ha a ticker historikusan érzékeny rá

**Nem tartozik bele:**
- bizonytalan időzítés („valamikor idén”)
- puszta narratíva katalizátor nélkül
- pletyka hivatalos megerősítés nélkül

### 10.2 Earnings – kötelező, kanonikus input (Gist)
A jelentés előtt átadott kanonikus Gist (`summary_report_1.md`) tartalmazza a következő **7 nap** earnings dátumait.  
Ezeket a riport **MINDIG** beépíti a „Közeli katalizátorok” blokkba.

**Megjelenítés:**
- Darabszámos tickereknél: minden earnings esemény kötelezően megjelenik, ha **≤ 7 napon belül** esedékes.
- Watchlist tickereknél: minden earnings esemény kötelezően megjelenik, ha **≤ 7 napon belül** esedékes.

**Autoritív forrás:** az earnings dátumok forrása kizárólag a kanonikus Gist; külső earnings naptár nem használható felülírásra.

### 10.3 Nem-earnings katalizátorok – szűrés
- Darabszámos tickereknél: minden nem-earnings esemény megjelenik, ha **≤ 7 napon belül** van.
- Watchlist tickereknél: csak akkor jelenik meg, ha **≤ 7 napon belül** van **ÉS** anyagilag lényeges (lásd 7. pont) vagy historikusan árérzékeny a ticker.

### 10.4 Források (publikus, webesen ellenőrizhető)
- Earnings dátum: kanonikus Gist
- Események: vállalati IR/Newsroom, SEC EDGAR, Reuters/AP, Business Wire/PR Newswire
- Makró események: FederalReserve.gov, BLS/BEA, U.S. Treasury, Reuters/AP


---

## 11. Listán kívüli high-conviction jelöltek

**Cél:** kizárólag olyan, a saját portfólió- és watchlistán kívüli tickerek felsorolása, amelyeknél ismétlődő, több forrásból alátámasztott, rövid–középtávú (1–6 hó) pozitív jelzés látszik.

### 11.1 Alapszabályok
- **Tiltás:** a blokkba **soha** nem kerülhet olyan ticker, amely a portfólióban vagy watchlisten szerepel.
- A blokk **csak akkor jelenik meg**, ha van valóban erős, ismétlődő jelzés (különben teljesen kimarad).

### 11.2 Időtáv-sávok
- **1–3 hónap**
- **3–6 hónap**

### 11.3 Bekerülési feltételek
Egy ticker akkor kerülhet be, ha **legalább 2** teljesül az alábbiak közül:
- **2–3+ friss** felminősítés/céláremelés nagy házaktól
- pozitív guidance / iránymutatás-emelés
- konszenzus EPS/árbevétel **felfelé** módosulása (estimate‑felhúzás)
- közelgő, konkrét katalizátor (1–6 hónap)
- relatív erő / 52w csúcs-közeli teljesítmény

### 11.4 Forrásminimum (publikus, webesen ellenőrizhető)
- Elsődleges: **Yahoo Finance** és **MarketBeat**
- Minimum: legalább **1** MarketBeat‑alapú elemzői jelzés **ÉS** legalább **1** további megerősítés (Yahoo Finance / Reuters / IR/EDGAR / PR wire).
- Pletyka önmagában nem elegendő.



---

## Forráselérhetőség jelzése (KANONIKUS SZABÁLY)

Amennyiben egy jelentési blokkhoz szükséges forrás
nem érhető el, nem lekérdezhető, vagy hiányos,
ezt a jelentésben **explicit módon jelezni kell**.

Ilyen esetben **TILOS** a „nincs hír” vagy „nincs jelzés”
megfogalmazás használata.

Kötelező megkülönböztetés:
- **Forrás elérve, de küszöb nem teljesült** → blokk üres, indoklással
- **Forrás nem elérhető / hiányos** → blokk nem értékelhető, jelzéssel

---

## 12. Zárás

Ez a webbiblia lezárt, kanonikus állapot.
Módosítás kizárólag verzióváltással történhet.
