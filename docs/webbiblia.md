# WEBBIBLIA – KANONIKUS SPECIFIKÁCIÓ
## Automatikus #1 / #2 / #3 részvényjelentések

**Hivatalos forrás (RAW):**  
https://raw.githubusercontent.com/zoleevegh/watchlist/main/docs/webbiblia.md

**Verzió:** v2.1.1  
**Utolsó módosítás:** 2026-01-19 09:50:31  

🔒 **Ez az egyetlen hivatalos webbiblia.**  
Rövidített, alternatív, kivonatolt vagy „chat‑verzió” **nem létezik**.

---

## 0. Alapelvek (KANONIKUS)

- A webbiblia **normatív specifikáció**, nem magyarázó jegyzet.
- Ami itt nincs kimondva, **nem létezik szabályként**.
- A jelentések kizárólag **publikusan, webesen elérhető forrásokra** támaszkodhatnak.
- Ha adat vagy forrás nem érhető el, azt **explicit módon jelezni kell**.

---

## 1. Jelentéstípusok

- **#1 jelentés:** After‑hours + Premarket (ár + hír)
- **#2 jelentés:** Előző kereskedési nap (Open → Close)
- **#3 jelentés:** Aktuális kereskedési nap (Open → Most)

---

## 2. Időablakok

### 2.1 Ármozgások
- After‑hours: 22:00–02:00 CET/CEST
- Premarket: 10:00–15:30 CET/CEST
- Rendes kereskedés: 15:30–22:00 CET/CEST

### 2.2 Hírek
- Előző napi zárástól a lekérdezés pillanatáig
- Nem kötődnek az ár‑időablakokhoz

---

## 3. Árforrás – PRICE ENGINE (KRITIKUS)

**KANONIKUS SZABÁLY**
- Az ármozgások forrása **kizárólag** a jelentés előtt átadott **kanonikus Gist**.
- Más árforrás (Yahoo, Google, Investing stb.) **TILOS**.
- 0.00%-os feltöltés, becslés, interpoláció **TILOS**.

Ha egy tickerhez nincs ár a Gistben:
- nem kerül felsorolásra,
- a lefedettségi blokkban **HIÁNYOS** státuszt kell jelezni.

---

## 4. Lefedettség‑ellenőrzés (KÖTELEZŐ)

**TELJES**
- minden kötelező adatblokkhoz a szükséges forrás elérhető volt

**HIÁNYOS**
- bármely kötelező adatblokk részben vagy egészben hiányzott

HIÁNYOS esetén **kötelező megjelölni**, mely adatblokk érintett.

---

## 5. Ármozgások jelentése

### 5.1 Darabszámos tickerek (pozíciók)
- Küszöb **NINCS**
- Csak azok szerepelnek, amelyekhez a Gist ármozgást tartalmaz
- Hiányzó ticker **nem pótlódik**

### 5.2 Watchlist tickerek
- Csak **abs(±3%) vagy nagyobb** elmozdulás esetén jelenhetnek meg
- ±3% alatt **nem jelenhetnek meg**
- Ilyenkor a blokk üres, ezt explicit jelezni kell

---

## 6. Bejelentések és elemzői lépések

### 6.1 Publikus, webes források (KANONIKUS)

**Tier A – gyors hírek**
- Reuters
- AP (Associated Press)

**Tier B – vállalati csatornák**
- SEC EDGAR
- vállalati IR / Newsroom
- Business Wire / PR Newswire / GlobeNewswire

**Tier C – elemzői források (kiegészítő)**
- MarketBeat (elsődleges)
- StreetInsider (headline szint)
- The Fly – *Analyst headlines only*
- TipRanks (kiegészítő)

**Tier D – makró**
- FederalReserve.gov
- BLS / BEA / U.S. Treasury
- Reuters / AP politikai hírek (csak piaci relevancia esetén)

---

## 6.2 KIZÁRT FORRÁSOK (NEM LÉTEZNEK)

Az alábbi források **nem szerepelnek** a webbibliában,
ezért a jelentésekben **semmilyen formában nem nevezhetők meg**:

- Yahoo (bármilyen LIVE / aggregált forma)
- Benzinga / Benzinga Pro
- Dow Jones Newswires
- Bloomberg (ha nincs publikus webes hivatkozás a webbibliában)

Ha egy információ csak ilyen forrásból lenne elérhető,
azt **nem szabad jelenteni**.

---

## 7. „Nincs hír” ≠ „Nincs adat” (KRITIKUS)

TILOS összemosni:
- **Forrás elérve, de nincs esemény** → szűrési eredmény
- **Forrás nem elérhető** → adat‑ vagy forráshiány

Forráshiány esetén a „nincs hír” kifejezés **TILOS**.

---

## 8. Közeli katalizátorok

### 8.1 Earnings (KANONIKUS)
- A Gistben szereplő **következő 7 napos earnings**
  **MINDIG** bekerülnek
- Az earnings **nem külön fejezet**, kizárólag itt
- Külső earnings naptár **nem használható**

### 8.2 Nem‑earnings katalizátorok
- ≤7 napon belüli
- anyagilag lényeges
- publikus forrásból igazolt események

---

## 9. Listán kívüli high‑conviction jelöltek

### 9.1 Alapfeltételek
- Nem lehet portfólióban
- Nem lehet watchlisten

### 9.2 Időtáv
- 1–3 hónap
- 3–6 hónap

### 9.3 Bekerülési feltételek
Legalább **2 feltétel** teljesül:
- több friss felminősítés / céláremelés
- pozitív guidance
- konszenzus estimate‑emelés
- konkrét katalizátor
- relatív erő / 52w csúcsközeli ár

### 9.4 Forrásminimum (KÖTELEZŐ)
- MarketBeat **ÉS**
- legalább egy további **KANONIKUS, PUBLIKUS** forrás:
  - Reuters
  - AP
  - vállalati IR / SEC EDGAR

⚠️ **Yahoo, Benzinga, Dow Jones itt sem használható.**

---

## 10. Zárás

Ez a webbiblia **lezárt, kanonikus állapot**.
Módosítás kizárólag:
- teljes fájl cseréjével,
- új verziószámmal,
- letölthető formában történhet.
