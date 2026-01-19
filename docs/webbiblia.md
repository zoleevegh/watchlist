# WEBBIBLIA – KANONIKUS SPECIFIKÁCIÓ
## Automatikus #1 / #2 / #3 részvényjelentések

**Hivatalos forrás (RAW):**  
https://raw.githubusercontent.com/zoleevegh/watchlist/main/docs/webbiblia.md

**Verzió:** v2.1.0  
**Utolsó módosítás:** 2026-01-19 09:15:08  
**Megjegyzés:** Ez az egyetlen hivatalos webbiblia. Rövidített, alternatív vagy kivonatolt változat nem létezik.

---

## 0. Alapelvek (KANONIKUS)

- A webbiblia **normatív specifikáció**, nem leíró dokumentum.
- Ami itt nincs kimondva, **nem létezik szabályként**.
- Minden jelentés **ennek a dokumentumnak** köteles megfelelni.
- Ha adat vagy forrás nem érhető el, azt **explicit módon jelezni kell**.

---

## 1. Jelentéstípusok

- **#1 jelentés:** After-hours + Premarket fókusz (ár + hír)
- **#2 jelentés:** Előző kereskedési nap (Open → Close)
- **#3 jelentés:** Aktuális kereskedési nap (Open → Most)

---

## 2. Időablakok

### 2.1 Ármozgások
- After-hours: 22:00–02:00 CET/CEST
- Premarket: 10:00–15:30 CET/CEST
- Rendes kereskedés: 15:30–22:00 CET/CEST

### 2.2 Hírek
- Előző napi zárástól a lekérdezés pillanatáig
- Nem kötődnek az ár-időablakokhoz

---

## 3. Árforrás – PRICE ENGINE (KRITIKUS)

**KANONIKUS SZABÁLY:**
- Az ármozgások forrása **kizárólag** a jelentés előtt átadott **kanonikus Gist**.
- Más árforrás (Yahoo, Google, Investing stb.) **TILOS** pótlásra, ellenőrzésre vagy becslésre.
- 0.00%-os feltöltés, becslés vagy interpoláció **TILOS**.

Ha egy tickerhez nincs ár a Gistben:
- nem kerül felsorolásra,
- a lefedettségi blokkban **HIÁNYOS** státuszt kell jelezni.

---

## 4. Lefedettség-ellenőrzés (KÖTELEZŐ BLOKK)

**TELJES:**  
- minden kötelező adatblokkhoz minden szükséges forrás elérhető volt

**HIÁNYOS:**  
- bármely kötelező adatblokk (pl. árak a Gistből) részben vagy egészben hiányzott

HIÁNYOS esetén **kötelező megjelölni**, mely blokk érintett.

---

## 5. Ármozgások jelentése

### 5.1 Darabszámos tickerek (pozíciók)
- Küszöb **NINCS**
- Csak azok szerepelnek, amelyekhez a Gist ármozgást tartalmaz
- A hiányzó tickerek **nem pótlódnak**, ez forráskorlát

### 5.2 Watchlist tickerek
- **KIZÁRÓLAG** abszolút **±3% vagy nagyobb** elmozdulás esetén szerepelhetnek
- ±3% alatt **nem jelenhetnek meg**
- Ilyenkor a blokk üres, ezt explicit jelezni kell

---

## 6. Bejelentések és elemzői lépések

### Források (publikus, webes)
- MarketBeat (elsődleges)
- The Fly / StreetInsider (headline)
- Reuters / AP
- SEC EDGAR (8-K)
- Vállalati IR / Newsroom

### Jelentjük, ha:
- fel-/leminősítés
- ≥±10% célár-változás
- új coverage
- earningsen kívüli vállalati bejelentés
- buyback / osztalék
- M&A
- CEO / CFO váltás

---

## 7. Forráselérhetőség megkülönböztetése (KRITIKUS)

TILOS összemosni:
- **„Forrás elérve, de nincs esemény”** (szűrési eredmény)
- **„Forrás nem elérhető”** (adat-/forráshiány)

Forráshiány esetén a „nincs hír” kifejezés **TILOS**.

---

## 8. Közeli katalizátorok

### 8.1 Earnings (KANONIKUS)
- A kanonikus Gistben szereplő **következő 7 napos earnings** események
  **MINDIG** bekerülnek
- Az earnings **soha nem külön fejezet**, kizárólag itt
- Külső earnings naptár **nem írhatja felül**

### 8.2 Nem-earnings katalizátorok
- ≤7 napon belüli
- anyagilag lényeges
- publikus forrásból igazolt események

---

## 9. Listán kívüli high-conviction jelöltek

### Alapfeltételek
- Nem lehet portfólióban
- Nem lehet watchlisten

### Időtáv
- 1–3 hónap
- 3–6 hónap

### Bekerülés
Legalább **2 feltétel** teljesül:
- 2–3+ friss felminősítés / céláremelés
- pozitív guidance
- konszenzus estimate-emelés
- konkrét katalizátor
- relatív erő / 52w csúcsközeli ár

### Forrásminimum
- MarketBeat **ÉS**
- legalább egy további publikus forrás (Yahoo / Reuters / IR)

---

## 10. Zárás

Ez a webbiblia **lezárt, kanonikus állapot**.
Módosítás kizárólag:
- teljes fájl cseréjével,
- új verziószámmal,
- letölthető formában történhet.
