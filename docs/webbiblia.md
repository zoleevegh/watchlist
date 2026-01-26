# WEBBIBLIA – KANONIKUS SPECIFIKÁCIÓ  
## Automatikus #1 / #2 / #3 részvényjelentések

**Hivatalos forrás (RAW):** https://raw.githubusercontent.com/zoleevegh/watchlist/main/docs/webbiblia.md  
**Verzió:** v2.1.1  
**Utolsó módosítás:** 2026-01-26  
**Megjegyzés:** Ez az egyetlen hivatalos webbiblia. Rövidített, alternatív vagy kivonatolt változat nem létezik.

---

## 0. Alapelvek (KANONIKUS)

- A webbiblia **normatív specifikáció**, nem leíró dokumentum.
- Ami itt nincs kimondva, **nem létezik szabályként**.
- Minden jelentés **ennek a dokumentumnak** köteles megfelelni.
- Ha adat vagy forrás nem érhető el, azt **explicit módon jelezni kell**.
- **Verziófegyelem (kötelező):** bármely fájl módosításakor a verziószámot **folytatólagosan növelni kell**, kihagyás nélkül.

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
- a lefedettségi blokkban **HIÁNYOS** státuszt kell jelezni (érintett blokk: „Ármozgások”).

---

## 4. Lefedettség-ellenőrzés (KÖTELEZŐ BLOKK)

**TELJES:**
- minden kötelező adatblokkhoz minden szükséges forrás elérhető volt

**HIÁNYOS:**
- bármely kötelező adatblokk részben vagy egészben hiányzott

HIÁNYOS esetén **kötelező megjelölni**, mely blokk érintett (pl. „Ármozgások”, „Elemzői feed”, „Earnings”).

---

## 5. Ármozgások jelentése

### 5.1 Darabszámos tickerek (pozíciók)
- Küszöb **NINCS**
- Csak azok szerepelnek, amelyekhez a Gist ármozgást tartalmaz
- A hiányzó tickerek **nem pótlódnak**, ez forráskorlát

### 5.2 Watchlist tickerek
- **KIZÁRÓLAG** abszolút **±3% vagy nagyobb** elmozdulás esetén szerepelhetnek
- ±3% alatt **nem jelenhetnek meg**
- Ha nincs ≥±3% esemény, a blokk üres, ezt explicit jelezni kell

---

## 6. Bejelentések és elemzői lépések (GIST-ALAPÚ)

**KANONIKUS SZABÁLY:**
- A „Bejelentések és elemzői lépések” blokk **nem** webes aggregálásból készül.
- A blokk **elsődleges és alapértelmezett** adatforrása a kanonikus Gist **„Elemzői feed (MarketBeat)”** szekciója.

### 6.1 Elemzői lépések – adatforrás
- **Kizárólag** a Gist „Elemzői feed (MarketBeat)” szekciójában szereplő tételek jelenthetők.
- A Gistben megjelenő mezők (pl. dátum, ház, akció, rating, célár) **változtatás nélkül** kerülnek a jelentésbe (legfeljebb rövidítés/nyelvi tömörítés megengedett).
- **TILOS** külső MarketBeat scrape/lookup a blokk pótlására vagy „ellenőrzésére”.

### 6.2 Vállalati bejelentések – adatforrás
- Vállalati bejelentés **csak akkor** kerülhet ebbe a blokkba, ha a kanonikus Gist **külön, dedikált „Bejelentések” (vagy ekvivalens) szekciót** tartalmaz.
- Ha ilyen Gist-szekció nincs, akkor a blokk **csak** az „Elemzői feed (MarketBeat)” elemeit tartalmazza, és ezt 1 mondatban jelezni kell („Vállalati bejelentés-blokk: nincs Gist-forrás”).

### 6.3 Jelentjük, ha (a Gist feed alapján)
- fel-/leminősítés
- ≥±10% célár-változás
- új coverage / coverage megszüntetés
- (ha Gist külön tartalmazza) earningsen kívüli vállalati bejelentés, buyback/osztalék, M&A, CEO/CFO váltás

---

## 7. Forráselérhetőség megkülönböztetése (KRITIKUS)

TILOS összemosni:
- **„Forrás elérve, de nincs esemény”** (szűrési eredmény)
- **„Forrás nem elérhető”** (adat-/forráshiány)

Forráshiány esetén a „nincs hír” kifejezés **TILOS**.

---

## 8. Közeli katalizátorok (GIST-VEZÉRELT + WEBES HÍRKERESÉS)

### 8.1 Earnings (KANONIKUS, Gist-ből)
- A kanonikus Gistben szereplő **következő 7 napos earnings** események **MINDIG** bekerülnek.
- Az earnings **soha nem külön fejezet**, kizárólag itt.
- Külső earnings naptár **nem írhatja felül** a Gistben szereplő listát.

### 8.2 Webes hírkeresés a Gist katalizátor-lista alapján (ÚJ KÖTELEZŐ SZABÁLY)
A „Közeli katalizátorok” blokk **nem csak** felsorolás: a Gistből vett (≤7 napos) katalizátorokhoz **kötelező webes híreket keresni**.

**Módszer (kanonikus):**
1) Alaplista: a Gist „Earnings — következő 7 nap” (és minden további ≤7 napos katalizátor-szekció, ha létezik).  
2) Ticker-szinten webes keresés: minden érintett tickerhez **max. 1–2** releváns, friss (tipikusan 72 órán belüli) headline/összefoglaló, ami:
   - earnings preview / várakozások / guide-kockázat, vagy
   - konkrét, dátumhoz kötött esemény (FDA/PDUFA, bíróság, szabályozás, termék launch, investor day, makró-érzékeny katalizátor).
3) Forrásprioritás (publikus): Reuters/AP (ha elérhető), SEC/IR (ha releváns), majd nagy aggregátorok (pl. Yahoo Finance).  
4) Ha nincs releváns friss hír, azt **„forrás elérve, de nincs releváns friss hír”** formában jelölni kell (nem „nincs hír”).  
5) Ha a webes források/keresés nem működik, azt **„forrás nem elérhető”** formában kell jelezni (összhangban a 7. ponttal).

### 8.3 Nem-earnings katalizátorok
- ≤7 napon belüli
- anyagilag lényeges
- publikus forrásból igazolt események
- A webes hírkeresés logikája megegyezik a 8.2 ponttal (Gistből indul, majd weben validál/kontextualizál).

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
