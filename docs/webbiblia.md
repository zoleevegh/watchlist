# WEBBIBLIA – v1.7.1 (KANONIKUS)
KANONIKUS RAW LINK (mindig ez az irányadó):
https://raw.githubusercontent.com/zoleevegh/watchlist/main/docs/webbiblia.md

VERZIÓZÁS – KŐSZABÁLY:
- Minden módosításnál a verziószámot FOLYTATÓLAGOSAN növeljük.
- Új verzió = előző FULL tartalom + hozzáadások (törlés csak indokoltan, külön jelölve).
- A fájlban szereplő verziószámnak EGYEZNIE kell a fájlnév verziójával.

Változások v1.7.0 → v1.7.1 (összefoglaló):
- Yahoo Finance LIVE (Market Today) kötelező makró/nyitáskép kontextusforrás marad.
- ALL-IN kapu/checklist pontosítva: „Yahoo LIVE átnézve” csak **makró kontextus**, nem trigger; „szellem hír” tiltás változatlan.
- Earnings-dátum forrásváltás: **Yahoo helyett** stabil, API key nélküli lánc: **Nasdaq Earnings Calendar (primary)** → **Investing.com (secondary)** → „adat nem elérhető” (explicit).
- Earnings audit mostantól külön modul (earnings.py), és a #1 reportban a kimenet a **## Pozíciók elé** kerül (MASTER-szűréssel, 7 napos ablakkal).
- Cél: 0 félreértés a 7 napon belüli earnings auditban; a riportban kötelező a lefedettségi összegzés és a forrás-jelzés.

---

## 🔒 Earnings-dátum – KANONIKUS SZABÁLY (#1 ALL-IN)

**Earnings / event dátumok kanonikus forráslánca (API key nélkül):**
1) **Nasdaq – Earnings Calendar** (primary)
2) **Investing.com – Earnings Calendar** (secondary)
3) Ha egyik sem ad dátumot: **„Earnings: n/a (forrás nem adott dátumot)”** (explicit jelzés)

Engedélyezett mezők (ha a forrás adja):
- **Date** (dátum)
- **Time** (pl. pre-market / after-hours / not supplied)
- **EPS forecast** és **Revenue forecast** (ha a naptár megadja – ezek „estimate”-ek, nem mi becsüljük)

Tilos:
- saját „becslés” vagy következtetés (historical timing, EarningsWhispers, MarketBeat, stb.),
- ticker-szintű, kulcsos API-k (TradingEconomics API key nélkül, stb.),
- Yahoo „Earnings Date” (NEM használt) mezőre támaszkodni (jelenleg instabil / blokkolt).

Ha nincs adat: **nem találunk ki semmit**, hanem jelezzük, hogy nincs dátum (és a lefedettséget a blokk végén kötelezően kiírjuk).

---
# WEBBIBLIA – KANONIKUS FORRÁS
**Mindig ezt a linket tekintsd irányadónak:**
https://raw.githubusercontent.com/zoleevegh/watchlist/main/docs/webbiblia.md

## ✅ Kötelező módszertan – hogyan „auditáljuk” az earnings dátumot (NEHOGY KIMARADJON)

**Cél:** 0 db kihagyás. Nincs „fejből”, nincs becslés, nincs naptár-átvétel. Csak determinisztikus ellenőrzés.

### Lépések (EARNINGS AUDIT RUNBOOK)

**Cél:** 7 napon belüli közelgő earnings / event lista a MASTER tickerekre – determinisztikusan, automatával, API key nélkül.

1) **Univerzum rögzítése (MASTER)**
   - A vizsgálat alapja **kizárólag a MASTER** (Google Sheets → CSV).
   - A script kiírja: „Találat: K / N ticker”.

2) **Forrás1 – Nasdaq Earnings Calendar (primary)**
   - A script a következő 7 nap **napi** naptár-oldalait kéri le.
   - Kinyeri a listát: (ticker, date, time, EPS estimate, revenue estimate – ha van).
   - **MASTER-szűrés**: csak a MASTER-ben szereplő tickerek maradnak.

3) **Forrás2 – Investing.com Earnings (secondary)**
   - Csak akkor fut, ha a Nasdaq naptár adott nap(okon) blokkolt / nem ad adatot.
   - Ugyanaz a kimenet: (ticker, date, time/period – ha van).
   - **MASTER-szűrés** itt is kötelező.

4) **Riport-követelmény**
   - A #1 reportban a blokk neve: **„## Közelgő katalizátorok (ellenőrzött)”**.
   - A blokk a **## Pozíciók elé** kerül.
   - A végén kötelező a lefedettségi sor(ok): sikeres napok, blokkolt napok, és „dátum nem elérhető” összesítés.

5) **Tiltások (hogy ne csússzunk el)**
   - Nincs „UTC-trükközés” a dátumokra: **lokális naptári nap** a mérvadó (Europe/Budapest szerint kiírva).
   - Nincs saját becslés: csak amit a naptár forrás közöl.
   - Ha nincs adat: **explicit n/a** (nem „üresen hagyjuk”, nem „nincs a következő 7 napban”, ha valójában csak nincs lefedettség).


## 0️⃣ Hogyan használd ezt a dokumentumot

Ez a dokumentum a **webes forrásokra épülő jelentéskészítés kanonikus szabálykönyve**.
Célja nem az, hogy minden ármozgást megmagyarázzon, hanem hogy:

> **SEM MI FONTOS NE MARADJON KI**,  
> miközben a zaj következetesen kiszűrésre kerül.

Alapelv:
- Script ≠ Web
- Ármozgás ≠ Trigger
- Kontextus **csak trigger után**
- „Nincs hír” is lehet **érvényes állapot**

---

## 1️⃣ #1 ALL‑IN – KÖTELEZŐ MINI‑CHECKLIST (GATE)

Ez a **kapu**. Ha ez nincs végigpipálva, **NINCS #ALL‑IN jelentés**.

1. Időintervallum helyesen meghatározva  
2. Utolsó tényleges zárás azonosítva (hétfő / ünnep kezelve)  
3. Ármozgás és trigger szétválasztva  
4. Elemzői lépések teljes körűen ellenőrizve  
5. **Yahoo Finance analyst overlay külön ellenőrizve**  
6. Reuters adat vs Reuters hír különválasztva  
7. 7 napon belüli earnings ellenőrizve  
8. Kontextus csak trigger után  
9. Zajszűrés lefuttatva  
10. Tudunk válaszolni: *„miért maradt ki?”*

---

## 2️⃣ Jelentéstípusok és időablakok

### #1 – After‑hours + Premarket
- **Script (PRICE ENGINE):**
  - AH: előző kereskedési nap 22:00–02:00 CET
  - PM: 10:00–15:30 CET
- **Webes lekérdezés:**
  - **előző tényleges zárástól a lekérdezés pillanatáig**
  - hétfőn: péntek 22:00 CET‑től
  - ünnep esetén: utolsó nyitva tartó nap zárásától

Mit kapsz:
- Mi történt zárás után / nyitás előtt
- Mire reagál a piac
- Várható hatás a nyitáskor

Mit NEM kapsz:
- intraday max/min
- open→now
- zaj

### #2 – Open → Close  
### #3 – Open → Now  

(A szabályok megegyeznek, az időablak különbözik.)

---

## 3️⃣ Forrás‑hierarchia (KANONIKUS)

### 3.1 Hivatalos cégforrások
- SEC: 8‑K, 6‑K, 10‑Q, 10‑K
- IR / Newsroom
- PR Newswire, Business Wire

### 3.2 Gyors wire
- **Reuters (elsődleges)**
- AP (megerősítés)
- Bloomberg / Dow Jones / The Fly (ha elérhető)

### 3.3 Elemzői források
- **MarketBeat (kanonikus)**
- **Yahoo Finance – FAST TRIGGER (kivételszabály)**
- StreetInsider / The Fly (kiegészítés)
- TipRanks (ellenőrzés)

### 3.4 IBKR FYI helye
- Lagging indicator
- Konszenzus‑eloszlás változás
- **Önmagában nem jelenthető**
- Csak megjegyzésként, ha más trigger van

---


### 3.5 Yahoo Finance – Live Market Coverage (MAKRÓ KIVÉTEL)

A Yahoo Finance **Live / Market Today** jellegű cikkei **makró / piaci hangulat kontextusban
JELENTHETŐK**, ha az alábbi feltételek TELJESÜLNEK:

- az időablakon belül frissültek (óra/perc jelölés)
- összpiaci fókuszúak (indexek, futures, hozamok, makró adat, geopolitika)
- NEM ticker-specifikus pletykák
- NEM elemzői ajánlások helyettesítői

Felhasználás:
- kizárólag a **Makró / FED / politika** blokkban
- **kontextusra**, nem triggerként
- segíti a *várható mai nyitás* megfogalmazását

Tipikus témák:
- jobs data
- CPI / PPI
- geopolitika (olaj, szankciók)
- index futures irány


## 4️⃣ Hírkeresési és szűrési logika (WEB)

### 4.1 Kötelező szűrők
- Publikációs idő az intervallumban
- Hiteles forrás
- Duplikáció kizárása
- **3 napos „beragadt” hír kizárása**

### 4.2 Kulcsszó / parancslogika
- upgrade / downgrade
- cuts target / raises target
- guidance / outlook
- SEC formák
- earnings miss / beat

### 4.3 Szellem hír definíció
- nincs új információ
- nincs időbeli relevancia
- nincs piaci hatás

→ **nem jelentjük**

---

## 5️⃣ #ALL‑IN JELENTÉS – KANONIKUS FELÉPÍTÉS (OUTPUT)

1. Lefedettség‑ellenőrzés  
2. Makró / FED / politika  
   - éles makróhír (ha van)
   - előző napi piaci hangulat
   - várható nyitási irány
3. Ármozgások (PRICE ENGINE)  
4. 🔴 Elemzői lépések & filingek (**ELSŐDLEGES TRIGGEREK**)  
5. Kontextus / szektor (kiegészítés)  
6. **Közelgő katalizátorok**  
7. Listán kívüli high‑conviction  
   - 1–3 hónap  
   - 3–6 hónap  
8. Összkép / narratíva  

---

## 6️⃣ Közelgő katalizátorok (OUTPUT)

Ha releváns ticker van:

- Earnings dátum (EST + CET)
- Konszenzus várakozás (EPS / Revenue)
- Elemzői elvárás (ha van)
- **Várható hatás**
  - ↑ pozitív
  - ↓ negatív
  - vegyes
  - bináris
- 1 mondatos értelmezés

Ha nincs:
> „Ellenőrizve, 7 napon belül nincs releváns earnings.”

---

## 7️⃣ Közelgő katalizátorok – SZABÁLYRENDSZER (RULES)

- Earnings katalizátor = 7 napon belüli jelentés
- Forrás: Yahoo, IR oldal, Reuters earnings calendar
- Kötelező, ha:
  - nagy pozíció
  - magas implied move
- Tilos, ha:
  - dátum bizonytalan
  - nem releváns ticker
- AH vs PM earnings külön jelölendő

---

## 8️⃣ Mit NEM jelentünk

- IBKR FYI önmagában
- Konszenzus‑statisztika trigger nélkül
- Kontextus esemény nélkül
- Ármagyarázat hír nélkül (kivéve jelölve)

---

## 9️⃣ Edge case‑ek

- Reuters adat van, hír nincs
- Yahoo hoz le, más nem
- Elemzői lépés árreakció nélkül
- Árreakció hír nélkül

Mindig dokumentálni kell a **miértet**.

---

## 🔚 Záróelv

> **Nem az a cél, hogy minden mozgást megmagyarázzunk,  
hanem hogy semmi lényeges ne maradjon ki – és semmi zaj ne keveredjen be.**

---

Verzió: **v1.7.1 – KANONIKUS**
