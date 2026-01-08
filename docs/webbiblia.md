# WEBBIBLIA – v1.7.0 (KANONIKUS)
KANONIKUS RAW LINK (mindig ez az irányadó):
https://raw.githubusercontent.com/zoleevegh/watchlist/main/docs/webbiblia.md

VERZIÓZÁS – KŐSZABÁLY:
- Minden módosításnál a verziószámot FOLYTATÓLAGOSAN növeljük.
- Új verzió = előző FULL tartalom + hozzáadások (törlés csak indokoltan, külön jelölve).
- A fájlban szereplő verziószámnak EGYEZNIE kell a fájlnév verziójával.

Változások v1.6.4 → v1.7.0 (összefoglaló):
- Yahoo Finance LIVE (Market Today) kötelező makró/nyitáskép kontextusforrás lett.
- ALL-IN kapu/checklist kiegészült: „Yahoo LIVE átnézve” és „nincs szellem hír” tiltás erősítve.
- Earnings-dátum: továbbra is Yahoo „Earnings Date” a kanonikus mező, de eltérés esetén kötelező megjegyzés és ellenőrző link.
- Cél: 0 kihagyás a 7 napon belüli earnings auditban, determinisztikus runbook szerint.

---

## 🔒 Earnings-dátum – KANONIKUS SZABÁLY (#1 ALL-IN)

**Earnings dátum forrása kizárólag: Yahoo Finance – “Earnings Date” mező.**

Tilos használni:
- becsült earnings window-kat,
- EarningsWhispers / MarketBeat / historical timing dátumokat,
- elemzői „várható” időablakokat.

Logika:
- csak akkor kerül be a jelentésbe,
  - ha a ticker **a Gist / MASTER listában szerepel**, ÉS
  - a Yahoo Finance **konkrét dátumot** mutat, ÉS
  - az **≤ 7 napon belül** van a futás idejétől.

Ha a Yahoo mező üres / tartományos → **nem jelentjük**.


# WEBBIBLIA – KANONIKUS FORRÁS
**Mindig ezt a linket tekintsd irányadónak:**
https://raw.githubusercontent.com/zoleevegh/watchlist/main/docs/webbiblia.md

## ✅ Kötelező módszertan – hogyan „auditáljuk” az earnings dátumot (NEHOGY KIMARADJON)

**Cél:** 0 db kihagyás. Nincs „fejből”, nincs becslés, nincs naptár-átvétel. Csak determinisztikus ellenőrzés.

### Lépések (EARNINGS AUDIT RUNBOOK)
1) **Univerzum rögzítése**
   - A vizsgálat alapja **kizárólag a Gist / MASTER tickerei**.
   - Először kiírjuk: „Tickerek száma: N = …” (pozíciók + watchlist).

2) **Ticker → Yahoo „Earnings Date” mező (KANONIKUS)**
   - Minden tickerre külön, **egyenként** megnézzük a Yahoo Finance oldalon az **„Earnings Date”** mezőt.
   - Ha a mező **konkrét dátumot** ad: rögzítjük.
   - Ha a mező üres / tartományos / „—”: **NEM kerül be**.

3) **7 napos szűrő**
   - Csak az kerül a „Közelgő katalizátorok” blokkba, ahol:
     - ticker ∈ Gist/Master, **ÉS**
     - Yahoo „Earnings Date” **konkrét**, **ÉS**
     - dátum **≤ futás ideje + 7 nap**.

4) **Bizonyíték-követelmény (auditálhatóság)**
   - A riportban az earnings-listához **kötelező**:
     - a ticker,
     - a dátum,
     - és legalább **egy forráslink**, ami a Yahoo quote oldalra mutat:  
       `https://finance.yahoo.com/quote/<TICKER>/`
   - (Nem kell mindenre külön hosszan idézni, de a link ott legyen, hogy visszanézhető legyen.)

5) **Lefedettségi zárás**
   - A blokk végén kötelező sor:
     - „Earnings audit lefedettség: N/N ticker ellenőrizve; 7 napon belüli találat: K db.”

### Tiltások (hogy ne legyen több Netflix‑01.14 típusú félrecsúszás)
- TILOS: EarningsWhispers / MarketBeat / „estimated window” / historikus mintázat alapján dátumot mondani.
- TILOS: listán kívüli ticker bevonása („calendar dobta ki”) – csak Gist/Master univerzum.

# 📘 WEBBIBLIA v1.7.0
Web‑alapú #1 / #ALL‑IN jelentések kanonikus szabálykönyve

---

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

Verzió: **v1.7.0 – KANONIKUS**
