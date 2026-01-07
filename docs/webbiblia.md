# 📘 WEBBIBLIA v1.6.0
Web‑alapú #1 / #ALL‑IN jelentések kanonikus szabálykönyve

---

## 0️⃣ Hogyan használd ezt a dokumentumot

Ez a dokumentum a **webes forrásokra épülő jelentéskészítés kanonikus szabálykönyve**.
Célja nem az, hogy minden ármozgást megmagyarázzon, hanem hogy:

> **SEMMI FONTOS NE MARADJON KI**,  
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

Verzió: **v1.6.0 – KANONIKUS REBUILD**
