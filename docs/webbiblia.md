# 📘 WEBBIBLIA.md
## Webes hír- és kontextus-lekérdezési szabálykönyv (KANONIKUS)
*(AH/PM PRICE ENGINE script mellett, manuális / on-demand riportokhoz)*

---

## 0️⃣ Alapfilozófia (nem alkuképes)

A webbiblia célja **nem a hírfogyasztás**, hanem a **szűrt, időfegyelmezett döntéstámogatás**.

- ❌ nincs szellemhír
- ❌ nincs beragadt (2–3 napos) adat
- ❌ nincs találgatás
- ❌ nincs „hátha érdekes” zaj

- ✅ időablak-fegyelem
- ✅ forrás-hierarchia
- ✅ ellenőrzött kontextus
- ✅ döntést segítő összkép

> **Ami az adott időablakon kívül van → nem létezik.**

---

## 1️⃣ Riporttípusok

### `#1` — After-hours + Premarket
- Ár: **PRICE ENGINE script**
- Hírek és kontextus: **web**
- Cél: *mi történt zárás után / nyitás előtt, és milyen hangulatba nyitunk bele*

### `#1 all-in`
- A teljes kanonikus struktúra
- Üres blokkok **megmaradnak** (ellenőrzés bizonyítása)

### `#2`
- Előző kereskedési nap (Open → Close)
- ≥±3% mozgások **okkal**

### `#3`
- Mai Open → Most
- ≥±3% mozgások **friss katalizátorral**

---

## 2️⃣ Időintervallum-szabályok (KRITIKUS)

### Aranyszabály
**Minden hírnél ellenőrizni kell a publikálási időt.**
Ha nem esik bele az adott riport időablakába → **kizárás**.

### Időablakok
- `#1`: utolsó tényleges piaczárás → lekérdezés pillanata
- `#2`: adott nap Open → Close
- `#3`: mai Open → Most

---

## 3️⃣ Forrás-hierarchia (mi számít igaznak)

1. SEC EDGAR (8-K, 6-K, 10-Q, 10-K)
2. Company IR / Newsroom
3. Reuters / AP
4. Bloomberg / Dow Jones / The Fly
5. MarketBeat (elemzői lépések – kanonikus)
6. **Yahoo Finance (aggregált, kontextus-jellegű)**
7. StreetInsider
8. TipRanks (csak ellenőrzésre)

---

## 4️⃣ Szellemhír-tilalom (ZERO TOLERANCE)

Kizárva:
- időablakon kívüli publikáció
- régi earnings újramelegítése
- „korábban azt mondta” típusú visszautalás
- aggregátor által újracímkézett régi sztori

Ha nem friss → **nem kerül be**.

---

## 5️⃣ Elemzői lépések — kötelező forma

Elemzői lépés **csak akkor jelenhet meg**, ha:
- időablakon belüli
- egyértelmű **from → to** van

Példák:
- Buy → Hold
- PT: 155 $ → 180 $

Aggregátor (Yahoo) **nem** kanonikus elemzőforrás.

---

## 6️⃣ Makró / piaci hangulat blokk (LAZÍTOTT, DE SZABÁLYOZOTT)

### A) Friss makró / FED / politikai hír
- csak ha időablakos
- csak ha tényleges piaci hatása van

### B) Általános piaci hangulat (előző kereskedési nap)
Megengedett:
- indexek iránya
- hozamok iránya
- USD / olaj / BTC irány
- risk-on / risk-off érzet

### C) Várható mai nyitás
- futures / overnight irány
- indikáció, nem jóslat

---

## 7️⃣ Kontextus-jellegű hírek (Yahoo Finance)

### Mikor használható?
- ha nincs egyetlen kanonikus headline
- de a piac érthetően áraz (szektor / hozam / sentiment)
- publikáció időablakon belüli

### Hogyan jelenik meg?
Yahoo **soha nem tényként**, hanem **kontektsusként**.

---

## 8️⃣ Hírkeresési szűrők (RÉSZLETES)

1. Időszűrő — időablakon belül?
2. Típus — filing / IR / analyst / makró / kontextus?
3. Relevancia — ármozgással összeegyeztethető?
4. Forrás — kanonikus vagy kiegészítő?
5. Duplikáció — már szerepelt korábban?

---

## 9️⃣ Közelgő katalizátorok

Earnings, regulatory, event — ha nincs, explicit jelezni.

---

## 🔟 Listán kívüli high-conviction

### 1–3 hónap
- konkrét katalizátor
- ≥2 megerősítő jelzés

### 3–6 hónap
- strukturális sztori
- elemzői konvergencia

---

## 1️⃣1️⃣ `#1 all-in` jelentése
Teljes struktúra, üres blokkokkal (ellenőrzési bizonyíték).

---

## 1️⃣2️⃣ Amit soha nem kapsz
Intraday max/min, pletyka, találgatás, régi hír.

---

## 1️⃣3️⃣ Meta-szabály
A webbiblia szűrő, nem hírolvasó.
