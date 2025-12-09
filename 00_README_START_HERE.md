# 00_README_START_HERE.md
DOKUMENTUM VERZIÓ: v3.4.0

# Részvények Projekt – BELÉPÉSI PONT
# Verzió: 1.1.0 (2025-12-05)

Ez a fájl a **projekt induló dokumentuma**.  
Ha új ChatGPT sessiont nyitsz, vagy 0-ról kell újraépíteni a működést,  
**ezt az egy fájlt kell megnyitni elsőként**, innen minden másra rá lehet csatlakozni.

---

# 🔵 1. A projekt célja

Automatizált, biblia‑szabályok szerinti #1 / #2 / #3 napi riportok generálása:

- After-hours + Premarket (#1)
- Előző napi open→close (#2)
- Mai open→most (#3)

A riportok a teljes portfólió + watchlist tickerkészletre futnak,  
szigorú szabályrendszer alapján.

---

# 🔵 2. A rendszer elemei (RÖVID ÖSSZEFOGLALÓ)

## 📁 Könyvtárak

- **scripts/** → minden futási logika  
- **reports/** → generált jelentések  
- **data/** → master-listák, watchlist, inputok  

## 🔧 Fő fájlok

| Fájl | Szerep |
|------|--------|
| **scripts/report_runner.py** | A #1/#2/#3 jelentések futtatója |
| **scripts/postprocess_report.py** | #1 jelentés makró + analyst + catalyst + high‑conv beépítése |
| **scripts/macro_fetcher.py** | Makró hírek lekérése a MACRO_FEED_URL_1/2/3 Apps Script webappból |
| **scripts/highconv_builder.py** | High‑conv JSON generátor |
| **scripts/biblia_helper.py** | Formázások, blokkok, %-logika, helper függvények |

## 📄 Dokumentumok

- **biblia_leiras.md** → a projekt *hivatalos szabálykönyve*  
- **CHANGELOG.md** → verzióváltozások

---

# 🔵 3. Hogyan generálódik a #1 jelentés? (Pipeline)

Ez a legfontosabb rész – ha bármi elromlik, ezt kövesd végig.

### 1️⃣ report_runner.py (mode=1)
- Lefedi a tickerlistát  
- Lekéri az árakat a Yahoo v8 chart API-ból  
- Meghívja a makró feedet:  
  `fetch_macro_text(report=1, out_path="reports/macro_1.txt", base_url_env="MACRO_FEED_URL_1")`  
- Összerakja az elsődleges vázat a summary_report_1.md-be

### 2️⃣ highconv_builder.py
- Elemzői feed → `high_conv_1.json`

### 3️⃣ analyst_feed_parser.py
- Elemzői minősítések → `analyst_1.json`

### 4️⃣ catalysts feed
- Apps Script / Webapp → `catalysts_1.json`

### 5️⃣ postprocess_report.py
Beépíti a summary_report_1.md végére a következő blokkokat:

- **Politika / FED / Makró** (macro_fetcher + webapp output alapján)  
- **Bejelentések & fel/lemínősítések**  
- **Közelgő katalizátorok**  
- **High‑conv blokk**  
- + törli a „Job summary generated at run‑time” sort

---

# 🔵 4. Szigorú szabályok (röviden)

## ⏰ Időablakok (#1)

- **After-hours (AH): 22:00–02:00 CEST**
- **Premarket (PM): 10:00–15:30 CEST**

## 📉 Mozgások számítása

- Yahoo v8 chart, range=2d, interval=5m, includePrePost=true  
- Formula: (lastPrice − previousClose) / previousClose × 100  
- Két tized, sáv jelölése: **(AH)** vagy **(PM)**

## 📌 Sorrend

1. Lefedettség blokk  
2. Makró blokk  
3. Darabszámos tickerek  
4. Watchlist (feltételes)  
5. Analyst blokk  
6. Katalizátor blokk  
7. High-conv blokk

---

# 🔵 5. Hogyan indítom a riportokat?

```bash
python scripts/report_runner.py --report 1
python scripts/report_runner.py --report 2
python scripts/report_runner.py --report 3
```

Vagy mindent egyszerre:

```bash
python scripts/report_runner.py --report all
```

---

# 🔵 6. Ha új ChatGPT sessiont nyitsz:

Egyszerűen ezt mondd:

> „Ez a Részvények projekt. Itt van a 00_README_START_HERE.md és a biblia_leiras.md. Innen folytasd a munkát.”

És minden működni fog.

---

# 🔵 7. Fájlok

- **00_README_START_HERE.md** (ez a dokumentum)
- **biblia_leiras.md**
- **CHANGELOG.md**
- **scripts/** (runner, postprocess, macro fetcher, highconv, helpers)
- **reports/** (#1/#2/#3 jelentések)
- **data/** (master, tickerek)

---

# Készen vagyunk ✔️
Ez a fájl mostantól a projekt **belépési pontja**.

## Új Python modulok a #1-es jelentéshez

- `scripts/analyst_block_builder.py` – az `reports/1/analyst_1.json` alapján felépíti a „Bejelentések & fel/lemínősítések” blokkot.
- `scripts/highconv_block_builder.py` – a `reports/1/high_conv_1.json` és `reports/1/catalysts_1.json` alapján felépíti a „Közelgő katalizátorok” és „High-conv jelöltek” blokkokat.
- `scripts/postprocess_report.py` – a nyers `reports/1/summary_report_1.md`-hez hozzáadja a makró + analyst + katalizátor + high-conv blokkokat, és eltávolítja a „Job summary generated at run-time …” sort.

A #1-es napi workflow-ban a `report_runner.py` után mindig fusson le:

```bash
python scripts/postprocess_report.py --md reports/1/summary_report_1.md --bundle-dir reports/1
```

Így a gist-re már a teljes, biblia szerinti #1-es jelentés kerül ki.


## 3.1 ChatGPT által generált #1 BIBLIA riport (hivatalos fallback)

Ha a #1-es jelentést a pipeline bármely része (runner, postprocess, gist írás) nem állítja elő
a BIBLIA szerinti formátumban, akkor ChatGPT képes automatikusan elkészíteni a teljes #1 riportot
a Gist RAW tartalmából.

### Hogyan kell kérni?

*Kérek egy teljes #1 bibliás jelentést erre a Gist linkre: <raw gist link>*

### A generált riport blokkjai:

1. Lefedettség
2. Politika / FED / Makró
3. Darabszámos tickerek (AH/PM)
4. Watchlist tickerek (AH/PM)
5. Bejelentések & fel/lemínősítések
6. Közeli katalizátorok
7. Listán kívüli high-conv (3–12 hó)

Ez a fallback akkor is működik, ha a scriptek hibásak vagy hiányos a summary_report_1.md.
