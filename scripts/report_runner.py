import argparse, sys, math, time, json
from pathlib import Path
from datetime import datetime, timedelta, time as dtime
import pandas as pd
import requests
import yfinance as yf
import pytz
import feedparser
from bs4 import BeautifulSoup

# --- Kódolás / TZ-ek --------------------------------------------------------
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TZ_NY   = pytz.timezone("America/New_York")
TZ_CET  = pytz.timezone("Europe/Budapest")  # riport-időbélyegek
OPEN_T  = dtime(9, 30)
CLOSE_T = dtime(16, 0)

SKIP_TICKERS = {"PKN.WA"}  # kérés szerint

# CSV oszlop-heurisztika
TICKER_COLS = ["ticker","symbol","tiker","tic","name","név","papír"]
QTY_COLS    = ["qty","quantity","db","darab","darabszam","darabszám","shares","pcs","mennyiseg","mennyiség"]
K_COLS      = ["k","min_move_pct","min_intraday_pct","min_pct"]
L_COLS      = ["l","vol_mult","unusual_vol_mult","volx"]
M_COLS      = ["m","max_dist_52w_pct","dist_52w_pct","dist_pct"]

# --- Utilok ----------------------------------------------------------------
def pct(a, b):
    try:
        if a is None or b is None or a == 0: return None
        return (b - a) / a * 100.0
    except Exception:
        return None

def _find_col(df, cand):
    cols = {c.lower(): c for c in df.columns}
    for k in cand:
        if k in cols: return cols[k]
    return None

def read_rows(csv_path, default_k=3.0, default_l=2.0, default_m=1.0):
    df = pd.read_csv(csv_path)
    tcol = _find_col(df, TICKER_COLS) or df.columns[0]
    qcol = _find_col(df, QTY_COLS)
    kcol = _find_col(df, K_COLS)
    lcol = _find_col(df, L_COLS)
    mcol = _find_col(df, M_COLS)

    out, seen = [], set()
    for _, r in df.iterrows():
        sym = str(r[tcol]).strip().upper()
        if not sym or sym == "NAN" or sym in SKIP_TICKERS: continue
        if sym in seen: continue
        seen.add(sym)

        qty = None
        if qcol and pd.notna(r.get(qcol)):
            try: qty = float(r[qcol])
            except: qty = None

        K = default_k
        if kcol and pd.notna(r.get(kcol)):
            try: K = float(r[kcol])
            except: K = default_k

        L = default_l
        if lcol and pd.notna(r.get(lcol)):
            try: L = float(r[lcol])
            except: L = default_l

        M = default_m
        if mcol and pd.notna(r.get(mcol)):
            try: M = float(r[mcol])
            except: M = default_m

        out.append({"symbol": sym, "qty": qty, "K": K, "L": L, "M": M})
    return out

def _tz(df, tz):
    if df.empty: return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(tz)
    else:
        df.index = df.index.tz_convert(tz)
    return df

def _pick_open(df):
    # 09:30 bar vagy első >=09:30 (max +3 perc tolerancia)
    if df.empty: return None
    exact = df[df.index.time == OPEN_T]
    if not exact.empty: return float(exact.iloc[0]["Open"])
    later = df[df.index.time >= OPEN_T]
    if not later.empty:
        i0 = later.index[0]
        ref = i0.replace(hour=9, minute=30, second=0, microsecond=0)
        if (i0 - ref).total_seconds() <= 180:
            return float(later.iloc[0]["Open"])
    return None

# --- Áradatok ---------------------------------------------------------------
def fetch_today_open_now(sym):
    t = yf.Ticker(sym)
    # 1m today
    h = t.history(period="1d", interval="1m", prepost=False, actions=False)
    open_px = last_px = high_px = low_px = None
    if h is not None and not h.empty:
        h = _tz(h, TZ_NY)
        today = datetime.now(TZ_NY).date()
        df = h[h.index.date == today]
        if not df.empty:
            open_px = _pick_open(df)
            last_px = float(df["Close"].iloc[-1])
            high_px = float(df["High"].max())
            low_px  = float(df["Low"].min())
    # fallback 5m
    if open_px is None or last_px is None:
        h5 = t.history(period="5d", interval="5m", prepost=False, actions=False)
        if h5 is not None and not h5.empty:
            h5 = _tz(h5, TZ_NY)
            today = datetime.now(TZ_NY).date()
            df5 = h5[h5.index.date == today]
            if not df5.empty:
                if open_px is None: open_px = _pick_open(df5)
                if last_px is None: last_px = float(df5["Close"].iloc[-1])
                if high_px is None: high_px = float(df5["High"].max())
                if low_px  is None: low_px  = float(df5["Low"].min())
    # previous close
    prev = None
    try:
        q = requests.get("https://query1.finance.yahoo.com/v7/finance/quote",
                         params={"symbols": sym}, headers={"User-Agent":"Mozilla/5.0"}, timeout=10
                        ).json()["quoteResponse"]["result"][0]
        if prev is None: prev = q.get("regularMarketPreviousClose") or None
        if last_px is None: last_px = q.get("regularMarketPrice") or None
        if open_px is None: open_px = q.get("regularMarketOpen") or None
    except Exception:
        pass
    if prev is None:
        try:
            fi = getattr(t, "fast_info", None)
            prev = getattr(fi, "previous_close", None) if fi else None
        except Exception:
            prev = None
    return {
        "open": open_px,
        "last": last_px,
        "open_to_now_pct": None if open_px in (None,0) or last_px is None else round(pct(open_px, last_px), 2),
        "prev_to_now_pct": None if prev in (None,0) or last_px is None else round(pct(prev, last_px), 2),
        "high_over_open_pct": None if open_px in (None,0) or high_px is None else round(pct(open_px, high_px), 2),
        "low_over_open_pct":  None if open_px in (None,0) or low_px  is None else round(pct(open_px, low_px), 2),
        "error": None if (open_px is not None and last_px is not None) else "no_price_data"
    }

def fetch_prev_open_close(sym, prev_trading_date):
    # Előző kereskedési nap Open→Close
    t = yf.Ticker(sym)
    h = t.history(start=prev_trading_date, end=prev_trading_date + timedelta(days=1),
                  interval="1m", prepost=False, actions=False)
    open_px = close_px = None
    if h is not None and not h.empty:
        h = _tz(h, TZ_NY)
        d = h[h.index.date == prev_trading_date]
        if not d.empty:
            open_px = _pick_open(d)
            # close a legutolsó 16:00 körüli bar close-a
            lastbars = d[d.index.time <= CLOSE_T]
            if not lastbars.empty:
                close_px = float(lastbars["Close"].iloc[-1])
    return {
        "open": open_px,
        "close": close_px,
        "open_to_close_pct": None if open_px in (None,0) or close_px is None else round(pct(open_px, close_px), 2),
        "error": None if (open_px is not None and close_px is not None) else "no_price_data"
    }

def fetch_window_change(sym, start_cet: dtime, end_cet: dtime):
    """#1 ablak: CEST/CET szerint ± időtartam (AH 22:00–02:00, PM 10:00–15:30).
       prepost=True perces adatokkal, CEST/CET-re konvertálva, a sáv első és utolsó Close alapján.
    """
    t = yf.Ticker(sym)
    h = t.history(period="2d", interval="1m", prepost=True, actions=False)
    if h is None or h.empty:
        return {"chg_pct": None, "error": "no_1m_prepost"}
    h = _tz(h, TZ_CET)  # szűréshez CET-ben
    # ablak áthúzhat éjfélen (22:00→02:00)
    now_cet = datetime.now(TZ_CET)
    today = now_cet.date()
    prev = today - timedelta(days=1)

    # készítsünk két szettet: tegnap és ma e sávra vetítve
    def extract(date):
        sdt = datetime.combine(date, start_cet, tzinfo=TZ_CET)
        edt = datetime.combine(date, end_cet, tzinfo=TZ_CET)
        if end_cet < start_cet:  # áthúz éjfélen
            edt += timedelta(days=1)
        return h[(h.index >= sdt) and (h.index <= edt)]

    df = extract(prev) if start_cet > end_cet else pd.DataFrame()
    df2 = extract(today)
    win = pd.concat([df, df2]).sort_index()
    if win.empty:
        return {"chg_pct": None, "error": "no_bars_in_window"}
    first = float(win["Close"].iloc[0]); last = float(win["Close"].iloc[-1])
    return {"chg_pct": round(pct(first, last), 2), "error": None}

# --- SEC CIK map ------------------------------------------------------------
_SEC_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
def load_sec_map():
    try:
        r = requests.get(_SEC_MAP_URL, headers={"User-Agent":"Részvények-bot (contact@example.com)"}, timeout=20)
        j = r.json()
        # formátum: {"0":{"cik_str":..., "ticker":"A","title":"Agilent"}, ...}
        out = {}
        for _, v in j.items():
            out[v["ticker"].upper()] = str(v["cik_str"]).zfill(10)
        return out
    except Exception:
        return {}

def fetch_sec_atom(cik):
    try:
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&owner=exclude&count=100&output=atom"
        r = requests.get(url, headers={"User-Agent":"Részvények-bot (contact@example.com)"}, timeout=20)
        return feedparser.parse(r.text)
    except Exception:
        return None

# --- Hír-aggregátor ---------------------------------------------------------
REUTERS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/USBusinessNews"
]
AP_FEEDS = [
    # AP RSS elérhetőség ingadozó; ha nem jön, csendben kihagyjuk
    "https://apnews.com/hub/apf-topnews?utm_source=apnews.com&utm_medium=referral&utm_campaign=ap_rss"
]
IR_PR_FEEDS = [
    "https://www.businesswire.com/portal/site/home/news/subject/?vnsId=31328&rss=1",  # Tech BW
    "https://www.prnewswire.com/rss/technology-latest-news.rss",
    "https://www.globenewswire.com/RssFeed/sector-technology.xml"
]
def yahoo_ticker_feed(sym):  # always available fallback per ticker
    return f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={sym}&region=US&lang=en-US"

def _parse_feed(url):
    try:
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=20)
        return feedparser.parse(r.text)
    except Exception:
        return None

def _strip_html(text):
    try:
        return BeautifulSoup(text or "", "html.parser").get_text(" ", strip=True)
    except Exception:
        return text or ""

def _in_window(dt_obj, start_dt, end_dt):
    if dt_obj is None: return False
    return (dt_obj >= start_dt) and (dt_obj <= end_dt)

def _parse_dt(e):
    for key in ("published_parsed","updated_parsed","created_parsed"):
        t = e.get(key)
        if t:  # time.struct_time
            try:
                dt = datetime.fromtimestamp(time.mktime(t), tz=pytz.UTC).astimezone(TZ_CET)
                return dt
            except Exception:
                pass
    return None

def company_names(sym):
    try:
        t = yf.Ticker(sym)
        nm = []
        info = getattr(t, "info", {}) or {}
        longName = info.get("longName") or info.get("shortName")
        if longName:
            nm.append(longName)
            # gyakori toldások
            nm.append(longName.replace(", Inc.","").replace(" Inc.","").replace(" Corporation",""))
        nm.append(sym)
        return [n for n in set([x for x in nm if x])]
    except Exception:
        return [sym]

def collect_news_for(sym, window_start: datetime, window_end: datetime, cik_map):
    names = company_names(sym)
    hits = []

    # A) Reuters
    for feed in REUTERS_FEEDS:
        fp = _parse_feed(feed)
        if not fp or not fp.entries: continue
        for e in fp.entries:
            dt = _parse_dt(e)
            if not _in_window(dt, window_start, window_end): continue
            title = (e.get("title") or "").strip()
            summ  = _strip_html(e.get("summary") or "")
            text  = (title + " " + summ).upper()
            if any(n.upper() in text for n in names):
                hits.append(("Reuters", title, dt, e.get("link") or ""))

    # B) AP
    for feed in AP_FEEDS:
        fp = _parse_feed(feed)
        if not fp or not fp.entries: continue
        for e in fp.entries:
            dt = _parse_dt(e)
            if not _in_window(dt, window_start, window_end): continue
            title = (e.get("title") or "").strip()
            summ  = _strip_html(e.get("summary") or "")
            text  = (title + " " + summ).upper()
            if any(n.upper() in text for n in names):
                hits.append(("AP", title, dt, e.get("link") or ""))

    # C) IR/PR-wire (BusinessWire/PRNewswire/GlobeNewswire)
    for feed in IR_PR_FEEDS:
        fp = _parse_feed(feed)
        if not fp or not fp.entries: continue
        for e in fp.entries:
            dt = _parse_dt(e)
            if not _in_window(dt, window_start, window_end): continue
            title = (e.get("title") or "").strip()
            summ  = _strip_html(e.get("summary") or "")
            text  = (title + " " + summ).upper()
            if any(n.upper() in text for n in names):
                hits.append(("IR/PR", title, dt, e.get("link") or ""))

    # D) SEC EDGAR (ha van CIK)
    cik = cik_map.get(sym)
    if cik:
        fp = fetch_sec_atom(cik)
        if fp and fp.entries:
            for e in fp.entries:
                dt = _parse_dt(e)
                if not _in_window(dt, window_start, window_end): continue
                title = (e.get("title") or "").strip()
                hits.append(("SEC", title, dt, e.get("link") or ""))

    # E) Yahoo Finance ticker-RSS (fallback, ticker-specifikus)
    fp = _parse_feed(yahoo_ticker_feed(sym))
    if fp and fp.entries:
        for e in fp.entries:
            dt = _parse_dt(e)
            if not _in_window(dt, window_start, window_end): continue
            title = (e.get("title") or "").strip()
            hits.append(("YahooFin", title, dt, e.get("link") or ""))

    # deduplikáció cím alapján
    uniq = {}
    for src, title, dt, link in hits:
        k = (title or "").strip()
        if k not in uniq: uniq[k] = (src, title, dt, link)
    # forrás-sorrend (kanonikus): Reuters/AP → SEC/IR/PR → Yahoo
    prio = {"Reuters":1,"AP":1,"SEC":2,"IR/PR":2,"YahooFin":3}
    out = sorted(uniq.values(), key=lambda x: (prio.get(x[0], 9), x[2] or datetime.min))
    return out

# --- Jelentések -------------------------------------------------------------
def write_summary(md_path, text):
    try:
        with open(md_path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except Exception:
        pass

def fmt_pct(v):
    if v is None or (isinstance(v,float) and math.isnan(v)): return "–"
    return f"{v:.2f}%"

def section_bejelentesek(tickers, news_map):
    lines = []
    lines.append("\n### Bejelentések & fel/lemínősítések")
    any_line = False
    for sym, qty in tickers:
        items = news_map.get(sym, [])
        if not items: 
            if qty is not None:  # darabszámosnál mindent listázunk (akár üres sort nem írunk ki)
                continue
            else:
                continue
        any_line = True
        for src, title, dt, link in items:
            when = dt.astimezone(TZ_CET).strftime("%Y-%m-%d %H:%M")
            lines.append(f"- **{sym}** — {src} — {title} — {when} CEST")
    if not any_line:
        lines.append("_(nincs releváns bejegyzés a vizsgált ablakban)_")
    return lines

def report_1(rows, override_pct):
    """#1 – AH (22:00–02:00 CEST) + PM (10:00–15:30 CEST)."""
    # Darabszámos / WL
    pos = [(r["symbol"], r["qty"]) for r in rows if r["qty"] not in (None, 0)]
    wl  = [(r["symbol"], r["qty"]) for r in rows if r["qty"] in (None, 0)]

    # Árak (ablakok CEST)
    AH_S = dtime(22,0); AH_E = dtime(2,0)
    PM_S = dtime(10,0); PM_E = dtime(15,30)

    # SEC map  (egyben töltjük le)
    cik_map = load_sec_map()

    # Hírablakok (CET)
    now_cet = datetime.now(TZ_CET)
    ah_start = now_cet.replace(hour=22, minute=0, second=0, microsecond=0)
    ah_end   = (ah_start + timedelta(days=1)).replace(hour=2, minute=0)  # áthúz éjfél
    pm_start = now_cet.replace(hour=10, minute=0, second=0, microsecond=0)
    pm_end   = now_cet.replace(hour=15, minute=30, second=0, microsecond=0)

    price_rows = []
    news_map = {}

    for r in rows:
        sym = r["symbol"]
        # árak
        ah = fetch_window_change(sym, AH_S, AH_E)
        pm = fetch_window_change(sym, PM_S, PM_E)
        price_rows.append((sym, r["qty"], ah, pm, r["K"]))
        # hírek (egyszer gyűjtjük és újra felhasználjuk)
        news_map[sym] = collect_news_for(sym, ah_start, pm_end, cik_map)

    # összefoglaló
    lines = []
    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST")
    # lefedettség (ha bármelyik ár-ablak totál üres volt)
    missing = [sym for sym,_,ah,pm,_ in price_rows if (ah["error"] and pm["error"])]
    if missing:
        lines.append(f"**Lefedettség:** HIÁNYOS – nem elérhető árablak: {', '.join(missing)}")
    else:
        lines.append("**Lefedettség:** TELJES")

    # Darabszámos előre
    def fmt_line(sym, qty, ah, pm, K):
        chunks = []
        if ah["chg_pct"] is not None and abs(ah["chg_pct"]) >= float(K):
            chunks.append(f"AH {ah['chg_pct']:.2f}%")
        if pm["chg_pct"] is not None and abs(pm["chg_pct"]) >= float(K):
            chunks.append(f"PM {pm['chg_pct']:.2f}%")
        if not chunks:
            return None
        return f"- **{sym}** — " + " / ".join(chunks) + (" · (POS)" if qty else "")

    lines.append("\n### Darabszámos tickerek (≥ K%)")
    any_pos = False
    for sym, qty, ah, pm, K in price_rows:
        if qty:
            s = fmt_line(sym, qty, ah, pm, K)
            if s: lines.append(s); any_pos = True
    if not any_pos:
        lines.append("_nincs_")

    lines.append("\n### Watchlist (≥ K% vagy lényeges hír)")
    any_wl = False
    for sym, qty, ah, pm, K in price_rows:
        if qty: continue
        s = fmt_line(sym, qty, ah, pm, K)
        important_news = news_map.get(sym)
        if s:
            lines.append(s); any_wl = True
        elif important_news:  # ha nincs ≥K%, de van hír, akkor is listázzuk
            lines.append(f"- **{sym}** — releváns hír az ablakban"); any_wl = True
    if not any_wl:
        lines.append("_nincs_")

    # Bejelentések & fel/lemínősítések blokk
    lines += section_bejelentesek(pos + wl, news_map)

    return "\n".join(lines), news_map

def previous_trading_day(date_ny):
    # Buta közelítés: visszalépünk 1 nappal, ha hétvége, lépkedünk tovább
    d = date_ny - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d

def report_2(rows):
    """#2 – Előző kereskedési nap Open→Close (15:30–22:00 CEST)."""
    pos = [(r["symbol"], r["qty"]) for r in rows if r["qty"] not in (None, 0)]
    wl  = [(r["symbol"], r["qty"]) for r in rows if r["qty"] in (None, 0)]

    now_ny = datetime.now(TZ_NY)
    prev_day = previous_trading_day(now_ny.date())

    res = []
    missing = []
    for r in rows:
        sym = r["symbol"]
        m = fetch_prev_open_close(sym, prev_day)
        if m["error"]:
            missing.append(sym)
        res.append((sym, r["qty"], r["K"], m))

    lines = []
    lines.append("## #2 – Tegnapi Open→Close (15:30–22:00 CEST)")
    if missing:
        lines.append(f"**Lefedettség:** HIÁNYOS – hiányos ár: {', '.join(missing)}")
    else:
        lines.append("**Lefedettség:** TELJES")

    # Jelzések (≥K%)
    lines.append("\n### Darabszámos tickerek (abs(Open→Close) ≥ K):")
    any_pos = False
    for sym, qty, K, m in res:
        if not qty: continue
        ch = m["open_to_close_pct"]
        if ch is not None and abs(ch) >= float(K):
            lines.append(f"- **{sym}** — Open→Close: **{ch:.2f}%** · (POS)")
            any_pos = True
    if not any_pos:
        lines.append("_nincs_")

    lines.append("\n### Watchlist (abs(Open→Close) ≥ K):")
    any_wl = False
    for sym, qty, K, m in res:
        if qty: continue
        ch = m["open_to_close_pct"]
        if ch is not None and abs(ch) >= float(K):
            lines.append(f"- **{sym}** — Open→Close: **{ch:.2f}%**")
            any_wl = True
    if not any_wl:
        lines.append("_nincs_")

    # #2-hez most rövid (opcionális) hírgyűjtést nem futtatunk alapból – kérésre be tudjuk kapcsolni
    return "\n".join(lines)

def report_3(rows, override_pct):
    """#3 – Ma Open→Most (15:30→most CEST) + opcionális napi extrém blokk."""
    pos = [(r["symbol"], r["qty"]) for r in rows if r["qty"] not in (None, 0)]
    wl  = [(r["symbol"], r["qty"]) for r in rows if r["qty"] in (None, 0)]

    results, missing = [], []
    for r in rows:
        sym = r["symbol"]
        m = fetch_today_open_now(sym)
        if m["error"]:
            missing.append(sym)
        results.append((sym, r["qty"], r["K"], m))

    # táblák
    lines = []
    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most)")
    if missing:
        lines.append(f"**Lefedettség:** HIÁNYOS – ár hiányzik: {', '.join(missing)}")
    else:
        lines.append("**Lefedettség:** TELJES")

    # fő szűrő
    lines.append("\n### Jelzések (abs(Open→Most) ≥ K):")
    any_main = False
    for sym, qty, K, m in results:
        ch = m["open_to_now_pct"]
        if ch is not None and abs(ch) >= float(K):
            lines.append(f"- **{sym}** — Open→Most: **{ch:.2f}%**" + (" · (POS)" if qty else ""))
            any_main = True
    if not any_main:
        lines.append("_nincs_")

    # override (PrevClose→Now)
    if override_pct and float(override_pct) > 0:
        lines.append(f"\n### Napi extrém (abs(PrevClose→Most) ≥ {float(override_pct):.2f}%):")
        any_ovr = False
        for sym, qty, K, m in results:
            ch = m["prev_to_now_pct"]
            if ch is not None and abs(ch) >= float(override_pct) and not (m["open_to_now_pct"] and abs(m["open_to_now_pct"]) >= float(K)):
                lines.append(f"- **{sym}** — PrevClose→Most: **{ch:.2f}%**" + (" · (POS)" if qty else ""))
                any_ovr = True
        if not any_ovr:
            lines.append("_nincs_")

    return "\n".join(lines)

# --- Main -------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="3")  # "1" | "2" | "3"
    ap.add_argument("--csv", required=True)
    ap.add_argument("--summary", default="")
    ap.add_argument("--override-pct", type=float, default=8.0)
    ap.add_argument("--default-k", type=float, default=3.0)
    ap.add_argument("--default-l", type=float, default=2.0)
    ap.add_argument("--default-m", type=float, default=1.0)
    args = ap.parse_args()

    reports = Path("reports"); reports.mkdir(parents=True, exist_ok=True)

    rows = read_rows(args.csv, args.default_k, args.default_l, args.default_m)
    if not rows:
        msg = "Nincs ticker a CSV-ben."
        print(msg); 
        if args.summary: write_summary(args.summary, f"**Hiba:** {msg}")
        sys.exit(1)

    # Jelentés kiválasztás
    rep = str(args.report).strip()
    if rep == "1":
        text, news_map = report_1(rows, args.override_pct)
        # hír export (ha kell böngészni később)
        # laposított CSV
        news_rows = []
        for sym in [r["symbol"] for r in rows]:
            for src, title, dt, link in news_map.get(sym, []):
                news_rows.append([sym, src, title, dt.strftime("%Y-%m-%d %H:%M"), link])
        if news_rows:
            pd.DataFrame(news_rows, columns=["symbol","source","title","time_cest","link"]).to_csv(reports/"news_report1.csv", index=False)
        with open(reports/"summary_report1.md","w",encoding="utf-8") as f: f.write(text)
        print(text)
        if args.summary: write_summary(args.summary, text)

    elif rep == "2":
        text = report_2(rows)
        with open(reports/"summary_report2.md","w",encoding="utf-8") as f: f.write(text)
        print(text)
        if args.summary: write_summary(args.summary, text)

    else:  # "3"
        text = report_3(rows, args.override_pct)
        with open(reports/"summary_report3.md","w",encoding="utf-8") as f: f.write(text)
        print(text)
        if args.summary: write_summary(args.summary, text)

if __name__ == "__main__":
    main()
