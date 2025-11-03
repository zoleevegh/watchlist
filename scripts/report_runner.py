import argparse, sys, math, time
from pathlib import Path
from datetime import datetime, timedelta, time as dtime
import pandas as pd
import requests
import yfinance as yf
import pytz
import feedparser
from bs4 import BeautifulSoup

# --- Encoding / TZ ----------------------------------------------------------
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TZ_NY  = pytz.timezone("America/New_York")
TZ_CET = pytz.timezone("Europe/Budapest")
OPEN_T, CLOSE_T = dtime(9,30), dtime(16,0)

SKIP_TICKERS = {"PKN.WA"}  # kérés szerint

# CSV oszlopnevek
TICKER_COLS = ["ticker","symbol","tiker","tic","name","név","papír"]
QTY_COLS    = ["qty","quantity","db","darab","darabszam","darabszám","shares","pcs","mennyiseg","mennyiség"]
K_COLS      = ["k","min_move_pct","min_intraday_pct","min_pct"]
L_COLS      = ["l","vol_mult","unusual_vol_mult","volx"]
M_COLS      = ["m","max_dist_52w_pct","dist_52w_pct","dist_pct"]

# --- HTTP session -----------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})
HTTP_TIMEOUT = 10

# --- Gyorsított „fordító”: csak rövidítés ----------------------------------
def translate_hu(text: str, max_len: int = 200) -> str:
    """Nem fordít, csak rövidíti az angol címet, hogy 1 sorban maradjon."""
    base = str(text or "").strip()
    if len(base) > max_len:
        base = base[:max_len].rsplit(" ", 1)[0] + "…"
    return base

# --- Szentiment -------------------------------------------------------------
POS_WORDS = [
    "UPGRADE", "UPGRADED", "RAISE PRICE TARGET", "RAISED PRICE TARGET",
    "BEAT", "BEATS", "TOPS", "SURGE", "JUMP", "JUMPS", "SOARS",
    "RALLY", "RALLIES", "OUTPERFORM", "OVERWEIGHT", "BUY RATING", "INITIATED WITH BUY"
]
NEG_WORDS = [
    "DOWNGRADE", "DOWNGRADED", "CUT PRICE TARGET", "CUTS PRICE TARGET",
    "MISS", "MISSES", "MISSED", "PLUNGE", "PLUNGES", "SINKS", "TUMBLES",
    "SLUMP", "SLUMPS", "UNDERPERFORM", "UNDERWEIGHT", "SELL RATING"
]

def classify_sentiment_en(text: str) -> str:
    """Egyszerű szentiment-angol cím alapján: pozitív / negatív / semleges."""
    t = (text or "").upper()
    pos = any(w in t for w in POS_WORDS)
    neg = any(w in t for w in NEG_WORDS)
    if pos and not neg:
        return "pozitív"
    if neg and not pos:
        return "negatív"
    return "semleges"

# --- Segédfüggvények --------------------------------------------------------
def pct(a,b):
    try:
        if a is None or b is None or a == 0: return None
        return (b-a)/a*100.0
    except Exception:
        return None

def _find_col(df, cands):
    m = {c.lower(): c for c in df.columns}
    for c in cands:
        if c in m: return m[c]
    return None

def read_rows(csv_path, Kd=3.0, Ld=2.0, Md=1.0):
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
        K = Kd
        if kcol and pd.notna(r.get(kcol)):
            try: K = float(r[kcol])
            except: K = Kd
        L = Ld
        if lcol and pd.notna(r.get(lcol)):
            try: L = float(r[lcol])
            except: L = Ld
        M = Md
        if mcol and pd.notna(r.get(mcol)):
            try: M = float(r[mcol])
            except: M = Md
        out.append({"symbol":sym,"qty":qty,"K":K,"L":L,"M":M})
    return out

def _tz(df, tz):
    if df.empty: return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(tz)
    else:
        df.index = df.index.tz_convert(tz)
    return df

def _pick_open(df):
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

def previous_trading_day(date_ny):
    d = date_ny - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d

# --- Fetch Prices (egyesített AH+PM) ---------------------------------------
def fetch_ah_pm_changes(sym):
    try:
        now_ny = datetime.now(TZ_NY)
        last_td = previous_trading_day(now_ny.date())
        t = yf.Ticker(sym)
        h = t.history(
            start=last_td - timedelta(days=1),
            end=last_td + timedelta(days=2),
            interval="1m",
            prepost=True,
            actions=False
        )
        if h is None or h.empty:
            return {"ah": {"chg_pct": None}, "pm": {"chg_pct": None}}
        h = _tz(h, TZ_NY)

        prev_day = last_td
        next_day = last_td + timedelta(days=1)
        rth = h[(h.index.date == prev_day) & (h.index.time <= CLOSE_T)]
        if rth.empty:
            return {"ah": {"chg_pct": None}, "pm": {"chg_pct": None}}
        prev_close = float(rth["Close"].iloc[-1])

        def win_chg(start_dt, end_dt):
            win = h[(h.index >= start_dt) & (h.index <= end_dt)]
            if win.empty: return None
            return round(pct(prev_close, float(win["Close"].iloc[-1])), 2)

        ah_start = TZ_NY.localize(datetime.combine(prev_day, CLOSE_T))
        ah_end   = ah_start + timedelta(hours=4)
        pm_start = TZ_NY.localize(datetime.combine(next_day, dtime(4,0)))
        pm_end   = TZ_NY.localize(datetime.combine(next_day, dtime(9,30)))

        return {"ah": {"chg_pct": win_chg(ah_start, ah_end)},
                "pm": {"chg_pct": win_chg(pm_start, pm_end)}}
    except Exception:
        return {"ah": {"chg_pct": None}, "pm": {"chg_pct": None}}

# --- News Feeds -------------------------------------------------------------
REUTERS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/USBusinessNews",
]
IR_PR_FEEDS = [
    "https://www.businesswire.com/portal/site/home/news/subject/?vnsId=31328&rss=1",
    "https://www.prnewswire.com/rss/technology-latest-news.rss",
    "https://www.globenewswire.com/RssFeed/sector-technology.xml",
]
def yahoo_ticker_feed(sym): return f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={sym}&region=US&lang=en-US"

def _parse_feed(url):
    try:
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        return feedparser.parse(r.text)
    except Exception:
        return None

def _strip_html(t): 
    try: return BeautifulSoup(t or "", "html.parser").get_text(" ", strip=True)
    except Exception: return t or ""

def _dt(e):
    for k in ("published_parsed","updated_parsed","created_parsed"):
        v = e.get(k)
        if v:
            return datetime.fromtimestamp(time.mktime(v), tz=pytz.UTC).astimezone(TZ_CET)
    return None

def collect_news_for(sym, window_start, window_end):
    hits = []
    for feed in REUTERS_FEEDS + IR_PR_FEEDS + [yahoo_ticker_feed(sym)]:
        fp = _parse_feed(feed)
        if not fp or not fp.entries: continue
        for e in fp.entries:
            dt = _dt(e)
            if not dt or dt < window_start or dt > window_end: continue
            title = (e.get("title") or "").strip()
            summ  = _strip_html(e.get("summary") or "")
            text  = (title + " " + summ).upper()
            if sym.upper() in text:
                hits.append((feed.split('/')[2], title, dt))
    seen, out = set(), []
    for src, title, dt in hits:
        if title not in seen:
            out.append((src, title, dt))
            seen.add(title)
    out.sort(key=lambda x: x[2])
    return out

def short_news_reason(sym, news_map, chg_for_sent=None):
    items = news_map.get(sym) or []
    if items:
        src, title, dt = items[-1]
        sent = classify_sentiment_en(title)
        short = translate_hu(title)
        return f"[{sent}] {short}"
    if chg_for_sent is not None:
        sent = "pozitív" if chg_for_sent > 0 else "negatív"
        return f"[{sent}] nagyobb elmozdulás az AH/PM sávban, hír nélkül – valószínű szektorhatás."
    return ""

# --- Jelentés 1 -------------------------------------------------------------
def report_1(rows):
    AH_S, AH_E = dtime(22,0), dtime(2,0)
    PM_S, PM_E = dtime(10,0), dtime(15,30)
    now_cet = datetime.now(TZ_CET)
    ah_start = now_cet.replace(hour=22, minute=0)
    ah_end   = (ah_start + timedelta(days=1)).replace(hour=2, minute=0)
    pm_start = now_cet.replace(hour=10, minute=0)
    pm_end   = now_cet.replace(hour=15, minute=30)

    price_rows, news_map = [], {}
    for r in rows:
        sym = r["symbol"]
        chg = fetch_ah_pm_changes(sym)
        price_rows.append((sym, r["qty"], r["K"], chg["ah"], chg["pm"]))
        news_map[sym] = collect_news_for(sym, ah_start, pm_end)

    missing = [sym for sym,_,_,ah,pm in price_rows if (ah["chg_pct"] is None and pm["chg_pct"] is None)]
    lines = []
    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST")
    lines.append(f"_Vizsgált ablakok_: AH {ah_start:%Y-%m-%d %H:%M} → {ah_end:%H:%M}, PM {pm_start:%H:%M} → {pm_end:%H:%M}")
    lines.append("**Lefedettség:** " + ("HIÁNYOS – nem elérhető ticker(ek): " + ", ".join(missing) if missing else "TELJES"))

    # Darabszámos
    lines.append("\n### Darabszámos (≥K%):")
    anypos = False
    for sym, qty, K, ah, pm in price_rows:
        if not qty: continue
        chunks = []
        if ah["chg_pct"] and abs(ah["chg_pct"]) >= K: chunks.append(f"AH {ah['chg_pct']:.2f}%")
        if pm["chg_pct"] and abs(pm["chg_pct"]) >= K: chunks.append(f"PM {pm['chg_pct']:.2f}%")
        if chunks:
            chg = pm["chg_pct"] if pm["chg_pct"] else ah["chg_pct"]
            reason = short_news_reason(sym, news_map, chg)
            lines.append(f"- **{sym}** — {' | '.join(chunks)} – {reason}")
            anypos = True
    if not anypos: lines.append("_nincs_")

    # Watchlist
    lines.append("\n### Watchlist (≥K% vagy hír):")
    anywl = False
    for sym, qty, K, ah, pm in price_rows:
        if qty: continue
        chunks = []
        if ah["chg_pct"] and abs(ah["chg_pct"]) >= K: chunks.append(f"AH {ah['chg_pct']:.2f}%")
        if pm["chg_pct"] and abs(pm["chg_pct"]) >= K: chunks.append(f"PM {pm['chg_pct']:.2f}%")
        news = news_map.get(sym)
        if not chunks and not news: continue
        chg = pm["chg_pct"] if pm["chg_pct"] else ah["chg_pct"]
        reason = short_news_reason(sym, news_map, chg)
        base = " | ".join(chunks) if chunks else "releváns hír"
        lines.append(f"- **{sym}** — {base} – {reason}")
        anywl = True
    if not anywl: lines.append("_nincs_")

    # Hírblokk
    lines.append("\n### Bejelentések & fel/lemínősítések")
    anynews = False
    for sym in [r["symbol"] for r in rows]:
        for (src, title, dt) in news_map.get(sym, []):
            lines.append(f"- **{sym}** — {src} — {translate_hu(title)} — {dt:%Y-%m-%d %H:%M} CEST")
            anynews = True
    if not anynews: lines.append("_(nincs releváns bejegyzés az ablakban)_")

    return "\n".join(lines)

# --- Jelentés 2 -------------------------------------------------------------
def report_2(rows):
    now_ny = datetime.now(TZ_NY)
    prev = previous_trading_day(now_ny.date())
    res, miss = [], []
    for r in rows:
        sym = r["symbol"]
        h = yf.Ticker(sym).history(start=prev, end=prev+timedelta(days=1), interval="1m")
        if h.empty:
            miss.append(sym)
            continue
        h = _tz(h, TZ_NY)
        o = _pick_open(h)
        c = float(h["Close"].iloc[-1])
        if not o: continue
        res.append((sym, r["qty"], r["K"], round(pct(o, c), 2)))
    lines = []
    lines.append("## #2 – Tegnapi Open→Close (15:30–22:00 CEST)")
    lines.append("**Lefedettség:** " + ("HIÁNYOS – "+", ".join(miss) if miss else "TELJES"))
    for sym, qty, K, ch in res:
        if ch and abs(ch) >= K:
            lines.append(f"- **{sym}** — {ch:+.2f}%")
    return "\n".join(lines)

# --- Jelentés 3 -------------------------------------------------------------
def report_3(rows):
    res, miss = [], []
    for r in rows:
        sym = r["symbol"]
        m = yf.Ticker(sym).history(period="1d", interval="1m")
        if m.empty:
            miss.append(sym)
            continue
        m = _tz(m, TZ_NY)
        o = _pick_open(m)
        l = float(m["Close"].iloc[-1])
        if not o: continue
        res.append((sym, r["qty"], r["K"], round(pct(o, l), 2)))
    lines = []
    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most)")
    lines.append("**Lefedettség:** " + ("HIÁNYOS – "+", ".join(miss) if miss else "TELJES"))
    for sym, qty, K, ch in res:
        if ch and abs(ch) >= K:
            lines.append(f"- **{sym}** — {ch:+.2f}%")
    return "\n".join(lines)

# --- Main -------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="3")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--summary", default="")
    ap.add_argument("--default-k", type=float, default=3.0)
    ap.add_argument("--default-l", type=float, default=2.0)
    ap.add_argument("--default-m", type=float, default=1.0)
    ap.add_argument("--macro", default="auto")
    args = ap.parse_args()

    Path("reports").mkdir(parents=True, exist_ok=True)
    rows = read_rows(args.csv, args.default_k, args.default_l, args.default_m)
    if not rows:
        print("Nincs ticker a CSV-ben."); sys.exit(1)

    rep = str(args.report)
    if rep == "1":
        text = report_1(rows)
        Path("reports/summary_report1.md").write_text(text, encoding="utf-8")
    elif rep == "2":
        text = report_2(rows)
        Path("reports/summary_report2.md").write_text(text, encoding="utf-8")
    else:
        text = report_3(rows)
        Path("reports/summary_report3.md").write_text(text, encoding="utf-8")

    print(text)
    if args.summary:
        with open(args.summary, "a", encoding="utf-8") as f: f.write(text)

if __name__ == "__main__":
    main()
    sys.exit(0)
