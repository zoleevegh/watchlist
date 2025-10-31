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

# --- Prices -----------------------------------------------------------------
def fetch_today_open_now(sym):
    t = yf.Ticker(sym)
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
    t = yf.Ticker(sym)
    h = t.history(start=prev_trading_date, end=prev_trading_date + timedelta(days=1),
                  interval="1m", prepost=False, actions=False)
    open_px = close_px = None
    if h is not None and not h.empty:
        h = _tz(h, TZ_NY)
        d = h[h.index.date == prev_trading_date]
        if not d.empty:
            open_px = _pick_open(d)
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
    t = yf.Ticker(sym)
    h = t.history(period="2d", interval="1m", prepost=True, actions=False)
    if h is None or h.empty:
        return {"chg_pct": None, "error": "no_1m_prepost"}
    h = _tz(h, TZ_CET)
    now_cet = datetime.now(TZ_CET)
    today = now_cet.date()
    prev  = today - timedelta(days=1)
    def extract(date):
        sdt = datetime.combine(date, start_cet, tzinfo=TZ_CET)
        edt = datetime.combine(date, end_cet, tzinfo=TZ_CET)
        if end_cet < start_cet: edt += timedelta(days=1)
        return h[(h.index >= sdt) & (h.index <= edt)]
    win = pd.concat([extract(prev) if end_cet < start_cet else pd.DataFrame(), extract(today)]).sort_index()
    if win.empty:
        return {"chg_pct": None, "error": "no_bars_in_window"}
    first, last = float(win["Close"].iloc[0]), float(win["Close"].iloc[-1])
    return {"chg_pct": round(pct(first, last), 2), "error": None}

# --- News (light) -----------------------------------------------------------
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
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=20)
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

def collect_news_for(sym, window_start: datetime, window_end: datetime):
    names = [sym]
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
            if any(n in text for n in [sym, sym.upper()]):
                hits.append((feed.split('/')[2], title, dt))
    # dedup címre
    seen, out = set(), []
    for src, title, dt in hits:
        if title not in seen:
            out.append((src, title, dt))
            seen.add(title)
    out.sort(key=lambda x: x[2])
    return out

# --- Reports ----------------------------------------------------------------
def write_summary(path, text):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except Exception:
        pass

def previous_trading_day(date_ny):
    d = date_ny - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d

def report_1(rows):
    AH_S, AH_E = dtime(22,0), dtime(2,0)
    PM_S, PM_E = dtime(10,0), dtime(15,30)
    now_cet = datetime.now(TZ_CET)
    ah_start = now_cet.replace(hour=22, minute=0, second=0, microsecond=0)
    ah_end   = (ah_start + timedelta(days=1)).replace(hour=2, minute=0)
    pm_start = now_cet.replace(hour=10, minute=0, second=0, microsecond=0)
    pm_end   = now_cet.replace(hour=15, minute=30, second=0, microsecond=0)

    price_rows, news_map = [], {}
    for r in rows:
        sym = r["symbol"]
        ah = fetch_window_change(sym, AH_S, AH_E)
        pm = fetch_window_change(sym, PM_S, PM_E)
        price_rows.append((sym, r["qty"], r["K"], ah, pm))
        news_map[sym] = collect_news_for(sym, ah_start, pm_end)

    missing = [sym for sym,_,_,ah,pm in price_rows if (ah["error"] and pm["error"])]
    lines = []
    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST")
    lines.append("**Lefedettség:** " + ("HIÁNYOS – " + ", ".join(missing) if missing else "TELJES"))

    lines.append("\n### Darabszámos (≥K%):")
    any_pos = False
    for sym, qty, K, ah, pm in price_rows:
        chunks = []
        if ah["chg_pct"] is not None and abs(ah["chg_pct"]) >= float(K): chunks.append(f"AH {ah['chg_pct']:.2f}%")
        if pm["chg_pct"] is not None and abs(pm["chg_pct"]) >= float(K): chunks.append(f"PM {pm['chg_pct']:.2f}%")
        if qty and chunks:
            lines.append(f"- **{sym}** — " + " / ".join(chunks))
            any_pos = True
    if not any_pos: lines.append("_nincs_")

    lines.append("\n### Watchlist (≥K% vagy hír):")
    any_wl = False
    for sym, qty, K, ah, pm in price_rows:
        if qty: continue
        chunks = []
        if ah["chg_pct"] is not None and abs(ah["chg_pct"]) >= float(K): chunks.append(f"AH {ah['chg_pct']:.2f}%")
        if pm["chg_pct"] is not None and abs(pm["chg_pct"]) >= float(K): chunks.append(f"PM {pm['chg_pct']:.2f}%")
        if chunks or news_map.get(sym):
            lines.append(f"- **{sym}** — " + (" / ".join(chunks) if chunks else "releváns hír"))
            any_wl = True
    if not any_wl: lines.append("_nincs_")

    # rövid hírblokk
    lines.append("\n### Bejelentések & fel/lemínősítések")
    any_news = False
    for sym in [r["symbol"] for r in rows]:
        for (src, title, dt) in news_map.get(sym, []):
            when = dt.strftime("%Y-%m-%d %H:%M")
            lines.append(f"- **{sym}** — {src} — {title} — {when} CEST")
            any_news = True
    if not any_news: lines.append("_(nincs releváns bejegyzés az ablakban)_")

    return "\n".join(lines)

def report_2(rows):
    now_ny = datetime.now(TZ_NY)
    prev_day = previous_trading_day(now_ny.date())
    res, missing = [], []
    for r in rows:
        sym = r["symbol"]
        m = fetch_prev_open_close(sym, prev_day)
        if m["error"]: missing.append(sym)
        res.append((sym, r["qty"], r["K"], m))
    lines = []
    lines.append("## #2 – Tegnapi Open→Close (15:30–22:00 CEST)")
    lines.append("**Lefedettség:** " + ("HIÁNYOS – " + ", ".join(missing) if missing else "TELJES"))
    lines.append("\n### Darabszámos (abs(Open→Close) ≥ K):")
    any_pos = False
    for sym, qty, K, m in res:
        if qty and m["open_to_close_pct"] is not None and abs(m["open_to_close_pct"]) >= float(K):
            lines.append(f"- **{sym}** — **{m['open_to_close_pct']:.2f}%**")
            any_pos = True
    if not any_pos: lines.append("_nincs_")
    lines.append("\n### Watchlist (abs(Open→Close) ≥ K):")
    any_wl = False
    for sym, qty, K, m in res:
        if (not qty) and m["open_to_close_pct"] is not None and abs(m["open_to_close_pct"]) >= float(K):
            lines.append(f"- **{sym}** — **{m['open_to_close_pct']:.2f}%**")
            any_wl = True
    if not any_wl: lines.append("_nincs_")
    return "\n".join(lines)

def report_3(rows, override_pct):
    res, missing = [], []
    for r in rows:
        sym = r["symbol"]
        m = fetch_today_open_now(sym)
        if m["error"]: missing.append(sym)
        res.append((sym, r["qty"], r["K"], m))
    lines = []
    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most)")
    lines.append("**Lefedettség:** " + ("HIÁNYOS – " + ", ".join(missing) if missing else "TELJES"))
    lines.append("\n### Jelzések (abs(Open→Most) ≥ K):")
    any_main = False
    for sym, qty, K, m in res:
        ch = m["open_to_now_pct"]
        if ch is not None and abs(ch) >= float(K):
            lines.append(f"- **{sym}** — **{ch:.2f}%**" + (" · (POS)" if qty else ""))
            any_main = True
    if not any_main: lines.append("_nincs_")
    if override_pct and float(override_pct) > 0:
        lines.append(f"\n### Napi extrém (abs(PrevClose→Most) ≥ {float(override_pct):.2f}%):")
        any_ovr = False
        for sym, qty, K, m in res:
            ch = m["prev_to_now_pct"]
            if ch is not None and abs(ch) >= float(override_pct) and not (m["open_to_now_pct"] and abs(m["open_to_now_pct"]) >= float(K)):
                lines.append(f"- **{sym}** — **{ch:.2f}%**" + (" · (POS)" if qty else ""))
                any_ovr = True
        if not any_ovr: lines.append("_nincs_")
    return "\n".join(lines)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="3")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--summary", default="")
    ap.add_argument("--override-pct", type=float, default=8.0)
    ap.add_argument("--default-k", type=float, default=3.0)
    ap.add_argument("--default-l", type=float, default=2.0)
    ap.add_argument("--default-m", type=float, default=1.0)
    args = ap.parse_args()

    Path("reports").mkdir(parents=True, exist_ok=True)
    rows = read_rows(args.csv, args.default_k, args.default_l, args.default_m)
    if not rows:
        msg = "Nincs ticker a CSV-ben."
        print(msg)
        if args.summary: write_summary(args.summary, f"**Hiba:** {msg}")
        sys.exit(1)

    rep = str(args.report).strip()
    if rep == "1":
        text = report_1(rows)
        Path("reports/summary_report1.md").write_text(text, encoding="utf-8")
    elif rep == "2":
        text = report_2(rows)
        Path("reports/summary_report2.md").write_text(text, encoding="utf-8")
    else:
        text = report_3(rows, args.override_pct)
        Path("reports/summary_report3.md").write_text(text, encoding="utf-8")

    print(text)
    if args.summary:
        write_summary(args.summary, text)

if __name__ == "__main__":
    main()
