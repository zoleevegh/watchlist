import argparse, sys, time, concurrent.futures, math
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

# --- HTTP session + timeouts ------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})
HTTP_TIMEOUT = 6  # feed / quote hívásokra

# --- Helperek ---------------------------------------------------------------
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

def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# --- Macro (Trump/FED) hibrid blokk ----------------------------------------
REUTERS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/worldNews",
]
# Yahoo Finance index feedek (összpiaci hírekhez)
YF_INDEX_FEEDS = [
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EDJI&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EIXIC&region=US&lang=en-US",
]

def _parse_feed(url):
    try:
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        return feedparser.parse(r.text)
    except Exception:
        return None

def _strip_html(t):
    try: return BeautifulSoup(t or "", "html.parser").get_text(" ", strip=True)
    except Exception: return t or ""

def _dt_entry(e):
    for k in ("published_parsed","updated_parsed","created_parsed"):
        v = e.get(k)
        if v:
            return datetime.fromtimestamp(time.mktime(v), tz=pytz.UTC).astimezone(TZ_CET)
    return None

def macro_blurb(mode="auto", lookback_hours=18):
    """
    mode: 'auto'|'off'|'strict'
    - auto: próbál hír, ha nincs -> placeholder
    - off: mindig placeholder
    - strict: csak élő hír; ha nincs, 'nincs releváns fejlemény'
    """
    now = datetime.now(TZ_CET)
    window_start = now - timedelta(hours=lookback_hours)
    if mode == "off":
        return "Trump-napihír: — (nincs releváns mai bejelentés). FED: —"

    feeds = REUTERS_FEEDS + YF_INDEX_FEEDS
    entries = []
    for f in feeds:
        fp = _parse_feed(f)
        if not fp or not fp.entries: continue
        for e in fp.entries:
            dt = _dt_entry(e)
            if not dt or dt < window_start or dt > now: continue
            title = (e.get("title") or "").strip()
            summ  = _strip_html(e.get("summary") or "")
            txt   = (title + " " + summ).upper()
            entries.append((dt, title, summ, txt))

    # Trump kulcsszavak
    trump_keys = ["TRUMP", "ELECTION", "BALLOT", "TARIFF", "SANCTION", "IMMIGRATION"]
    fed_keys   = ["FED", "FEDERAL RESERVE", "POWELL", "RATE", "DOT PLOT", "CPI", "PCE", "JOBS", "PAYROLLS"]

    trump_hit = next(( (dt,t,s) for (dt,t,s,txt) in sorted(entries, reverse=True)
                       if any(k in txt for k in trump_keys) ), None)
    fed_hit   = next(( (dt,t,s) for (dt,t,s,txt) in sorted(entries, reverse=True)
                       if any(k in txt for k in fed_keys) ), None)

    if trump_hit or fed_hit:
        parts = []
        if trump_hit:
            parts.append(f"Trump-napihír: {trump_hit[1]}")
        else:
            parts.append("Trump-napihír: —")
        if fed_hit:
            parts.append(f"FED: {fed_hit[1]}")
        else:
            parts.append("FED: —")
        return "  ".join(parts)

    if mode == "strict":
        return "Trump-napihír: —  FED: — (nincs releváns fejlemény az ablakban)"
    # auto fallback:
    return "Trump-napihír: — (nincs releváns mai bejelentés). FED: —"

# --- Prices: batch implementációk ------------------------------------------
def fetch_window_change_batch(symbols, start_cet: dtime, end_cet: dtime):
    """#1 reporthoz: 2 nap / 1m, prepost=True, minden ticker egyszerre."""
    if not symbols:
        return {}
    df = yf.download(
        tickers=symbols,
        period="2d",
        interval="1m",
        prepost=True,
        group_by="ticker",
        threads=True,
        auto_adjust=False,
        progress=False
    )
    res = {}

    def compute_from_frame(d):
        if d is None or d.empty:
            return {"chg_pct": None, "error": "no_1m_prepost"}
        di = _tz(d, TZ_CET)
        now_cet = datetime.now(TZ_CET)
        today = now_cet.date()
        prev  = today - timedelta(days=1)
        def extract(date):
            sdt = datetime.combine(date, start_cet, tzinfo=TZ_CET)
            edt = datetime.combine(date, end_cet, tzinfo=TZ_CET)
            if end_cet < start_cet:  # átlóg éjfél
                edt += timedelta(days=1)
            return di[(di.index >= sdt) & (di.index <= edt)]
        win = pd.concat([extract(prev) if end_cet < start_cet else pd.DataFrame(), extract(today)]).sort_index()
        if win.empty:
            return {"chg_pct": None, "error": "no_bars_in_window"}
        first, last = float(win["Close"].iloc[0]), float(win["Close"].iloc[-1])
        return {"chg_pct": round(pct(first, last), 2), "error": None}

    if isinstance(df.columns, pd.MultiIndex):
        for sym in symbols:
            try:
                res[sym] = compute_from_frame(df[sym])
            except Exception:
                res[sym] = {"chg_pct": None, "error": "download_fail"}
    else:
        sym = symbols[0]
        res[sym] = compute_from_frame(df)
    return res

def fetch_today_open_now_batch(symbols):
    """#3 reporthoz: 5 nap / 5m (gyorsabb), intraday Open→Most."""
    if not symbols:
        return {}
    df = yf.download(
        tickers=symbols,
        period="5d",
        interval="5m",
        prepost=False,
        group_by="ticker",
        threads=True,
        auto_adjust=False,
        progress=False
    )
    out = {}
    today_ny = datetime.now(TZ_NY).date()

    def from_frame(d):
        if d is None or d.empty:
            return {"open": None, "last": None, "open_to_now_pct": None,
                    "prev_to_now_pct": None, "high_over_open_pct": None,
                    "low_over_open_pct": None, "error": "no_price_data"}
        di = _tz(d, TZ_NY)
        dd = di[di.index.date == today_ny]
        if dd.empty:
            return {"open": None, "last": None, "open_to_now_pct": None,
                    "prev_to_now_pct": None, "high_over_open_pct": None,
                    "low_over_open_pct": None, "error": "no_today_bars"}
        open_px = _pick_open(dd)
        last_px = float(dd["Close"].iloc[-1])
        high_px = float(dd["High"].max())
        low_px  = float(dd["Low"].min())
        return {
            "open": open_px,
            "last": last_px,
            "open_to_now_pct": None if open_px in (None,0) else round(pct(open_px, last_px), 2),
            "prev_to_now_pct": None,  # külön quotes-ból
            "high_over_open_pct": None if open_px in (None,0) else round(pct(open_px, high_px), 2),
            "low_over_open_pct":  None if open_px in (None,0) else round(pct(open_px, low_px), 2),
            "error": None if (open_px is not None and last_px is not None) else "no_price_data"
        }

    if isinstance(df.columns, pd.MultiIndex):
        for sym in symbols:
            try:
                out[sym] = from_frame(df[sym])
            except Exception:
                out[sym] = {"open": None, "last": None, "open_to_now_pct": None,
                            "prev_to_now_pct": None, "high_over_open_pct": None,
                            "low_over_open_pct": None, "error": "download_fail"}
    else:
        sym = symbols[0]
        out[sym] = from_frame(df)
    return out

def yahoo_quotes_batch(symbols):
    """PrevClose / RegularMarketPrice gyors lekérése batchekben."""
    out = {s: {"prev": None, "price": None} for s in symbols}
    for chunk in _chunks(symbols, 50):
        try:
            resp = SESSION.get(
                "https://query1.finance.yahoo.com/v7/finance/quote",
                params={"symbols": ",".join(chunk)},
                timeout=HTTP_TIMEOUT
            )
            j = resp.json()
            for it in j.get("quoteResponse", {}).get("result", []):
                s = (it.get("symbol") or "").upper()
                if not s: continue
                out.setdefault(s, {})
                out[s]["prev"]  = it.get("regularMarketPreviousClose")
                out[s]["price"] = it.get("regularMarketPrice")
        except Exception:
            pass
    return out

def fetch_prev_open_close(sym, prev_trading_date):
    """#2-hez: egyenként. (Gyors elég.)"""
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

# --- News per ticker (Yahoo-only gyors) -------------------------------------
def yahoo_ticker_feed(sym): 
    return f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={sym}&region=US&lang=en-US"

def collect_news_for(sym, window_start: datetime, window_end: datetime):
    feeds = [yahoo_ticker_feed(sym)]
    hits = []
    for feed in feeds:
        fp = _parse_feed(feed)
        if not fp or not fp.entries: continue
        for e in fp.entries:
            dt = _dt_entry(e)
            if not dt or dt < window_start or dt > window_end: continue
            title = (e.get("title") or "").strip()
            summ  = _strip_html(e.get("summary") or "")
            text  = (title + " " + summ).upper()
            if sym.upper() in text or "UPGRADE" in text or "PRICE TARGET" in text or "DOWNGRADE" in text:
                src = "finance.yahoo.com"
                hits.append((src, title, dt))
    # dedup címre
    seen, out = set(), []
    for src, title, dt in hits:
        if title not in seen:
            out.append((src, title, dt))
            seen.add(title)
    out.sort(key=lambda x: x[2])
    return out

# --- MarketBeat „latest ratings” gyors scraper a HC-blokkhoz ----------------
def marketbeat_latest_ratings(max_pages=2):
    """
    Egyszerű, gyors lekérés: a MarketBeat 'latest ratings' publikus oldalain ticker, akció, ház.
    max_pages: 1-2 oldalt nézünk át (sebesség miatt).
    Vissza: lista dict-ekkel: {'symbol','action','broker','title','dt'}
    """
    base = "https://www.marketbeat.com/ratings/"
    out = []
    for page in range(1, max_pages+1):
        url = base if page == 1 else f"{base}/{page}/"
        try:
            r = SESSION.get(url, timeout=HTTP_TIMEOUT)
            soup = BeautifulSoup(r.text, "html.parser")
            # Heuriszta: táblázatsorok bejárása; a MarketBeat HTML változhat.
            rows = soup.select("table tr")
            for tr in rows:
                tds = tr.find_all("td")
                if len(tds) < 4: 
                    continue
                # tipikus szerkezet: dátum | bróker | akció | ticker | cím
                text_all = " ".join(td.get_text(" ", strip=True) for td in tds)
                text_up = text_all.upper()
                # ticker (4. oszlop vagy zárójelben a címben)
                sym = None
                # keresünk nagybetűs 1–5 hosszú 'ticker-szerű' szót
                for w in text_up.split():
                    if 1 <= len(w) <= 5 and w.isalpha():
                        sym = w
                        break
                if not sym:
                    continue
                action = "UNKNOWN"
                if "UPGRADE" in text_up: action = "UPGRADE"
                elif "RAISE" in text_up and "PRICE" in text_up: action = "PT_UP"
                elif "INITIATE" in text_up or "INITIATED" in text_up: action = "INIT"
                elif "DOWNGRADE" in text_up: action = "DOWNGRADE"

                # bróker és cím (heurisztika)
                broker = tds[1].get_text(" ", strip=True) if len(tds) >= 2 else ""
                title  = tds[-1].get_text(" ", strip=True)
                dt     = datetime.now(TZ_CET)  # pontos dt kinyerése oldalfüggő; CET-re állítjuk

                out.append({"symbol": sym, "action": action, "broker": broker, "title": title, "dt": dt})
        except Exception:
            continue
    return out

def high_conviction_candidates(universe_syms, excluded_syms, days=7, min_signals=2, max_check_per_ticker=3):
    """
    Kiválasztja azokat az univerzumbeli tickereket, amelyek nincsenek a felhasználó csv-jében (excluded_syms),
    és az elmúlt 'days' napban legalább 'min_signals' erős jelzést kaptak (UPGRADE / PT_UP / pozitív kulcsszavak).
    Yahoo Finance ticker feedekben ráerősítünk (upgrade/price target kulcsszavak).
    """
    universe = [s for s in set(sym.upper() for sym in universe_syms) if s not in excluded_syms and s not in SKIP_TICKERS]
    if not universe:
        return []

    # 1) MarketBeat gyors listázás (1-2 oldal)
    mb = marketbeat_latest_ratings(max_pages=2)
    recent = [x for x in mb if (datetime.now(TZ_CET) - x["dt"]).days <= days]

    # 2) jelzésszámlálás
    counts = {}
    details = {}
    strong_actions = {"UPGRADE","PT_UP","INIT"}
    for x in recent:
        s = x["symbol"]
        if s not in universe: 
            continue
        if x["action"] in strong_actions:
            counts[s] = counts.get(s, 0) + 1
            details.setdefault(s, []).append(f"{x['broker']}: {x['action']} – {x['title']}")

    # 3) Yahoo Finance ticker feed ráerősítés
    window_start = datetime.now(TZ_CET) - timedelta(days=days)
    for s in universe:
        if len(details.get(s, [])) >= max_check_per_ticker:
            continue
        try:
            fp = _parse_feed(yahoo_ticker_feed(s))
            if not fp or not fp.entries:
                continue
            add = 0
            for e in fp.entries:
                dt = _dt_entry(e)
                if not dt or dt < window_start: 
                    continue
                title = (e.get("title") or "").upper()
                summary = _strip_html(e.get("summary") or "").upper()
                txt = title + " " + summary
                if ("UPGRADE" in txt) or ("PRICE TARGET" in txt and "RAISE" in txt) or ("INITIAT" in txt):
                    add += 1
                    details.setdefault(s, []).append(f"Yahoo: {e.get('title','').strip()}")
                    if add >= 2: 
                        break
            if add:
                counts[s] = counts.get(s, 0) + add
        except Exception:
            continue

    # 4) kiválasztás
    picks = []
    for s, c in sorted(counts.items(), key=lambda kv: kv[1], reverse=True):
        if c >= min_signals:
            picks.append({"symbol": s, "signals": c, "notes": details.get(s, [])[:5]})
    return picks

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

def report_header_common(missing_syms, macro_line):
    cov = ("HIÁNYOS – nem elérhető ticker(ek): " + ", ".join(missing_syms)) if missing_syms else "TELJES"
    lines = []
    lines.append(f"**Lefedettség:** {cov}")
    lines.append(f"**Politika/FED:** {macro_line}")
    return lines

def report_1(rows, macro_mode):
    AH_S, AH_E = dtime(22,0), dtime(2,0)
    PM_S, PM_E = dtime(10,0), dtime(15,30)
    now_cet = datetime.now(TZ_CET)
    ah_start = now_cet.replace(hour=22, minute=0, second=0, microsecond=0)
    ah_end   = (ah_start + timedelta(days=1)).replace(hour=2, minute=0)
    pm_start = now_cet.replace(hour=10, minute=0, second=0, microsecond=0)
    pm_end   = now_cet.replace(hour=15, minute=30, second=0, microsecond=0)

    symbols = [r["symbol"] for r in rows]
    ah_map = fetch_window_change_batch(symbols, AH_S, AH_E)
    pm_map = fetch_window_change_batch(symbols, PM_S, PM_E)

    price_rows, news_map = [], {}
    for r in rows:
        sym = r["symbol"]
        price_rows.append((sym, r["qty"], r["K"], ah_map.get(sym, {"chg_pct": None, "error": "no"}), pm_map.get(sym, {"chg_pct": None, "error": "no"})))
    # hírblokk (ticker szint)
    for sym in symbols:
        news_map[sym] = collect_news_for(sym, ah_start, pm_end)

    missing = [sym for sym,_,_,ah,pm in price_rows if (ah.get("error") and pm.get("error"))]

    lines = []
    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST")
    lines += report_header_common(missing, macro_blurb(macro_mode))

    # Darabszámosok
    lines.append("\n### Darabszámos (≥K%):")
    any_pos = False
    for sym, qty, K, ah, pm in price_rows:
        if not qty: continue
        chunks = []
        if ah.get("chg_pct") is not None and abs(ah["chg_pct"]) >= float(K): chunks.append(f"AH {ah['chg_pct']:.2f}%")
        if pm.get("chg_pct") is not None and abs(pm["chg_pct"]) >= float(K): chunks.append(f"PM {pm['chg_pct']:.2f}%")
        if chunks:
            lines.append(f"- **{sym}** — " + " | ".join(chunks))
            any_pos = True
    if not any_pos: lines.append("_nincs_")

    # Watchlist (feltételes)
    lines.append("\n### Watchlist (≥K% vagy hír):")
    any_wl = False
    for sym, qty, K, ah, pm in price_rows:
        if qty: continue
        chunks = []
        if ah.get("chg_pct") is not None and abs(ah["chg_pct"]) >= float(K): chunks.append(f"AH {ah['chg_pct']:.2f}%")
        if pm.get("chg_pct") is not None and abs(pm["chg_pct"]) >= float(K): chunks.append(f"PM {pm['chg_pct']:.2f}%")
        has_news = bool(news_map.get(sym))
        if chunks or has_news:
            lines.append(f"- **{sym}** — " + (" | ".join(chunks) if chunks else "releváns hír"))
            any_wl = True
    if not any_wl: lines.append("_nincs_")

    # Bejelentések & fel/lemínősítések
    lines.append("\n### Bejelentések & fel/lemínősítések")
    any_news = False
    for sym in symbols:
        for (src, title, dt) in news_map.get(sym, []):
            when = dt.strftime("%Y-%m-%d %H:%M")
            lines.append(f"- **{sym}** — {src} — {title} — {when} CEST")
            any_news = True
    if not any_news: lines.append("_(nincs releváns bejegyzés az ablakban)_")

    # Közeli katalizátorok – (ha van saját listád, ide illeszthető)
    lines.append("\n### Közeli katalizátorok (pár nap)")
    lines.append("_(ha van beállított naptár/univerzum, itt listázzuk)_")

    return "\n".join(lines)

def report_2(rows, macro_mode):
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
    lines += report_header_common(missing, macro_blurb(macro_mode))

    lines.append("\n### Darabszámos (abs(Open→Close) ≥ K):")
    any_pos = False
    for sym, qty, K, m in res:
        if qty and m["open_to_close_pct"] is not None and abs(m["open_to_close_pct"]) >= float(K):
            lines.append(f"- **{sym}** — **Open→Close {m['open_to_close_pct']:.2f}%** — ok: (napközbeni hír/szektor/hozam)")
            any_pos = True
    if not any_pos: lines.append("_nincs_")

    lines.append("\n### Watchlist (abs(Open→Close) ≥ K):")
    any_wl = False
    for sym, qty, K, m in res:
        if (not qty) and m["open_to_close_pct"] is not None and abs(m["open_to_close_pct"]) >= float(K):
            lines.append(f"- **{sym}** — **Open→Close {m['open_to_close_pct']:.2f}%** — ok: (napközbeni hír/szektor/hozam)")
            any_wl = True
    if not any_wl: lines.append("_nincs_")

    lines.append("\n### Bejelentések & fel/lemínősítések")
    lines.append("_(darabszámosok minden lényeges bejelentésével; watchlisten csak lényeges eset)_")

    lines.append("\n### Közeli katalizátorok (pár nap)")
    lines.append("_(ha van beállított naptár/univerzum, itt listázzuk)_")

    return "\n".join(lines)

def report_3(rows, override_pct, macro_mode, universe_csv=None, hc_days=7):
    symbols = [r["symbol"] for r in rows]
    m_map = fetch_today_open_now_batch(symbols)

    # PrevClose/Now egyetlen batchelt Yahoo quotes hívással
    quotes = yahoo_quotes_batch(symbols)
    for s, mm in m_map.items():
        q = quotes.get(s, {})
        prev = q.get("prev")
        price = q.get("price") if q.get("price") is not None else mm.get("last")
        mm["prev_to_now_pct"] = None if (prev in (None,0) or price is None) else round(pct(prev, price), 2)

    res, missing = [], []
    for r in rows:
        sym = r["symbol"]
        m = m_map.get(sym, {"error":"no"})
        if m.get("error"): missing.append(sym)
        res.append((sym, r["qty"], r["K"], m))

    lines = []
    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most)")
    lines += report_header_common(missing, macro_blurb(macro_mode))

    lines.append("\n### Jelzések (abs(Open→Most) ≥ K):")
    any_main = False
    for sym, qty, K, m in res:
        ch = m["open_to_now_pct"]
        if ch is not None and abs(ch) >= float(K):
            lines.append(f"- **{sym}** — **Open→Most {ch:.2f}%**" + (" · (POS)" if qty else "") + " — ok: (intraday hír/szektor)")
            any_main = True
    if not any_main: lines.append("_nincs_")

    if override_pct and float(override_pct) > 0:
        thr = float(override_pct)
        lines.append(f"\n### Napi extrém (abs(PrevClose→Most) ≥ {thr:.2f}%):")
        any_ovr = False
        for sym, qty, K, m in res:
            ch = m["prev_to_now_pct"]
            if ch is not None and abs(ch) >= thr and not (m["open_to_now_pct"] and abs(m["open_to_now_pct"]) >= float(K)):
                lines.append(f"- **{sym}** — **PrevClose→Most {ch:.2f}%**" + (" · (POS)" if qty else ""))
                any_ovr = True
        if not any_ovr: lines.append("_nincs_")

    lines.append("\n### Bejelentések & fel/lemínősítések")
    lines.append("_(darabszámosok minden lényeges bejelentésével; watchlisten csak lényeges eset)_")

    lines.append("\n### Zárásig várható / közeli katalizátorok")
    lines.append("_(ha ma még jöhet katalizátor, itt jelezzük)_")

    # --- High-Conviction blokk (listán kívüli) ------------------------------
    lines.append("\n### Listán kívüli, 3–12 hónapos high-conviction jelöltek")
    hc_block_added = False
    if universe_csv:
        try:
            uni_df = pd.read_csv(universe_csv)
            tcol = _find_col(uni_df, TICKER_COLS) or uni_df.columns[0]
            universe_syms = [str(x).strip().upper() for x in uni_df[tcol].dropna().tolist()]
            excluded = set(symbols)  # portfólió + watchlist kizárása
            picks = high_conviction_candidates(universe_syms, excluded, days=int(hc_days), min_signals=2)
            if picks:
                for p in picks[:8]:
                    note = "; ".join(p["notes"])
                    lines.append(f"- **{p['symbol']}** — jelzések: {p['signals']} — indok: {note}")
                hc_block_added = True
        except Exception:
            pass
    if not hc_block_added:
        lines.append("_(nincs jelölt, vagy nincs megadva univerzum – add meg a `--universe-csv` paramétert)_")

    return "\n".join(lines)

# --- CLI --------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="3")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--summary", default="")
    ap.add_argument("--override-pct", type=float, default=8.0)
    ap.add_argument("--default-k", type=float, default=3.0)
    ap.add_argument("--default-l", type=float, default=2.0)
    ap.add_argument("--default-m", type=float, default=1.0)
    ap.add_argument("--macro", choices=["auto","off","strict"], default="auto",
                    help="Politika/FED blokk kitöltése: auto (alap), off (csak placeholder), strict (csak élő hír)")
    ap.add_argument("--universe-csv", default=None, help="High-conviction univerzum CSV (ticker oszloppal)")
    ap.add_argument("--hc-days", type=int, default=7, help="HC-ablak napokban (alap: 7)")
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
        text = report_1(rows, args.macro)
        Path("reports/summary_report1.md").write_text(text, encoding="utf-8")
    elif rep == "2":
        text = report_2(rows, args.macro)
        Path("reports/summary_report2.md").write_text(text, encoding="utf-8")
    else:
        text = report_3(rows, args.override_pct, args.macro, args.universe_csv, args.hc_days)
        Path("reports/summary_report3.md").write_text(text, encoding="utf-8")

    print(text)
    if args.summary:
        write_summary(args.summary, text)

if __name__ == "__main__":
    main()
    sys.exit(0)
