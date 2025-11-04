import argparse, sys, time
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

TZ_NY = pytz.timezone("America/New_York")
TZ_CET = pytz.timezone("Europe/Budapest")
OPEN_T, CLOSE_T = dtime(9, 30), dtime(16, 0)

SKIP_TICKERS = {"PKN.WA"}

# CSV oszlopnevek
TICKER_COLS = ["ticker", "symbol", "tiker", "tic", "name", "név", "papír"]
QTY_COLS = [
    "qty",
    "quantity",
    "db",
    "darab",
    "darabszam",
    "darabszám",
    "shares",
    "pcs",
    "mennyiseg",
    "mennyiség",
]
K_COLS = ["k", "min_move_pct", "min_intraday_pct", "min_pct"]
L_COLS = ["l", "vol_mult", "unusual_vol_mult", "volx"]
M_COLS = ["m", "max_dist_52w_pct", "dist_52w_pct", "dist_pct"]

# HTTP session
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})
HTTP_TIMEOUT = 8

# --- Szentiment -------------------------------------------------------------
POS_WORDS = [
    "UPGRADE",
    "UPGRADED",
    "RAISE PRICE TARGET",
    "RAISED PRICE TARGET",
    "BEAT",
    "BEATS",
    "TOPS",
    "SURGE",
    "JUMP",
    "JUMPS",
    "SOARS",
    "RALLY",
    "RALLIES",
    "OUTPERFORM",
    "OVERWEIGHT",
    "BUY RATING",
    "INITIATED WITH BUY",
]
NEG_WORDS = [
    "DOWNGRADE",
    "DOWNGRADED",
    "CUT PRICE TARGET",
    "CUTS PRICE TARGET",
    "MISS",
    "MISSES",
    "MISSED",
    "PLUNGE",
    "PLUNGES",
    "SINKS",
    "TUMBLES",
    "SLUMP",
    "SLUMPS",
    "UNDERPERFORM",
    "UNDERWEIGHT",
    "SELL RATING",
]


def classify_sentiment_en(text: str) -> str:
    t = (text or "").upper()
    pos = any(w in t for w in POS_WORDS)
    neg = any(w in t for w in NEG_WORDS)
    if pos and not neg:
        return "pozitív"
    if neg and not pos:
        return "negatív"
    return "semleges"


def shorten_en(text: str, max_len: int = 200) -> str:
    base = (text or "").strip()
    if len(base) <= max_len:
        return base
    return base[:max_len].rsplit(" ", 1)[0] + "…"


# --- Segédfüggvények --------------------------------------------------------
def pct(a, b):
    try:
        if a is None or b is None or a == 0:
            return None
        return (b - a) / a * 100.0
    except Exception:
        return None


def _find_col(df, cands):
    m = {c.lower(): c for c in df.columns}
    for c in cands:
        if c in m:
            return m[c]
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
        if not sym or sym == "NAN" or sym in SKIP_TICKERS:
            continue
        if sym in seen:
            continue
        seen.add(sym)

        qty = None
        if qcol and pd.notna(r.get(qcol)):
            try:
                qty = float(str(r[qcol]).replace(",", "."))
            except Exception:
                qty = None

        K = Kd
        if kcol and pd.notna(r.get(kcol)):
            try:
                K = float(str(r[kcol]).replace(",", "."))
            except Exception:
                K = Kd

        L = Ld
        if lcol and pd.notna(r.get(lcol)):
            try:
                L = float(str(r[lcol]).replace(",", "."))
            except Exception:
                L = Ld

        M = Md
        if mcol and pd.notna(r.get(mcol)):
            try:
                M = float(str(r[mcol]).replace(",", "."))
            except Exception:
                M = Md

        out.append({"symbol": sym, "qty": qty, "K": K, "L": L, "M": M})
    return out


def _tz(df, tz):
    if df.empty:
        return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(tz)
    else:
        df.index = df.index.tz_convert(tz)
    return df


def _pick_open(df):
    if df.empty:
        return None
    exact = df[df.index.time == OPEN_T]
    if not exact.empty:
        return float(exact.iloc[0]["Open"])
    later = df[df.index.time >= OPEN_T]
    if not later.empty:
        i0 = later.index[0]
        ref = i0.replace(hour=9, minute=30, second=0, microsecond=0)
        if (i0 - ref).total_seconds() <= 180:
            return float(later.iloc[0]["Open"])
        return float(later.iloc[0]["Open"])
    return None


def previous_trading_day(date_ny):
    d = date_ny - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


# --- Árfüggvények ----------------------------------------------------------
def fetch_ah_pm_changes(sym):
    """PrevClose→AH / PrevClose→PM változás egyetlen history() hívással."""
    try:
        now_ny = datetime.now(TZ_NY)
        last_td = previous_trading_day(now_ny.date())
        t = yf.Ticker(sym)
        h = t.history(
            start=last_td - timedelta(days=1),
            end=last_td + timedelta(days=2),
            interval="5m",
            prepost=True,
            actions=False,
        )
        if h is None or h.empty:
            return {
                "ah": {"chg_pct": None, "error": "no_5m_prepost"},
                "pm": {"chg_pct": None, "error": "no_5m_prepost"},
            }
        h = _tz(h, TZ_NY)

        prev_day = last_td
        next_day = last_td + timedelta(days=1)

        # előző rendes záró
        rth = h[(h.index.date == prev_day) & (h.index.time <= CLOSE_T)]
        if rth.empty:
            return {
                "ah": {"chg_pct": None, "error": "no_prev_close"},
                "pm": {"chg_pct": None, "error": "no_prev_close"},
            }
        prev_close = float(rth["Close"].iloc[-1])

        def win_pct(start_dt, end_dt):
            win = h[(h.index >= start_dt) & (h.index <= end_dt)]
            if win.empty:
                return None
            last_price = float(win["Close"].iloc[-1])
            return round(pct(prev_close, last_price), 2)

        # AH: prev_day 16:00–20:00 NY
        ah_start = TZ_NY.localize(datetime.combine(prev_day, CLOSE_T))
        ah_end = ah_start + timedelta(hours=4)
        # PM: next_day 04:00–09:30 NY
        pm_start = TZ_NY.localize(datetime.combine(next_day, dtime(4, 0)))
        pm_end = TZ_NY.localize(datetime.combine(next_day, dtime(9, 30)))

        ah_chg = win_pct(ah_start, ah_end)
        pm_chg = win_pct(pm_start, pm_end)
        return {
            "ah": {
                "chg_pct": ah_chg,
                "error": None if ah_chg is not None else "no_bars_ah",
            },
            "pm": {
                "chg_pct": pm_chg,
                "error": None if pm_chg is not None else "no_bars_pm",
            },
        }
    except Exception:
        return {
            "ah": {"chg_pct": None, "error": "exception"},
            "pm": {"chg_pct": None, "error": "exception"},
        }


def fetch_prev_open_close(sym, prev_trading_date):
    t = yf.Ticker(sym)
    h = t.history(
        start=prev_trading_date,
        end=prev_trading_date + timedelta(days=1),
        interval="5m",
        prepost=False,
        actions=False,
    )
    open_px = close_px = None
    if h is not None and not h.empty:
        h = _tz(h, TZ_NY)
        d = h[h.index.date == prev_trading_date]
        if not d.empty:
            open_px = _pick_open(d)
            lastbars = d[d.index.time <= CLOSE_T]
            if not lastbars.empty:
                close_px = float(lastbars["Close"].iloc[-1])
    chg = None
    if open_px not in (None, 0) and close_px is not None:
        chg = round(pct(open_px, close_px), 2)
    return {
        "open": open_px,
        "close": close_px,
        "open_to_close_pct": chg,
        "error": None if chg is not None else "no_price_data",
    }


def fetch_open_to_now(sym):
    """
    Dupla Yahoo fallback:
      1) intraday history (2d, 5m, prepost=False)
      2) quote API (regularMarketOpen, regularMarketPrice, regularMarketPreviousClose)
    """
    open_px = last_px = prev_close = None
    error = None

    # 1) intraday history
    try:
        t = yf.Ticker(sym)
        h = t.history(period="2d", interval="5m", prepost=False, actions=False)
        if h is not None and not h.empty:
            h = _tz(h, TZ_NY)
            today = datetime.now(TZ_NY).date()
            df = h[h.index.date == today]
            if not df.empty:
                open_px = _pick_open(df)
                if open_px is None and not df.empty:
                    open_px = float(df["Open"].iloc[0])
                last_px = float(df["Close"].iloc[-1])
    except Exception:
        pass

    # 2) quote fallback
    if open_px is None or last_px is None or prev_close is None:
        try:
            resp = SESSION.get(
                "https://query1.finance.yahoo.com/v7/finance/quote",
                params={"symbols": sym},
                timeout=HTTP_TIMEOUT,
            )
            q = resp.json()["quoteResponse"]["result"][0]
            if open_px is None:
                open_px = q.get("regularMarketOpen") or q.get(
                    "regularMarketPreviousClose"
                )
            if last_px is None:
                last_px = q.get("regularMarketPrice")
            if prev_close is None:
                prev_close = q.get("regularMarketPreviousClose")
        except Exception:
            pass

    open_to_now = None
    prev_to_now = None
    if open_px not in (None, 0) and last_px is not None:
        open_to_now = round(pct(open_px, last_px), 2)
    if prev_close not in (None, 0) and last_px is not None:
        prev_to_now = round(pct(prev_close, last_px), 2)
    if open_to_now is None and prev_to_now is None:
        error = "no_price_data"

    return {
        "open": open_px,
        "last": last_px,
        "prev_close": prev_close,
        "open_to_now_pct": open_to_now,
        "prev_to_now_pct": prev_to_now,
        "error": error,
    }


# --- News (könnyített – csak Yahoo RSS) -------------------------------------
def yahoo_ticker_feed(sym):
    return f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={sym}&region=US&lang=en-US"


def _parse_feed(url):
    try:
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        return feedparser.parse(r.text)
    except Exception:
        return None


def _strip_html(t):
    try:
        return BeautifulSoup(t or "", "html.parser").get_text(" ", strip=True)
    except Exception:
        return t or ""


def _dt(e):
    for k in ("published_parsed", "updated_parsed", "created_parsed"):
        v = e.get(k)
        if v:
            return datetime.fromtimestamp(time.mktime(v), tz=pytz.UTC).astimezone(
                TZ_CET
            )
    return None


def collect_news_for(sym, window_start: datetime, window_end: datetime):
    """Csak Yahoo Finance RSS – gyorsabb, mint Reuters + PR összevonva."""
    url = yahoo_ticker_feed(sym)
    fp = _parse_feed(url)
    if not fp or not fp.entries:
        return []
    hits = []
    for e in fp.entries:
        dt = _dt(e)
        if not dt or dt < window_start or dt > window_end:
            continue
        title = (e.get("title") or "").strip()
        summ = _strip_html(e.get("summary") or "")
        text = (title + " " + summ).upper()
        if sym.upper() in text:
            hits.append((url.split("/")[2], title, dt))
    # dedup címre
    seen, out = set(), []
    for src, title, dt in hits:
        if title not in seen:
            out.append((src, title, dt))
            seen.add(title)
    out.sort(key=lambda x: x[2])
    return out


def short_news_reason(sym, news_map, chg_for_sent=None, generic_for_move=True):
    items = news_map.get(sym) or []
    if items:
        src, title, dt = items[-1]
        sent = classify_sentiment_en(title)
        short = shorten_en(title)
        if sent == "pozitív":
            tone = "pozitívan"
        elif sent == "negatív":
            tone = "negatívan"
        else:
            tone = "inkább semlegesen"
        return (
            f"[{sent}] Friss hír érkezett: {short}. "
            f"A piac ezt jelenleg {tone} árazza."
        )
    if generic_for_move and chg_for_sent is not None:
        sent = "pozitív" if chg_for_sent > 0 else "negatív"
        tone = "pozitívan" if chg_for_sent > 0 else "negatívan"
        return (
            f"[{sent}] Nagyobb elmozdulás hír nélkül; a piac {tone} árazza a papírt, "
            "valószínűleg szektor- vagy flow-hatás miatt."
        )
    return ""


def write_summary(path, text):
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except Exception:
        pass


# --- Report #1 --------------------------------------------------------------
def report_1(rows):
    # CET ablakok a headerhez
    AH_S, AH_E = dtime(22, 0), dtime(2, 0)
    PM_S, PM_E = dtime(10, 0), dtime(15, 30)

    now_cet = datetime.now(TZ_CET)
    ah_start = now_cet.replace(
        hour=AH_S.hour, minute=AH_S.minute, second=0, microsecond=0
    )
    ah_end = (ah_start + timedelta(days=1)).replace(
        hour=AH_E.hour, minute=AH_E.minute
    )
    pm_start = now_cet.replace(
        hour=PM_S.hour, minute=PM_S.minute, second=0, microsecond=0
    )
    pm_end = now_cet.replace(
        hour=PM_E.hour, minute=PM_E.minute, second=0, microsecond=0
    )

    price_rows, news_map = [], {}
    for r in rows:
        sym = r["symbol"]
        chg = fetch_ah_pm_changes(sym)
        ah, pm = chg["ah"], chg["pm"]
        price_rows.append((sym, r["qty"], r["K"], ah, pm))
        news_map[sym] = collect_news_for(sym, ah_start, pm_end)
        time.sleep(0.2)  # ne verjük szét a Yahoo-t

    missing = [
        sym
        for sym, _, _, ah, pm in price_rows
        if (ah["chg_pct"] is None and pm["chg_pct"] is None)
    ]
    lines = []
    lines.append("## #1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST")
    lines.append(
        f"_Vizsgált ablakok_: AH {ah_start:%Y-%m-%d %H:%M} → {ah_end:%H:%M}, "
        f"PM {pm_start:%H:%M} → {pm_end:%H:%M}"
    )
    lines.append(
        "**Lefedettség:** "
        + (
            "HIÁNYOS – nem elérhető ticker(ek): " + ", ".join(missing)
            if missing
            else "TELJES"
        )
    )

    # Darabszámos
    lines.append("\n### Darabszámos (≥K%):")
    any_pos = False
    for sym, qty, K, ah, pm in price_rows:
        if not qty:
            continue
        chunks = []
        if ah["chg_pct"] is not None and abs(ah["chg_pct"]) >= float(K):
            chunks.append(f"AH {ah['chg_pct']:+.2f}%")
        if pm["chg_pct"] is not None and abs(pm["chg_pct"]) >= float(K):
            chunks.append(f"PM {pm['chg_pct']:+.2f}%")
        if chunks:
            ch_for_sent = (
                pm["chg_pct"] if pm["chg_pct"] is not None else ah["chg_pct"]
            )
            reason = short_news_reason(sym, news_map, ch_for_sent)
            lines.append(
                f"- **{sym}** — " + " | ".join(chunks) + (" – " + reason if reason else "")
            )
            any_pos = True
    if not any_pos:
        lines.append("_nincs_")

    # Watchlist
    lines.append("\n### Watchlist (≥K% vagy hír):")
    any_wl = False
    for sym, qty, K, ah, pm in price_rows:
        if qty:
            continue
        chunks = []
        if ah["chg_pct"] is not None and abs(ah["chg_pct"]) >= float(K):
            chunks.append(f"AH {ah['chg_pct']:+.2f}%")
        if pm["chg_pct"] is not None and abs(pm["chg_pct"]) >= float(K):
            chunks.append(f"PM {pm['chg_pct']:+.2f}%")
        news_items = news_map.get(sym) or []
        has_news = bool(news_items)
        if not (chunks or has_news):
            continue
        ch_for_sent = (
            pm["chg_pct"] if pm["chg_pct"] is not None else ah["chg_pct"]
        )
        reason = short_news_reason(sym, news_map, ch_for_sent)
        base = " | ".join(chunks) if chunks else "releváns hír"
        lines.append(
            f"- **{sym}** — {base}" + (" – " + reason if reason else "")
        )
        any_wl = True
    if not any_wl:
        lines.append("_nincs_")

    # Hírblokk
    lines.append("\n### Bejelentések & fel/lemínősítések")
    any_news = False
    for sym in [r["symbol"] for r in rows]:
        for (src, title, dt) in news_map.get(sym, []):
            sent = classify_sentiment_en(title)
            short = shorten_en(title)
            lines.append(
                f"- **{sym}** — [{sent}] {src} — {short} — {dt:%Y-%m-%d %H:%M} CEST"
            )
            any_news = True
    if not any_news:
        lines.append("_(nincs releváns bejegyzés az ablakban)_")

    return "\n".join(lines)


# --- Report #2 --------------------------------------------------------------
def report_2(rows):
    now_ny = datetime.now(TZ_NY)
    prev_day = previous_trading_day(now_ny.date())
    res, missing = [], []
    for r in rows:
        sym = r["symbol"]
        m = fetch_prev_open_close(sym, prev_day)
        if m["error"]:
            missing.append(sym)
        res.append((sym, r["qty"], r["K"], m))

    lines = []
    lines.append("## #2 – Tegnapi Open→Close (15:30–22:00 CEST)")
    lines.append(
        "**Lefedettség:** "
        + ("HIÁNYOS – " + ", ".join(missing) if missing else "TELJES")
    )

    # Darabszámos
    lines.append("\n### Darabszámos (abs(Open→Close) ≥ K):")
    any_pos = False
    for sym, qty, K, m in res:
        ch = m["open_to_close_pct"]
        if qty and ch is not None and abs(ch) >= float(K):
            lines.append(f"- **{sym}** — {ch:+.2f}%")
            any_pos = True
    if not any_pos:
        lines.append("_nincs_")

    # Watchlist
    lines.append("\n### Watchlist (abs(Open→Close) ≥ K):")
    any_wl = False
    for sym, qty, K, m in res:
        ch = m["open_to_close_pct"]
        if (not qty) and ch is not None and abs(ch) >= float(K):
            lines.append(f"- **{sym}** — {ch:+.2f}%")
            any_wl = True
    if not any_wl:
        lines.append("_nincs_")

    return "\n".join(lines)


# --- Report #3 --------------------------------------------------------------
def report_3(rows):
    # időablak: mai RTH (15:30 CEST-től mostanáig)
    price_rows = []
    for r in rows:
        sym = r["symbol"]
        m = fetch_open_to_now(sym)
        price_rows.append((sym, r["qty"], r["K"], m))
        time.sleep(0.2)  # ne verjük szét a Yahoo-t

    missing = [sym for sym, qty, K, m in price_rows if m["error"]]
    lines = []
    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most)")
    lines.append(
        "**Lefedettség:** "
        + (
            "HIÁNYOS – nem elérhető ticker(ek): " + ", ".join(missing)
            if missing
            else "TELJES"
        )
    )

    # Darabszámos jelzések – MINDEN pozíció, küszöb nélkül
    lines.append("\n### Darabszámos jelzések (Open→Most – minden pozíció):")
    any_main = False
    for sym, qty, K, m in price_rows:
        if not qty:
            continue
        ch_o = m["open_to_now_pct"]
        ch_p = m["prev_to_now_pct"]
        if ch_o is None and ch_p is None:
            continue

        # szentiment + indok
        base_sent = None
        if ch_o is not None and abs(ch_o) >= float(K):
            base_sent = "pozitív" if ch_o > 0 else "negatív" if ch_o < 0 else "semleges"
            reason = (
                f"[{base_sent}] nyitáshoz képest érdemi elmozdulás (≥{K:.2f}%), "
                "érdemes figyelni a nap végi zárót és a híreket."
            )
        else:
            base_sent = "pozitív" if (ch_o or 0) > 0 else "negatív" if (ch_o or 0) < 0 else "semleges"
            reason = (
                f"[{base_sent}] nyitás óta mérsékelt intranapi mozgás, "
                "egyelőre nincs küszöb feletti elmozdulás."
            )

        parts = []
        if ch_o is not None:
            parts.append(f"Open→Most {ch_o:+.2f}%")
        if ch_p is not None:
            parts.append(f"PrevClose→Most {ch_p:+.2f}%")

        lines.append(
            f"- **{sym}** — " + " | ".join(parts) + " – " + reason
        )
        any_main = True
    if not any_main:
        lines.append("_nincs_")

    # Watchlist jelzések – itt marad a K-s küszöb
    lines.append("\n### Watchlist (abs(Open→Most) ≥ K):")
    any_wl = False
    for sym, qty, K, m in price_rows:
        if qty:
            continue
        ch_o = m["open_to_now_pct"]
        ch_p = m["prev_to_now_pct"]
        if ch_o is None and ch_p is None:
            continue
        # szűrés: ha egyik sem lépi át K-t, nem kerül be
        use_ch = ch_o if ch_o is not None else ch_p
        if use_ch is None or abs(use_ch) < float(K):
            continue

        sent = "pozitív" if use_ch > 0 else "negatív" if use_ch < 0 else "semleges"
        reason = (
            f"[{sent}] nyitáshoz képest érdemi elmozdulás (≥{K:.2f}%), "
            "érdemes figyelni a híreket / szektormozgást."
        )

        parts = []
        if ch_o is not None:
            parts.append(f"Open→Most {ch_o:+.2f}%")
        if ch_p is not None:
            parts.append(f"PrevClose→Most {ch_p:+.2f}%")

        lines.append(
            f"- **{sym}** — " + " | ".join(parts) + " – " + reason
        )
        any_wl = True
    if not any_wl:
        lines.append("_nincs_")

    return "\n".join(lines)


# --- CLI --------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="3")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--summary", default="")
    ap.add_argument("--default-k", type=float, default=3.0)
    ap.add_argument("--default-l", type=float, default=2.0)
    ap.add_argument("--default-m", type=float, default=1.0)
    # workflow kompatibilitás, de nem használjuk külön:
    ap.add_argument("--macro", default="auto")
    args = ap.parse_args()

    Path("reports").mkdir(parents=True, exist_ok=True)
    rows = read_rows(args.csv, args.default_k, args.default_l, args.default_m)
    if not rows:
        msg = "Nincs ticker a CSV-ben."
        print(msg)
        write_summary(args.summary, f"**Hiba:** {msg}")
        sys.exit(1)

    rep = str(args.report).strip()
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
    write_summary(args.summary, text)


if __name__ == "__main__":
    main()
