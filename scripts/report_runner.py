import argparse, sys, time
from pathlib import Path
from datetime import datetime, timedelta, time as dtime
import pandas as pd
import yfinance as yf
import pytz, requests, feedparser
from bs4 import BeautifulSoup

# --- Encoding, időzóna -------------------------------------------------------
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TZ_NY  = pytz.timezone("America/New_York")
TZ_CET = pytz.timezone("Europe/Budapest")
OPEN_T, CLOSE_T = dtime(9,30), dtime(16,0)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})
TIMEOUT = 3  # gyorsabb feed timeout

# --- Egyszerű szentiment -----------------------------------------------------
POS = ["UPGRADE","RAISE","BUY","SURGE","BEAT","JUMP","RALLY","GAIN","OUTPERFORM"]
NEG = ["DOWNGRADE","CUT","SELL","MISS","DROP","PLUNGE","SLUMP","UNDERPERFORM"]

def classify_sentiment_en(title):
    t = title.upper()
    pos = any(w in t for w in POS)
    neg = any(w in t for w in NEG)
    if pos and not neg: return "pozitív"
    if neg and not pos: return "negatív"
    return "semleges"

def translate_short(text):
    text = text.strip()
    return text[:180] + "…" if len(text) > 180 else text

# --- CSV beolvasás -----------------------------------------------------------
def read_rows(csv_path, Kd=3.0, Ld=2.0, Md=1.0):
    df = pd.read_csv(csv_path)
    df.columns = [c.lower() for c in df.columns]
    out = []
    for _, r in df.iterrows():
        t = str(r[df.columns[0]]).strip().upper()
        if not t or t == "NAN" or t == "PKN.WA": continue
        qty = None
        if "darabszám" in df.columns and pd.notna(r["darabszám"]):
            qty = float(r["darabszám"])
        out.append({"symbol":t,"qty":qty,"K":Kd})
    return out

# --- Yahoo árak --------------------------------------------------------------
def pct(a,b):
    try: return (b-a)/a*100
    except: return None

def fetch_ah_pm_changes(sym):
    try:
        t = yf.Ticker(sym)
        h = t.history(period="2d", interval="5m", prepost=True)
        if h.empty: return {"ah": {"chg_pct": None}, "pm": {"chg_pct": None}}
        h.index = h.index.tz_convert(TZ_NY)
        prev_close = float(h.iloc[-1]["Close"]) if len(h)>1 else None
        ah = round(pct(prev_close, h["Close"].iloc[-1]),2) if prev_close else None
        return {"ah":{"chg_pct":ah},"pm":{"chg_pct":None}}
    except Exception:
        return {"ah":{"chg_pct":None},"pm":{"chg_pct":None}}

# --- Yahoo RSS ---------------------------------------------------------------
def yahoo_feed(sym):
    return f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={sym}&region=US&lang=en-US"

def collect_news(sym, start, end):
    url = yahoo_feed(sym)
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        fp = feedparser.parse(r.text)
        items=[]
        for e in fp.entries:
            if "published_parsed" not in e: continue
            dt = datetime.fromtimestamp(time.mktime(e.published_parsed), tz=pytz.UTC).astimezone(TZ_CET)
            if dt<start or dt>end: continue
            title=(e.get("title") or "").strip()
            items.append(("finance.yahoo.com", title, dt))
        return items
    except Exception:
        return []

def reason(sym, news, chg):
    if news:
        src,title,dt = news[-1]
        sent = classify_sentiment_en(title)
        short = translate_short(title)
        return f"[{sent}] {short}"
    if chg is not None:
        s = "pozitív" if chg>0 else "negatív"
        return f"[{s}] ármozgás hír nélkül – valószínű szektorhatás"
    return ""

# --- Jelentés 1 --------------------------------------------------------------
def report_1(rows):
    now = datetime.now(TZ_CET)
    start = now - timedelta(hours=16)
    end = now
    price_rows,news_map=[],{}
    for r in rows:
        sym=r["symbol"]
        chg=fetch_ah_pm_changes(sym)
        news=collect_news(sym,start,end)
        news_map[sym]=news
        price_rows.append((sym,r["qty"],r["K"],chg["ah"]["chg_pct"]))
        time.sleep(0.3)
    missing=[s for s,_,_,ch in price_rows if ch is None]
    lines=[]
    lines.append("## #1 – After-hours + Premarket (gyorsított Yahoo-only verzió)")
    lines.append("**Lefedettség:** "+("HIÁNYOS – "+", ".join(missing) if missing else "TELJES"))
    lines.append("\n### Darabszámos tickerek:")
    for s,q,K,ch in price_rows:
        if not q or ch is None: continue
        if abs(ch)>=K:
            reason_text=reason(s,news_map.get(s),ch)
            lines.append(f"- **{s}** — {ch:+.2f}% – {reason_text}")
    lines.append("\n### Watchlist (≥K% vagy hír):")
    for s,q,K,ch in price_rows:
        if q: continue
        n=news_map.get(s)
        if (ch and abs(ch)>=K) or n:
            reason_text=reason(s,n,ch)
            base=(f"{ch:+.2f}%" if ch else "releváns hír")
            lines.append(f"- **{s}** — {base} – {reason_text}")
    return "\n".join(lines)

# --- Main -------------------------------------------------------------------
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--report",default="1")
    ap.add_argument("--csv",required=True)
    ap.add_argument("--summary",default="")
    args=ap.parse_args()
    Path("reports").mkdir(exist_ok=True)
    rows=read_rows(args.csv)
    txt=report_1(rows)
    Path("reports/summary_report1.md").write_text(txt,encoding="utf-8")
    print(txt)
    if args.summary:
        with open(args.summary,"a",encoding="utf-8") as f: f.write(txt)

if __name__=="__main__":
    main()
