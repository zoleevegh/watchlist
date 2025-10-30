import argparse, sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import requests
import yfinance as yf
import pytz

# UTF-8 kimenet Windows runneren
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TZ_NY = pytz.timezone("America/New_York")
SKIP_TICKERS = {"PKN.WA"}  # PKN.WA alapból kihagyva

# ---- Segédek ---------------------------------------------------------------

def pct(a, b):
    try:
        if a is None or b is None or a == 0:
            return None
        return (b - a) / a * 100.0
    except Exception:
        return None

def read_tickers(csv_path: str):
    """Rugalmas beolvasás: megpróbáljuk megtalálni a ticker- és mennyiség-oszlopokat.
       Ha nincs qty oszlop, minden ticker watchlistként kezelődik.
    """
    df = pd.read_csv(csv_path)
    cols = {c.lower(): c for c in df.columns}

    # ticker oszlop
    ticker_col = None
    for k in ["ticker","symbol","tiker","tic","name","név","papír"]:
        if k in cols:
            ticker_col = cols[k]; break
    if ticker_col is None:
        ticker_col = df.columns[0]  # fallback: első oszlop

    # qty oszlop
    qty_col = None
    for k in ["qty","quantity","db","darab","darabszam","darabszám","shares","pcs","mennyiseg","mennyiség"]:
        if k in cols:
            qty_col = cols[k]; break

    out = []
    for _, row in df.iterrows():
        t = str(row[ticker_col]).strip()
        if not t or t == "nan":
            continue
        if t in SKIP_TICKERS:
            continue
        qty = None
        if qty_col is not None:
            try:
                val = row[qty_col]
                qty = float(val) if pd.notna(val) else None
            except Exception:
                qty = None
        out.append((t, qty))

    # duplikátumok kiszűrése (megtartjuk az első qty-t)
    seen = {}
    for t, q in out:
        if t not in seen:
            seen[t] = q
    return [(t, seen[t]) for t in seen.keys()]

# ---- Árlekérés (többlépcsős fallback) -------------------------------------

def fetch_metrics(symbol: str):
    """Open→Now a mai REGULAR perces adatokból, több lépcsős fallback-kel.
       Fallback sorrend:
         1) 1m / 1d REGULAR
         2) 5m / 5d REGULAR (ma szűrve)
         3) quote v7 mezők (regularMarketOpen/regularMarketPrice/regularMarketPreviousClose)
         4) fast_info.previous_close (ha elérhető)
    """
    try:
        t = yf.Ticker(symbol)

        # 1) 1 perces adatok, 1 nap
        hist = t.history(period="1d", interval="1m", prepost=False, actions=False)
        if hist is None or hist.empty:
            # 2) 5 perces fallback, 5 napból ma szűrve
            hist = t.history(period="5d", interval="5m", prepost=False, actions=False)
            if hist is None:
                hist = pd.DataFrame()

        open_px = last_px = high_px = low_px = None
        if not hist.empty:
            # TZ normalizálás
            if hist.index.tz is None:
                hist.index = hist.index.tz_localize("UTC").tz_convert(TZ_NY)
            else:
                hist.index = hist.index.tz_convert(TZ_NY)
            today = datetime.now(TZ_NY).date()
            df = hist[hist.index.date == today]
            if not df.empty:
                first = df.iloc[0]
                open_px = float(first["Open"])
                last_px = float(df["Close"].iloc[-1])
                high_px = float(df["High"].max())
                low_px  = float(df["Low"].min())

        prev = None

        # 3) ha percesből nincs adat, kérjünk quote v7-et
        if open_px is None or last_px is None:
            try:
                q = requests.get(
                    "https://query1.finance.yahoo.com/v7/finance/quote",
                    params={"symbols": symbol},
                    headers={"User-Agent":"Mozilla/5.0"},
                    timeout=10
                ).json()["quoteResponse"]["result"][0]
                if open_px is None:
                    open_px = q.get("regularMarketOpen") or None
                if last_px is None:
                    last_px = q.get("regularMarketPrice") or None
                prev = q.get("regularMarketPreviousClose") or None
            except Exception:
                pass

        # 4) previous close fast_info-ból (ha még mindig None)
        if prev is None:
            try:
                fi = getattr(t, "fast_info", None)
                prev = getattr(fi, "previous_close", None) if fi else None
            except Exception:
                prev = None

        res = {
            "symbol": symbol,
            "open": open_px,
            "last": last_px,
            "open_to_now_pct": None if open_px in (None,0) or last_px is None else round(pct(open_px, last_px), 2),
            "prevclose_to_now_pct": None if prev in (None,0) or last_px is None else round(pct(prev, last_px), 2),
            "high_over_open_pct": None if open_px in (None,0) or high_px is None else round(pct(open_px, high_px), 2),
            "low_over_open_pct":  None if open_px in (None,0) or low_px  is None else round(pct(open_px, low_px), 2),
            "error": None if (open_px is not None and last_px is not None) else "no_price_data"
        }
        return res
    except Exception as e:
        return {"symbol": symbol, "error": f"exc:{e}"}

# ---- Main (#3 jelentés) ----------------------------------------------------

def write_summary(md_path: str, text: str):
    try:
        with open(md_path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except Exception:
        pass

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="3")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--summary", default="")
    args = ap.parse_args()

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    tickers = read_tickers(args.csv)
    if not tickers:
        msg = "Nincs ticker a CSV-ben."
        print(msg)
        if args.summary: write_summary(args.summary, f"**Hiba:** {msg}")
        sys.exit(1)

    pos = [(t,q) for t,q in tickers if (q is not None and q>0)]
    wl  = [(t,q) for t,q in tickers if not (q is not None and q>0)]

    results, missing = [], []
    for t, _ in tickers:
        m = fetch_metrics(t)
        if m.get("error"):
            missing.append(f"{t} – {m['error']}")
        results.append(m)

    df = pd.DataFrame(results)
    df.to_csv(reports_dir / "all_metrics.csv", index=False)

    # #3 fő szűrő és override
    main_hits = df[df["open_to_now_pct"].abs() >= 3.00].copy()
    override_hits = df[df["prevclose_to_now_pct"].abs() >= 8.00].copy()

    pos_symbols = {t for t,_ in pos}
    main_hits["is_pos"] = main_hits["symbol"].isin(pos_symbols)
    override_hits["is_pos"] = override_hits["symbol"].isin(pos_symbols)

    main_hits = main_hits.sort_values(by=["is_pos","symbol"], ascending=[False, True])
    override_hits = override_hits.sort_values(by=["is_pos","symbol"], ascending=[False, True])

    main_hits.to_csv(reports_dir / "report3_open_to_now_hits.csv", index=False)
    override_hits.to_csv(reports_dir / "report3_override_prevclose_hits.csv", index=False)

    # Összefoglaló a Job Summary-ba is
    def fmt(r, k):
        v = r.get(k)
        return "–" if pd.isna(v) or v is None else f"{v:.2f}%"

    lines = []
    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most)")
    lines.append("_Szűrő: abs(Open→Most) ≥ **3,00%** · OVERRIDE: abs(PrevClose→Most) ≥ **8,00%**_")

    if missing:
        lines.append("")
        lines.append("**Lefedettség:** HIÁNYOS – nem elérhető ticker(ek): " + ", ".join(missing))

    if not main_hits.empty:
        lines.append("\n### Jelzések (Open→Most ≥ 3%):")
        for _, r in main_hits.iterrows():
            lines.append(f"- **{r['symbol']}** – Open→Most: **{fmt(r,'open_to_now_pct')}** | PrevClose→Most: {fmt(r,'prevclose_to_now_pct')} | High/Open: {fmt(r,'high_over_open_pct')} | Low/Open: {fmt(r,'low_over_open_pct')}")
    else:
        lines.append("\n### Jelzések (Open→Most ≥ 3%): nincs")

    if not override_hits.empty:
        lines.append("\n### OVERRIDE – Napi extrém (PrevClose→Most ≥ 8%):")
        for _, r in override_hits.iterrows():
            lines.append(f"- **{r['symbol']}** – Open→Most: {fmt(r,'open_to_now_pct')} | **PrevClose→Most: {fmt(r,'prevclose_to_now_pct')}** | High/Open: {fmt(r,'high_over_open_pct')} | Low/Open: {fmt(r,'low_over_open_pct')}")

    summary_md = "\n".join(lines)

    print(summary_md)  # UTF-8-ra kényszerítettük a futást
    with open(reports_dir / "summary_report3.md", "w", encoding="utf-8") as f:
        f.write(summary_md)

    if args.summary:
        write_summary(args.summary, summary_md)

if __name__ == "__main__":
    main()
