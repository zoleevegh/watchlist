import argparse, csv, io, sys, math, json, time
from pathlib import Path
import pandas as pd
import requests
import yfinance as yf
import pytz
from datetime import datetime
from tabulate import tabulate

TZ_NY = pytz.timezone("America/New_York")
SKIP_TICKERS = {"PKN.WA"}  # kérés szerint kihagyjuk alapból

def read_tickers(csv_path: str):
    """Rugalmas beolvasás: megpróbáljuk megtalálni a ticker és mennyiség oszlopokat.
       Ha nincs külön oszlop, az első oszlopból vesszük a tickereket.
    """
    df = pd.read_csv(csv_path)
    # oszlop-nevek normalizálása
    cols = {c.lower(): c for c in df.columns}
    # ticker oszlop
    ticker_col = None
    for k in ["ticker","symbol","tiker","tic","name"]:
        if k in cols: ticker_col = cols[k]; break
    if ticker_col is None:
        # első oszlop fallback
        ticker_col = df.columns[0]

    # mennyiség oszlop (darabszámos tickerek detektálása)
    qty_col = None
    for k in ["qty","quantity","db","darab","darabszam","darabszám","shares","pcs"]:
        if k in cols: qty_col = cols[k]; break

    # kész lista
    out = []
    for _, row in df.iterrows():
        t = str(row[ticker_col]).strip()
        if not t or t == "nan": continue
        if t in SKIP_TICKERS: continue
        qty = None
        if qty_col is not None:
            try:
                q = row[qty_col]
                qty = float(q) if pd.notna(q) else None
            except Exception:
                qty = None
        out.append((t, qty))
    # duplikátumok kiszűrése (megtartjuk az első qty-t)
    seen = {}
    for t, q in out:
        if t not in seen:
            seen[t] = q
    return [(t, seen[t]) for t in seen.keys()]

def pct(a, b):
    try:
        if a is None or b is None or a == 0: return None
        return (b - a) / a * 100.0
    except Exception:
        return None

def fetch_metrics(symbol: str):
    """Open→Now a mai REGULAR első 1p barból számolva + PrevClose→Now override-hoz."""
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="1d", interval="1m", prepost=False, actions=False)
        if hist.empty:
            return {"symbol": symbol, "error": "no_1m_data"}
        # TZ normalizálás
        if hist.index.tz is None:
            hist.index = hist.index.tz_localize("UTC").tz_convert(TZ_NY)
        else:
            hist.index = hist.index.tz_convert(TZ_NY)
        today = datetime.now(TZ_NY).date()
        df = hist[hist.index.date == today]
        if df.empty:
            return {"symbol": symbol, "error": "no_today_bars"}
        first = df.iloc[0]
        open_px = float(first["Open"])
        last_px = float(df["Close"].iloc[-1])
        high_px = float(df["High"].max())
        low_px  = float(df["Low"].min())
        fi = t.fast_info if hasattr(t, "fast_info") else None
        prev = getattr(fi, "previous_close", None) if fi else None

        res = {
            "symbol": symbol,
            "open": open_px,
            "last": last_px,
            "open_to_now_pct": None if open_px in (None,0) else round(pct(open_px, last_px), 2),
            "prevclose_to_now_pct": None if prev in (None,0) else round(pct(prev, last_px), 2),
            "high_over_open_pct": None if open_px in (None,0) else round(pct(open_px, high_px), 2),
            "low_over_open_pct": None if open_px in (None,0) else round(pct(open_px, low_px), 2),
            "error": None
        }
        return res
    except Exception as e:
        return {"symbol": symbol, "error": f"exc:{e}"}

def write_summary(md_path: str, text: str):
    try:
        with open(md_path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except Exception:
        pass

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="3")
    ap.add_argument("--csv", required=True, help="MASTER CSV fájl (útvonal)")
    ap.add_argument("--summary", default="", help="GitHub Step Summary fájl (GITHUB_STEP_SUMMARY)")
    args = ap.parse_args()

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    tickers = read_tickers(args.csv)
    if not tickers:
        msg = "Nincs ticker a CSV-ben."
        print(msg); 
        if args.summary: write_summary(args.summary, f"**Hiba:** {msg}")
        sys.exit(1)

    # szétválasztjuk darabszámos / nem darabszámos listára
    pos = [(t,q) for t,q in tickers if (q is not None and q>0)]
    wl  = [(t,q) for t,q in tickers if not (q is not None and q>0)]

    results = []
    missing = []
    for t, _ in tickers:
        m = fetch_metrics(t)
        if m.get("error"):
            missing.append(f"{t} – {m['error']}")
        results.append(m)

    df = pd.DataFrame(results)

    # #3 logika
    main_hits = df[df["open_to_now_pct"].abs() >= 3.00].copy()
    override_hits = df[df["prevclose_to_now_pct"].abs() >= 8.00].copy()

    # darabszámos előre rendezés
    pos_symbols = set([t for t,_ in pos])
    main_hits["is_pos"] = main_hits["symbol"].isin(pos_symbols)
    main_hits = main_hits.sort_values(by=["is_pos"], ascending=False)

    override_hits["is_pos"] = override_hits["symbol"].isin(pos_symbols)
    override_hits = override_hits.sort_values(by=["is_pos"], ascending=False)

    # mentés artefaktokba
    df.to_csv(reports_dir / "all_metrics.csv", index=False)
    main_hits.to_csv(reports_dir / "report3_open_to_now_hits.csv", index=False)
    override_hits.to_csv(reports_dir / "report3_override_prevclose_hits.csv", index=False)

    # összefoglaló (mobilon olvasható)
    lines = []
    lines.append("## #3 – Ma nyitástól mostanáig (Open→Most)")
    lines.append("_Szűrő: abs(Open→Most) ≥ **3,00%**; OVERRIDE: abs(PrevClose→Most) ≥ **8,00%**_")
    if missing:
        lines.append("")
        lines.append(f"**Lefedettség:** HIÁNYOS – nem elérhető ticker(ek): " + ", ".join(missing))

    def fmt_row(r):
        o = f"{r['open_to_now_pct']:.2f}%" if pd.notna(r['open_to_now_pct']) else "–"
        p = f"{r['prevclose_to_now_pct']:.2f}%" if pd.notna(r['prevclose_to_now_pct']) else "–"
        hi = f"{r['high_over_open_pct']:.2f}%" if pd.notna(r['high_over_open_pct']) else "–"
        lo = f"{r['low_over_open_pct']:.2f}%" if pd.notna(r['low_over_open_pct']) else "–"
        return f"**{r['symbol']}** – Open→Most: **{o}** | PrevClose→Most: {p} | High/Open: {hi} | Low/Open: {lo}"

    if not main_hits.empty:
        lines.append("\n### Jelzések (Open→Most ≥ 3%):")
        for _, r in main_hits.iterrows():
            lines.append("- " + fmt_row(r))
    else:
        lines.append("\n### Jelzések (Open→Most ≥ 3%): nincs")

    if not override_hits.empty:
        lines.append("\n### OVERRIDE – Napi extrém (PrevClose→Most ≥ 8%):")
        for _, r in override_hits.iterrows():
            lines.append("- " + fmt_row(r))

    summary_md = "\n".join(lines)
    print(summary_md)
    with open(reports_dir / "summary_report3.md", "w", encoding="utf-8") as f:
        f.write(summary_md)

    if args.summary:
        write_summary(args.summary, summary_md)

if __name__ == "__main__":
    main()
