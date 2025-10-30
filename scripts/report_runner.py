import argparse, sys
from pathlib import Path
from datetime import datetime, time as dtime
import pandas as pd
import requests
import yfinance as yf
import pytz

# Kimenet UTF-8 a Windows runneren
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TZ_NY = pytz.timezone("America/New_York")
OPEN_T = dtime(9, 30)  # 09:30:00 NY idő
SKIP_TICKERS = {"PKN.WA"}  # kérés szerint kihagyjuk alapból

# ---------- Segédek ---------------------------------------------------------

def pct(a, b):
    try:
        if a is None or b is None or a == 0:
            return None
        return (b - a) / a * 100.0
    except Exception:
        return None

def read_tickers(csv_path: str):
    """Rugalmas beolvasás: ticker + (opcionális) darabszám oszlopok felismerése."""
    df = pd.read_csv(csv_path)
    cols = {c.lower(): c for c in df.columns}

    # ticker oszlop
    ticker_col = None
    for k in ["ticker","symbol","tiker","tic","name","név","papír"]:
        if k in cols:
            ticker_col = cols[k]; break
    if ticker_col is None:
        ticker_col = df.columns[0]  # fallback

    # qty oszlop
    qty_col = None
    for k in ["qty","quantity","db","darab","darabszam","darabszám","shares","pcs","mennyiseg","mennyiség"]:
        if k in cols:
            qty_col = cols[k]; break

    out = []
    for _, row in df.iterrows():
        t = str(row[ticker_col]).strip().upper()
        if not t or t == "NAN":
            continue
        if t in SKIP_TICKERS:
            continue
        qty = None
        if qty_col is not None and pd.notna(row[qty_col]):
            try:
                qty = float(row[qty_col])
            except Exception:
                qty = None
        out.append((t, qty))

    # duplikátumok kiszűrése (megtartjuk az első qty-t)
    seen = {}
    for t, q in out:
        if t not in seen:
            seen[t] = q
    return [(t, seen[t]) for t in seen.keys()]

# ---------- Árlekérés (09:30-as open preferálva) ----------------------------

def _normalize_ny(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(TZ_NY)
    else:
        df.index = df.index.tz_convert(TZ_NY)
    return df

def _pick_session_open(df_today: pd.DataFrame):
    """Válaszd a 09:30:00-hoz legközelebbi REGULAR bart:
       1) pont 09:30:00
       2) az első >=09:30:00 (max +3 perc tolerancia)
       3) ha nincs, None
    """
    if df_today.empty:
        return None
    # 1) pontos egyezés
    exact = df_today[df_today.index.time == OPEN_T]
    if not exact.empty:
        return float(exact.iloc[0]["Open"])
    # 2) első >= 09:30:00
    later = df_today[df_today.index.time >= OPEN_T]
    if not later.empty:
        idx0 = later.index[0]
        # max +3 perc tolerancia
        if (idx0 - later.index[0].replace(hour=9, minute=30, second=0, microsecond=0)).total_seconds() <= 180:
            return float(later.iloc[0]["Open"])
    return None

def fetch_metrics(symbol: str):
    """Open→Now a mai REGULAR perces adatokból, szigorú 09:30 open prioritással.
       Fallback: 5m/5d, majd quote v7 open/price/prev, végül fast_info.previous_close.
    """
    try:
        t = yf.Ticker(symbol)

        # 1) 1m / 1d REGULAR
        hist = t.history(period="1d", interval="1m", prepost=False, actions=False)
        open_px = last_px = high_px = low_px = None
        if hist is not None and not hist.empty:
            hist = _normalize_ny(hist)
            today = datetime.now(TZ_NY).date()
            df = hist[hist.index.date == today]
            if not df.empty:
                open_px = _pick_session_open(df)
                last_px = float(df["Close"].iloc[-1])
                high_px = float(df["High"].max())
                low_px  = float(df["Low"].min())

        # 2) 5m / 5d fallback (ha nincs 1m vagy hiányzik a 09:30 open)
        if open_px is None or last_px is None:
            hist5 = t.history(period="5d", interval="5m", prepost=False, actions=False)
            if hist5 is not None and not hist5.empty:
                hist5 = _normalize_ny(hist5)
                today = datetime.now(TZ_NY).date()
                df5 = hist5[hist5.index.date == today]
                if not df5.empty:
                    if open_px is None:
                        open_px = _pick_session_open(df5)
                    if last_px is None:
                        last_px = float(df5["Close"].iloc[-1])
                    high_px = float(df5["High"].max()) if high_px is None else high_px
                    low_px  = float(df5["Low"].min())  if low_px  is None else low_px

        prev = None

        # 3) quote v7 fallback (ha nincs elég perces adat)
        if open_px is None or last_px is None or prev is None:
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

        # 4) fast_info.previous_close utolsó próbálkozásra
        if prev is None:
            try:
                fi = getattr(t, "fast_info", None)
                prev = getattr(fi, "previous_close", None) if fi else None
            except Exception:
                prev = None

        return {
            "symbol": symbol,
            "open": open_px,
            "last": last_px,
            "open_to_now_pct": None if open_px in (None,0) or last_px is None else round(pct(open_px, last_px), 2),
            "prevclose_to_now_pct": None if prev in (None,0) or last_px is None else round(pct(prev, last_px), 2),
            "high_over_open_pct": None if open_px in (None,0) or high_px is None else round(pct(open_px, high_px), 2),
            "low_over_open_pct":  None if open_px in (None,0) or low_px  is None else round(pct(open_px, low_px), 2),
            "error": None if (open_px is not None and last_px is not None) else "no_price_data"
        }
    except Exception as e:
        return {"symbol": symbol, "error": f"exc:{e}"}

# ---------- Jelentés (#3) ---------------------------------------------------

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

    # #3 fő szűrő (Open→Most) + OVERRIDE (PrevClose→Most)
    main_hits = df[df["open_to_now_pct"].abs() >= 3.00].copy()
    override_hits = df[df["prevclose_to_now_pct"].abs() >= 8.00].copy()

    pos_symbols = {t for t,_ in pos}
    main_hits["is_pos"] = main_hits["symbol"].isin(pos_symbols)
    override_hits["is_pos"] = override_hits["symbol"].isin(pos_symbols)

    main_hits = main_hits.sort_values(by=["is_pos","symbol"], ascending=[False, True])
    override_hits = override_hits.sort_values(by=["is_pos","symbol"], ascending=[False, True])

    main_hits.to_csv(reports_dir / "report3_open_to_now_hits.csv", index=False)
    override_hits.to_csv(reports_dir / "report3_override_prevclose_hits.csv", index=False)

    # Összefoglaló a Job Summary-ba
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

    # Külön sanity blokk – a sokat említett nevek mindig kiírásra kerülnek
    focus = ["AMZN","RGTI","GOOG","IREN","AMD","META","MSTR","CRWV"]
    focus_df = df[df["symbol"].isin(focus)].copy()
    if not focus_df.empty:
        lines.append("\n### Ellenőrzés – fókusz tickerek")
        for _, r in focus_df.iterrows():
            lines.append(f"- **{r['symbol']}** – Open→Most: {fmt(r,'open_to_now_pct')} | PrevClose→Most: {fmt(r,'prevclose_to_now_pct')}")

    summary_md = "\n".join(lines)

    print(summary_md)
    with open(reports_dir / "summary_report3.md", "w", encoding="utf-8") as f:
        f.write(summary_md)

    if args.summary:
        write_summary(args.summary, summary_md)

if __name__ == "__main__":
    main()
