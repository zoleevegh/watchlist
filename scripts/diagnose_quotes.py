- name: Diagnosztika (Open→Most & PrevClose→Most, 1p bar)
  if: ${{ github.event.inputs.diag == 'true' }}
  shell: pwsh
  run: |
    $code = @"
import yfinance as yf
import pandas as pd
from datetime import datetime
import pytz

TZ = pytz.timezone("America/New_York")

def session_open_openprice(sym: str):
    t = yf.Ticker(sym)
    # 1 nap, 1 perces adatok, csak REGULAR
    df = t.history(period="1d", interval="1m", prepost=False, actions=False)
    if df.empty:
        return None, None, None, None
    df = df.tz_localize("UTC").tz_convert(TZ)
    # keressük a mai nap első REGULAR bart (>= 09:30)
    today = datetime.now(TZ).date()
    df_today = df[df.index.date == today]
    if df_today.empty:
        return None, None, None, None
    # első elérhető bar a nyitás környékén
    first_bar = df_today.iloc[0]
    open_px = float(first_bar["Open"])
    last_px = float(df_today["Close"].iloc[-1])
    high_px = float(df_today["High"].max())
    low_px  = float(df_today["Low"].min())
    return open_px, last_px, high_px, low_px

def quick_prevclose(sym: str):
    # yfinance fast_info
    t = yf.Ticker(sym)
    fi = getattr(t, "fast_info", None)
    prev = getattr(fi, "previous_close", None) if fi else None
    last = getattr(fi, "last_price", None) if fi else None
    # fallback a perces adatok utolsó Close-ára
    if last is None:
        h = t.history(period="1d", interval="1m", prepost=False, actions=False)
        if not h.empty:
            last = float(h["Close"].iloc[-1])
    return prev, last

def pct(a, b):
    if a is None or b is None or a == 0:
        return None
    return (b - a) / a * 100.0

for sym in ["META","CRWV","MSTR"]:
    o,l,h,lo = session_open_openprice(sym)
    pc, lp = quick_prevclose(sym)
    open_to_now = pct(o, l)
    prev_to_now = pct(pc, lp)
    print(f"== {sym} ==")
    print(f" Open: {o}, Last: {l} -> Open→Now %: {None if open_to_now is None else round(open_to_now,2)}")
    print(f" PrevClose: {pc}, Last: {lp} -> PrevClose→Now %: {None if prev_to_now is None else round(prev_to_now,2)}")
    print(f" High/Open %: {None if (h is None or o in (None,0)) else round((h-o)/o*100,2)}; Low/Open %: {None if (lo is None or o in (None,0)) else round((lo-o)/o*100,2)}")
    print()
"@
    Set-Content -Encoding UTF8 -Path diag.py -Value $code
    python diag.py
    if ($LASTEXITCODE -ne 0) { exit 1 }
