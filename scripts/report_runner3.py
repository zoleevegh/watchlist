#!/usr/bin/env python3
# report_runner3.py — v0.1.0-price-engine-report3-runner-2026-02-05
#
# FIX / CÉL:
# - Hiányzó #3 runner pótlása: a GitHub Actions "Run PRICE ENGINE (#3)" lépés a report_runner3.py / scripts/report_runner3.py
#   fájlt keresi. Ez a fájl egy önálló #3 runner, nem nyúl a #1-hez.
# - #3 (WEBBIBLIA): "Ma nyitástól mostanáig" (Open→Most) százalékos mozgás ticker szinten, két tizedes.
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

import csv
import io
import json
import sys
import traceback
import argparse
import urllib.request
import time
import datetime
from typing import Any, Dict, Optional, List, Tuple

VERSION = "v0.1.0-price-engine-report3-runner-2026-02-05"

def http_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read())

def http_json_retry(url: str, retries: int = 3, base_sleep: float = 0.6) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            return http_json(url)
        except Exception as e:
            last_err = e
            time.sleep(base_sleep * (2 ** i))
    if last_err:
        raise last_err
    raise RuntimeError("http_json_retry: unknown error")

def _read_text_from_path_or_url(path_or_url: str) -> str:
    if path_or_url.lower().startswith(("http://", "https://")):
        req = urllib.request.Request(path_or_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
        return raw.decode("utf-8", errors="replace")
    with open(path_or_url, "r", encoding="utf-8", errors="replace", newline="") as f:
        return f.read()

def _to_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None

def load_master_tickers(path_or_url: str) -> List[str]:
    txt = _read_text_from_path_or_url(path_or_url)
    fobj = io.StringIO(txt)
    rdr = csv.DictReader(fobj)
    out: List[str] = []
    seen = set()
    for r in rdr:
        t = (r.get("Ticker") or r.get("ticker") or r.get("Symbol") or r.get("symbol") or "").strip().upper()
        if not t or t in seen:
            continue
        out.append(t)
        seen.add(t)
    return out

def yahoo_quote_batch(symbols: List[str], retries: int = 2) -> Dict[str, Dict[str, Any]]:
    syms = [s.strip().upper() for s in symbols if s and s.strip()]
    out: Dict[str, Dict[str, Any]] = {}
    if not syms:
        return out

    CHUNK = 80
    base = "https://query2.finance.yahoo.com/v7/finance/quote?symbols="
    for i in range(0, len(syms), CHUNK):
        chunk = syms[i:i + CHUNK]
        url = base + ",".join(chunk)
        data = http_json_retry(url, retries=retries)
        res = (((data or {}).get("quoteResponse") or {}).get("result")) or []
        for q in res:
            sym = (q.get("symbol") or "").upper()
            if sym:
                out[sym] = q
    return out

def pct(curr: Optional[float], base: Optional[float]) -> Optional[float]:
    if curr is None or base in (None, 0):
        return None
    try:
        return (float(curr) / float(base) - 1.0) * 100.0
    except Exception:
        return None

def fmt(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{'+' if x >= 0 else ''}{x:.2f}%"

def now_budapest() -> datetime.datetime:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/Budapest")
    except Exception:
        tz = datetime.timezone(datetime.timedelta(hours=1))
    return datetime.datetime.now(tz)

def write_header(f, interval_start: str, interval_end: str):
    rt = now_budapest().strftime("%H:%M")
    f.write("# #3 — Open→Most (PRICE ENGINE)\n\n")
    f.write(f"Verzió: {VERSION} | Futás ideje: {rt}\n")
    f.write(f"Időintervallum (ellenőrzés): {interval_start} – {interval_end}\n\n")

def budapest_us_open_local(today: datetime.date) -> str:
    return f"{today.isoformat()} 15:30"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_3.md")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    tickers = load_master_tickers(args.master)
    quotes = yahoo_quote_batch(tickers, retries=2)

    rows: List[Tuple[str, Optional[float]]] = []
    missing: List[str] = []

    for t in tickers:
        q = quotes.get(t)
        if not q:
            missing.append(t)
            rows.append((t, None))
            continue

        base_open = _to_float(q.get("regularMarketOpen"))
        prev_close = _to_float(q.get("regularMarketPreviousClose") or q.get("previousClose"))
        curr = _to_float(q.get("regularMarketPrice") or q.get("preMarketPrice") or q.get("postMarketPrice"))

        base = base_open if base_open not in (None, 0) else prev_close
        move = pct(curr, base)

        rows.append((t, move))

    def keyfn(r):
        _, mv = r
        if mv is None:
            return (0, -1e9)
        return (1, abs(float(mv)))

    rows.sort(key=keyfn, reverse=True)

    now_loc = now_budapest()
    interval_end = now_loc.strftime("%Y-%m-%d %H:%M")
    interval_start = budapest_us_open_local(now_loc.date())

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        write_header(f, interval_start, interval_end)

        f.write("## Open→Most (ma)\n\n")
        for t, mv in rows:
            f.write(f"- {t} — Open→Most {fmt(mv)}\n")

        if missing:
            f.write("\n## Lefedettség: HIÁNYOS — nem elérhető ticker(ek)\n\n")
            f.write("- " + ", ".join(missing) + "\n")
        else:
            f.write("\n## Lefedettség: TELJES\n")

        if args.debug:
            f.write("\n\n## Debug\n\n")
            f.write(f"- tickers_total: {len(tickers)}\n")
            f.write(f"- quotes_returned: {len(quotes)}\n")
            f.write(f"- missing: {len(missing)}\n")

    print(f"RUNNER_VERSION={VERSION}", file=sys.stderr, flush=True)
    print(f"tickers={len(tickers)} missing={len(missing)} out={args.out}", file=sys.stderr, flush=True)
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
