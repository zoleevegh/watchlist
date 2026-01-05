#!/usr/bin/env python3
# report_runner.py — v4.2.1-chartv8-only-accept-report-2026-01-05
#
# FIX: kompatibilitás a workflow-val: elfogadja a --report 1 argumentumot (no-op).
# Yahoo v7 quote továbbra is TELJESEN kikapcsolva (401 a runneren).
# Csak Yahoo Chart v8.
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

import csv
import json
import sys
import traceback
import argparse
import urllib.request

VERSION = "v4.2.1-chartv8-only-accept-report-2026-01-05"


def pct(a, b):
    if a is None or b in (None, 0):
        return None
    return (a / b - 1) * 100.0


def fmt(x):
    if x is None:
        return "n/a"
    return f"{'+' if x >= 0 else ''}{x:.2f}%"


def http_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def load_master(path: str):
    out = []
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            t = (r.get("ticker") or r.get("Ticker") or r.get("symbol") or r.get("Symbol") or "").strip().upper()
            if t:
                out.append(t)
    # dedupe preserve order
    seen = set()
    uniq = []
    for t in out:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq


def chart_prices(t: str):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d&includePrePost=true"
    j = http_json(url)
    res = j["chart"]["result"][0]
    meta = res.get("meta", {})
    prev = meta.get("previousClose") or meta.get("regularMarketPreviousClose")
    pre = meta.get("preMarketPrice")
    post = meta.get("postMarketPrice")
    return prev, pre, post


def main() -> int:
    ap = argparse.ArgumentParser()
    # Compatibility flags (workflow passes these)
    ap.add_argument("--report", type=int, default=1)   # no-op, kept for CLI compatibility
    ap.add_argument("--debug", action="store_true")    # no-op for now
    ap.add_argument("--master", default="reports/master.csv")
    ap.add_argument("--out", default="reports/summary_report_1.md")
    args = ap.parse_args()

    tickers = load_master(args.master)
    rows = []
    any_pm = False

    for t in tickers:
        try:
            prev, pre, post = chart_prices(t)
            pm = pct(pre, prev)
            ah = pct(post, prev)
            if pm is not None:
                any_pm = True
            rows.append((t, pm, ah))
        except Exception:
            rows.append((t, None, None))

    # 1 lista rendezés
    if any_pm:
        rows.sort(key=lambda r: abs(r[1] or 0.0), reverse=True)
        mode = "PM"
    else:
        rows.sort(key=lambda r: abs(r[2] or 0.0), reverse=True)
        mode = "AH"

    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write("# #1 — Premarket check (PRICE ENGINE)\n\n")
        f.write(f"Verzió: {VERSION}\n\n")
        f.write(f"Lista — {mode} abs% szerint\n\n")
        for t, pm, ah in rows:
            f.write(f"- {t} — PM {fmt(pm)} | AH {fmt(ah)}\n")

    if all(pm is None and ah is None for _, pm, ah in rows):
        print("ALL_NA: chart v8 nem adott pre/post adatot", file=sys.stderr, flush=True)
        return 5
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
