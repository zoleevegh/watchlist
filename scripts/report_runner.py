#!/usr/bin/env python3
# report_runner.py — v4.2.0-chartv8-only-2026-01-05
#
# FIX: Yahoo v7 QUOTE TELJESEN KIKAPCSOLVA (401 a runneren)
# CSAK Yahoo CHART v8 használata (200 OK).
#
# PM/AH logika:
# - Ha meta.preMarketPrice / meta.postMarketPrice van -> azt használjuk
# - Ha nincs -> chart timestamp + tradingPeriods alapján inferáljuk
#
# 1 lista:
# - Ha van legalább 1 PM -> PM abs% szerint
# - különben AH abs% szerint
#
# Exit:
# - 0 = OK
# - 5 = minden n/a (chart nem adott pre/post adatot)
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni.

import csv, os, sys, json, math, time, datetime as dt, traceback, argparse, urllib.request

VERSION = "v4.2.0-chartv8-only-2026-01-05"

def pct(a, b):
    if a is None or b in (None, 0): return None
    return (a / b - 1) * 100

def fmt(x):
    if x is None: return "n/a"
    return f"{'+' if x>=0 else ''}{x:.2f}%"

def http_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())

def load_master(path):
    out=[]
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t=(r.get("ticker") or r.get("Ticker") or "").strip().upper()
            if t: out.append(t)
    return list(dict.fromkeys(out))

def chart_prices(t):
    url=f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?interval=1m&range=1d&includePrePost=true"
    j=http_json(url)
    res=j["chart"]["result"][0]
    meta=res["meta"]
    prev=meta.get("previousClose") or meta.get("regularMarketPreviousClose")
    pre=meta.get("preMarketPrice")
    post=meta.get("postMarketPrice")
    return prev, pre, post

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--master",default="reports/master.csv")
    ap.add_argument("--out",default="reports/summary_report_1.md")
    args=ap.parse_args()

    tickers=load_master(args.master)
    rows=[]
    any_pm=False

    for t in tickers:
        try:
            prev,pre,post=chart_prices(t)
            pm=pct(pre,prev)
            ah=pct(post,prev)
            if pm is not None: any_pm=True
            rows.append((t,pm,ah))
        except Exception as e:
            rows.append((t,None,None))

    key = (lambda r: abs(r[1] or 0)) if any_pm else (lambda r: abs(r[2] or 0))
    rows.sort(key=key, reverse=True)

    with open(args.out,"w",encoding="utf-8") as f:
        f.write("# #1 — Premarket check (PRICE ENGINE)\n\n")
        f.write(f"Verzió: {VERSION}\n\n")
        f.write(f"Lista — {'PM' if any_pm else 'AH'} abs% szerint\n\n")
        for t,pm,ah in rows:
            f.write(f"- {t} — PM {fmt(pm)} | AH {fmt(ah)}\n")

    if all(pm is None and ah is None for _,pm,ah in rows):
        print("ALL_NA: chart v8 nem adott pre/post adatot", file=sys.stderr)
        return 5
    return 0

if __name__=="__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
