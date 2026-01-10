#!/usr/bin/env python3
# earnings.py
# v0.1.0-earnings-only-nasdaq-investing
# PURPOSE: Earnings date audit – NO Yahoo, NO runner integration

import csv
import json
import time
import requests
from pathlib import Path
from bs4 import BeautifulSoup

MASTER_CSV: "https://docs.google.com/spreadsheets/d/e/2PACX-1vQkgyw0ONwSfc2KnrmWGqf8fWVQxS-R08RySndB69KDJ8L1Cz-H2F1AZtyDfhBGedC0qdC1SFo_aDye/pub?output=csv"
OUT_JSON = "reports/earnings_audit.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (earnings-audit)"
}

def load_tickers():
    tickers = []
    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            t = r.get("Ticker") or r.get("ticker")
            if t:
                tickers.append(t.strip().upper())
    return sorted(set(tickers))

def fetch_nasdaq_earnings(ticker):
    url = f"https://api.nasdaq.com/api/company/{ticker}/earnings-surprise"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        rows = data.get("data", {}).get("earningsSurpriseTable", {}).get("rows", [])
        if not rows:
            return None
        # legközelebbi earnings = első sor
        return rows[0].get("earningsDate")
    except Exception:
        return None

def fetch_investing_earnings(ticker):
    url = f"https://www.investing.com/search/?q={ticker}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        link = soup.select_one("a.js-inner-all-results-quote-item")
        if not link:
            return None

        href = link.get("href")
        cal_url = f"https://www.investing.com{href}-earnings"
        r2 = requests.get(cal_url, headers=HEADERS, timeout=15)
        if r2.status_code != 200:
            return None

        soup2 = BeautifulSoup(r2.text, "html.parser")
        date_cell = soup2.select_one("td.earningsDate")
        if not date_cell:
            return None

        return date_cell.text.strip()
    except Exception:
        return None

def main():
    Path("reports").mkdir(exist_ok=True)

    tickers = load_tickers()
    out = {
        "meta": {
            "version": "v0.1.0",
            "generated": int(time.time()),
            "tickers_total": len(tickers)
        },
        "results": {}
    }

    for t in tickers:
        date = fetch_nasdaq_earnings(t)
        source = "nasdaq"

        if not date:
            date = fetch_investing_earnings(t)
            source = "investing"

        if not date:
            out["results"][t] = {
                "earnings_date": None,
                "source": "unavailable"
            }
        else:
            out["results"][t] = {
                "earnings_date": date,
                "source": source
            }

        time.sleep(0.3)  # rate-limit védelem

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"EARNINGS_AUDIT_DONE: {OUT_JSON}")

if __name__ == "__main__":
    main()
