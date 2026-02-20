#!/usr/bin/env python3
# analyst_marketbeat.py
# Version: v0.4.3-master-strict-robust-table
#
# Fixes:
# - Robust full-table parsing (no fragile section regex)
# - Guaranteed TMUS capture
# - MASTER + watchlist filter (master.csv)
# - 4-day cache window
# - Coverage reporting

import os
import re
import json
import sys
import html
import requests
from datetime import datetime, timedelta

BRIEFING_URL = "https://hosting.briefing.com/fidelity/Calendars/UpgradesDowngrades.htm"
CACHE_FILE = "reports/briefing_ud_cache.json"
WINDOW_DAYS = 4
TIMEOUT = 20

# ==========================
# Utilities
# ==========================

def ensure_reports_dir():
    os.makedirs("reports", exist_ok=True)

def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def load_master_tickers():
    tickers = set()
    path = "reports/master.csv"
    if not os.path.exists(path):
        return tickers
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(",")
            if not parts:
                continue
            t = parts[0].strip().upper()
            if t and t != "TICKER":
                tickers.add(t)
    return tickers

def fetch_briefing():
    r = requests.get(BRIEFING_URL, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def extract_updated_date(html_text):
    m = re.search(r"Updated:\s*([0-9]{1,2}-[A-Za-z]{3}-[0-9]{2})", html_text)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%d-%b-%y").strftime("%Y-%m-%d")

def strip_tags(text):
    text = re.sub(r"<.*?>", "", text)
    return html.unescape(text).strip()

def parse_all_tables(html_text):
    rows = re.findall(r"<tr.*?>(.*?)</tr>", html_text, re.S | re.I)
    events = []

    for row in rows:
        cols = re.findall(r"<td.*?>(.*?)</td>", row, re.S | re.I)
        if len(cols) < 4:
            continue

        company = strip_tags(cols[0])
        ticker = strip_tags(cols[1]).upper()
        firm = strip_tags(cols[2])
        rating = strip_tags(cols[3])
        pt = strip_tags(cols[4]) if len(cols) > 4 else ""

        if not ticker or len(ticker) > 6:
            continue

        events.append({
            "ticker": ticker,
            "company": company,
            "firm": firm,
            "rating": rating,
            "price_target": pt
        })

    return events

def dedupe(events):
    seen = set()
    unique = []
    for e in events:
        key = (e["ticker"], e["firm"], e["rating"], e["price_target"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(e)
    return unique

def build_window_dates(days):
    today = datetime.utcnow().date()
    return [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

# ==========================
# Main
# ==========================

def main():

    ensure_reports_dir()
    cache = load_cache()
    master = load_master_tickers()

    html_text = fetch_briefing()
    updated_date = extract_updated_date(html_text)

    if not updated_date:
        print("ERROR: Updated date not found")
        sys.exit(1)

    today_events = dedupe(parse_all_tables(html_text))

    cache[updated_date] = {
        "updated": updated_date,
        "events": today_events
    }

    save_cache(cache)

    window_dates = build_window_dates(WINDOW_DAYS)
    missing = [d for d in window_dates if d not in cache]

    window_events = []
    for d in window_dates:
        if d in cache:
            for e in cache[d]["events"]:
                if e["ticker"] in master:
                    e_copy = e.copy()
                    e_copy["date"] = d
                    window_events.append(e_copy)

    window_events = dedupe(window_events)

    coverage_status = "TELJES" if not missing else f"HIÁNYOS ({len(window_dates)-len(missing)}/{WINDOW_DAYS}) missing: {missing}"

    print(f"_forrás státusz: updated={updated_date} | ablak: {window_dates[-1]} -> {window_dates[0]} | lefedettség: {coverage_status} | MASTER találat: {len(window_events)}_")
    print()

    if not window_events:
        print("_Nincs releváns elemzői esemény a megadott ablakban a MASTER tickereidre._")
        return

    grouped = {}
    for e in window_events:
        grouped.setdefault(e["ticker"], []).append(e)

    for ticker in sorted(grouped.keys()):
        print(f"## {ticker}")
        for e in grouped[ticker]:
            print(f"- {e['date']} – {e['firm']} – {e['rating']} | Célár: {e['price_target']}")
        print()

if __name__ == "__main__":
    main()
