#!/usr/bin/env python3
# analyst_marketbeat.py
# Version: v0.4.0-briefing-cache-4day-coverage

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

def fetch_briefing():
    r = requests.get(BRIEFING_URL, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def extract_updated_date(html_text):
    m = re.search(r"Updated:\s*([0-9]{1,2}-[A-Za-z]{3}-[0-9]{2})", html_text)
    if not m:
        return None
    date_str = m.group(1)
    return datetime.strptime(date_str, "%d-%b-%y").strftime("%Y-%m-%d")

def strip_tags(text):
    text = re.sub(r"<.*?>", "", text)
    return html.unescape(text).strip()

def parse_table(html_text, section_name):
    section_pattern = rf"{section_name}.*?<table.*?</table>"
    section_match = re.search(section_pattern, html_text, re.S | re.I)
    if not section_match:
        return []

    section_html = section_match.group(0)
    rows = re.findall(r"<tr.*?>(.*?)</tr>", section_html, re.S | re.I)

    parsed = []
    for row in rows[1:]:
        cols = re.findall(r"<td.*?>(.*?)</td>", row, re.S | re.I)
        if len(cols) < 4:
            continue

        company = strip_tags(cols[0])
        ticker = strip_tags(cols[1])
        firm = strip_tags(cols[2])
        rating = strip_tags(cols[3])
        pt = strip_tags(cols[4]) if len(cols) > 4 else ""

        if not ticker:
            continue

        parsed.append({
            "ticker": ticker,
            "company": company,
            "firm": firm,
            "rating": rating,
            "price_target": pt
        })

    return parsed

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

def main():
    ensure_reports_dir()
    cache = load_cache()

    html_text = fetch_briefing()
    updated_date = extract_updated_date(html_text)

    if not updated_date:
        print("ERROR: Could not detect Updated date from Briefing page.")
        sys.exit(1)

    upgrades = parse_table(html_text, "Upgrades")
    downgrades = parse_table(html_text, "Downgrades")
    coverage = parse_table(html_text, "Coverage Initiated")

    today_events = dedupe(upgrades + downgrades + coverage)

    cache[updated_date] = {
        "updated": updated_date,
        "events": today_events
    }

    save_cache(cache)

    window_dates = build_window_dates(WINDOW_DAYS)
    missing = [d for d in window_dates if d not in cache]
    coverage_status = "TELJES" if not missing else f"HIÁNYOS – missing: {missing}"

    window_events = []
    for d in window_dates:
        if d in cache:
            for e in cache[d]["events"]:
                e_copy = e.copy()
                e_copy["date"] = d
                window_events.append(e_copy)

    window_events = dedupe(window_events)

    print(f"_forrás státusz: updated={updated_date} | ablak: {window_dates[-1]} -> {window_dates[0]} | lefedettség: {coverage_status}_")
    print()

    if not window_events:
        print("_Nincs releváns elemzői esemény a megadott ablakban._")
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
