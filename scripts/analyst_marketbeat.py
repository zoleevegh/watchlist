#!/usr/bin/env python3
# analyst_marketbeat.py
# Verzió: v0.4.5-hu-local

import os
import re
import json
import sys
import html
import urllib.request
from datetime import datetime, timedelta

BRIEFING_URL = "https://hosting.briefing.com/fidelity/Calendars/UpgradesDowngrades.htm"
CACHE_FILE = "reports/briefing_ud_cache.json"
WINDOW_DAYS = 4
TIMEOUT = 20
OUT_MD = "reports/analyst_last4d.md"
OUT_JSON = "reports/analyst_last4d.json"

RATING_MAP = {
    "Strong Buy": "Erős vétel",
    "Buy": "Vétel",
    "Overweight": "Felülsúlyozás",
    "Outperform": "Felülteljesítő",
    "Hold": "Tartás",
    "Equal Weight": "Piaci súly",
    "Market Perform": "Piaci teljesítő",
    "Underweight": "Alulsúlyozás",
    "Sell": "Eladás",
    "Neutral": "Semleges"
}

def translate_rating(text):
    if not text:
        return text
    for eng, hu in RATING_MAP.items():
        text = text.replace(eng, hu)
    text = text.replace("»", "→")
    return text

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
    req = urllib.request.Request(
        BRIEFING_URL,
        headers={"User-Agent": "Mozilla/5.0"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        data = resp.read()
    return data.decode("utf-8", errors="replace")

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

        ticker = strip_tags(cols[1]).upper()
        firm = strip_tags(cols[2])
        rating = translate_rating(strip_tags(cols[3]))
        pt = strip_tags(cols[4]) if len(cols) > 4 else ""

        if not ticker or len(ticker) > 6:
            continue

        events.append({
            "ticker": ticker,
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

def build_window_dates(anchor_ymd, days):
    anchor = datetime.strptime(anchor_ymd, "%Y-%m-%d").date()
    return [(anchor - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

def main():
    ensure_reports_dir()
    cache = load_cache()
    master = load_master_tickers()

    html_text = fetch_briefing()
    updated_date = extract_updated_date(html_text)

    if not updated_date:
        print("HIBA: Nem található frissítési dátum")
        sys.exit(1)

    today_events = dedupe(parse_all_tables(html_text))

    cache[updated_date] = {"updated": updated_date, "events": today_events}
    save_cache(cache)

    window_dates = build_window_dates(updated_date, WINDOW_DAYS)
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

    coverage_status = "TELJES" if not missing else f"HIÁNYOS ({len(window_dates)-len(missing)}/{WINDOW_DAYS}) hiányzik: {missing}"

    lines = []
    lines.append(
        f"_Forrás: {updated_date} | Időablak: {window_dates[-1]} → {window_dates[0]} | Lefedettség: {coverage_status} | Találatok: {len(window_events)}_"
    )
    lines.append("")

    if not window_events:
        lines.append("_Nincs releváns elemzői esemény a kiválasztott időablakban._")
        md = "\n".join(lines).strip() + "\n"
        with open(OUT_MD, "w", encoding="utf-8") as f:
            f.write(md)
        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump({"updated": updated_date, "window_days": WINDOW_DAYS, "events": []}, f, ensure_ascii=False, indent=2)
        print(md)
        return

    grouped = {}
    for e in window_events:
        grouped.setdefault(e["ticker"], []).append(e)

    for ticker in sorted(grouped.keys()):
        lines.append(f"## {ticker}")
        for e in grouped[ticker]:
            pt = e.get("price_target") or ""
            pt_part = f" | Célár: {pt}" if pt else ""
            lines.append(f"- {e['date']} – {e['firm']} – {e['rating']}{pt_part}")
        lines.append("")

    md = "\n".join(lines).strip() + "\n"
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write(md)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump({"updated": updated_date, "window_days": WINDOW_DAYS, "events": window_events}, f, ensure_ascii=False, indent=2)

    print(md)

if __name__ == "__main__":
    main()
