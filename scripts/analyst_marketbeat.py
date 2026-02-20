#!/usr/bin/env python3
# analyst_marketbeat.py
# Version: v0.4.2-briefing-cache-coveragecheck-masterfilter-nodeps
#
# Source: Briefing.com (Fidelity) Upgrades/Downgrades snapshot page.
# Note: The source page itself is not historical; we build a multi-day window via a persistent daily cache.

import os
import re
import json
import sys
import html
import csv
from datetime import datetime, timedelta
from urllib import request as urlrequest

BRIEFING_URL = "https://hosting.briefing.com/fidelity/Calendars/UpgradesDowngrades.htm"
CACHE_FILE = "reports/briefing_ud_cache.json"
MASTER_CSV = "reports/master.csv"

WINDOW_DAYS = 4
TIMEOUT = 20

# --- HU terminology ---
ACTION_HU = {
    "upgrade": "felminősítés",
    "downgrade": "leminősítés",
    "initiation": "lefedés indítása",
}

# Conservative mapping; unknown terms are left as-is.
RATING_HU = {
    "strong buy": "Erős vétel",
    "strong sell": "Erős eladás",
    "buy": "Vétel",
    "hold": "Tartás",
    "sell": "Eladás",
    "neutral": "Semleges",
    "outperform": "Piac feletti teljesítés",
    "underperform": "Piac alatti teljesítés",
    "overweight": "Túlsúly",
    "underweight": "Alulsúly",
    "equal-weight": "Semleges súly",
    "sector weight": "Szektorsúly",
    "market perform": "Piaci teljesítés",
    "peer perform": "Szektorátlag",
    "reduce": "Csökkentés",
    "accumulate": "Gyűjtés",
    "perform": "Teljesítés",
    "in-line": "Várakozásoknak megfelelő",
}

def ensure_reports_dir() -> None:
    os.makedirs("reports", exist_ok=True)

def load_cache() -> dict:
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_cache(cache: dict) -> None:
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

def _http_get(url: str) -> str:
    req = urlrequest.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; PriceEngine/1.0; +https://github.com/)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        method="GET",
    )
    with urlrequest.urlopen(req, timeout=TIMEOUT) as resp:
        data = resp.read()
    # Best-effort decode
    try:
        return data.decode("utf-8", errors="replace")
    except Exception:
        return data.decode(errors="replace")

def fetch_briefing() -> str:
    return _http_get(BRIEFING_URL)

def extract_updated_date(html_text: str):
    # Example: "Updated: 19-Feb-26 08:43 ET"
    m = re.search(r"Updated:\s*([0-9]{1,2}-[A-Za-z]{3}-[0-9]{2})", html_text)
    if not m:
        return None
    date_str = m.group(1)
    return datetime.strptime(date_str, "%d-%b-%y").strftime("%Y-%m-%d")

def _strip_tags(text: str) -> str:
    text = re.sub(r"<.*?>", "", text, flags=re.S)
    return html.unescape(text).strip()

def _find_section_table(html_text: str, label: str) -> str:
    # Find a heading-like token: "> Upgrades <" (prevents matching page title "Upgrades/Downgrades")
    m = re.search(rf">\s*{re.escape(label)}\s*<", html_text, flags=re.I)
    if not m:
        return ""
    tail = html_text[m.end():]
    t0 = re.search(r"<table\b", tail, flags=re.I)
    if not t0:
        return ""
    tail2 = tail[t0.start():]
    t1 = re.search(r"</table>", tail2, flags=re.I)
    if not t1:
        return ""
    return tail2[:t1.end()]

def parse_table(html_text: str, section_label: str, action_key: str) -> list:
    table_html = _find_section_table(html_text, section_label)
    if not table_html:
        return []

    rows = re.findall(r"<tr\b.*?>(.*?)</tr>", table_html, re.S | re.I)
    if not rows:
        return []

    parsed = []
    # Skip header row
    for row in rows[1:]:
        cols = re.findall(r"<td\b.*?>(.*?)</td>", row, re.S | re.I)
        if len(cols) < 4:
            continue

        company = _strip_tags(cols[0])
        ticker = _strip_tags(cols[1]).upper()
        firm = _strip_tags(cols[2])
        rating = _strip_tags(cols[3])
        pt = _strip_tags(cols[4]) if len(cols) > 4 else ""

        if not ticker:
            continue

        parsed.append({
            "ticker": ticker,
            "company": company,
            "firm": firm,
            "rating": rating,
            "price_target": pt,
            "action": action_key,
        })
    return parsed

def _dedupe(events: list) -> list:
    # include action and date (if present) to avoid false dedupe across sections/days
    seen = set()
    unique = []
    for e in events:
        key = (
            e.get("date", ""),
            e.get("ticker", ""),
            e.get("action", ""),
            e.get("firm", ""),
            e.get("rating", ""),
            e.get("price_target", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(e)
    return unique

def build_window_dates(days: int) -> list:
    today = datetime.utcnow().date()
    return [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]

def load_master_tickers() -> set:
    if not os.path.exists(MASTER_CSV):
        return set()
    tickers = set()
    try:
        with open(MASTER_CSV, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return set()
            # Find likely ticker column
            fields = [c.strip() for c in reader.fieldnames]
            lower_map = {c.lower(): c for c in fields}
            col = None
            for cand in ("ticker", "symbol", "tickers", "code"):
                if cand in lower_map:
                    col = lower_map[cand]
                    break
            if col is None:
                # fallback: first column
                col = fields[0]
            for row in reader:
                v = (row.get(col) or "").strip().upper()
                if not v:
                    continue
                # handle multiple tickers in one cell (rare)
                for part in re.split(r"[\s,;|]+", v):
                    p = part.strip().upper()
                    if p:
                        tickers.add(p)
    except Exception:
        return set()
    return tickers

def _translate_rating(text: str) -> str:
    if not text:
        return text
    out = text

    # Normalize separators
    out = out.replace("»", "→").replace(">>", "→").replace("->", "→")

    # Replace multi-word keys first (strong buy, equal-weight etc.)
    keys = sorted(RATING_HU.keys(), key=lambda s: -len(s))
    for k in keys:
        hu = RATING_HU[k]
        # word-boundary-ish, case-insensitive; allow hyphen or space matching for keys with hyphen/space
        pattern = re.escape(k)
        # turn escaped spaces into "\s+"
        pattern = pattern.replace("\ ", r"\s+")
        out = re.sub(rf"(?i)\b{pattern}\b", hu, out)
    # cleanup extra spaces
    out = re.sub(r"\s+", " ", out).strip()
    return out

def _format_line(e: dict) -> str:
    date = e.get("date", "n/a")
    firm = e.get("firm", "n/a")
    action = ACTION_HU.get(e.get("action", ""), e.get("action", "n/a"))
    rating = _translate_rating(e.get("rating", "n/a"))
    pt = (e.get("price_target") or "").strip()
    if pt:
        return f"- {date} – {firm} – {action} | Ajánlás: {rating} | Célár: {pt}"
    return f"- {date} – {firm} – {action} | Ajánlás: {rating}"

def main():
    ensure_reports_dir()
    cache = load_cache()

    # Master tickers = positions + watchlist together
    master_tickers = load_master_tickers()
    master_ok = bool(master_tickers)

    html_text = fetch_briefing()
    updated_date = extract_updated_date(html_text)

    if not updated_date:
        print("ERROR: Nem sikerült kinyerni az 'Updated' dátumot a Briefing oldalból.")
        sys.exit(1)

    upgrades = parse_table(html_text, "Upgrades", "upgrade")
    downgrades = parse_table(html_text, "Downgrades", "downgrade")
    coverage_init = parse_table(html_text, "Coverage Initiated", "initiation")

    today_events = _dedupe(upgrades + downgrades + coverage_init)

    cache[updated_date] = {
        "updated": updated_date,
        "events": today_events,
    }
    save_cache(cache)

    window_dates = build_window_dates(WINDOW_DAYS)
    missing = [d for d in window_dates if d not in cache]
    got_days = sum(1 for d in window_dates if d in cache)
    need_days = len(window_dates)

    coverage_status = "TELJES" if not missing else f"HIÁNYOS – van={got_days}/{need_days} nap (hiányzik: {missing})"
    coverage_note = ""
    if need_days > 1 and got_days < need_days:
        coverage_note = "_Megjegyzés: a forrás oldal nem historikus (nem lapozható dátum szerint); az előző napokat csak a perzisztens cache tudja felépíteni több napi futásból._"

    window_events = []
    for d in window_dates:
        if d in cache:
            for e in cache[d].get("events", []):
                e_copy = dict(e)
                e_copy["date"] = d
                window_events.append(e_copy)

    window_events = _dedupe(window_events)

    total_events = len(window_events)

    # Filter to user's MASTER tickers (positions + watchlist)
    if master_ok:
        window_events = [e for e in window_events if e.get("ticker", "").upper() in master_tickers]
    filtered_events = len(window_events)

    # Status header (printed to stdout; workflow captures into block)
    win_lo = window_dates[-1]
    win_hi = window_dates[0]
    master_info = "MASTER:OK" if master_ok else "MASTER:HIÁNYZIK (reports/master.csv)"
    print(f"_forrás státusz: updated={updated_date} | ablak: {win_lo} -> {win_hi} | lefedettség: {coverage_status} | {master_info} | találat: {filtered_events}/{total_events}_")
    if coverage_note:
        print(coverage_note)
    print()

    if not master_ok:
        print("_FIGYELEM: a reports/master.csv nem elérhető, ezért nincs ticker-szűrés (minden esemény kilistázva)._")
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
            print(_format_line(e))
        print()

if __name__ == "__main__":
    main()
