#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyst_marketbeat.py — MarketBeat analyst feed (upgrades/downgrades + PT changes)
PRIMARY source: https://www.marketbeat.com/ratings/us/ (broad tape-like table)

Version: v0.3.34 (2026-02-02)
IMÁDSÁG (2 sor):
bocsáss meg uram, mert megint a weboldal "reporting date" mezőjét nem olvastam ki rendesen.
adj erőt, hogy a CI-ban is azt lássam, amit a böngészőben látok. 🙏

Key points:
- NO hard dependency on `requests` (stdlib urllib).
- Does NOT rely on pandas.read_html / lxml.
- If the /ratings/us/ table has no per-row date column, infers date from the "Reporting Date" control on the page.
- Produces valid JSON even when empty ([]), and a non-empty markdown section with explicit status.
- Persistent cache (event + last_seen) so items that disappear from the live table can still be shown within N-day window.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
MB_URL = "https://www.marketbeat.com/ratings/us/"

DEFAULT_CACHE_PATH = "reports/marketbeat_events.json"  # persistent event cache (in repo + actions cache)

# ----------------------------
# Utilities
# ----------------------------

def utc_today() -> dt.date:
    return dt.datetime.utcnow().date()

def parse_reporting_date_any(s: str) -> Optional[dt.date]:
    """
    Accepts strings like:
      "2026. 02. 02."
      "2026-02-02"
      "02/02/2026"
    Returns date or None.
    """
    s = (s or "").strip()
    if not s:
        return None
    # Normalize common variants
    s2 = re.sub(r"\s+", " ", s)
    s2 = s2.replace(" .", ".").replace(". ", ".")
    # 2026.02.02. or 2026. 02. 02.
    m = re.search(r"(\d{4})\.\s*(\d{2})\.\s*(\d{2})\.", s2)
    if m:
        y, mo, d = map(int, m.groups())
        return dt.date(y, mo, d)
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s2)
    if m:
        y, mo, d = map(int, m.groups())
        return dt.date(y, mo, d)
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", s2)
    if m:
        mo, d, y = map(int, m.groups())
        return dt.date(y, mo, d)
    return None

def is_challenge_or_blocked(html: str) -> bool:
    h = (html or "").lower()
    # Typical bot/challenge signatures
    needles = [
        "cloudflare", "attention required", "verify you are human", "captcha",
        "enable javascript", "bot detection", "access denied", "unusual traffic"
    ]
    return any(n in h for n in needles)

def http_get(url: str, timeout: int = 20) -> Tuple[int, str]:
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html,*/*"})
    with urlopen(req, timeout=timeout) as resp:
        status = getattr(resp, "status", 200)
        body = resp.read()
        try:
            txt = body.decode("utf-8", errors="replace")
        except Exception:
            txt = body.decode(errors="replace")
        return int(status), txt

def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)

def safe_read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return None

def safe_write_text(path: str, text: str) -> None:
    ensure_dir_for_file(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def safe_write_json(path: str, obj) -> None:
    ensure_dir_for_file(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=False)

def load_cache(cache_path: str) -> Dict[str, dict]:
    raw = safe_read_text(cache_path)
    if raw is None:
        return {}
    raw = raw.strip()
    if not raw:
        return {}
    if raw.lower() == "placeholder":
        # quarantine
        bad = cache_path + ".bad"
        try:
            safe_write_text(bad, raw)
        except Exception:
            pass
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            # older list format -> convert
            out = {}
            for it in data:
                if isinstance(it, dict) and "key" in it:
                    out[str(it["key"])] = it
            return out
        return {}
    except Exception:
        bad = cache_path + ".bad"
        try:
            safe_write_text(bad, raw)
        except Exception:
            pass
        return {}

def save_cache(cache_path: str, cache: Dict[str, dict]) -> None:
    safe_write_json(cache_path, cache)

def cache_key(ticker: str, date: str, action: str, firm: str, analyst: str, pt: str, rating: str) -> str:
    return "|".join([
        (ticker or "").upper(),
        date or "",
        (action or "").strip(),
        (firm or "").strip(),
        (analyst or "").strip(),
        (pt or "").strip(),
        (rating or "").strip(),
    ])

# ----------------------------
# HTML parsing for /ratings/us/
# ----------------------------

@dataclass
class RatingRow:
    ticker: str
    action: str
    brokerage: str
    analyst: str
    current_price: str
    price_target: str
    rating: str

class RatingsUSParser(HTMLParser):
    """
    Minimal HTML parser:
    - Extracts the first large table under the ratings page (contains tickers like AAPL).
    - Collects text in <td>.
    - Also tries to capture "Reporting Date" input value.
    """
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_tr = False
        self.in_td = False
        self.td_text = []
        self.current_row = []
        self.rows: List[List[str]] = []
        self.table_depth = 0

        # reporting date capture
        self.reporting_date_value: Optional[str] = None
        self._in_input = False

        # heuristics: pick the first table that has ticker-like cells
        self._tables: List[List[List[str]]] = []
        self._current_table_rows: List[List[str]] = []
        self._current_table_rows_started = False

    def handle_starttag(self, tag, attrs):
        attrs_d = dict(attrs)
        if tag == "input":
            # common: name="reportingDate" value="2026-02-02" or similar
            name = (attrs_d.get("name") or attrs_d.get("id") or "").lower()
            if "report" in name and "date" in name:
                val = attrs_d.get("value") or ""
                if val:
                    self.reporting_date_value = val
        if tag == "table":
            self.in_table = True
            self.table_depth += 1
            self._current_table_rows = []
            self._current_table_rows_started = True
        elif tag == "tr" and self.in_table:
            self.in_tr = True
            self.current_row = []
        elif tag == "td" and self.in_tr:
            self.in_td = True
            self.td_text = []

    def handle_endtag(self, tag):
        if tag == "td" and self.in_td:
            self.in_td = False
            text = " ".join("".join(self.td_text).split())
            self.current_row.append(text)
        elif tag == "tr" and self.in_tr:
            self.in_tr = False
            # Store row if has some content
            if any(c.strip() for c in self.current_row):
                self._current_table_rows.append(self.current_row)
        elif tag == "table" and self.in_table:
            self.table_depth -= 1
            if self.table_depth <= 0:
                self.in_table = False
                self.table_depth = 0
                if self._current_table_rows_started:
                    self._tables.append(self._current_table_rows)
                self._current_table_rows_started = False

    def handle_data(self, data):
        if self.in_td:
            self.td_text.append(data)

def parse_ratings_us(html: str) -> Tuple[Optional[dt.date], List[RatingRow]]:
    parser = RatingsUSParser()
    parser.feed(html)

    # Determine reporting date
    rep = None
    if parser.reporting_date_value:
        rep = parse_reporting_date_any(parser.reporting_date_value)
    if rep is None:
        # Try to find visible date patterns in the page
        # (works with "2026. 02. 02." style)
        m = re.search(r"(\d{4}\.\s*\d{2}\.\s*\d{2}\.)", html)
        if m:
            rep = parse_reporting_date_any(m.group(1))

    # Choose the best table: one whose rows look like [AAPL, ...]
    best_table = None
    best_score = -1
    for t in getattr(parser, "_tables", []):
        score = 0
        for r in t[:50]:
            if not r:
                continue
            # ticker tends to be in first cell, uppercase 1-5 chars
            c0 = (r[0] or "").strip()
            if re.fullmatch(r"[A-Z]{1,6}", c0):
                score += 2
            if any("Outperform".lower() in (c or "").lower() for c in r):
                score += 1
        if score > best_score:
            best_score = score
            best_table = t

    rows: List[RatingRow] = []
    if not best_table:
        return rep, rows

    # The table may include header row; detect by presence of "Company" etc.
    for r in best_table:
        if not r or len(r) < 3:
            continue
        if any((c or "").strip().lower() in ("company", "action", "brokerage", "analyst", "rating") for c in r):
            continue

        # Expected columns (from screenshot): Company/Ticker, Action, Brokerage, Analyst, Current Price, Price Target, Rating, Details...
        # But sometimes current/pt may be missing in a row.
        ticker = (r[0] or "").strip().upper()
        if not re.fullmatch(r"[A-Z]{1,6}", ticker):
            # Sometimes first cell is like "AAPL Apple" -> extract
            m = re.match(r"^([A-Z]{1,6})\b", (r[0] or "").strip())
            if m:
                ticker = m.group(1)
            else:
                continue

        action = r[1].strip() if len(r) > 1 else ""
        brokerage = r[2].strip() if len(r) > 2 else ""
        analyst = r[3].strip() if len(r) > 3 else ""
        current_price = r[4].strip() if len(r) > 4 else ""
        price_target = r[5].strip() if len(r) > 5 else ""
        rating = r[6].strip() if len(r) > 6 else ""

        # Normalize empty locks (some cells may be empty because login required)
        # Keep row anyway; missing fields are acceptable.
        rows.append(RatingRow(
            ticker=ticker,
            action=action,
            brokerage=brokerage,
            analyst=analyst,
            current_price=current_price,
            price_target=price_target,
            rating=rating
        ))

    return rep, rows

# ----------------------------
# Master tickers
# ----------------------------

def load_master_tickers(master_path: str) -> List[str]:
    if master_path.startswith("http://") or master_path.startswith("https://"):
        # Download master CSV
        status, txt = http_get(master_path, timeout=25)
        if status >= 400:
            raise RuntimeError(f"MASTER download failed HTTP {status}")
        lines = txt.splitlines()
        reader = csv.DictReader(lines)
        return extract_tickers_from_dictreader(reader)

    if not os.path.isfile(master_path):
        raise FileNotFoundError(master_path)

    with open(master_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        return extract_tickers_from_dictreader(reader)

def extract_tickers_from_dictreader(reader: csv.DictReader) -> List[str]:
    # Find best ticker column
    fieldnames = [c or "" for c in (reader.fieldnames or [])]
    lower = [c.lower() for c in fieldnames]
    candidates = ["ticker", "symbol", "tickers", "symbols"]
    idx = None
    for cand in candidates:
        if cand in lower:
            idx = lower.index(cand)
            break
    if idx is None:
        # fallback: first column
        idx = 0
    col = fieldnames[idx]
    tickers = []
    for row in reader:
        v = (row.get(col) or "").strip().upper()
        if not v:
            continue
        # support comma-separated
        for t in re.split(r"[,\s]+", v):
            t = t.strip().upper()
            if re.fullmatch(r"[A-Z][A-Z0-9\.\-]{0,9}", t):
                tickers.append(t)
    # unique preserve order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

# ----------------------------
# Main processing
# ----------------------------

def build_events_from_rows(rows: List[RatingRow], reporting_date: dt.date, master_set: set) -> List[dict]:
    events = []
    for rr in rows:
        if rr.ticker not in master_set:
            continue
        ev_date = reporting_date.isoformat()
        events.append({
            "ticker": rr.ticker,
            "date": ev_date,
            "action": rr.action or "",
            "firm": rr.brokerage or "",
            "analyst": rr.analyst or "",
            "current_price": rr.current_price or "",
            "price_target": rr.price_target or "",
            "rating": rr.rating or "",
            "source": "marketbeat_ratings_us",
            "url": MB_URL,
            "date_inferred": True,  # per-row date not present; inferred from page filter
        })
    return events

def within_window(ev_date: dt.date, days: int, today: dt.date) -> bool:
    start = today - dt.timedelta(days=days-1)
    return start <= ev_date <= today

def render_markdown(events: List[dict], days: int, status: str, version: str) -> str:
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    lines = []
    lines.append(f"## Elemzői feed (MarketBeat) — fel/leminősítések + célár (utolsó {days} naptári nap)")
    lines.append("")
    lines.append(f"Verzió: {version}")
    lines.append(f"Generálva (UTC): {now}")
    lines.append("")
    if status:
        lines.append(f"_{status}_")
        lines.append("")
    if not events:
        if not status:
            lines.append("_NO_EVENTS_")
        return "\n".join(lines) + "\n"

    # group by ticker
    by: Dict[str, List[dict]] = {}
    for e in events:
        by.setdefault(e["ticker"], []).append(e)
    for t in sorted(by.keys()):
        lines.append(f"### {t}")
        for e in sorted(by[t], key=lambda x: (x.get("date",""), x.get("firm",""), x.get("action","")) , reverse=True):
            date = e.get("date","")
            action = e.get("action","").strip() or "Action"
            firm = e.get("firm","").strip() or "Firm"
            analyst = e.get("analyst","").strip()
            pt = e.get("price_target","").strip()
            rating = e.get("rating","").strip()
            bits = [date, action, firm]
            if analyst:
                bits.append(analyst)
            if pt:
                bits.append(f"PT {pt}")
            if rating:
                bits.append(rating)
            lines.append("- " + " — ".join(bits))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="Path or URL to master CSV (must contain ticker/symbol column).")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--out-json", default="reports/analyst_last3d.json")
    ap.add_argument("--out-md", default="reports/analyst_last3d.md")
    ap.add_argument("--cache", default=DEFAULT_CACHE_PATH)
    args = ap.parse_args()

    version = "v0.3.34-stdout-urllib-ratingsus-dateinfer-2026-02-02"

    # Load master tickers
    try:
        tickers = load_master_tickers(args.master)
    except Exception as e:
        safe_write_json(args.out_json, [])
        md = render_markdown([], args.days, f"ERROR(master_unreadable: {e})", version)
        safe_write_text(args.out_md, md)
        print("ANALYST_EXIT=2")
        return 2

    master_set = set(tickers)

    cache = load_cache(args.cache)
    today = utc_today()

    # Fetch ratings/us
    status_code = 0
    html = ""
    try:
        status_code, html = http_get(MB_URL, timeout=25)
    except HTTPError as e:
        status_code = int(getattr(e, "code", 0) or 0)
        html = ""
    except URLError:
        status_code = 0
        html = ""

    blocked = False
    if status_code >= 400 or not html:
        blocked = True
    else:
        blocked = is_challenge_or_blocked(html)

    events_live: List[dict] = []
    reporting_date = today
    if not blocked:
        rep_date, rows = parse_ratings_us(html)
        if rep_date:
            reporting_date = rep_date
        else:
            reporting_date = today
        events_live = build_events_from_rows(rows, reporting_date, master_set)

    # Update cache last_seen and store events
    now_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    for e in events_live:
        k = cache_key(e["ticker"], e["date"], e["action"], e["firm"], e["analyst"], e["price_target"], e["rating"])
        rec = cache.get(k, {})
        rec.update({
            "key": k,
            "ticker": e["ticker"],
            "date": e["date"],
            "action": e["action"],
            "firm": e["firm"],
            "analyst": e["analyst"],
            "price_target": e["price_target"],
            "rating": e["rating"],
            "source": e["source"],
            "url": e["url"],
            "first_seen": rec.get("first_seen") or now_iso,
            "last_seen": now_iso,
            "date_inferred": True,
        })
        cache[k] = rec

    # Filter output within window:
    out_events: List[dict] = []
    # Use live if available; otherwise use cache within window
    source_pool = []
    if events_live:
        source_pool = events_live
    else:
        # fall back to cache
        for rec in cache.values():
            if not isinstance(rec, dict):
                continue
            if rec.get("ticker") not in master_set:
                continue
            d = parse_reporting_date_any(rec.get("date",""))
            if d is None:
                continue
            if within_window(d, args.days, today):
                source_pool.append({
                    "ticker": rec.get("ticker",""),
                    "date": rec.get("date",""),
                    "action": rec.get("action",""),
                    "firm": rec.get("firm",""),
                    "analyst": rec.get("analyst",""),
                    "current_price": "",
                    "price_target": rec.get("price_target",""),
                    "rating": rec.get("rating",""),
                    "source": rec.get("source","marketbeat_cache"),
                    "url": rec.get("url", MB_URL),
                    "date_inferred": rec.get("date_inferred", True),
                })

    # Dedup pool -> out_events
    seenk = set()
    for e in source_pool:
        d = parse_reporting_date_any(e.get("date",""))
        if d is None:
            continue
        if not within_window(d, args.days, today):
            continue
        k = cache_key(e["ticker"], e["date"], e.get("action",""), e.get("firm",""), e.get("analyst",""), e.get("price_target",""), e.get("rating",""))
        if k in seenk:
            continue
        seenk.add(k)
        out_events.append(e)

    # Save cache
    try:
        save_cache(args.cache, cache)
    except Exception:
        pass

    # Write outputs
    safe_write_json(args.out_json, out_events)

    status_txt = ""
    if blocked:
        status_txt = "BLOCKED (MarketBeat challenge/bot or HTTP error) — cache used if available"
    elif not out_events:
        status_txt = "NO_EVENTS"
    md = render_markdown(out_events, args.days, status_txt, version)
    safe_write_text(args.out_md, md)

    print("ANALYST_EXIT=0")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
