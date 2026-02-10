#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyst_benzinga.py — v0.1.0-benzinga-scrape-2026-02-10

PURPOSE
- Benzinga analyst ratings "per ticker" scraper (public HTML) with cache.
- Designed to be a drop-in replacement for the old MarketBeat analyst block generator in your PRICE ENGINE.

WHAT IT DOES
- Reads tickers from MASTER CSV (first column is assumed to include ticker symbols).
- Fetches https://www.benzinga.com/quote/{TICKER}/analyst-ratings for each ticker (HTML).
- Extracts the "Analyst Ratings" table rows (date, firm, action, rating change, price target change if present).
- Filters rows to a rolling calendar window: today + previous (days-1) days (default: 4).
- Writes:
    - Markdown summary to --out-md
    - JSON (optional) to --out-json
- Uses a persistent cache file (default: reports/benzinga_events.json) to:
    - avoid refetching within TTL
    - retain recent events even if page rendering changes temporarily

IMPORTANT LIMITATIONS
- This is HTML scraping. If Benzinga changes markup, the parser may need an update.
- Many quote pages are server-rendered enough to scrape; if a specific ticker returns "Loading..." only,
  that ticker will be marked as "adat nem elérhető".

CLI
  --master <csv>
  --days <int>             (default 4 = ma + előző 3 naptári nap)
  --out-md <path>
  --out-json <path>        (optional)
  --cache <path>           (default reports/benzinga_events.json)
  --cache-ttl-hours <int>  (default 24)
  --sleep-ms <int>         (default 150) polite pacing
  --debug                  (more verbose output to stderr)

OUTPUT STYLE (HU)
- Action and rating terminology is translated to Hungarian.
- Only one “Ajánlás változatlan (…)” line is printed if prev==new, per your latest rule.
- "Forrás:" is NOT printed.
"""

from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# ----------------------------
# HU terminology
# ----------------------------
_RATING_MAP = {
    "Buy": "Vétel",
    "Strong Buy": "Erős vétel",
    "Hold": "Tartás",
    "Sell": "Eladás",
    "Strong Sell": "Erős eladás",
    "Neutral": "Semleges",
    "Outperform": "Felülteljesítés",
    "Underperform": "Alulteljesítés",
    "Overweight": "Felülsúlyozás",
    "Underweight": "Alulsúlyozás",
    "Equal-Weight": "Semleges súly",
    "Equal Weight": "Semleges súly",
    "Market Perform": "Piaci teljesítés",
    "Peer Perform": "Szektorszintű",
    "Sector Perform": "Szektorszintű",
}

_ACTION_MAP = {
    "Upgraded": "felminősítés",
    "Downgraded": "leminősítés",
    "Initiated": "kezdeményezés",
    "Reiterated": "megerősítés",
    "Maintains": "megerősítés",
    "Reiterates": "megerősítés",
    "Maintained": "megerősítés",
    "Resumed": "újraindítás",
    "Set": "célár megadás",
    "Raised": "célár emelés",
    "Lowered": "célár csökkentés",
}


def _hu_rating(s: str) -> str:
    s = (s or "").strip()
    return _RATING_MAP.get(s, s)


def _hu_action(s: str) -> str:
    s = (s or "").strip()
    # Benzinga rows often have verbs like: "Reiterates", "Initiates", etc.
    return _ACTION_MAP.get(s, s.lower() if s else "n/a")


# ----------------------------
# Models
# ----------------------------
@dataclass
class Event:
    symbol: str
    date: str  # YYYY-MM-DD
    firm: str
    action: str
    prev_rating: str
    new_rating: str
    pt_change: str  # e.g. "$130 → $160" or "n/a"
    raw: Dict[str, Any]


# ----------------------------
# Helpers
# ----------------------------
def _today_utc() -> _dt.date:
    return _dt.datetime.utcnow().date()


def _parse_date_mmddyyyy(s: str) -> Optional[str]:
    s = (s or "").strip()
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if not m:
        return None
    mm, dd, yyyy = map(int, m.groups())
    try:
        d = _dt.date(yyyy, mm, dd)
    except ValueError:
        return None
    return d.isoformat()


def _in_window(iso_date: str, start: _dt.date, end: _dt.date) -> bool:
    try:
        d = _dt.date.fromisoformat(iso_date)
    except Exception:
        return False
    return start <= d <= end


def _load_master_tickers(master_csv: Path) -> List[str]:
    if not master_csv.exists():
        raise FileNotFoundError(f"MASTER CSV not found: {master_csv}")
    tickers: List[str] = []
    with master_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            t = (row[0] or "").strip()
            if i == 0 and t.lower() in ("ticker", "symbol"):
                continue
            if not t:
                continue
            # Normalize: keep dots (e.g., BRK.B) as-is
            tickers.append(t.upper())
    # De-dup preserve order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9,hu;q=0.8",
        }
    )
    return s


# ----------------------------
# Benzinga HTML parsing
# ----------------------------
# We scrape rows from the "Analyst Ratings" table.
# In practice, Benzinga quote pages usually contain a table with columns like:
# Date | Action | Firm | Price Target Change | Previous/Current Rating
#
# The markup can shift; therefore we:
# - first try to locate the table by the presence of multiple mm/dd/yyyy dates
# - then parse each <tr> for cells
#
_DATE_RE = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _strip_html(s: str) -> str:
    s = _TAG_RE.sub(" ", s)
    s = s.replace("&nbsp;", " ").replace("&amp;", "&").replace("&#39;", "'").replace("&quot;", '"')
    s = _WS_RE.sub(" ", s).strip()
    return s


def _extract_table_rows(html: str) -> List[List[str]]:
    # Find all <tr>...</tr> blocks; then extract <td>...</td> / <th>...</th>
    rows: List[List[str]] = []
    for tr in re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", html):
        cells = re.findall(r"(?is)<t[dh][^>]*>(.*?)</t[dh]>", tr)
        if not cells:
            continue
        txt_cells = [_strip_html(c) for c in cells]
        # Heuristic: keep rows that contain a date
        if any(_DATE_RE.search(c) for c in txt_cells):
            rows.append(txt_cells)
    return rows


def _parse_events_from_rows(symbol: str, rows: List[List[str]], debug: bool = False) -> List[Event]:
    events: List[Event] = []
    for cells in rows:
        # Try to locate a date in the row
        date_cell = next((c for c in cells if _DATE_RE.search(c)), "")
        m = _DATE_RE.search(date_cell)
        if not m:
            continue
        iso = _parse_date_mmddyyyy(m.group(0))
        if not iso:
            continue

        # Best-effort mapping from row text:
        # Common patterns:
        # [Date, Company, Current Price, Upside/Down, Analyst, Price Target Change, Previous/Current Rating]
        # or on quote pages:
        # [Date, Action, Analyst/Firm, Price Target Change, Previous / Current Rating]
        #
        joined = " | ".join(cells)

        # Action verb
        action = "n/a"
        for k in _ACTION_MAP.keys():
            if re.search(rf"\b{k}\b", joined, flags=re.IGNORECASE):
                action = k
                break

        # Firm: attempt to find a known firm cell (often one of the middle cells with letters)
        firm = "n/a"
        firm_candidates = [c for c in cells if c and c.lower() not in ("date", "action", "company")]
        # pick the longest alpha-ish candidate excluding price/ratings
        scored: List[Tuple[int, str]] = []
        for c in firm_candidates:
            if "$" in c or "%" in c:
                continue
            if "→" in c or "->" in c:
                continue
            if re.search(r"\b(buy|hold|sell|outperform|overweight|neutral|underperform|underweight)\b", c, re.I):
                continue
            score = sum(ch.isalpha() for ch in c)
            scored.append((score, c))
        if scored:
            scored.sort(reverse=True)
            firm = scored[0][1]

        # Price target change cell (contains $ or arrow)
        pt_change = "n/a"
        pt_cell = next((c for c in cells if ("$" in c and ("→" in c or "->" in c)) or ("→" in c) or ("->" in c)), "")
        if pt_cell:
            pt_change = pt_cell.replace("->", "→").strip()

        # Rating change: look for pattern "X → Y" within cells, or split by "→" / "->"
        prev_rating = "n/a"
        new_rating = "n/a"
        rating_cell = next(
            (
                c
                for c in cells
                if re.search(r"\b(Buy|Hold|Sell|Neutral|Outperform|Underperform|Overweight|Underweight|Market Perform|Peer Perform|Equal Weight)\b", c, re.I)
                and ("→" in c or "->" in c)
            ),
            "",
        )
        if rating_cell:
            rating_cell = rating_cell.replace("->", "→")
            parts = [p.strip() for p in rating_cell.split("→", 1)]
            if len(parts) == 2:
                prev_rating, new_rating = parts[0], parts[1]
        else:
            # sometimes rating is presented as "Previous / Current Rating" with slash
            rating_cell2 = next((c for c in cells if "/" in c and re.search(r"\b(Buy|Hold|Sell|Neutral|Outperform|Overweight)\b", c, re.I)), "")
            if rating_cell2:
                parts = [p.strip() for p in rating_cell2.split("/", 1)]
                if len(parts) == 2:
                    prev_rating, new_rating = parts[0], parts[1]

        ev = Event(
            symbol=symbol,
            date=iso,
            firm=firm,
            action=action,
            prev_rating=prev_rating,
            new_rating=new_rating,
            pt_change=pt_change,
            raw={"cells": cells},
        )
        events.append(ev)

    # Dedup by (date, firm, action, prev, new, pt)
    seen = set()
    out: List[Event] = []
    for e in events:
        k = (e.symbol, e.date, e.firm, e.action, e.prev_rating, e.new_rating, e.pt_change)
        if k in seen:
            continue
        seen.add(k)
        out.append(e)
    # Most recent first
    out.sort(key=lambda x: x.date, reverse=True)
    return out


def _fetch_symbol_events(
    sess: requests.Session,
    symbol: str,
    cache: Dict[str, Any],
    cache_ttl_hours: int,
    sleep_ms: int,
    debug: bool,
) -> Tuple[List[Event], bool, str]:
    """
    Returns (events, cache_hit, status)
    status: "ok" | "no_data" | "http_<code>" | "error:<msg>"
    """
    now = int(time.time())
    key = symbol.upper()
    ttl = max(1, int(cache_ttl_hours)) * 3600

    # Cache entry structure:
    # cache[symbol] = {"ts": <epoch>, "events": [eventdict...], "status": "ok|..."}
    entry = cache.get(key) or {}
    if isinstance(entry, dict) and entry.get("ts") and (now - int(entry["ts"])) < ttl and entry.get("events"):
        evs = [Event(**e) for e in entry["events"]]
        return evs, True, entry.get("status") or "ok"

    url = f"https://www.benzinga.com/quote/{key}/analyst-ratings"
    try:
        r = sess.get(url, timeout=25)
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)
        if r.status_code != 200:
            cache[key] = {"ts": now, "events": [], "status": f"http_{r.status_code}"}
            return [], False, f"http_{r.status_code}"
        html = r.text or ""
        # If page is heavily client-rendered for this ticker, table may be absent.
        if len(_DATE_RE.findall(html)) < 1:
            cache[key] = {"ts": now, "events": [], "status": "no_data"}
            return [], False, "no_data"

        rows = _extract_table_rows(html)
        if not rows:
            cache[key] = {"ts": now, "events": [], "status": "no_data"}
            return [], False, "no_data"

        evs = _parse_events_from_rows(key, rows, debug=debug)
        cache[key] = {"ts": now, "events": [e.__dict__ for e in evs], "status": "ok"}
        return evs, False, "ok"

    except Exception as e:
        cache[key] = {"ts": now, "events": [], "status": f"error:{type(e).__name__}"}
        if debug:
            print(f"[{key}] fetch error: {e}", file=sys.stderr)
        return [], False, f"error:{type(e).__name__}"


def _format_md(events_by_ticker: Dict[str, List[Event]], days: int, status_line: str) -> str:
    md: List[str] = []
    md.append(f"## Elemzői feed (Benzinga) – fel/leminősítések + célár (utolsó {days} naptári nap)")
    md.append("")
    md.append(status_line)
    md.append("")

    any_rows = 0
    for t in sorted(events_by_ticker.keys()):
        rows = events_by_ticker[t]
        if not rows:
            continue
        md.append(f"## {t}")
        for r in rows:
            prev_g = _hu_rating(r.prev_rating) if r.prev_rating != "n/a" else "n/a"
            new_g = _hu_rating(r.new_rating) if r.new_rating != "n/a" else "n/a"
            action = _hu_action(r.action)
            firm = r.firm or "n/a"
            date = r.date or "n/a"

            # Your requested style:
            # - if rating unchanged: "Ajánlás változatlan (X)"
            # - else: "Ajánlás: X → Y"
            if prev_g != "n/a" and new_g != "n/a" and prev_g == new_g:
                rating_part = f"Ajánlás változatlan ({new_g})"
            elif prev_g != "n/a" and new_g != "n/a":
                rating_part = f"Ajánlás: {prev_g} → {new_g}"
            else:
                rating_part = "Ajánlás: n/a"

            # price target part (optional)
            pt_part = ""
            if r.pt_change and r.pt_change != "n/a":
                pt_part = f" | Célár: {r.pt_change}"

            md.append(f"- {date} – {firm} – {action} | {rating_part}{pt_part}")
            any_rows += 1
        md.append("")

    if any_rows == 0:
        md.append("_Nincs Benzinga esemény a megadott ablakban._")
        md.append("")
    return "\n".join(md).rstrip() + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="Path to MASTER CSV")
    ap.add_argument("--days", type=int, default=4, help="Rolling calendar window in days (default 4)")
    ap.add_argument("--out-md", default="reports/analyst_last2d.md", help="Markdown output path")
    ap.add_argument("--out-json", default="", help="Optional JSON output path")
    ap.add_argument("--cache", default="reports/benzinga_events.json", help="Cache file path")
    ap.add_argument("--cache-ttl-hours", type=int, default=24, help="Cache TTL hours")
    ap.add_argument("--sleep-ms", type=int, default=150, help="Sleep between requests (ms)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    master = Path(args.master)
    out_md = Path(args.out_md)
    out_json = Path(args.out_json) if args.out_json else None
    cache_path = Path(args.cache)

    tickers = _load_master_tickers(master)

    cache = _read_json(cache_path)
    if not isinstance(cache, dict):
        cache = {}

    sess = _requests_session()

    end = _today_utc()
    start = end - _dt.timedelta(days=max(1, int(args.days)) - 1)

    events_by_ticker: Dict[str, List[Event]] = {}
    ok = 0
    fail = 0
    cache_hits = 0
    no_data = 0

    for t in tickers:
        evs, hit, status = _fetch_symbol_events(
            sess, t, cache, cache_ttl_hours=args.cache_ttl_hours, sleep_ms=args.sleep_ms, debug=args.debug
        )
        if hit:
            cache_hits += 1

        if status == "ok":
            ok += 1
        elif status == "no_data":
            no_data += 1
        else:
            fail += 1

        # Filter by window
        filtered = [e for e in evs if e.date and _in_window(e.date, start, end)]
        if filtered:
            events_by_ticker[t] = filtered

    # Persist cache
    _write_json(cache_path, cache)

    status_line = (
        f"_forrás státusz: ok={ok}, nincs_adat={no_data}, fail={fail}, cache_hit={cache_hits} | "
        f"ablak: {start.isoformat()} → {end.isoformat()}_"
    )

    md = _format_md(events_by_ticker, int(args.days), status_line)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(md, encoding="utf-8")

    if out_json:
        # Only windowed events go to json (keeps it small)
        j = {
            "window": {"start": start.isoformat(), "end": end.isoformat(), "days": int(args.days)},
            "status": {"ok": ok, "no_data": no_data, "fail": fail, "cache_hit": cache_hits},
            "events_by_ticker": {
                t: [e.__dict__ for e in evs] for t, evs in events_by_ticker.items()
            },
        }
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(j, ensure_ascii=False, indent=2), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
