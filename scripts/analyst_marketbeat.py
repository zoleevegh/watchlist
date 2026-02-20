#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyst_marketbeat.py — v0.2.1-briefing-4day-2026-02-19

PURPOSE
- Analyst upgrades/downgrades block generator for your PRICE ENGINE.
- Source: Briefing/Fidelity Upgrades & Downgrades calendar (static HTML table):
    https://hosting.briefing.com/fidelity/Calendars/UpgradesDowngrades.htm

WHY THIS
- The prior free feeds (e.g., top-10 global endpoints) are not watchlist-usable.
- Briefing page is a single, static HTML table (no JS pagination), so it is stable to fetch/parse.
- One HTTP request per run (not per ticker) → avoids rate-limit disasters.

WHAT IT DOES
- Reads tickers from MASTER CSV (first column OR any column named like ticker/symbol).
- Fetches the Briefing upgrades/downgrades table once.
- Extracts rows (date, ticker, company, action/type, firm, old/new rating, old/new target if present).
- Filters to a rolling calendar window: today + previous (days-1) days (default: 4).
- Filters to your MASTER tickers (with basic normalization).
- Writes a Markdown block to --out-md (and optional JSON to --out-json).

OUTPUT STYLE (HU)
- Terminology is Hungarian:
  * Upgrade → Felminősítés, Downgrade → Leminősítés, Initiation → Új ajánlás, Reiterated/Maintained → Ajánlás változatlan
  * Buy → Vétel, Strong Buy → Erős vétel, Hold → Tartás, Neutral → Semleges, Sell → Eladás, etc.
- If previous_grade == new_grade, prints ONLY the “Ajánlás változatlan (…)" line (as requested).
- No "Forrás:" lines.

CLI
  --master <csv>
  --days <int>             (default 4 = ma + előző 3 naptári nap)
  --out-md <path>
  --out-json <path>        (optional)
  --debug                  (more verbose output to stderr)

NOTES
- Briefing page is usually "today-focused". If the HTML row has no parsable date, we treat it as "today".
- This script does not attempt any login/paywall bypass.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup


import html
BRIEFING_URL = "https://hosting.briefing.com/fidelity/Calendars/UpgradesDowngrades.htm"

# ---- Hungarian mappings ------------------------------------------------------

RATING_MAP = {
    "strong buy": "Erős vétel",
    "buy": "Vétel",
    "overweight": "Felülsúlyozás",
    "outperform": "Piac feletti teljesítés",
    "market outperform": "Piac feletti teljesítés",
    "accumulate": "Felhalmozás",
    "positive": "Pozitív",
    "neutral": "Semleges",
    "hold": "Tartás",
    "equal weight": "Piaccal megegyező súly",
    "market perform": "Piaccal megegyező teljesítés",
    "sector perform": "Szektornak megfelelő teljesítés",
    "underperform": "Piac alatti teljesítés",
    "underweight": "Alulsúlyozás",
    "sell": "Eladás",
    "strong sell": "Erős eladás",
    "reduce": "Csökkentés",
    "negative": "Negatív",
}

ACTION_MAP = {
    "upgrade": "Felminősítés",
    "downgrade": "Leminősítés",
    "initiated": "Új ajánlás",
    "initiation": "Új ajánlás",
    "reiterated": "Ajánlás változatlan",
    "maintained": "Ajánlás változatlan",
    "reinstated": "Visszaállítva",
    "resumed": "Követés újraindítva",
    "started": "Követés indítva",
    "coverage initiated": "Követés indítva",
    "coverage resumed": "Követés újraindítva",
    "target raised": "Célár emelve",
    "target lowered": "Célár csökkentve",
    "pt raised": "Célár emelve",
    "pt lowered": "Célár csökkentve",
}

TYPE_TO_ACTION = {
    "upgrades": "Felminősítés",
    "downgrades": "Leminősítés",
    "initiations": "Új ajánlás",
    "reiterations": "Ajánlás változatlan",
    "re-iterations": "Ajánlás változatlan",
    "target price changes": "Célár változás",
    "target price change": "Célár változás",
}

# ---- Data model --------------------------------------------------------------

@dataclass
class AnalystEvent:
    event_date: str  # YYYY-MM-DD
    ticker: str
    company: str
    action_hu: str
    firm: str
    previous_grade: str
    new_grade: str
    previous_target: str
    new_target: str
    raw_type: str = ""


# ---- Helpers ----------------------------------------------------------------

def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)

def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _hu_rating(r: str) -> str:
    r0 = _norm_space(r).lower()
    if not r0 or r0 in {"n/a", "na", "-"}:
        return "n/a"
    return RATING_MAP.get(r0, r.strip())

def _ticker_norm(t: str) -> str:
    t = (t or "").strip().upper().replace(" ", "")
    return t

def _is_valid_ticker(t: str) -> bool:
    return bool(t) and bool(re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,9}", t))

def _parse_date_any(s: str, today: date) -> Optional[date]:
    s = _norm_space(s)
    if not s:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass

    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})", s)
    if m:
        try:
            return date(today.year, int(m.group(1)), int(m.group(2)))
        except Exception:
            return None

    for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d", "%B %d"):
        try:
            d = datetime.strptime(s, fmt).date()
            if fmt in ("%b %d", "%B %d"):
                d = date(today.year, d.month, d.day)
            return d
        except Exception:
            pass

    return None

def read_master_tickers(csv_path: str, debug: bool = False) -> List[str]:
    tickers: List[str] = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample) if sample.strip() else csv.excel
        except Exception:
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        rows = list(reader)

    if not rows:
        return tickers

    header = [c.strip().lower() for c in rows[0]]
    ticker_col_idx = None
    for i, name in enumerate(header):
        if name in {"ticker", "symbol", "tickers"}:
            ticker_col_idx = i
            break

    start_row = 1 if ticker_col_idx is not None else 0
    if ticker_col_idx is None:
        ticker_col_idx = 0

    for r in rows[start_row:]:
        if not r or ticker_col_idx >= len(r):
            continue
        t = _ticker_norm(r[ticker_col_idx])
        if t == "PKN.WA":
            continue
        if _is_valid_ticker(t):
            tickers.append(t)

    seen = set()
    out: List[str] = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    if debug:
        eprint(f"[master] tickers={len(out)}")
    return out

def fetch_briefing_html(timeout: int = 25) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = requests.get(BRIEFING_URL, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def parse_briefing_events(html: str, today: date, debug: bool = False) -> List[AnalystEvent]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if debug:
        eprint(f"[briefing] tables={len(tables)}")

    events: List[AnalystEvent] = []

    def table_headers(table) -> List[str]:
        tr = table.find("tr")
        if not tr:
            return []
        ths = tr.find_all("th")
        if not ths:
            return []
        return [_norm_space(th.get_text(" ", strip=True)) for th in ths]

    for table in tables:
        headers = table_headers(table)
        headers_l = [h.lower() for h in headers]
        trs = table.find_all("tr")
        if not trs or len(trs) < 2:
            continue

        colmap: Dict[str, int] = {}
        if headers:
            for idx, h in enumerate(headers_l):
                if "date" in h:
                    colmap["date"] = idx
                elif "ticker" in h or "symbol" in h:
                    colmap["ticker"] = idx
                elif "company" in h:
                    colmap["company"] = idx
                elif "type" in h or "action" in h:
                    colmap["type"] = idx
                elif "firm" in h or "broker" in h or "research" in h:
                    colmap["firm"] = idx
                elif "from" in h and "rating" in h:
                    colmap["from_rating"] = idx
                elif ("to" in h and "rating" in h) or ("new" in h and "rating" in h):
                    colmap["to_rating"] = idx
                elif "from" in h and ("pt" in h or "target" in h):
                    colmap["from_pt"] = idx
                elif ("to" in h and ("pt" in h or "target" in h)) or ("new" in h and ("pt" in h or "target" in h)):
                    colmap["to_pt"] = idx

        for tr in trs[1:]:
            tds = tr.find_all(["td", "th"])
            if not tds:
                continue
            cols = [_norm_space(td.get_text(" ", strip=True)) for td in tds]
            if len(cols) <= 2:
                continue

            raw_date = cols[colmap["date"]] if "date" in colmap and colmap["date"] < len(cols) else ""
            d = _parse_date_any(raw_date, today) or today

            ticker = ""
            if "ticker" in colmap and colmap["ticker"] < len(cols):
                ticker = _ticker_norm(cols[colmap["ticker"]])
            else:
                m = re.search(r"\b[A-Z]{1,5}(?:\.[A-Z])?\b", " ".join(cols))
                ticker = _ticker_norm(m.group(0)) if m else ""

            if ticker == "PKN.WA" or not _is_valid_ticker(ticker):
                continue

            company = cols[colmap["company"]] if "company" in colmap and colmap["company"] < len(cols) else "n/a"
            raw_type = cols[colmap["type"]] if "type" in colmap and colmap["type"] < len(cols) else ""
            firm = cols[colmap["firm"]] if "firm" in colmap and colmap["firm"] < len(cols) else "n/a"

            prev_rating = cols[colmap["from_rating"]] if "from_rating" in colmap and colmap["from_rating"] < len(cols) else ""
            new_rating = cols[colmap["to_rating"]] if "to_rating" in colmap and colmap["to_rating"] < len(cols) else ""

            prev_pt = cols[colmap["from_pt"]] if "from_pt" in colmap and colmap["from_pt"] < len(cols) else ""
            new_pt = cols[colmap["to_pt"]] if "to_pt" in colmap and colmap["to_pt"] < len(cols) else ""

            action_hu = TYPE_TO_ACTION.get(raw_type.strip().lower(), "")
            if not action_hu:
                action_hu = "Célár változás" if (prev_pt or new_pt) else "n/a"

            ev = AnalystEvent(
                event_date=d.isoformat(),
                ticker=ticker,
                company=company or "n/a",
                action_hu=action_hu,
                firm=firm or "n/a",
                previous_grade=_hu_rating(prev_rating) if prev_rating else "n/a",
                new_grade=_hu_rating(new_rating) if new_rating else "n/a",
                previous_target=prev_pt or "n/a",
                new_target=new_pt or "n/a",
                raw_type=raw_type or "",
            )
            events.append(ev)

    uniq: Dict[str, AnalystEvent] = {}
    for e in events:
        k = "|".join([e.event_date, e.ticker, e.firm, e.action_hu, e.previous_grade, e.new_grade, e.previous_target, e.new_target])
        uniq[k] = e
    out = list(uniq.values())
    out.sort(key=lambda x: (x.event_date, x.ticker), reverse=True)
    if debug:
        eprint(f"[briefing] parsed={len(out)}")
    return out

def filter_events(events: List[AnalystEvent], tickers_set: set, start_d: date, end_d: date) -> List[AnalystEvent]:
    out: List[AnalystEvent] = []
    for e in events:
        try:
            d = datetime.strptime(e.event_date, "%Y-%m-%d").date()
        except Exception:
            continue
        if d < start_d or d > end_d:
            continue
        if e.ticker not in tickers_set:
            continue
        out.append(e)
    out.sort(key=lambda x: (x.event_date, x.ticker), reverse=True)
    return out

def format_markdown(events: List[AnalystEvent], start_d: date, end_d: date) -> str:
    lines: List[str] = []
    lines.append(f"## Elemzői frissítések (fel-/leminősítések) — {start_d.isoformat()} → {end_d.isoformat()}")
    lines.append("")
    if not events:
        lines.append("_Nincs releváns elemzői esemény a megadott ablakban._")
        lines.append("")
        return "\n".join(lines)

    by_t: Dict[str, List[AnalystEvent]] = {}
    for e in events:
        by_t.setdefault(e.ticker, []).append(e)

    for t in sorted(by_t.keys()):
        lines.append(f"**{t}**")
        for r in sorted(by_t[t], key=lambda x: x.event_date, reverse=True):
            date_s = r.event_date
            firm = r.firm
            action = r.action_hu
            prev_g = r.previous_grade or "n/a"
            new_g = r.new_grade or "n/a"

            if prev_g == new_g and prev_g != "n/a":
                lines.append(f"- {date_s} — {firm} — {action} | Ajánlás változatlan ({prev_g})")
            elif prev_g != "n/a" or new_g != "n/a":
                lines.append(f"- {date_s} — {firm} — {action} | Ajánlás: {prev_g} → {new_g}")
            else:
                lines.append(f"- {date_s} — {firm} — {action}")

            if (r.previous_target and r.previous_target != "n/a") or (r.new_target and r.new_target != "n/a"):
                lines.append(f"  - Célár: {r.previous_target} → {r.new_target}")
        lines.append("")
    return "\n".join(lines)

def write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="MASTER CSV path")
    ap.add_argument("--days", type=int, default=4, help="Rolling window in calendar days (default: 4)")
    ap.add_argument("--out-md", required=True, help="Output markdown path")
    ap.add_argument("--out-json", default="", help="Optional output JSON path")
    ap.add_argument("--debug", action="store_true", help="Verbose stderr logging")
    args = ap.parse_args()

    today = date.today()
    days = max(1, int(args.days))
    start_d = today - timedelta(days=days - 1)
    end_d = today

    tickers = read_master_tickers(args.master, debug=args.debug)
    tickers_set = set(tickers)

    if args.debug:
        eprint(f"[window] {start_d.isoformat()} -> {end_d.isoformat()} (days={days})")

    try:
        html = fetch_briefing_html()
    except Exception as ex:
        md = "\n".join([
            f"## Elemzői frissítések (fel-/leminősítések) — {start_d.isoformat()} → {end_d.isoformat()}",
            "",
            f"_Adat nem elérhető (forrás hiba): {type(ex).__name__}: {ex}_",
            "",
        ])
        write_text(args.out_md, md)
        if args.out_json:
            write_text(args.out_json, json.dumps({"error": str(ex)}, ensure_ascii=False, indent=2))
        return 0

    events_all = parse_briefing_events(html, today=today, debug=args.debug)
    events = filter_events(events_all, tickers_set=tickers_set, start_d=start_d, end_d=end_d)

    md = format_markdown(events, start_d=start_d, end_d=end_d)
    write_text(args.out_md, md)

    if args.out_json:
        payload = {
            "version": "v0.2.0-briefing-4day-2026-02-19",
            "window": {"start": start_d.isoformat(), "end": end_d.isoformat(), "days": days},
            "events": [asdict(e) for e in events],
        }
        write_text(args.out_json, json.dumps(payload, ensure_ascii=False, indent=2))

    if args.debug:
        eprint(f"[out] md={args.out_md} events={len(events)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

