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

import html
import urllib.request
import urllib.error
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

def _strip_tags(s: str) -> str:
    if s is None:
        return ""
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    return " ".join(s.split()).strip()


def _parse_updated_date(page_html: str) -> Optional[date]:
    # Example: "Updated: 19-Feb-26 07:50 ET"
    m = re.search(r"Updated:\s*([0-9]{1,2}-[A-Za-z]{3}-[0-9]{2})", page_html)
    if not m:
        return None
    ds = m.group(1)
    try:
        return datetime.strptime(ds, "%d-%b-%y").date()
    except Exception:
        return None


def _find_section_table(page_html: str, section_title: str) -> Optional[str]:
    pat = re.compile(rf"{re.escape(section_title)}[\s\S]*?(<table[\s\S]*?</table>)", re.IGNORECASE)
    m = pat.search(page_html)
    return m.group(1) if m else None


def _extract_table_rows(table_html: str) -> List[List[str]]:
    rows: List[List[str]] = []
    for tr in re.findall(r"<tr[\s\S]*?</tr>", table_html, flags=re.IGNORECASE):
        if re.search(r"<th", tr, flags=re.IGNORECASE):
            continue
        tds = re.findall(r"<td[\s\S]*?</td>", tr, flags=re.IGNORECASE)
        if not tds:
            continue
        cells = [_strip_tags(td) for td in tds]
        cells = [c for c in cells if c != ""]
        if cells:
            rows.append(cells)
    return rows


def _split_change(s: str) -> Tuple[str, str]:
    s = (s or "").strip()
    if not s:
        return ("n/a", "n/a")
    for sep in ["»", "→", "->", "=>"]:
        if sep in s:
            a, b = s.split(sep, 1)
            return (a.strip(), b.strip())
    return (s.strip(), s.strip())


def parse_briefing_events(page_html: str, today: date, days: int, debug: int = 0) -> List[Dict[str, str]]:
    """
    Parse hosting.briefing.com upgrades/downgrades page tables.

    Output keys:
      symbol, date, grading_company, action,
      previous_grade, new_grade, previous_pt, new_pt, section
    """
    updated = _parse_updated_date(page_html) or today
    start_date = today - timedelta(days=max(0, days - 1))

    # Page is a "today" page; if its updated date is outside the window, return nothing.
    if updated < start_date or updated > today:
        return []

    sections = [
        ("Upgrades", "upgrade"),
        ("Downgrades", "downgrade"),
        ("Coverage Initiated", "initiation"),
        ("Coverage Reiterated/Price Tgt Changed", "reiterate"),
    ]

    out: List[Dict[str, str]] = []

    for section_title, action in sections:
        table_html = _find_section_table(page_html, section_title)
        if not table_html:
            continue

        rows = _extract_table_rows(table_html)

        for cells in rows:
            # Expected cols: Company | Ticker | Brokerage Firm | Ratings Change | Price Target
            company = cells[0] if len(cells) >= 1 else "n/a"
            symbol = cells[1] if len(cells) >= 2 else "n/a"
            firm = cells[2] if len(cells) >= 3 else "n/a"
            rating_change = cells[3] if len(cells) >= 4 else ""
            pt_change = cells[4] if len(cells) >= 5 else ""

            prev_g, new_g = _split_change(rating_change) if rating_change else ("n/a", "n/a")
            prev_pt, new_pt = _split_change(pt_change) if pt_change else ("n/a", "n/a")

            out.append({
                "symbol": symbol.upper().strip(),
                "company": company,
                "grading_company": firm,
                "action": action,
                "previous_grade": prev_g,
                "new_grade": new_g,
                "previous_pt": prev_pt,
                "new_pt": new_pt,
                "date": updated.isoformat(),
                "section": section_title,
            })

    # Dedup identical rows (page sometimes repeats)
    seen = set()
    dedup: List[Dict[str, str]] = []
    for r in out:
        key = (
            r["symbol"], r["grading_company"], r["action"],
            r["previous_grade"], r["new_grade"],
            r["previous_pt"], r["new_pt"],
            r["date"], r["section"]
        )
        if key in seen:
            continue
        seen.add(key)
        dedup.append(r)

    return dedup

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
def fetch_briefing_html(timeout: int = 25) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    req = urllib.request.Request(BRIEFING_URL, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Briefing HTTP error: {e.code}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Briefing URL error: {getattr(e, 'reason', e)}") from e

    return data.decode("utf-8", errors="replace")


if __name__ == "__main__":
    raise SystemExit(main())

