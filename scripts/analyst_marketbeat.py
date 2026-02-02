#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyst_marketbeat.py — MarketBeat analyst feed (upgrades/downgrades + PT changes) with persistent cache
Version: v0.3.31 (2026-02-02)

IMÁDSÁG (2 sor):
bocsáss meg uram, mert balfék voltam, és hagytam hogy egy placeholder szétverje a futást.
adj erőt, hogy a cache mindig JSON legyen, a jelentés meg mindig éljen. 🙏

Key guarantees:
- Never crashes on invalid/placeholder cache files (auto-quarantine + reset).
- Distinguishes "NO_EVENTS" from "BLOCKED/CHALLENGE" and writes explicit status.
- Produces valid JSON output even when empty ([]) and a non-empty markdown section.
- Keeps a persistent event-cache so items that disappear from MarketBeat "latest" can still show within N-day window.

Notes:
- MarketBeat pages often return HTTP 200 with bot/challenge HTML. This is detected and treated as BLOCKED.
- If blocked, this script can still emit cached items from previous runs (within the requested window).
- Optional fallback sources are stubbed (FMP/Finnhub) — wire-in keys if you want.

Usage:
  python analyst_marketbeat.py --master master.csv --days 3 --out-json analyst_last3d.json --out-md analyst_last3d.md

Master CSV requirements:
- Must contain a column with tickers. The script will try: 'ticker', 'Ticker', 'symbol', 'Symbol'.
- If multiple columns exist, the first match is used.

Outputs:
- JSON: list of events, each with at least: ticker, date, action, firm, analyst, summary, source.
- MD: a concise markdown list grouped by ticker.

Exit codes:
- 0: success (including NO_EVENTS or BLOCKED, but outputs written)
- 2: fatal IO/config error (e.g., master unreadable)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0 Safari/537.36"
)

MB_RATINGS_URL = "https://www.marketbeat.com/stocks/{ticker}/ratings/"

MB_US_RATINGS_URL = "https://www.marketbeat.com/ratings/us/"  # broad feed (often accessible when per-ticker is blocked)

BOT_PATTERNS = [
    r"cf-chl", r"cloudflare", r"attention required", r"checking your browser",
    r"verify you are human", r"enable javascript", r"captcha", r"access denied",
    r"incapsula", r"akamai", r"distil", r"datadome"
]


@dataclass
class AnalystEvent:
    ticker: str
    date: str  # YYYY-MM-DD
    action: str  # upgrade/downgrade/initiated/reiterated/pt_change/other
    firm: str
    analyst: str
    summary: str
    source: str  # marketbeat|cache|fallback
    url: str


def _today_utc() -> dt.date:
    return dt.datetime.utcnow().date()


def _parse_date(s: str) -> Optional[dt.date]:
    s = (s or "").strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def _iso(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def _is_bot_html(text: str) -> bool:
    t = (text or "").lower()
    return any(re.search(p, t) for p in BOT_PATTERNS)


def _safe_read_json(path: Path) -> Tuple[Optional[object], Optional[str]]:
    """
    Returns (obj, error). If error is not None, obj is None.
    """
    if not path.exists():
        return None, "missing"
    try:
        raw = path.read_text(encoding="utf-8", errors="replace").strip()
        # Handle placeholder / non-json garbage
        if not raw or raw.lower() == "placeholder":
            return None, "placeholder"
        return json.loads(raw), None
    except Exception as e:
        return None, f"invalid_json:{e.__class__.__name__}"


def _quarantine_bad_cache(path: Path) -> None:
    try:
        ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        bad_path = path.with_suffix(path.suffix + f".bad.{ts}")
        path.replace(bad_path)
    except Exception:
        # If rename fails, we do nothing; the caller will overwrite.
        pass


def _load_master_tickers(master_path: Path) -> List[str]:
    if not master_path.exists():
        raise FileNotFoundError(f"master not found: {master_path}")

    with master_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("master has no header row")

        candidates = ["ticker", "Ticker", "symbol", "Symbol"]
        col = None
        for c in candidates:
            if c in reader.fieldnames:
                col = c
                break
        if col is None:
            raise ValueError(f"master missing ticker column (tried {candidates}); got {reader.fieldnames}")

        tickers = []
        for row in reader:
            t = (row.get(col) or "").strip()
            if not t:
                continue
            # Normalize
            t = t.upper()
            tickers.append(t)

    # unique preserve order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _http_get(url: str, timeout: int = 20) -> Tuple[int, str]:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
    return r.status_code, r.text or ""


def _extract_events_from_ratings_html(ticker: str, url: str, html: str) -> List[AnalystEvent]:
    """
    MarketBeat ratings pages contain a table of analyst actions & PT.
    We use resilient regex parsing (no heavy bs4 dependency) to keep the script portable.
    """
    events: List[AnalystEvent] = []

    # Heuristic: look for rows with dates in the first column.
    # Example patterns vary, so we parse broadly.
    # We'll search for table rows and then extract text inside <td>...</td>.
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.S | re.I)
    for row_html in rows:
        cols = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.S | re.I)
        cols_text = [re.sub(r"<[^>]+>", " ", c) for c in cols]
        cols_text = [" ".join(re.split(r"\s+", c)).strip() for c in cols_text]
        cols_text = [c for c in cols_text if c]

        if len(cols_text) < 3:
            continue

        # Find a date-like cell
        date_cell = None
        date_val = None
        for c in cols_text[:3]:
            d = _parse_date(c)
            if d:
                date_cell = c
                date_val = d
                break
        if not date_val:
            continue

        # Remaining cells often include: firm, action (upgrade/downgrade/etc), PT (new/old), rating
        text_join = " | ".join(cols_text)
        firm = cols_text[1] if len(cols_text) > 1 else ""
        action_raw = cols_text[2] if len(cols_text) > 2 else ""
        analyst = ""  # MarketBeat often doesn't list individual analyst name
        summary = text_join

        action = "other"
        ar = action_raw.lower()
        if "upgrade" in ar:
            action = "upgrade"
        elif "downgrade" in ar:
            action = "downgrade"
        elif "initi" in ar:
            action = "initiated"
        elif "reiter" in ar or "maintain" in ar:
            action = "reiterated"
        elif "price target" in ar or "pt" in ar or "$" in ar:
            action = "pt_change"

        events.append(
            AnalystEvent(
                ticker=ticker,
                date=_iso(date_val),
                action=action,
                firm=firm,
                analyst=analyst,
                summary=summary,
                source="marketbeat",
                url=url,
            )
        )
    return events



def _action_hu_from_text(action_text: str, rating_text: str = "") -> str:
    a = (action_text or "").lower()
    r = (rating_text or "").lower()
    if "upgrad" in a:
        return "Felminősítés"
    if "downgrad" in a:
        return "Leminősítés"
    if "initiated" in a or "initiates" in a:
        return "Elemzés indítása"
    if "target raised" in a or "raises target" in a:
        return "Célár emelés"
    if "target lowered" in a or "lowers target" in a:
        return "Célár csökkentés"
    if "target set" in a or "sets target" in a:
        return "Célár beállítás"
    if "price target" in a:
        return "Célár változás"
    if "reiterated" in a:
        return "Megerősítés"
    # If rating shows arrow, treat as rating change (often with upgrades/downgrades)
    if "➝" in rating_text or "->" in rating_text:
        return "Értékelés változás"
    return "Egyéb"


def _extract_events_from_us_html(html: str, tickers_set: set[str], as_of_date: str) -> List[AnalystEvent]:
    """
    Parse https://www.marketbeat.com/ratings/us/ (broad feed).
    This page often renders server-side enough to see today's rows even when per-ticker pages are challenged.
    It does NOT reliably include an explicit date per-row for non-subscribers; we stamp as_of_date (UTC) and rely on cache persistence.
    """
    try:
        from bs4 import BeautifulSoup  # optional dependency present on GH runners in this repo
    except Exception:
        return []

    soup = BeautifulSoup(html, "html.parser")

    # Find the first table that looks like the ratings table
    table = None
    for t in soup.find_all("table"):
        headers = " ".join([th.get_text(" ", strip=True) for th in t.find_all("th")[:8]])
        if "Company" in headers and "Action" in headers and ("Brokerage" in headers or "Analyst" in headers):
            table = t
            break
    if table is None:
        return []

    out: List[AnalystEvent] = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        # Company cell typically contains "TICKER Company"
        company_txt = tds[0].get_text(" ", strip=True)
        m = re.match(r"^([A-Z.\-]{1,8})\b", company_txt)
        if not m:
            continue
        ticker = m.group(1).upper()
        if tickers_set and ticker not in tickers_set:
            continue

        action_txt = tds[1].get_text(" ", strip=True)

        firm = tds[2].get_text(" ", strip=True) if len(tds) > 2 else ""
        analyst = tds[3].get_text(" ", strip=True) if len(tds) > 3 else ""

        # Rating / PT cells vary; capture compactly
        cur_price = tds[4].get_text(" ", strip=True) if len(tds) > 4 else ""
        pt = tds[5].get_text(" ", strip=True) if len(tds) > 5 else ""
        rating = tds[6].get_text(" ", strip=True) if len(tds) > 6 else ""

        action_hu = _action_hu_from_text(action_txt, rating)
        summary_parts = []
        if action_txt:
            summary_parts.append(action_txt)
        if firm:
            summary_parts.append(firm)
        if analyst:
            summary_parts.append(analyst)
        if pt:
            summary_parts.append(f"PT: {pt}")
        if rating:
            summary_parts.append(f"Rating: {rating}")
        summary = " | ".join([p for p in summary_parts if p])

        # try to find details link in row
        row_url = ""
        a = tr.find("a", href=True)
        if a and a["href"]:
            href = a["href"]
            if href.startswith("http"):
                row_url = href
            else:
                row_url = "https://www.marketbeat.com" + href

        out.append(
            AnalystEvent(
                ticker=ticker,
                date=as_of_date,
                action=action_hu,
                firm=firm,
                analyst=analyst,
                summary=summary,
                source="marketbeat_us",
                url=row_url or MB_US_RATINGS_URL,
            )
        )
    return out
def _dedupe_events(events: List[AnalystEvent]) -> List[AnalystEvent]:
    """
    Dedupe by (ticker, date, firm, action, summary normalized).
    """
    seen = set()
    out = []
    for e in events:
        key = (
            e.ticker,
            e.date,
            (e.firm or "").strip().lower(),
            e.action,
            re.sub(r"\s+", " ", (e.summary or "").strip().lower()),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def _within_window(date_iso: str, days: int, today: dt.date) -> bool:
    d = _parse_date(date_iso) or _parse_date(date_iso.replace("-", "-"))
    if not d:
        try:
            d = dt.datetime.strptime(date_iso, "%Y-%m-%d").date()
        except Exception:
            return False
    start = today - dt.timedelta(days=days)
    return start <= d <= today


def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_md(path: Path, title: str, status: str, events: List[AnalystEvent], days: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append(f"## {title} (utolsó {days} naptári nap)")
    if status == "OK":
        pass
    elif status == "NO_EVENTS":
        lines.append("")
        lines.append("_Nincs releváns elemzői esemény ebben az ablakban._")
    elif status.startswith("BLOCKED"):
        lines.append("")
        lines.append(f"_{status}_")
    else:
        lines.append("")
        lines.append(f"_{status}_")

    if events:
        lines.append("")
        # group by ticker
        by_t: Dict[str, List[AnalystEvent]] = {}
        for e in events:
            by_t.setdefault(e.ticker, []).append(e)
        for t in sorted(by_t.keys()):
            lines.append(f"### {t}")
            # sort newest first
            evs = sorted(by_t[t], key=lambda x: x.date, reverse=True)
            for e in evs:
                # keep it short; the report runner can further format
                firm = f"{e.firm}" if e.firm else "Analyst"
                act = e.action.replace("_", " ")
                lines.append(f"- {e.date} — {firm}: {act}. {e.url}")
            lines.append("")
        if lines and lines[-1] == "":
            lines.pop()

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="master csv path")
    ap.add_argument("--days", type=int, default=3, help="lookback days (calendar days)")
    ap.add_argument("--cache", default="marketbeat_events.json", help="persistent cache path")
    ap.add_argument("--out-json", default="analyst_last3d.json", help="output json path")
    ap.add_argument("--out-md", default="analyst_last3d.md", help="output markdown path")
    ap.add_argument("--sleep", type=float, default=0.3, help="sleep between requests")
    args = ap.parse_args()

    master_path = Path(args.master)
    cache_path = Path(args.cache)
    out_json = Path(args.out_json)
    out_md = Path(args.out_md)
    days = max(1, int(args.days))
    today = _today_utc()

    try:
        tickers = _load_master_tickers(master_path)
    except Exception as e:
        # write explicit outputs even on fatal config, to avoid breaking runner
        _write_json(out_json, [])
        _write_md(out_md, "Elemzői feed (MarketBeat)", f"FATAL: {e}", [], days)
        return 2

    # Load persistent cache
    cache_obj, cache_err = _safe_read_json(cache_path)
    if cache_err and cache_path.exists():
        # quarantine broken cache then reset
        _quarantine_bad_cache(cache_path)
    if isinstance(cache_obj, dict) and "events" in cache_obj and isinstance(cache_obj["events"], list):
        cached_events_raw = cache_obj["events"]
    elif isinstance(cache_obj, list):
        cached_events_raw = cache_obj
    else:
        cached_events_raw = []

    cached_events: List[AnalystEvent] = []
    for item in cached_events_raw:
        if not isinstance(item, dict):
            continue
        try:
            cached_events.append(
                AnalystEvent(
                    ticker=str(item.get("ticker", "")).upper(),
                    date=str(item.get("date", "")),
                    action=str(item.get("action", "other")),
                    firm=str(item.get("firm", "")),
                    analyst=str(item.get("analyst", "")),
                    summary=str(item.get("summary", "")),
                    source=str(item.get("source", "cache")),
                    url=str(item.get("url", "")),
                )
            )
        except Exception:
            continue

    # Fetch new events
    fetched_events: List[AnalystEvent] = []
    blocked_count = 0
    ok_count = 0

    for t in tickers:
        url = MB_RATINGS_URL.format(ticker=t)
        try:
            status, html = _http_get(url)
            if status != 200:
                blocked_count += 1
                continue
            if _is_bot_html(html):
                blocked_count += 1
                continue
            ok_count += 1
            fetched_events.extend(_extract_events_from_ratings_html(t, url, html))
        except Exception:
            blocked_count += 1
        time.sleep(max(0.0, float(args.sleep)))

    fetched_events = _dedupe_events(fetched_events)

    # Fallback: MarketBeat broad feed (/ratings/us/) often works even when per-ticker pages are challenged.
    # If per-ticker fetched nothing (or all blocked), try to pull today's rows and filter by our tickers.
    if (not fetched_events) or (ok_count == 0 and blocked_count > 0):
        try:
            st2, html2 = _http_get(MB_US_RATINGS_URL)
            if st2 == 200 and not _is_bot_html(html2):
                ok_count += 1
                fetched_events.extend(_extract_events_from_us_html(html2, set(tickers), today))
            else:
                blocked_count += 1
        except Exception:
            blocked_count += 1


    # Merge cache + fetched; prefer marketbeat source for same key
    merged = cached_events + fetched_events
    merged = _dedupe_events(merged)

    # Update persistent cache (keep last 30 calendar days to control growth)
    keep_days = 30
    merged_keep = [e for e in merged if _within_window(e.date, keep_days, today)]
    merged_keep = _dedupe_events(merged_keep)
    _write_json(cache_path, {"updated_utc": dt.datetime.utcnow().isoformat() + "Z", "events": [asdict(e) for e in merged_keep]})

    # Select window for output
    window_events = [e for e in merged_keep if _within_window(e.date, days, today)]
    window_events = _dedupe_events(window_events)

    # Status logic
    status = "OK"
    if ok_count == 0 and blocked_count > 0:
        status = "BLOCKED (MarketBeat challenge/bot)"
    elif not window_events:
        # differentiate between true empty and blocked-but-has-cache-empty
        if blocked_count > 0 and ok_count == 0:
            status = "BLOCKED (MarketBeat challenge/bot)"
        else:
            status = "NO_EVENTS"

    # Write outputs (always valid)
    _write_json(out_json, [asdict(e) for e in window_events])
    _write_md(out_md, "Elemzői feed (MarketBeat)", status, window_events, days)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
