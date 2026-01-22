#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyst_marketbeat.py — MarketBeat (FREE) "ratings pages" scraper (Solution #2)
- No per-ticker search (avoids mass HTTP 403)
- Pulls central ratings lists once and filters to MASTER tickers
- Persists a small "first_seen" cache (reports/marketbeat_seen.json) so the "last N calendar days"
  window is stable even if MarketBeat is temporarily blocked (HTTP 403) on a given run.
- Hungarian output.

Versioning: keep bumping VERSION for every change.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import http.cookiejar
import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


VERSION="v0.3.19-marketbeat-free-hu-2026-01-22"

BASE = "https://www.marketbeat.com"
DEFAULT_SEEN_FILE = "reports/marketbeat_seen.json"

# Central free pages (fast + low risk of 403 compared to per-ticker search)
RATINGS_SOURCES: List[Tuple[str, str]] = [
    ("upgrade", "/ratings/upgrades/"),
    ("downgrade", "/ratings/downgrades/"),
    ("pt_change", "/ratings/pricetargetchanges/"),
]

RATING_WORDS = [
    "Strong Buy",
    "Buy",
    "Outperform",
    "Overweight",
    "Market Perform",
    "Sector Perform",
    "Neutral",
    "Hold",
    "Underperform",
    "Underweight",
    "Sell",
]


def _log(msg: str) -> None:
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] {msg}", flush=True)


def _build_opener(user_agent: str) -> urllib.request.OpenerDirector:
    """Create an urllib opener with a cookie jar (session-like)."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = [
        ("User-Agent", user_agent),
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        ("Accept-Language", "en-US,en;q=0.9"),
        ("Cache-Control", "no-cache"),
        ("Pragma", "no-cache"),
        ("Connection", "close"),
    ]
    return opener


# A small pool of realistic browser UAs to reduce bot-blocking variance
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


def _http_get(
    opener: urllib.request.OpenerDirector,
    url: str,
    timeout: int,
    debug_dir: Optional[Path],
    debug_name: str,
    referer: Optional[str] = None,
) -> Tuple[int, str]:
    """HTTP GET with cookies + optional Referer. Returns (status, html)."""
    hdrs = {}
    if referer:
        hdrs["Referer"] = referer
        hdrs["Upgrade-Insecure-Requests"] = "1"
    req = urllib.request.Request(url, headers=hdrs, method="GET")

    status = 0
    html = ""
    try:
        with opener.open(req, timeout=timeout) as r:
            status = getattr(r, "status", 200) or 200
            raw = r.read()
            html = raw.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        status = int(getattr(e, "code", 500) or 500)
        try:
            html = e.read().decode("utf-8", errors="replace")
        except Exception:
            html = ""
    except Exception:
        status = 0
        html = ""

    if debug_dir is not None:
        try:
            debug_dir.mkdir(parents=True, exist_ok=True)
            (debug_dir / f"{debug_name}.status.txt").write_text(str(status), encoding="utf-8")
            (debug_dir / f"{debug_name}.html").write_text(html, encoding="utf-8")
        except Exception:
            pass

    return status, html


def _clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    return s


def _parse_date_any(s: str) -> Optional[dt.date]:
    s = _clean_text(s)
    if not s:
        return None
    # Typical MarketBeat format: "Jan 21, 2026"
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


def _extract_urls_from_row_html(tr_html: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    # First href is often the stock page; ratings page link can vary
    hrefs = re.findall(r'href="([^"]+)"', tr_html, flags=re.I)
    for h in hrefs:
        if not h:
            continue
        if h.startswith("//"):
            h = "https:" + h
        elif h.startswith("/"):
            h = BASE + h
        if "marketbeat.com" not in h:
            continue
        if "/stocks/" in h and "stock" not in out:
            out["stock"] = h
        if "/ratings/" in h and "details" not in out:
            out["details"] = h
    return out


def _extract_ticker_from_row(tr_html: str, cells: List[str]) -> str:
    # Try from any cell that looks like "NFLX" or "NYSE:NFLX"
    joined = " ".join(cells)
    m = re.search(r"\b([A-Z]{1,5})\b", joined)
    if m:
        return m.group(1)
    # Fallback: from stock URL /stocks/nasdaq/nflx/
    m2 = re.search(r"/stocks/[^/]+/([a-z0-9.\-]+)/", tr_html, flags=re.I)
    if m2:
        return m2.group(1).upper()
    return ""


def _guess_firm(cells: List[str], joined: str) -> str:
    # Often firm is 2nd column; otherwise try to find a plausible firm-like substring
    if len(cells) >= 2:
        c = cells[1]
        if c and len(c) > 2:
            return c
    # Fallback: first long-ish token group
    parts = [c for c in cells if c and len(c) > 2]
    return parts[0] if parts else ""


def _kind_to_action(kind: str, joined: str) -> str:
    if kind == "upgrade":
        return "Felminősítés"
    if kind == "downgrade":
        return "Leminősítés"
    # price target change: decide direction later
    return "Célár változás"


def _extract_rating_change(text: str) -> Tuple[Optional[str], Optional[str]]:
    t = text or ""
    # Look for "from X to Y" patterns
    m = re.search(r"\bfrom\b\s+([A-Za-z ]+?)\s+\bto\b\s+([A-Za-z ]+)", t, flags=re.I)
    if m:
        return _clean_text(m.group(1)), _clean_text(m.group(2))
    # Or explicit arrows: "X → Y"
    m2 = re.search(r"\b(" + "|".join([re.escape(w) for w in RATING_WORDS]) + r")\b\s*(?:→|->)\s*\b(" + "|".join([re.escape(w) for w in RATING_WORDS]) + r")\b", t)
    if m2:
        return _clean_text(m2.group(1)), _clean_text(m2.group(2))
    return None, None


def _parse_money(s: str) -> Optional[float]:
    s = _clean_text(s)
    if not s:
        return None
    s = s.replace("$", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def _extract_pt_change(text: str) -> Tuple[Optional[float], Optional[float], str]:
    t = text or ""
    # "from $145.00 to $130.00"
    m = re.search(r"\bfrom\b\s*\$?([0-9]{1,6}(?:\.[0-9]{1,2})?)\s+\bto\b\s*\$?([0-9]{1,6}(?:\.[0-9]{1,2})?)", t, flags=re.I)
    if m:
        return float(m.group(1)), float(m.group(2)), "USD"
    # Arrow form: "$145.00 -> $130.00"
    m2 = re.search(r"\$?([0-9]{1,6}(?:\.[0-9]{1,2})?)\s*(?:→|->)\s*\$?([0-9]{1,6}(?:\.[0-9]{1,2})?)", t)
    if m2:
        return float(m2.group(1)), float(m2.group(2)), "USD"
    return None, None, "USD"


def _event_fingerprint(e: "AnalystEvent") -> str:
    # Stable identity for "same item" across runs
    raw = "|".join(
        [
            e.ticker.upper(),
            e.firm or "",
            e.action or "",
            e.rating_from or "",
            e.rating_to or "",
            "" if e.pt_from is None else f"{e.pt_from:.4f}",
            "" if e.pt_to is None else f"{e.pt_to:.4f}",
            e.source_url or "",
        ]
    ).encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()


def _load_seen_db(path: Path) -> Dict[str, dict]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_seen_db(path: Path, db: Dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(db, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _fresh_events_from_seen(db: Dict[str, dict], cutoff_iso: str) -> List["AnalystEvent"]:
    out: List[AnalystEvent] = []
    for v in db.values():
        fs = str(v.get("first_seen", ""))
        if fs and fs >= cutoff_iso:
            ev = v.get("event") or {}
            ev["date"] = fs  # first_seen drives the "last N days" window
            try:
                out.append(AnalystEvent(**ev))
            except Exception:
                continue
    return out


def _update_seen_with_today(db: Dict[str, dict], events_today: List["AnalystEvent"], today_iso: str) -> None:
    for e in events_today:
        key = _event_fingerprint(e)
        item = db.get(key) or {}
        first_seen = item.get("first_seen") or today_iso
        db[key] = {
            "first_seen": first_seen,
            "last_seen": today_iso,
            "event": asdict(e),
        }


@dataclass
class AnalystEvent:
    ticker: str
    date: str  # ISO date YYYY-MM-DD (we use first_seen for output stability)
    firm: str
    action: str
    rating_from: Optional[str]
    rating_to: Optional[str]
    pt_from: Optional[float]
    pt_to: Optional[float]
    currency: str
    source_url: str


def fetch_events_from_ratings_pages(
    master_tickers: List[str],
    days: int,
    timeout: int,
    sleep_s: float,
    debug_dir: Optional[Path],
) -> Tuple[List[AnalystEvent], Dict[str, str], bool, Optional[str]]:
    today = dt.datetime.utcnow().date()
    cutoff = today - dt.timedelta(days=days - 1)
    master_set = {t.upper() for t in master_tickers}

    statuses: Dict[str, str] = {}
    fetch_ok = False

    # Session-like opener with cookies + realistic UA; try UA fallbacks if blocked
    opener = _build_opener(UA_POOL[0])

    # Warmup home to get cookies (some runs require it)
    _http_get(opener, BASE + "/", timeout, debug_dir, "warmup_home", referer=None)

    out: List[AnalystEvent] = []
    seen: set = set()

    for kind, path in RATINGS_SOURCES:
        url = BASE + path
        status, page_html = _http_get(opener, url, timeout, debug_dir, f"ratings_{kind}", referer=BASE + '/')
        statuses[kind] = f"HTTP {status}" if status else "HTTP ?"

        # If blocked, retry once with alternate UA (new cookie jar)
        if status == 403 and len(UA_POOL) > 1:
            opener = _build_opener(UA_POOL[1])
            _http_get(opener, BASE + "/", timeout, debug_dir, "warmup_home_alt", referer=None)
            status, page_html = _http_get(opener, url, timeout, debug_dir, f"ratings_{kind}_alt", referer=BASE + "/")
            statuses[kind] = f"HTTP {status}" if status else "HTTP ?"

        if status >= 400 or status == 0:
            _log(f"RATINGS {kind}: {statuses[kind]} (skip)")
            time.sleep(sleep_s)
            continue

        fetch_ok = True

        # Iterate <tr> blocks
        for tr in re.findall(r"<tr[^>]*>.*?</tr>", page_html, flags=re.I | re.S):
            tds = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.I | re.S)
            if not tds:
                continue
            cells = [_clean_text(re.sub(r"<[^>]+>", " ", c)) for c in tds]
            if not cells:
                continue

            # Date (usually first cell)
            date_obj = _parse_date_any(cells[0]) if cells else None
            if not date_obj:
                # If missing, treat as today so it can be cached; still filtered later via first_seen
                date_obj = today

            if date_obj < cutoff or date_obj > today:
                continue

            ticker = _extract_ticker_from_row(tr, cells)
            if not ticker or ticker.upper() not in master_set:
                continue

            urls = _extract_urls_from_row_html(tr)
            stock_url = urls.get("stock", "")
            details_url = urls.get("details", "")
            source_url = details_url or stock_url or url

            joined = " ".join([c for c in cells if c]).strip()
            firm = _guess_firm(cells, joined)
            action = _kind_to_action(kind, joined)

            rating_from, rating_to = _extract_rating_change(joined)
            pt_from, pt_to, currency = _extract_pt_change(joined)

            # For PT change decide direction label
            if kind == "pt_change":
                if pt_from is not None and pt_to is not None:
                    if pt_to > pt_from:
                        action = "Célár emelés"
                    elif pt_to < pt_from:
                        action = "Célár csökkentés"
                    else:
                        action = "Célár változatlan"
                else:
                    action = "Célár változás"

            e = AnalystEvent(
                ticker=ticker.upper(),
                date=date_obj.isoformat(),  # will be replaced by first_seen on output
                firm=firm,
                action=action,
                rating_from=rating_from,
                rating_to=rating_to,
                pt_from=pt_from,
                pt_to=pt_to,
                currency=currency,
                source_url=source_url,
            )
            key = (
                e.ticker,
                e.firm,
                e.action,
                e.rating_from,
                e.rating_to,
                e.pt_from,
                e.pt_to,
                e.source_url,
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(e)

        time.sleep(sleep_s)

    note = None
    if not fetch_ok:
        # If everything is blocked, expose that clearly (we will still output cache if available)
        if any("403" in v for v in statuses.values()):
            note = None
        else:
            note = None

    return out, statuses, fetch_ok, note


def write_outputs(
    out_md: Path,
    out_json: Optional[Path],
    events: List[AnalystEvent],
    days: int,
    fetch_ok: bool = True,
    statuses: Optional[Dict[str, str]] = None,
    note: Optional[str] = None,
) -> None:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = []
    lines.append(f"# Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {days} naptári nap) (első észlelés alapján)")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Generálva (UTC): {now}")



    lines.append("")

    if not events:
        if fetch_ok:
            # True empty (source reachable but no items)
            lines.append(f"_Nincs új fel/leminősítés vagy célár‑változás az elmúlt {days} naptári napban (MarketBeat)._")
        else:
            # Error / blocked (do NOT show the misleading "no fresh" line)
            lines.append("_N/A._")
    else:
        by: Dict[str, List[AnalystEvent]] = {}
        for e in events:
            by.setdefault(e.ticker, []).append(e)
        for t in sorted(by.keys()):
            lines.append(f"## {t}")
            for e in sorted(by[t], key=lambda x: x.date, reverse=True):
                parts = [f"- {e.date} — {e.firm} — {e.action}"]
                if e.rating_from or e.rating_to:
                    parts.append(f"Ajánlás: {e.rating_from or '—'} → {e.rating_to or '—'}")
                if e.pt_from is not None or e.pt_to is not None:
                    if e.pt_from is not None and e.pt_to is not None:
                        parts.append(f"Célár: {e.currency} {e.pt_from:.2f} → {e.pt_to:.2f}")
                    elif e.pt_to is not None:
                        parts.append(f"Célár: {e.currency} {e.pt_to:.2f}")
                parts.append(f"Forrás: {e.source_url}")
                lines.append(" | ".join(parts))
            lines.append("")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    if out_json:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": VERSION,
            "generated_utc": now,
            "days": days,
            "count": len(events),
            "events": [asdict(e) for e in events],
        }
        out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_master_tickers(master_csv: Path) -> List[str]:
    # CSV exported from Google Sheets: contains "Ticker" column (or similar)
    txt = master_csv.read_text(encoding="utf-8", errors="replace")
    lines = [l for l in txt.splitlines() if l.strip()]
    if not lines:
        return []
    header = [h.strip().strip('"') for h in lines[0].split(",")]
    # Try common column names
    idx = None
    for cand in ("Ticker", "ticker", "TICKER", "Symbol", "symbol"):
        if cand in header:
            idx = header.index(cand)
            break
    if idx is None:
        # Fallback: first column
        idx = 0
    out: List[str] = []
    for l in lines[1:]:
        cols = [c.strip().strip('"') for c in l.split(",")]
        if idx < len(cols):
            t = cols[idx].strip().upper()
            if t and re.fullmatch(r"[A-Z0-9.\-]{1,12}", t):
                out.append(t)
    return sorted(set(out))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-json", default="")
    ap.add_argument("--seen-file", default=DEFAULT_SEEN_FILE)
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug-dir", default="reports/debug_marketbeat")
    args = ap.parse_args()

    master = Path(args.master)
    if not master.exists():
        _log(f"ERROR: MASTER CSV not found: {master}")
        # Still write a small md so the main report doesn't break
        out_md = Path(args.out_md)
        write_outputs(out_md, None, [], args.days, fetch_ok=False, statuses={"master": "missing"}, note=None)
        return 0

    debug_dir = Path(args.debug_dir) if args.debug else None

    _log(f"START {VERSION} days={args.days} master={master} mode=ratings_pages")

    today_iso = dt.datetime.utcnow().date().isoformat()
    cutoff_iso = (dt.datetime.utcnow().date() - dt.timedelta(days=args.days - 1)).isoformat()

    tickers = read_master_tickers(master)

    seen_path = Path(args.seen_file)
    seen_db = _load_seen_db(seen_path)

    # Fetch today's items (best effort)
    events_today: List[AnalystEvent] = []
    statuses: Dict[str, str] = {}
    fetch_ok = False

    note: Optional[str] = None

    try:
        events_today, statuses, fetch_ok, note = fetch_events_from_ratings_pages(
            master_tickers=tickers,
            days=args.days,
            timeout=args.timeout,
            sleep_s=max(args.sleep, 0.35),
            debug_dir=debug_dir,
        )
    except Exception as e:
        fetch_ok = False
        note = None
        _log(f"WARN: exception during fetch: {e}")

    if fetch_ok and events_today:
        _update_seen_with_today(seen_db, events_today, today_iso)
        _save_seen_db(seen_path, seen_db)
    elif fetch_ok:
        # Source ok but no events: still update db 'last_seen' is irrelevant; keep db as-is
        _save_seen_db(seen_path, seen_db)
    else:
        # Source not ok: do not overwrite cache; keep db untouched
        pass

    # Output from cache window (first_seen-based)
    events_out = _fresh_events_from_seen(seen_db, cutoff_iso)
    events_out.sort(key=lambda e: (e.date, e.ticker), reverse=True)

    out_md = Path(args.out_md)
    out_json = Path(args.out_json) if args.out_json else None

    write_outputs(out_md, out_json, events_out, args.days, fetch_ok=fetch_ok, statuses=statuses, note=note)
    _log(f"DONE events={len(events_out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
