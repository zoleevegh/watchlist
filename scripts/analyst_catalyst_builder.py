#!/usr/bin/env python3
"""
analyst_catalyst_builder.py

Version: v1.1.0 (2025-12-20)

Goal
----
Produce NON-BLOCKING analyst + catalyst artifacts for report #1/#2/#3 even when
MarketWatch/MarketBeat are blocked.

Outputs (for a given report N):
- reports/{N}/analyst_{N}.json
- reports/{N}/catalysts_{N}.json
- reports/{N}/health_analyst_{N}.json

Primary data sources (block-resistant):
1) TheFly RSS (ratings / PT changes / upgrades-downgrades headlines)
2) Yahoo Finance QuoteSummary calendarEvents (earnings dates) for catalysts
3) SEC EDGAR "current events" feed (optional, broad catalysts)

Notes
-----
- This builder is designed to "always return something": even if analyst feeds are empty,
  catalysts from Yahoo calendarEvents can still populate the report.
- All network failures are captured in health_analyst_{N}.json so the report can explain
  empty blocks honestly (but still keep structure).

CLI
---
python scripts/analyst_catalyst_builder.py --report 1 --reports-dir reports --max-tickers 120

Env overrides
-------------
THEFLY_RSS_URLS: comma-separated list of RSS URLs to try (first successful used)
USER_AGENT: custom UA
YAHOO_SLEEP_MS: delay between Yahoo requests (default 80ms)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any

DEFAULT_UA = os.getenv("USER_AGENT", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36")
YAHOO_SLEEP_MS = int(os.getenv("YAHOO_SLEEP_MS", "80"))

DEFAULT_THEFLY_RSS_URLS = [
    # These endpoints may change; you can override via THEFLY_RSS_URLS env var.
    "https://thefly.com/rss.php",  # broad market news RSS
]

THEFLY_RSS_URLS = [u.strip() for u in os.getenv("THEFLY_RSS_URLS", "").split(",") if u.strip()] or DEFAULT_THEFLY_RSS_URLS

# Keywords that typically indicate analyst actions
ANALYST_KEYWORDS = [
    "upgraded",
    "downgraded",
    "initiated",
    "reiterated",
    "raised price target",
    "cut price target",
    "price target",
    "pt raised",
    "pt cut",
    "to buy",
    "to neutral",
    "to sell",
    "to outperform",
    "to underperform",
    "to overweight",
    "to equal weight",
    "to market perform",
]

# Basic ticker patterns; we later filter by MASTER universe to avoid false positives.
TICKER_RE = re.compile(r"\b[A-Z]{1,5}(?:\.[A-Z]{1,2})?\b")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def http_get(url: str, timeout: int = 20) -> Tuple[bool, int, str, bytes, float]:
    """Return (ok, status, error, content, ms)."""
    t0 = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": DEFAULT_UA, "Accept": "*/*"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            content = resp.read()
            ms = (time.time() - t0) * 1000
            return True, int(status), "", content, ms
    except urllib.error.HTTPError as e:
        ms = (time.time() - t0) * 1000
        try:
            body = e.read() or b""
        except Exception:
            body = b""
        return False, int(getattr(e, "code", 0) or 0), f"HTTP_{getattr(e, 'code', '')}", body, ms
    except Exception as e:
        ms = (time.time() - t0) * 1000
        return False, 0, f"{type(e).__name__}: {e}", b"", ms


def parse_rss_items(xml_bytes: bytes) -> List[Dict[str, str]]:
    """
    Parse RSS 2.0 or Atom into normalized list of {title, link, published}.
    """
    items: List[Dict[str, str]] = []
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return items

    # Atom
    if root.tag.endswith("feed"):
        ns = {"a": root.tag.split("}")[0].strip("{")} if "}" in root.tag else {}
        for entry in root.findall(".//a:entry", ns) if ns else root.findall(".//entry"):
            title_el = entry.find("a:title", ns) if ns else entry.find("title")
            link_el = entry.find("a:link", ns) if ns else entry.find("link")
            pub_el = entry.find("a:updated", ns) if ns else entry.find("updated")
            if pub_el is None:
                pub_el = entry.find("a:published", ns) if ns else entry.find("published")
            title = (title_el.text or "").strip() if title_el is not None else ""
            link = (link_el.get("href") or "").strip() if link_el is not None else ""
            published = (pub_el.text or "").strip() if pub_el is not None else ""
            if title:
                items.append({"title": title, "link": link, "published": published})
        return items

    # RSS 2.0
    for it in root.findall(".//item"):
        title_el = it.find("title")
        link_el = it.find("link")
        pub_el = it.find("pubDate")
        title = (title_el.text or "").strip() if title_el is not None else ""
        link = (link_el.text or "").strip() if link_el is not None else ""
        published = (pub_el.text or "").strip() if pub_el is not None else ""
        if title:
            items.append({"title": title, "link": link, "published": published})
    return items


def load_master_tickers(master_csv_path: str, max_tickers: int) -> List[str]:
    if not os.path.exists(master_csv_path):
        return []
    tickers: List[str] = []
    with open(master_csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        # Try common column names
        for row in reader:
            t = (row.get("Ticker") or row.get("ticker") or row.get("TICKER") or "").strip()
            if not t:
                # sometimes first column unnamed
                for k in row.keys():
                    if k and k.strip().lower() in ("symbol", "symbols"):
                        t = (row.get(k) or "").strip()
                        break
            if t:
                tickers.append(t.upper())
            if len(tickers) >= max_tickers:
                break
    # Deduplicate preserving order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def normalize_published(raw: str) -> str:
    if not raw:
        return ""
    # Keep raw if parsing fails
    # Common RSS date format: "Sat, 20 Dec 2025 14:00:00 GMT"
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return raw


def extract_tickers_from_title(title: str, universe_set: set) -> List[str]:
    cands = TICKER_RE.findall(title.upper())
    # Filter common false positives
    stop = {"US", "USA", "GDP", "FED", "CPI", "PMI", "FOMC", "SEC", "AI", "ETF"}
    out = []
    for c in cands:
        if c in stop:
            continue
        if c in universe_set:
            out.append(c)
    # Deduplicate
    return list(dict.fromkeys(out))


def is_analyst_headline(title: str) -> bool:
    tl = title.lower()
    return any(k in tl for k in ANALYST_KEYWORDS)


def build_analyst_events(universe: List[str], max_items: int = 200) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Fetch TheFly RSS and extract analyst-like headlines; map to tickers in universe.
    """
    universe_set = set(universe)
    health = {"source": "TheFly RSS", "ok": False, "httpStatus": None, "error": "", "bytes": 0, "ms": 0, "usedUrl": "", "rawItems": 0, "matchedItems": 0}
    events: List[Dict[str, Any]] = []

    for url in THEFLY_RSS_URLS:
        ok, status, err, content, ms = http_get(url, timeout=25)
        health.update({"httpStatus": status, "error": err, "bytes": len(content), "ms": round(ms, 2), "usedUrl": url})
        if not ok or len(content) < 200:
            continue

        items = parse_rss_items(content)
        health["rawItems"] = len(items)
        if not items:
            continue

        for it in items:
            title = it.get("title", "").strip()
            if not title:
                continue
            if not is_analyst_headline(title):
                continue
            tickers = extract_tickers_from_title(title, universe_set)
            if not tickers:
                continue
            events.append({
                "tickers": tickers,
                "headline": title,
                "source": "TheFly",
                "url": it.get("link", ""),
                "published": normalize_published(it.get("published", "")),
                "kind": "analyst_action",
            })
            if len(events) >= max_items:
                break

        health["matchedItems"] = len(events)
        health["ok"] = True  # even if 0 matched, RSS worked
        break

    # De-dup by (headline, url)
    dedup = {}
    for e in events:
        key = (e.get("headline", ""), e.get("url", ""))
        dedup[key] = e
    events = list(dedup.values())
    return events, health


def yahoo_calendar_for_ticker(ticker: str) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]:
    """
    Fetch Yahoo quoteSummary calendarEvents (earnings dates).
    """
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=calendarEvents"
    ok, status, err, content, ms = http_get(url, timeout=20)
    h = {"url": url, "ok": ok, "httpStatus": status, "error": err, "bytes": len(content), "ms": round(ms, 2)}
    if not ok or not content:
        return ticker, None, h
    try:
        data = json.loads(content.decode("utf-8", errors="ignore"))
    except Exception as e:
        h["ok"] = False
        h["error"] = f"JSONDecodeError: {e}"
        return ticker, None, h

    try:
        result = data["quoteSummary"]["result"][0]["calendarEvents"]
    except Exception:
        return ticker, None, h
    return ticker, result, h


def build_catalysts(universe: List[str], days_ahead: int = 45, max_workers: int = 10) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Build catalysts primarily from Yahoo earnings calendar.
    """
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=days_ahead)

    catalysts: List[Dict[str, Any]] = []
    health = {
        "source": "Yahoo QuoteSummary calendarEvents",
        "ok": True,
        "errors": [],
        "fetchCount": 0,
        "okCount": 0,
        "failCount": 0,
        "samples": [],
    }

    # Threaded fetch with mild rate limiting
    def task(t: str):
        time.sleep(YAHOO_SLEEP_MS / 1000.0)
        return yahoo_calendar_for_ticker(t)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(task, t): t for t in universe}
        for fut in as_completed(futs):
            t = futs[fut]
            health["fetchCount"] += 1
            try:
                ticker, cal, h = fut.result()
            except Exception as e:
                health["failCount"] += 1
                health["errors"].append(f"{t}: {type(e).__name__}: {e}")
                continue

            if h.get("ok"):
                health["okCount"] += 1
            else:
                health["failCount"] += 1
                if h.get("error"):
                    health["errors"].append(f"{t}: {h['error']}")
            if len(health["samples"]) < 8:
                health["samples"].append({**h, "ticker": t})

            if not cal:
                continue

            # Earnings date
            try:
                ed = cal.get("earnings", {}).get("earningsDate", [])
                if ed and isinstance(ed, list) and ed[0].get("raw") is not None:
                    ts = int(ed[0]["raw"])
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    if now <= dt <= cutoff:
                        catalysts.append({
                            "tickers": [ticker],
                            "headline": f"Earnings expected on {dt.date().isoformat()}",
                            "source": "Yahoo Finance (calendarEvents)",
                            "url": f"https://finance.yahoo.com/quote/{ticker}",
                            "published": utc_now_iso(),
                            "kind": "earnings",
                            "eventDate": dt.isoformat(),
                        })
            except Exception:
                pass

            # Other calendar events can be added here if needed

    # Dedup
    dedup = {}
    for c in catalysts:
        key = (tuple(c.get("tickers", [])), c.get("kind", ""), c.get("eventDate", ""))
        dedup[key] = c
    catalysts = list(dedup.values())

    if health["failCount"] > 0 and health["okCount"] == 0:
        health["ok"] = False
    return catalysts, health


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, choices=["1", "2", "3"], help="Report number")
    ap.add_argument("--reports-dir", default="reports", help="Reports root dir")
    ap.add_argument("--max-tickers", type=int, default=120, help="Max tickers read from master.csv")
    ap.add_argument("--days-ahead", type=int, default=45, help="How many days ahead to include catalysts")
    ap.add_argument("--max-workers", type=int, default=10, help="Parallel workers for Yahoo calls")
    args = ap.parse_args()

    report = args.report
    reports_root = args.reports_dir
    out_dir = os.path.join(reports_root, report)
    ensure_dir(out_dir)

    master_csv = os.path.join(reports_root, "master.csv")
    universe = load_master_tickers(master_csv, args.max_tickers)

    # If master.csv is not found in root, also try inside report dir (legacy)
    if not universe:
        master_csv2 = os.path.join(out_dir, "master.csv")
        universe = load_master_tickers(master_csv2, args.max_tickers)

    # Build analyst events
    analyst_events, analyst_health = build_analyst_events(universe)

    # Build catalysts
    catalysts, catalysts_health = build_catalysts(universe, days_ahead=args.days_ahead, max_workers=args.max_workers)

    # Write outputs
    analyst_path = os.path.join(out_dir, f"analyst_{report}.json")
    catalysts_path = os.path.join(out_dir, f"catalysts_{report}.json")
    health_path = os.path.join(out_dir, f"health_analyst_{report}.json")

    write_json(analyst_path, analyst_events)
    write_json(catalysts_path, catalysts)

    health_obj = {
        "ok": True,
        "type": "health",
        "report": report,
        "generatedAt": utc_now_iso(),
        "version": "1.1.0-noblock-rss+yahoo",
        "analystCount": len(analyst_events),
        "catalystCount": len(catalysts),
        "sources": {
            "TheFly RSS": analyst_health,
            "Yahoo calendarEvents": catalysts_health,
        },
    }
    # Overall ok if at least one source fetched ok, even if no matches
    if not analyst_health.get("ok") and not catalysts_health.get("ok"):
        health_obj["ok"] = False

    write_json(health_path, health_obj)

    print(f"[OK] wrote {analyst_path} ({len(analyst_events)} items)")
    print(f"[OK] wrote {catalysts_path} ({len(catalysts)} items)")
    print(f"[OK] wrote {health_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
