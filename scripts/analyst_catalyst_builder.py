#!/usr/bin/env python3
"""
analyst_catalyst_builder.py

Version: v1.1.1 (2025-12-20)

Purpose
-------
"Always produce data" analyst + catalyst artifacts without relying on MarketWatch/MarketBeat.

Non-blocking sources:
- TheFly RSS (headline-level analyst actions / PT changes / upgrades/downgrades)
- Yahoo Finance QuoteSummary calendarEvents (earnings catalysts)

Outputs (default, report N):
- reports/{N}/analyst_{N}.json
- reports/{N}/catalysts_{N}.json
- reports/{N}/health_analyst_{N}.json

⚠️ Compatibility mode
---------------------
Your current workflow (run_report.yml) may call this script with:
  --analyst-out reports/analyst_{N}.json
  --catalysts-out reports/catalysts_{N}.json
and expects files in repo root "reports/" (not in reports/{N}/).

This script supports BOTH:
- If --analyst-out / --catalysts-out are provided → it writes exactly there.
- Otherwise → writes into reports/{N}/ as above.

CLI examples
------------
python scripts/analyst_catalyst_builder.py --report 1 --reports-dir reports
python scripts/analyst_catalyst_builder.py --report 1 --analyst-out reports/analyst_1.json --catalysts-out reports/catalysts_1.json

Env
---
THEFLY_RSS_URLS: comma-separated RSS URLs to try (first successful used)
USER_AGENT: custom UA
YAHOO_SLEEP_MS: delay between Yahoo requests (default 100ms)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any

DEFAULT_UA = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
)
YAHOO_SLEEP_MS = int(os.getenv("YAHOO_SLEEP_MS", "100"))

DEFAULT_THEFLY_RSS_URLS = [
    # TheFly RSS endpoint(s). Override via THEFLY_RSS_URLS.
    "https://thefly.com/rss.php",
]
THEFLY_RSS_URLS = [u.strip() for u in os.getenv("THEFLY_RSS_URLS", "").split(",") if u.strip()] or DEFAULT_THEFLY_RSS_URLS

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

TICKER_RE = re.compile(r"\b[A-Z]{1,5}(?:\.[A-Z]{1,2})?\b")
STOPWORDS = {"US", "USA", "GDP", "FED", "CPI", "PMI", "FOMC", "SEC", "AI", "ETF"}


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
    """Parse RSS 2.0 or Atom into normalized list of {title, link, published}."""
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
        for row in reader:
            t = (row.get("Ticker") or row.get("ticker") or row.get("TICKER") or "").strip()
            if t:
                tickers.append(t.upper())
            if len(tickers) >= max_tickers:
                break
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
    out = []
    for c in cands:
        if c in STOPWORDS:
            continue
        if c in universe_set:
            out.append(c)
    return list(dict.fromkeys(out))


def is_analyst_headline(title: str) -> bool:
    tl = title.lower()
    return any(k in tl for k in ANALYST_KEYWORDS)


def build_analyst_events(universe: List[str], max_items: int = 250) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    universe_set = set(universe)
    health = {
        "source": "TheFly RSS",
        "ok": False,
        "httpStatus": None,
        "error": "",
        "bytes": 0,
        "ms": 0,
        "usedUrl": "",
        "rawItems": 0,
        "matchedItems": 0,
    }
    events: List[Dict[str, Any]] = []

    for url in THEFLY_RSS_URLS:
        ok, status, err, content, ms = http_get(url, timeout=25)
        health.update({"httpStatus": status, "error": err, "bytes": len(content), "ms": round(ms, 2), "usedUrl": url})

        if not ok or len(content) < 200:
            continue

        items = parse_rss_items(content)
        health["rawItems"] = len(items)

        for it in items:
            title = (it.get("title") or "").strip()
            if not title or not is_analyst_headline(title):
                continue
            tickers = extract_tickers_from_title(title, universe_set)
            if not tickers:
                continue
            events.append(
                {
                    "tickers": tickers,
                    "headline": title,
                    "source": "TheFly",
                    "url": it.get("link", ""),
                    "published": normalize_published(it.get("published", "")),
                    "kind": "analyst_action",
                }
            )
            if len(events) >= max_items:
                break

        health["matchedItems"] = len(events)
        health["ok"] = True  # RSS fetched OK (even if 0 matches)
        break

    # de-dup
    dedup = {}
    for e in events:
        dedup[(e.get("headline", ""), e.get("url", ""))] = e
    return list(dedup.values()), health


def yahoo_calendar_for_ticker(ticker: str) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]:
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
                        catalysts.append(
                            {
                                "tickers": [ticker],
                                "headline": f"Earnings expected on {dt.date().isoformat()}",
                                "source": "Yahoo Finance (calendarEvents)",
                                "url": f"https://finance.yahoo.com/quote/{ticker}",
                                "published": utc_now_iso(),
                                "kind": "earnings",
                                "eventDate": dt.isoformat(),
                            }
                        )
            except Exception:
                pass

    # Dedup
    dedup = {}
    for c in catalysts:
        dedup[(tuple(c.get("tickers", [])), c.get("kind", ""), c.get("eventDate", ""))] = c
    catalysts = list(dedup.values())

    if health["failCount"] > 0 and health["okCount"] == 0:
        health["ok"] = False
    return catalysts, health


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, obj: Any) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, choices=["1", "2", "3"], help="Report number")
    ap.add_argument("--reports-dir", default="reports", help="Reports root dir")
    ap.add_argument("--max-tickers", type=int, default=160, help="Max tickers read from master.csv")
    ap.add_argument("--days-ahead", type=int, default=45, help="How many days ahead to include catalysts")
    ap.add_argument("--max-workers", type=int, default=10, help="Parallel workers for Yahoo calls")

    # Compatibility flags (used by your workflow)
    ap.add_argument("--analyst-out", default="", help="Write analyst JSON to this path (compat)")
    ap.add_argument("--catalysts-out", default="", help="Write catalysts JSON to this path (compat)")
    ap.add_argument("--health-out", default="", help="Write health JSON to this path (optional)")

    args = ap.parse_args()

    report = args.report
    reports_root = args.reports_dir

    # Universe from master.csv
    master_csv = os.path.join(reports_root, "master.csv")
    universe = load_master_tickers(master_csv, args.max_tickers)
    if not universe:
        # legacy fallback
        master_csv2 = os.path.join(reports_root, report, "master.csv")
        universe = load_master_tickers(master_csv2, args.max_tickers)

    analyst_events, analyst_health = build_analyst_events(universe)
    catalysts, catalysts_health = build_catalysts(universe, days_ahead=args.days_ahead, max_workers=args.max_workers)

    # Output paths
    if args.analyst_out.strip() and args.catalysts_out.strip():
        analyst_path = args.analyst_out.strip()
        catalysts_path = args.catalysts_out.strip()
        # health default next to analyst_out if not provided
        health_path = args.health_out.strip() or os.path.join(os.path.dirname(analyst_path) or "reports", f"health_analyst_{report}.json")
    else:
        out_dir = os.path.join(reports_root, report)
        analyst_path = os.path.join(out_dir, f"analyst_{report}.json")
        catalysts_path = os.path.join(out_dir, f"catalysts_{report}.json")
        health_path = args.health_out.strip() or os.path.join(out_dir, f"health_analyst_{report}.json")

    write_json(analyst_path, analyst_events)
    write_json(catalysts_path, catalysts)

    health_obj = {
        "ok": True,
        "type": "health",
        "report": report,
        "generatedAt": utc_now_iso(),
        "version": "1.1.1-noblock-rss+yahoo",
        "analystCount": len(analyst_events),
        "catalystCount": len(catalysts),
        "sources": {"TheFly RSS": analyst_health, "Yahoo calendarEvents": catalysts_health},
    }
    if not analyst_health.get("ok") and not catalysts_health.get("ok"):
        health_obj["ok"] = False

    write_json(health_path, health_obj)

    print(f"[OK] wrote {analyst_path} ({len(analyst_events)} items)")
    print(f"[OK] wrote {catalysts_path} ({len(catalysts)} items)")
    print(f"[OK] wrote {health_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
