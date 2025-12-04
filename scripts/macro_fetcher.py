#!/usr/bin/env python3
# scripts/macro_fetcher.py
# Version: 1.0.0 - Macro news collector (Reuters + Yahoo Finance + Bloomberg)
#
# Output:
#   reports/1/macro_news_1.json
#
# Schema:
# {
#   "macro_news": [
#       {
#           "headline": "...",
#           "snippet": "...",
#           "source": "Reuters|Yahoo Finance|Bloomberg",
#           "time": "ISO8601 or short string"
#       },
#       ...
#   ]
# }

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "reports" / "1"
OUT_PATH = OUT_DIR / "macro_news_1.json"


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def fetch_reuters_top() -> List[Dict[str, Any]]:
    """Fetch macro / markets headlines from Reuters.

    Note: HTML may change over time; this is a best-effort parser.
    """
    url = "https://www.reuters.com/markets/economicData/"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    items: List[Dict[str, Any]] = []

    for article in soup.select("article.story, article.story-card, article"):
        headline_el = article.select_one("h3, h2, a")
        if not headline_el:
            continue
        headline = _clean_text(headline_el.get_text())
        if not headline:
            continue

        snippet_el = article.select_one("p")
        snippet = _clean_text(snippet_el.get_text()) if snippet_el else ""

        time_el = article.select_one("time")
        time_str = time_el.get("datetime") if time_el and time_el.has_attr("datetime") else ""
        time_str = time_str or _clean_text(time_el.get_text()) if time_el else ""

        items.append(
            {
                "headline": headline,
                "snippet": snippet,
                "source": "Reuters",
                "time": time_str,
            }
        )

    return items


def fetch_yahoo_macro() -> List[Dict[str, Any]]:
    """Fetch macro / market headlines from Yahoo Finance US markets page."""
    url = "https://finance.yahoo.com/topic/economic-news/"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    items: List[Dict[str, Any]] = []

    for li in soup.select("li.js-stream-content"):
        headline_el = li.select_one("h3, h2, a")
        if not headline_el:
            continue
        headline = _clean_text(headline_el.get_text())
        if not headline:
            continue

        snippet_el = li.select_one("p")
        snippet = _clean_text(snippet_el.get_text()) if snippet_el else ""

        time_el = li.select_one("time")
        time_str = time_el.get("datetime") if time_el and time_el.has_attr("datetime") else ""
        time_str = time_str or _clean_text(time_el.get_text()) if time_el else ""

        items.append(
            {
                "headline": headline,
                "snippet": snippet,
                "source": "Yahoo Finance",
                "time": time_str,
            }
        )

    return items


def fetch_bloomberg_macro() -> List[Dict[str, Any]]:
    """Fetch macro / economics headlines from Bloomberg.

    Bloomberg is often partly paywalled, but headlines are usually visible.
    """
    url = "https://www.bloomberg.com/markets/economics"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    items: List[Dict[str, Any]] = []

    # Best-effort selectors; may need adjustment if Bloomberg changes layout.
    for article in soup.select("article, li"):
        headline_el = article.select_one("h3, h2, a")
        if not headline_el:
            continue
        headline = _clean_text(headline_el.get_text())
        if not headline:
            continue

        snippet_el = article.select_one("p")
        snippet = _clean_text(snippet_el.get_text()) if snippet_el else ""

        time_el = article.select_one("time")
        time_str = time_el.get("datetime") if time_el and time_el.has_attr("datetime") else ""
        time_str = time_str or _clean_text(time_el.get_text()) if time_el else ""

        items.append(
            {
                "headline": headline,
                "snippet": snippet,
                "source": "Bloomberg",
                "time": time_str,
            }
        )

    return items


def dedupe_and_sort(items: List[Dict[str, Any]], max_total: int = 15) -> List[Dict[str, Any]]:
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for it in items:
        key = (it.get("headline") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append(it)
    # Try to keep original order; if time strings are ISO-like, we could parse and sort.
    return uniq[:max_total]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_items: List[Dict[str, Any]] = []

    # Priority stack: Reuters -> Yahoo -> Bloomberg (as requested: Bloomberg at the end)
    try:
        all_items.extend(fetch_reuters_top())
    except Exception as e:
        print(f"[macro_fetcher] Reuters fetch failed: {e}")

    try:
        all_items.extend(fetch_yahoo_macro())
    except Exception as e:
        print(f"[macro_fetcher] Yahoo fetch failed: {e}")

    try:
        all_items.extend(fetch_bloomberg_macro())
    except Exception as e:
        print(f"[macro_fetcher] Bloomberg fetch failed: {e}")

    all_items = dedupe_and_sort(all_items, max_total=20)

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "macro_news": all_items,
    }

    OUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[macro_fetcher] Wrote {len(all_items)} items to {OUT_PATH}")


if __name__ == "__main__":
    main()
