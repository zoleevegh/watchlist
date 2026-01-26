#!/usr/bin/env python3
"""
Yahoo Live probe + backoff retry (v1.0.1)

Fix:
- v1.0.1: import Path from pathlib (NameError fix)

Purpose:
- Deterministically test Yahoo Finance "Live" page fetch reliability.
- Implements retries with exponential backoff (1s, 3s, 9s) + jitter.
- Produces a small markdown report (yahoo_live_probe_report.md) summarizing:
  - final HTTP status
  - whether blocked/challenged
  - extracted <title> when available
  - a short "macro" fallback note (stub) when blocked

Inputs (env):
- YAHOO_LIVE_URL (optional): defaults to a known Yahoo Live URL
- MAX_ATTEMPTS (optional): default 4
- TIMEOUT_SECS (optional): default 20
"""

from __future__ import annotations

import os
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests


DEFAULT_URL = (
    "https://finance.yahoo.com/news/live/"
    "stock-market-today-dow-sp-500-nasdaq-falter-with-big-week-of-big-tech-earnings-fed-meeting-ahead-110341565.html"
)

VERSION = "v1.0.1"


@dataclass
class FetchResult:
    ok: bool
    status: Optional[int]
    attempts: int
    reason: str
    title: Optional[str] = None


def _clean_title(raw: str) -> str:
    raw = re.sub(r"\s+", " ", raw or "").strip()
    return raw[:200] if raw else ""


def fetch_with_backoff(url: str, max_attempts: int = 4, timeout_secs: int = 20) -> FetchResult:
    # 1s, 3s, 9s (+15 as last fallback) with jitter
    delays = [1, 3, 9, 15]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    last_status: Optional[int] = None
    last_reason = "unknown"

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[{VERSION}] Attempt {attempt}/{max_attempts}: GET {url}", flush=True)
            r = requests.get(url, headers=headers, timeout=timeout_secs, allow_redirects=True)
            last_status = r.status_code

            if r.status_code in (429, 403):
                last_reason = "blocked_or_ratelimited"
                print(f"[{VERSION}] -> HTTP {r.status_code} (blocked/rate-limited)", flush=True)
                if attempt < max_attempts:
                    base = delays[min(attempt - 1, len(delays) - 1)]
                    sleep_s = base + random.uniform(0.0, 0.7)
                    print(f"[{VERSION}] Backoff sleep: {sleep_s:.2f}s", flush=True)
                    time.sleep(sleep_s)
                    continue
                return FetchResult(ok=False, status=r.status_code, attempts=attempt, reason=last_reason)

            if r.status_code != 200:
                last_reason = "non_200"
                print(f"[{VERSION}] -> HTTP {r.status_code} (non-200)", flush=True)
                if attempt < max_attempts:
                    sleep_s = 0.7 + random.uniform(0.0, 0.5)
                    print(f"[{VERSION}] Short sleep: {sleep_s:.2f}s", flush=True)
                    time.sleep(sleep_s)
                    continue
                return FetchResult(ok=False, status=r.status_code, attempts=attempt, reason=last_reason)

            html = r.text or ""
            if len(html) < 2000:
                last_reason = "too_short_html"
                print(f"[{VERSION}] -> HTTP 200 but suspiciously short HTML ({len(html)} chars)", flush=True)
                if attempt < max_attempts:
                    base = delays[min(attempt - 1, len(delays) - 1)]
                    sleep_s = base + random.uniform(0.0, 0.7)
                    print(f"[{VERSION}] Backoff sleep: {sleep_s:.2f}s", flush=True)
                    time.sleep(sleep_s)
                    continue
                return FetchResult(ok=False, status=200, attempts=attempt, reason=last_reason)

            m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
            title = _clean_title(m.group(1)) if m else None

            print(f"[{VERSION}] -> HTTP 200 OK", flush=True)
            if title:
                print(f"[{VERSION}] Title: {title}", flush=True)

            return FetchResult(ok=True, status=200, attempts=attempt, reason="ok", title=title)

        except requests.RequestException as e:
            last_reason = f"network_error: {type(e).__name__}"
            print(f"[{VERSION}] -> Network error: {e}", flush=True)
            if attempt < max_attempts:
                base = delays[min(attempt - 1, len(delays) - 1)]
                sleep_s = base + random.uniform(0.0, 0.7)
                print(f"[{VERSION}] Backoff sleep: {sleep_s:.2f}s", flush=True)
                time.sleep(sleep_s)
                continue
            return FetchResult(ok=False, status=last_status, attempts=attempt, reason=last_reason)

    return FetchResult(ok=False, status=last_status, attempts=max_attempts, reason=last_reason)


def write_report(url: str, res: FetchResult, path: str = "yahoo_live_probe_report.md") -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    lines = []
    lines.append(f"# Yahoo Live Probe Report ({VERSION})")
    lines.append("")
    lines.append(f"- **UTC time:** {ts}")
    lines.append(f"- **URL:** {url}")
    lines.append(f"- **Final status:** {res.status}")
    lines.append(f"- **Attempts:** {res.attempts}")
    lines.append(f"- **Result:** {'OK' if res.ok else 'FAILED'}")
    lines.append(f"- **Reason:** {res.reason}")
    if res.title:
        lines.append(f"- **Title:** {res.title}")
    lines.append("")
    lines.append("## Interpretation for #1 report macro block")
    if res.reason == "blocked_or_ratelimited":
        lines.append("- Yahoo Live: **forrás nem elérhető (429/403 / challenge / rate limit)**")
        lines.append("- Kötelező fallback: Reuters/MarketWatch/AP összefoglaló (külön modulból).")
    elif res.ok:
        lines.append("- Yahoo Live: **elérve**, a macro blokk megírható a live stream összefoglalójából.")
    else:
        lines.append("- Yahoo Live: **nem elérhető** (nem 200 / hálózati hiba).")
        lines.append("- Kötelező fallback: Reuters/MarketWatch/AP összefoglaló (külön modulból).")
    lines.append("")

    Path(path).write_text("\n".join(lines), encoding="utf-8")
    print(f"[{VERSION}] Wrote report: {path}", flush=True)


def main() -> int:
    url = os.getenv("YAHOO_LIVE_URL", DEFAULT_URL).strip()
    max_attempts = int(os.getenv("MAX_ATTEMPTS", "4"))
    timeout_secs = int(os.getenv("TIMEOUT_SECS", "20"))

    res = fetch_with_backoff(url, max_attempts=max_attempts, timeout_secs=timeout_secs)
    write_report(url, res)

    # Exit code for CI visibility:
    # - 0 if OK
    # - 2 if blocked/rate-limited
    # - 1 otherwise
    if res.ok:
        return 0
    if res.reason == "blocked_or_ratelimited":
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
