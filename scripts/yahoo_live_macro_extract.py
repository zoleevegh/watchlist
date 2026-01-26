#!/usr/bin/env python3
"""
scripts/yahoo_live_macro_extract.py — Yahoo Live "macro" kivonat #1 jelentéshez (v1.0.0)

Cél:
- Yahoo Live /news/live URL-t letölteni "böngésző-szerű" headerekkel
- 429/403/challenge esetén 2–3 retry backoff-fal (1s, 3s, 9s) + jitter
- Ha elérhető (HTTP 200 + nem consent/challenge), akkor:
  - Title + "market-moving" bulletok: Fed / rates / Powell / inflation / Treasury / White House / earnings / geopolitics
  - Kimenet: macro_yahoo_live.md (Markdown blokk, ami beilleszthető a #1 jelentés elejére)

Megjegyzés:
- Yahoo Live gyakran dinamikus; a teljes szöveg nem mindig van szerver-oldalon HTML-ben.
- Ez a script több stratégiát próbál:
  1) <title> + meta/ld+json headline
  2) HTML-ből "látható" szöveg kinyerés (durva strip) + kulcsszavas mondat-válogatás
  3) Ha található nagy JSON blob (root.App.main / __NEXT_DATA__), abból snippet vadászat (best-effort)

Verziófegyelem: minden módosításnál verziószámot folytatólagosan növelni kell.
"""

from __future__ import annotations

import json
import os
import random
import re
import time
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Optional, List, Tuple

import requests

VERSION = "v1.0.0"

DEFAULT_URL = (
    "https://finance.yahoo.com/news/live/"
    "stock-market-today-dow-sp-500-nasdaq-falter-with-big-week-of-big-tech-earnings-fed-meeting-ahead-110341565.html"
)

OUT_MD = "macro_yahoo_live.md"


@dataclass
class Fetch:
    ok: bool
    status: Optional[int]
    html: str
    reason: str
    attempts: int


def _headers() -> dict:
    return {
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


def fetch_with_backoff(url: str, max_attempts: int = 4, timeout_secs: int = 25) -> Fetch:
    delays = [1, 3, 9, 15]
    last_status: Optional[int] = None
    last_reason = "unknown"
    last_html = ""

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[{VERSION}] Attempt {attempt}/{max_attempts}: GET {url}", flush=True)
            r = requests.get(url, headers=_headers(), timeout=timeout_secs, allow_redirects=True)
            last_status = r.status_code
            last_html = r.text or ""

            if r.status_code in (429, 403):
                last_reason = "blocked_or_ratelimited"
                if attempt < max_attempts:
                    base = delays[min(attempt - 1, len(delays) - 1)]
                    time.sleep(base + random.uniform(0.0, 0.7))
                    continue
                return Fetch(False, last_status, last_html, last_reason, attempt)

            if r.status_code != 200:
                last_reason = "non_200"
                if attempt < max_attempts:
                    time.sleep(0.7 + random.uniform(0.0, 0.5))
                    continue
                return Fetch(False, last_status, last_html, last_reason, attempt)

            # Soft validation: too short or looks like consent/challenge
            low = (last_html[:20000] or "").lower()
            if len(last_html) < 2000:
                last_reason = "too_short_html"
            elif "consent.yahoo.com" in low or "consent" in low and "privacy" in low and "yahoo" in low:
                last_reason = "consent_page"
            elif "captcha" in low or "are you a robot" in low or "challenge" in low:
                last_reason = "challenge_page"
            else:
                return Fetch(True, 200, last_html, "ok", attempt)

            if attempt < max_attempts:
                base = delays[min(attempt - 1, len(delays) - 1)]
                time.sleep(base + random.uniform(0.0, 0.7))
                continue
            return Fetch(False, 200, last_html, last_reason, attempt)

        except requests.RequestException as e:
            last_reason = f"network_error: {type(e).__name__}"
            if attempt < max_attempts:
                base = delays[min(attempt - 1, len(delays) - 1)]
                time.sleep(base + random.uniform(0.0, 0.7))
                continue
            return Fetch(False, last_status, last_html, last_reason, attempt)

    return Fetch(False, last_status, last_html, last_reason, max_attempts)


def extract_title(html: str) -> Optional[str]:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    t = unescape(re.sub(r"\s+", " ", m.group(1)).strip())
    return t[:200] if t else None


def extract_ldjson_headlines(html: str) -> List[str]:
    out: List[str] = []
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                         html, flags=re.IGNORECASE | re.DOTALL):
        blob = m.group(1).strip()
        if not blob:
            continue
        try:
            data = json.loads(blob)
        except Exception:
            continue
        # common fields
        for key in ("headline", "name", "description"):
            v = data.get(key) if isinstance(data, dict) else None
            if isinstance(v, str) and v.strip():
                s = re.sub(r"\s+", " ", v).strip()
                if s and s not in out:
                    out.append(s[:240])
    return out[:6]


def strip_visible_text(html: str) -> str:
    # Drop scripts/styles quickly
    html = re.sub(r"<script\b[^>]*>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r"<style\b[^>]*>.*?</style>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    # Remove tags
    txt = re.sub(r"<[^>]+>", " ", html)
    txt = unescape(txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def pick_keyword_sentences(text: str, max_items: int = 8) -> List[str]:
    # naive sentence split
    sents = re.split(r"(?<=[\.\!\?])\s+", text)
    keywords = [
        "federal reserve", "fed", "powell", "rates", "rate", "inflation", "treasury",
        "bond", "yields", "white house", "doj", "meeting", "fomc", "cpi", "pce",
        "earnings", "guidance", "nasdaq", "s&p", "dow", "futures",
    ]
    scored: List[Tuple[int, str]] = []
    for s in sents:
        s2 = s.strip()
        if len(s2) < 60 or len(s2) > 280:
            continue
        low = s2.lower()
        score = sum(1 for k in keywords if k in low)
        if score >= 2:  # require some signal
            scored.append((score, s2))
    scored.sort(key=lambda x: (-x[0], len(x[1])))
    out: List[str] = []
    for _, s in scored:
        if s not in out:
            out.append(s)
        if len(out) >= max_items:
            break
    return out


def try_extract_big_json_snippets(html: str) -> List[str]:
    # Best-effort: look for large JSON-like assignments and pull short keyword-containing fragments
    patterns = [
        r"root\.App\.main\s*=\s*({.*?});\s*\}\(this\)",   # seen on some Yahoo pages
        r"__NEXT_DATA__\s*=\s*({.*?})</script>",
    ]
    snippets: List[str] = []
    for pat in patterns:
        m = re.search(pat, html, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            continue
        blob = m.group(1)
        if not blob or len(blob) < 5000:
            continue
        # Don't fully parse (can be huge); just scan for fragments
        low = blob.lower()
        for kw in ("federal reserve", "powell", "rates", "inflation", "treasury", "white house"):
            if kw in low:
                # take ~240 chars around first occurrence
                idx = low.find(kw)
                frag = blob[max(0, idx-120): idx+240]
                frag = re.sub(r"\\u002F", "/", frag)
                frag = re.sub(r"\\n", " ", frag)
                frag = re.sub(r"\s+", " ", frag).strip()
                if frag and frag not in snippets:
                    snippets.append(frag[:300])
        if snippets:
            break
    return snippets[:6]


def write_macro_md(url: str, fetch: Fetch) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    lines: List[str] = []
    lines.append(f"## Makró/FED – Yahoo Live (probe+extract) [{VERSION}]")
    lines.append(f"- UTC: {ts}")
    lines.append(f"- URL: {url}")
    lines.append(f"- Status: {fetch.status} | attempts: {fetch.attempts} | reason: {fetch.reason}")
    lines.append("")

    if not fetch.ok:
        lines.append("**Yahoo Live: nem elérhető / nem használható ebben a futásban.**")
        lines.append("Kötelező fallback: Reuters + AP + (ha elérhető) Bloomberg summary / MarketWatch.")
        Path(OUT_MD).write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"[{VERSION}] Wrote: {OUT_MD}", flush=True)
        return

    html = fetch.html
    title = extract_title(html)
    if title:
        lines.append(f"**Cím:** {title}")
        lines.append("")

    bullets: List[str] = []
    bullets += extract_ldjson_headlines(html)

    text = strip_visible_text(html)
    bullets += pick_keyword_sentences(text)

    if len(bullets) < 4:
        bullets += try_extract_big_json_snippets(html)

    # de-dupe + limit
    clean: List[str] = []
    for b in bullets:
        b2 = re.sub(r"\s+", " ", b).strip()
        if not b2:
            continue
        if b2 not in clean:
            clean.append(b2)
        if len(clean) >= 10:
            break

    if clean:
        lines.append("**Kivonat (best-effort, kulcsszavas):**")
        for b in clean[:10]:
            lines.append(f"- {b}")
    else:
        lines.append("**Kivonat:** (nem találtam stabilan kinyerhető, kulcsszavas szöveget a HTML-ből)")
        lines.append("- Javaslat: fallback wire-ekre (Reuters/AP) ebben a futásban.")

    lines.append("")
    Path(OUT_MD).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[{VERSION}] Wrote: {OUT_MD}", flush=True)


def main() -> int:
    url = os.getenv("YAHOO_LIVE_URL", DEFAULT_URL).strip()
    max_attempts = int(os.getenv("MAX_ATTEMPTS", "4"))
    timeout_secs = int(os.getenv("TIMEOUT_SECS", "25"))

    f = fetch_with_backoff(url, max_attempts=max_attempts, timeout_secs=timeout_secs)
    write_macro_md(url, f)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
