#!/usr/bin/env python3
"""
scripts/yahoo_live_macro_extract.py — Yahoo Live "macro" kivonat #1 jelentéshez (v1.0.1)

Miért kell:
- Yahoo Live néha 200 OK-t ad, néha 200-as "consent" oldalt (EU/consent gate), néha 429/403 challenge-t.
- A cél nem az, hogy mindig Yahoo-ból írjunk, hanem hogy:
  (1) mindig próbáljuk meg,
  (2) ha consent/challenge -> legyen automatizált webes fallback, ami mégis kiad szöveget,
  (3) ha semmi sem megy -> jelzett fallback Reuters/AP/MarketWatch/Bloomberg.

Változás v1.0.1:
- Consent/challenge esetén automatikus fallback próba a "r.jina.ai" text-proxyval.
  Ez gyakran átadja a cikk/livestream szövegét szerver-oldali formában, consent gate nélkül.
- A kimenet továbbra is: macro_yahoo_live.md

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

VERSION = "v1.0.1"

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
    final_url: Optional[str] = None


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
        "Upgrade-Insecure-Requests": "1",
    }


def _classify_html(html: str) -> str:
    low = (html[:40000] or "").lower()
    if len(html) < 2000:
        return "too_short_html"
    # Yahoo consent gate signatures
    if "consent.yahoo.com" in low or ("consent" in low and "privacy" in low and "yahoo" in low):
        return "consent_page"
    # Generic bot/challenge hints
    if "captcha" in low or "are you a robot" in low or "challenge" in low or "unusual traffic" in low:
        return "challenge_page"
    return "ok"


def fetch_with_backoff(url: str, max_attempts: int = 4, timeout_secs: int = 25) -> Fetch:
    delays = [1, 3, 9, 15]
    last_status: Optional[int] = None
    last_reason = "unknown"
    last_html = ""
    last_final_url: Optional[str] = None

    sess = requests.Session()

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[{VERSION}] Attempt {attempt}/{max_attempts}: GET {url}", flush=True)
            r = sess.get(url, headers=_headers(), timeout=timeout_secs, allow_redirects=True)
            last_status = r.status_code
            last_final_url = str(r.url) if getattr(r, "url", None) else None
            last_html = r.text or ""

            if r.status_code in (429, 403):
                last_reason = "blocked_or_ratelimited"
                if attempt < max_attempts:
                    base = delays[min(attempt - 1, len(delays) - 1)]
                    time.sleep(base + random.uniform(0.0, 0.7))
                    continue
                return Fetch(False, last_status, last_html, last_reason, attempt, last_final_url)

            if r.status_code != 200:
                last_reason = "non_200"
                if attempt < max_attempts:
                    time.sleep(0.7 + random.uniform(0.0, 0.5))
                    continue
                return Fetch(False, last_status, last_html, last_reason, attempt, last_final_url)

            cls = _classify_html(last_html)
            if cls == "ok":
                return Fetch(True, 200, last_html, "ok", attempt, last_final_url)

            last_reason = cls
            if attempt < max_attempts:
                base = delays[min(attempt - 1, len(delays) - 1)]
                time.sleep(base + random.uniform(0.0, 0.7))
                continue
            return Fetch(False, 200, last_html, last_reason, attempt, last_final_url)

        except requests.RequestException as e:
            last_reason = f"network_error: {type(e).__name__}"
            if attempt < max_attempts:
                base = delays[min(attempt - 1, len(delays) - 1)]
                time.sleep(base + random.uniform(0.0, 0.7))
                continue
            return Fetch(False, last_status, last_html, last_reason, attempt, last_final_url)

    return Fetch(False, last_status, last_html, last_reason, max_attempts, last_final_url)


def fetch_jina_text(url: str, timeout_secs: int = 25) -> Fetch:
    # r.jina.ai returns text/markdown-ish content. Treat it as "html" for downstream stripping.
    proxy_url = f"https://r.jina.ai/{url}"
    try:
        print(f"[{VERSION}] Jina fallback: GET {proxy_url}", flush=True)
        r = requests.get(proxy_url, headers=_headers(), timeout=timeout_secs, allow_redirects=True)
        txt = r.text or ""
        if r.status_code == 200 and len(txt) > 1000:
            return Fetch(True, 200, txt, "ok_jina", 1, proxy_url)
        return Fetch(False, r.status_code, txt, "jina_failed", 1, proxy_url)
    except requests.RequestException as e:
        return Fetch(False, None, "", f"jina_network_error: {type(e).__name__}", 1, proxy_url)


def extract_title(html: str) -> Optional[str]:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if m:
        t = unescape(re.sub(r"\s+", " ", m.group(1)).strip())
        return t[:200] if t else None
    # Jina content might start with markdown headings
    m2 = re.search(r"^\s*#\s+(.+)$", html, flags=re.MULTILINE)
    if m2:
        t = re.sub(r"\s+", " ", m2.group(1)).strip()
        return t[:200] if t else None
    return None


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
        for key in ("headline", "name", "description"):
            v = data.get(key) if isinstance(data, dict) else None
            if isinstance(v, str) and v.strip():
                s = re.sub(r"\s+", " ", v).strip()
                if s and s not in out:
                    out.append(s[:240])
    return out[:6]


def strip_visible_text(html: str) -> str:
    # If it's already plain text (Jina), don't over-strip.
    if "<" not in html[:5000]:
        txt = unescape(html)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt
    html = re.sub(r"<script\b[^>]*>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r"<style\b[^>]*>.*?</style>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    txt = re.sub(r"<[^>]+>", " ", html)
    txt = unescape(txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def pick_keyword_sentences(text: str, max_items: int = 10) -> List[str]:
    sents = re.split(r"(?<=[\.\!\?])\s+", text)
    keywords = [
        "federal reserve", "fed", "powell", "rates", "rate cut", "rate hike",
        "inflation", "treasury", "bond", "yields", "white house", "doj",
        "meeting", "fomc", "cpi", "pce", "jobs", "payrolls",
        "earnings", "guidance", "nasdaq", "s&p", "dow", "futures",
    ]
    scored: List[Tuple[int, str]] = []
    for s in sents:
        s2 = s.strip()
        if len(s2) < 70 or len(s2) > 320:
            continue
        low = s2.lower()
        score = sum(1 for k in keywords if k in low)
        if score >= 2:
            scored.append((score, s2))
    scored.sort(key=lambda x: (-x[0], len(x[1])))
    out: List[str] = []
    for _, s in scored:
        if s not in out:
            out.append(s)
        if len(out) >= max_items:
            break
    return out


def write_macro_md(url: str, fetch: Fetch) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    lines: List[str] = []
    lines.append(f"## Makró/FED – Yahoo Live (probe+extract) [{VERSION}]")
    lines.append(f"- UTC: {ts}")
    lines.append(f"- URL: {url}")
    if fetch.final_url:
        lines.append(f"- Final URL: {fetch.final_url}")
    lines.append(f"- Status: {fetch.status} | attempts: {fetch.attempts} | reason: {fetch.reason}")
    lines.append("")

    if not fetch.ok:
        lines.append("**Yahoo Live: nem elérhető / nem használható ebben a futásban.**")
        lines.append("Kötelező fallback: Reuters + AP + (ha elérhető) Bloomberg summary / MarketWatch.")
        Path(OUT_MD).write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"[{VERSION}] Wrote: {OUT_MD}", flush=True)
        return

    title = extract_title(fetch.html)
    if title:
        lines.append(f"**Cím:** {title}")
        lines.append("")

    bullets: List[str] = []
    # ld+json only in real HTML
    bullets += extract_ldjson_headlines(fetch.html)

    text = strip_visible_text(fetch.html)
    bullets += pick_keyword_sentences(text)

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
        lines.append("**Kivonat:** (nem találtam stabilan kinyerhető, kulcsszavas szöveget)")
        lines.append("- Javaslat: fallback wire-ekre (Reuters/AP) ebben a futásban.")

    lines.append("")
    Path(OUT_MD).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[{VERSION}] Wrote: {OUT_MD}", flush=True)


def main() -> int:
    url = os.getenv("YAHOO_LIVE_URL", DEFAULT_URL).strip()
    max_attempts = int(os.getenv("MAX_ATTEMPTS", "4"))
    timeout_secs = int(os.getenv("TIMEOUT_SECS", "25"))

    f = fetch_with_backoff(url, max_attempts=max_attempts, timeout_secs=timeout_secs)

    # Consent/challenge esetén automatikus text-proxy fallback
    if (not f.ok) and f.reason in ("consent_page", "challenge_page", "too_short_html", "blocked_or_ratelimited"):
        jf = fetch_jina_text(url, timeout_secs=timeout_secs)
        if jf.ok:
            # override with jina result
            f = jf

    write_macro_md(url, f)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
