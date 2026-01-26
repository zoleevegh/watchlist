#!/usr/bin/env python3
"""
scripts/yahoo_live_macro_extract.py — Yahoo Live "macro" kivonat #1 jelentéshez (v1.0.2)

Állapot a gyakorlatban:
- Yahoo Live néha 200 OK valódi tartalom, néha 200-as EU consent gate, néha 429/403 challenge.
- V1.0.2 cél: consent gate esetén több automatikus kerülőút, és MINDEN kísérlet naplózása a kimenetben.

Változás v1.0.2:
1) Consent/challenge esetén próbálja az AMPHTML variánst:
   finance.yahoo.com/news/...  -> finance.yahoo.com/amphtml/news/...
2) Consent/challenge esetén text-proxy fallback (r.jina.ai) mind az eredeti, mind az amphtml URL-re.
3) A macro_yahoo_live.md tartalmazza:
   - melyik útvonalon jutottunk adathoz (direct/amp/jina),
   - ha nem sikerül: miért és a proxy státuszát/hosszát.

Kimenet: macro_yahoo_live.md
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
from typing import Optional, List, Tuple, Dict

import requests

VERSION = "v1.0.2"

DEFAULT_URL = (
    "https://finance.yahoo.com/news/live/"
    "stock-market-today-dow-sp-500-nasdaq-falter-with-big-week-of-big-tech-earnings-fed-meeting-ahead-110341565.html"
)

OUT_MD = "macro_yahoo_live.md"


@dataclass
class Fetch:
    ok: bool
    status: Optional[int]
    body: str
    reason: str
    attempts: int
    final_url: Optional[str] = None
    via: str = "direct"   # direct | amp | jina
    bytes_len: int = 0


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


def to_amphtml(url: str) -> str:
    # Best-effort: insert /amphtml after domain
    return url.replace("finance.yahoo.com/", "finance.yahoo.com/amphtml/", 1)


def _classify_body(body: str) -> str:
    low = (body[:60000] or "").lower()
    if len(body) < 2000:
        return "too_short"
    if "consent.yahoo.com" in low or ("consent" in low and "privacy" in low and "yahoo" in low):
        return "consent"
    if "captcha" in low or "are you a robot" in low or "challenge" in low or "unusual traffic" in low:
        return "challenge"
    return "ok"


def fetch_with_backoff(url: str, max_attempts: int = 4, timeout_secs: int = 25, via: str = "direct") -> Fetch:
    delays = [1, 3, 9, 15]
    last_status: Optional[int] = None
    last_reason = "unknown"
    last_body = ""
    last_final_url: Optional[str] = None

    sess = requests.Session()

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[{VERSION}] {via} attempt {attempt}/{max_attempts}: GET {url}", flush=True)
            r = sess.get(url, headers=_headers(), timeout=timeout_secs, allow_redirects=True)
            last_status = r.status_code
            last_final_url = str(r.url) if getattr(r, "url", None) else None
            last_body = r.text or ""

            if r.status_code in (429, 403):
                last_reason = "blocked_or_ratelimited"
                if attempt < max_attempts:
                    base = delays[min(attempt - 1, len(delays) - 1)]
                    time.sleep(base + random.uniform(0.0, 0.7))
                    continue
                return Fetch(False, last_status, last_body, last_reason, attempt, last_final_url, via, len(last_body))

            if r.status_code != 200:
                last_reason = "non_200"
                if attempt < max_attempts:
                    time.sleep(0.7 + random.uniform(0.0, 0.5))
                    continue
                return Fetch(False, last_status, last_body, last_reason, attempt, last_final_url, via, len(last_body))

            cls = _classify_body(last_body)
            if cls == "ok":
                return Fetch(True, 200, last_body, "ok", attempt, last_final_url, via, len(last_body))

            last_reason = cls
            if attempt < max_attempts:
                base = delays[min(attempt - 1, len(delays) - 1)]
                time.sleep(base + random.uniform(0.0, 0.7))
                continue
            return Fetch(False, 200, last_body, last_reason, attempt, last_final_url, via, len(last_body))

        except requests.RequestException as e:
            last_reason = f"network_error: {type(e).__name__}"
            if attempt < max_attempts:
                base = delays[min(attempt - 1, len(delays) - 1)]
                time.sleep(base + random.uniform(0.0, 0.7))
                continue
            return Fetch(False, last_status, last_body, last_reason, attempt, last_final_url, via, len(last_body))

    return Fetch(False, last_status, last_body, last_reason, max_attempts, last_final_url, via, len(last_body))


def fetch_jina_text(url: str, timeout_secs: int = 25, via: str = "jina") -> Fetch:
    proxy_url = f"https://r.jina.ai/{url}"
    try:
        print(f"[{VERSION}] {via} GET {proxy_url}", flush=True)
        r = requests.get(proxy_url, headers=_headers(), timeout=timeout_secs, allow_redirects=True)
        body = r.text or ""
        cls = _classify_body(body)
        if r.status_code == 200 and cls == "ok":
            return Fetch(True, 200, body, "ok", 1, proxy_url, via, len(body))
        reason = "jina_" + (cls if r.status_code == 200 else f"status_{r.status_code}")
        return Fetch(False, r.status_code, body, reason, 1, proxy_url, via, len(body))
    except requests.RequestException as e:
        return Fetch(False, None, "", f"jina_network_error: {type(e).__name__}", 1, proxy_url, via, 0)


def extract_title(body: str) -> Optional[str]:
    m = re.search(r"<title[^>]*>(.*?)</title>", body, flags=re.IGNORECASE | re.DOTALL)
    if m:
        t = unescape(re.sub(r"\s+", " ", m.group(1)).strip())
        return t[:200] if t else None
    m2 = re.search(r"^\s*#\s+(.+)$", body, flags=re.MULTILINE)
    if m2:
        t = re.sub(r"\s+", " ", m2.group(1)).strip()
        return t[:200] if t else None
    return None


def extract_ldjson_headlines(body: str) -> List[str]:
    out: List[str] = []
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                         body, flags=re.IGNORECASE | re.DOTALL):
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


def strip_visible_text(body: str) -> str:
    if "<" not in body[:5000]:
        txt = unescape(body)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt
    body = re.sub(r"<script\b[^>]*>.*?</script>", " ", body, flags=re.IGNORECASE | re.DOTALL)
    body = re.sub(r"<style\b[^>]*>.*?</style>", " ", body, flags=re.IGNORECASE | re.DOTALL)
    txt = re.sub(r"<[^>]+>", " ", body)
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


def best_effort_extract(fetch: Fetch) -> List[str]:
    bullets: List[str] = []
    bullets += extract_ldjson_headlines(fetch.body)
    text = strip_visible_text(fetch.body)
    bullets += pick_keyword_sentences(text)
    # de-dupe
    clean: List[str] = []
    for b in bullets:
        b2 = re.sub(r"\s+", " ", b).strip()
        if not b2:
            continue
        if b2 not in clean:
            clean.append(b2)
        if len(clean) >= 10:
            break
    return clean


def write_macro_md(url: str, main_fetch: Fetch, attempts: List[Fetch]) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    lines: List[str] = []
    lines.append(f"## Makró/FED – Yahoo Live (probe+extract) [{VERSION}]")
    lines.append(f"- UTC: {ts}")
    lines.append(f"- URL: {url}")
    lines.append("")

    lines.append("### Kísérletek (debug)")
    for f in attempts:
        lines.append(
            f"- via={f.via} | status={f.status} | reason={f.reason} | bytes={f.bytes_len} | final={f.final_url}"
        )
    lines.append("")

    if not main_fetch.ok:
        lines.append("**Yahoo Live: nem elérhető / nem használható ebben a futásban.**")
        lines.append("Kötelező fallback: Reuters + AP + (ha elérhető) Bloomberg summary / MarketWatch.")
        Path(OUT_MD).write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"[{VERSION}] Wrote: {OUT_MD}", flush=True)
        return

    title = extract_title(main_fetch.body)
    if title:
        lines.append(f"**Cím:** {title}")
        lines.append("")
    lines.append(f"**Forrás útvonal:** {main_fetch.via} (final: {main_fetch.final_url})")
    lines.append("")

    bullets = best_effort_extract(main_fetch)
    if bullets:
        lines.append("**Kivonat (best-effort, kulcsszavas):**")
        for b in bullets[:10]:
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

    attempts: List[Fetch] = []

    # 1) direct
    f1 = fetch_with_backoff(url, max_attempts=max_attempts, timeout_secs=timeout_secs, via="direct")
    attempts.append(f1)
    if f1.ok:
        write_macro_md(url, f1, attempts)
        return 0

    # 2) amphtml (csak ha consent/challenge/blocked/too_short)
    if f1.reason in ("consent", "challenge", "blocked_or_ratelimited", "too_short"):
        amp = to_amphtml(url)
        f2 = fetch_with_backoff(amp, max_attempts=max_attempts, timeout_secs=timeout_secs, via="amp")
        attempts.append(f2)
        if f2.ok:
            write_macro_md(url, f2, attempts)
            return 0

        # 3) jina on original
        f3 = fetch_jina_text(url, timeout_secs=timeout_secs, via="jina")
        attempts.append(f3)
        if f3.ok:
            write_macro_md(url, f3, attempts)
            return 0

        # 4) jina on amphtml
        f4 = fetch_jina_text(amp, timeout_secs=timeout_secs, via="jina_amp")
        attempts.append(f4)
        if f4.ok:
            write_macro_md(url, f4, attempts)
            return 0

    # none succeeded
    write_macro_md(url, f1, attempts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
