#!/usr/bin/env python3
# analyst_marketbeat.py — v0.3.27-marketbeat-date-range-hu-2026-02-02
# MarketBeat FREE analyst feed collector (CI-friendly).
#
# Ima (v0.3.15): bocsáss meg Uram, hogy megint bool-t hívtam függvényként.
# Ima (v0.3.15): adj erőt, hogy egyetlen zárójelet se tegyek oda, ahová nem kell.
# Changelog (v0.3.6):
# - FIX: ratings pages have no explicit date column; previous logic dropped all rows.
# - Parse tickers from ratings table (data-clean / stock URL) and treat event date as "run day (UTC)".
# - Better empty-state messaging: "no fresh" only when source OK; otherwise "source blocked/unavailable" with HTTP codes.
#
# Changelog (v0.3.13):
# - FIX: MarketBeat Cloudflare 'challenge' gyakran HTTP 200-zal jön; ezt detektáljuk és Jina proxy-val újrapróbáljuk.
# - Ha a Jina is challenge-t ad, akkor korrekt 'blocked/challenge' státuszt írunk, nem félrevezető üres listát.
#
# Ima (2 sor):
# Bocsáss meg Uram, mert balfék voltam, és 200-as challenget sikernek hittem.
# Adj nekünk tiszta HTML-t, hogy a riport ne legyen N/A. Ámen.
#
# Ima (v0.3.23): bocsáss meg Uram, hogy a MASTER tickert az 'All Ratings' oldalon hagytam, miközben csak upgrade/downgrade-ot néztem.
# Ima (v0.3.23): adj egy extra forrást (ratings/us), hogy a QCOM se tűnjön el. Ámen.
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import io
import json
import hashlib
import random
import re
import html
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, asdict
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Dict, List, Optional, Tuple


VERSION = "v0.3.27-marketbeat-date-range-hu-2026-02-02"
BASE = "https://www.marketbeat.com"

# Magyar megnevezések a reporthoz
ACTION_HU = {
    "upgrade": "felminősítés",
    "downgrade": "leminősítés",
    "pt_change": "célár módosítás",
}

RATING_HU = {
    "Strong Buy": "Erős vétel",
    "Buy": "Vétel",
    "Outperform": "Felülteljesítés",
    "Overweight": "Felülsúlyozás",
    "Accumulate": "Gyűjtés",
    "Positive": "Pozitív",
    "Moderate Buy": "Mérsékelt vétel",
    "Neutral": "Semleges",
    "Hold": "Tartás",
    "Equal-Weight": "Semleges súly",
    "Market Perform": "Piaci teljesítés",
    "In-Line": "Piaccal megegyező",
    "Peer Perform": "Szektortársakkal azonos",
    "Underperform": "Alulteljesítés",
    "Underweight": "Alulsúlyozás",
    "Reduce": "Csökkentés",
    "Sell": "Eladás",
    "Strong Sell": "Erős eladás",
}

# MarketBeat "Today's" ratings lists (FREE).
RATINGS_SOURCES: List[Tuple[str, str]] = [
    ("all", "/ratings/us/"),
    ("upgrade", "/ratings/upgrades/"),
    ("downgrade", "/ratings/downgrades/"),
    ("pt_change", "/ratings/pricetargetchanges/"),
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

@dataclass
class AnalystEvent:
    ticker: str
    date: str  # ISO date YYYY-MM-DD (first seen date, cache-based)
    firm: str
    action: str
    current_price: Optional[float]
    rating_from: Optional[str]
    rating_to: Optional[str]
    pt_from: Optional[float]
    pt_to: Optional[float]
    currency: str
    source: str
def _event_key(e: "AnalystEvent") -> str:
    """
    Stable key for de-dup across runs.
    Note: action is already normalized HU text; rating strings are normalized too.
    """
    def f(x):
        if x is None:
            return ""
        if isinstance(x, float):
            return f"{x:.4f}"
        return str(x).strip()

    raw = "|".join([
        e.ticker.upper().strip(),
        f(e.firm),
        f(e.action),
        f(e.rating_from),
        f(e.rating_to),
        f(e.pt_from),
        f(e.pt_to),
    ])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def load_seen_cache(path: Path) -> Dict[str, str]:
    """
    Returns mapping: event_key -> first_seen_date (YYYY-MM-DD).
    """
    try:
        if path.exists():
            obj = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                # keep only sane ISO dates
                out: Dict[str, str] = {}
                for k, v in obj.items():
                    if isinstance(k, str) and isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", v):
                        out[k] = v
                return out
    except Exception:
        pass
    return {}


def save_seen_cache(path: Path, cache: Dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def apply_seen_dates_and_filter(
    events: List["AnalystEvent"],
    days: int,
    cache: Dict[str, str],
    today_iso: str,
) -> List["AnalystEvent"]:
    """Windowing + persistence.

    - `e.date` is the MarketBeat *reporting date* (YYYY-MM-DD) coming from the scraped page.
      We DO NOT overwrite it.
    - `cache[key]` stores the *first-seen date* (YYYY-MM-DD) so events can remain visible
      even if they disappear from MarketBeat's "latest" lists.
    - Inclusion rules:
        1) report_date must be within the last N calendar days
        2) first_seen must also be within the last N calendar days (so the window expires cleanly)
    """
    if days < 1:
        return []

    today = dt.date.fromisoformat(today_iso)
    since = today - dt.timedelta(days=days - 1)

    out: List[AnalystEvent] = []
    for e in events:
        # parse report date (from page)
        try:
            report_dt = dt.date.fromisoformat(e.date)
        except Exception:
            report_dt = today

        # hard filter by report date (prevents accidental backfills from leaking in)
        if report_dt < since or report_dt > today:
            continue

        k = _event_key(e)
        first = cache.get(k)
        if not first:
            cache[k] = today_iso
            first = today_iso

        try:
            first_dt = dt.date.fromisoformat(first)
        except Exception:
            first_dt = today

        if first_dt < since:
            continue

        out.append(e)

    return out
def dedup_merge_events(events: List[AnalystEvent]) -> List[AnalystEvent]:
    """De-duplicate/merge duplicates across MarketBeat lists.

    The same underlying change may appear on multiple pages:
    - /ratings/us/ (generic "Elemzői frissítés")
    - /ratings/upgrades/ or /ratings/downgrades/
    - /ratings/pricetargetchanges/

    We keep ONE row per (ticker, date, brokerage) and, when multiple action
    labels exist for the same key, we merge them into a single action string.
    """

    def _source_priority(url: str) -> int:
        u = (url or '').lower()
        if 'pricetargetchanges' in u:
            return 3
        if '/upgrades' in u or '/downgrades' in u or '/initiations' in u:
            return 4
        if '/ratings/us' in u:
            return 1
        return 0

    merged = {}  # key -> dict

    for ev in events:
        key = (ev.ticker, ev.asof_date, ev.brokerage)
        cur = merged.get(key)
        if cur is None:
            merged[key] = {
                'ticker': ev.ticker,
                'asof_date': ev.asof_date,
                'brokerage': ev.brokerage,
                'analyst': ev.analyst,
                'actions': {ev.action} if ev.action else set(),
                'rating_from': ev.rating_from,
                'rating_to': ev.rating_to,
                'pt_from': ev.pt_from,
                'pt_to': ev.pt_to,
                'current_price': ev.current_price,
                'currency': ev.currency,
                'source': ev.source,
                'source_prio': _source_priority(ev.source),
            }
            continue

        # merge actions
        if ev.action:
            cur['actions'].add(ev.action)

        # keep the most informative fields
        if not cur.get('analyst') and ev.analyst:
            cur['analyst'] = ev.analyst
        if cur.get('rating_from') in (None, '') and ev.rating_from:
            cur['rating_from'] = ev.rating_from
        if cur.get('rating_to') in (None, '') and ev.rating_to:
            cur['rating_to'] = ev.rating_to
        if cur.get('pt_from') is None and ev.pt_from is not None:
            cur['pt_from'] = ev.pt_from
        if cur.get('pt_to') is None and ev.pt_to is not None:
            cur['pt_to'] = ev.pt_to
        if cur.get('current_price') is None and ev.current_price is not None:
            cur['current_price'] = ev.current_price
        if not cur.get('currency') and ev.currency:
            cur['currency'] = ev.currency

        # keep the best source URL
        pr = _source_priority(ev.source)
        if pr > cur.get('source_prio', 0):
            cur['source'] = ev.source
            cur['source_prio'] = pr

    out: List[AnalystEvent] = []
    for key, cur in merged.items():
        actions = set(a for a in cur['actions'] if a)
        # If we have a specific action (upgrade/downgrade/pt change), drop the generic one.
        if len(actions) > 1 and 'Elemzői frissítés' in actions:
            actions.remove('Elemzői frissítés')
        action = ' + '.join(sorted(actions)) if actions else 'Elemzői frissítés'

        out.append(AnalystEvent(
            ticker=cur['ticker'],
            asof_date=cur['asof_date'],
            action=action,
            brokerage=cur['brokerage'],
            analyst=cur['analyst'],
            rating_from=cur['rating_from'],
            rating_to=cur['rating_to'],
            pt_from=cur['pt_from'],
            pt_to=cur['pt_to'],
            current_price=cur['current_price'],
            currency=cur['currency'] or 'USD',
            source=cur['source'],
        ))

    # stable order for report
    out.sort(key=lambda e: (e.ticker, e.asof_date, e.brokerage, e.action))
    return out

def _log(msg: str) -> None:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now} UTC] {msg}", flush=True)


def _make_opener() -> urllib.request.OpenerDirector:
    cj = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    return opener


def _decode_body(resp: urllib.response.addinfourl, raw: bytes) -> str:
    enc = resp.headers.get("Content-Encoding", "").lower()
    if "gzip" in enc:
        try:
            raw = gzip.decompress(raw)
        except Exception:
            # sometimes servers lie; best-effort
            pass
    # marketbeat is utf-8
    return raw.decode("utf-8", errors="replace")


def _is_challenge_page(html_text: str) -> bool:
    """Detect Cloudflare / bot protection pages that may still return HTTP 200."""
    if not html_text:
        return False
    h = html_text.lower()
    # Common Cloudflare / bot-defense markers
    markers = [
        # Strong bot-defense markers (avoid generic 'captcha' which appears on normal pages as reCAPTCHA widgets)
        "cf-challenge", "cloudflare", "attention required", "checking your browser",
        "/cdn-cgi/challenge", "/cdn-cgi/", "verify you are human",
        "just a moment", "browser verification", "cf-turnstile", "cf_chl_",
    ]
    return any(x in h for x in markers)



def _jina_url(url: str) -> str:
    # r.jina.ai can proxy-render HTML in CI environments.
    # Format: https://r.jina.ai/http(s)://example.com/path
    if url.startswith("https://"):
        return "https://r.jina.ai/https://" + url[len("https://") :]
    if url.startswith("http://"):
        return "https://r.jina.ai/http://" + url[len("http://") :]
    return "https://r.jina.ai/https://" + url


def _http_get(
    opener: urllib.request.OpenerDirector,
    url: str,
    timeout: int,
    debug_dir: Optional[Path],
    tag: str,
    *,
    allow_jina_fallback: bool = True,
    max_tries: int = 3,
) -> Tuple[int, str]:
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip",
        "Connection": "close",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    }

    last_status = 0
    last_text = ""
    for attempt in range(1, max_tries + 1):
        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with opener.open(req, timeout=timeout) as resp:
                raw = resp.read()
                status = getattr(resp, "status", 200)
                text = _decode_body(resp, raw)
                last_status, last_text = status, text
        except urllib.error.HTTPError as e:
            status = int(getattr(e, "code", 0) or 0)
            try:
                raw = e.read()
            except Exception:
                raw = b""
            text = raw.decode("utf-8", errors="replace") if raw else ""
            last_status, last_text = status, text
        except Exception as e:
            last_status, last_text = 0, f"EXC: {type(e).__name__}: {e}"

        if debug_dir:
            debug_dir.mkdir(parents=True, exist_ok=True)
            (debug_dir / f"{tag}_direct_{attempt}.status.txt").write_text(str(last_status), encoding="utf-8")
            (debug_dir / f"{tag}_direct_{attempt}.html").write_text(last_text, encoding="utf-8")

        # Success (but MarketBeat may return Cloudflare 'challenge' with HTTP 200)
        if 200 <= last_status < 300:
            if allow_jina_fallback and _is_challenge_page(last_text):
                # Try Jina-rendered proxy once to bypass 200-challenge.
                jurl = _jina_url(url)
                try:
                    req = urllib.request.Request(
                        jurl,
                        headers={"User-Agent": UA, "Accept-Encoding": "gzip", "Accept": "text/html,*/*"},
                        method="GET",
                    )
                    with opener.open(req, timeout=timeout) as resp:
                        raw = resp.read()
                        status = int(getattr(resp, "status", 200))
                        text = _decode_body(resp, raw)
                        if debug_dir:
                            debug_dir.mkdir(parents=True, exist_ok=True)
                            (debug_dir / f"{tag}_jina_on_200.status.txt").write_text(str(status), encoding="utf-8")
                            (debug_dir / f"{tag}_jina_on_200.html").write_text(text, encoding="utf-8")
                        if 200 <= status < 300 and not _is_challenge_page(text):
                            return status, text
                except Exception:
                    pass
            return last_status, last_text

        # Retry on 403/429/5xx with backoff+jitter
        if last_status in (403, 429) or last_status >= 500 or last_status == 0:
            time.sleep(0.8 + random.random() * 0.9)
            continue

        # Other 4xx: don't spin
        break

    # Jina fallback if still blocked and enabled
    if allow_jina_fallback and last_status in (0, 403, 429):
        jurl = _jina_url(url)
        try:
            req = urllib.request.Request(jurl, headers={"User-Agent": UA, "Accept-Encoding": "gzip"}, method="GET")
            with opener.open(req, timeout=timeout) as resp:
                raw = resp.read()
                status = getattr(resp, "status", 200)
                text = _decode_body(resp, raw)
                if debug_dir:
                    (debug_dir / f"{tag}_jina.status.txt").write_text(str(status), encoding="utf-8")
                    (debug_dir / f"{tag}_jina.html").write_text(text, encoding="utf-8")
                return int(status), text
        except Exception as e:
            if debug_dir:
                (debug_dir / f"{tag}_jina.status.txt").write_text("0", encoding="utf-8")
                (debug_dir / f"{tag}_jina.html").write_text(f"EXC: {type(e).__name__}: {e}", encoding="utf-8")
            return 0, f"EXC: {type(e).__name__}: {e}"

    return last_status, last_text


def _warmup_session(opener: urllib.request.OpenerDirector, timeout: int, debug_dir: Optional[Path]) -> int:
    status, _ = _http_get(opener, BASE + "/", timeout, debug_dir, "warmup_home", allow_jina_fallback=False, max_tries=2)
    return status


# Back-compat alias (in case older main() calls warmup_session)
def warmup_session(opener: urllib.request.OpenerDirector, timeout: int, debug_dir: Optional[Path]) -> int:
    return _warmup_session(opener, timeout, debug_dir)


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()



def _clean_firm(s: str) -> str:
    """Remove MarketBeat paywall boilerplate that sometimes gets injected into the firm cell."""
    s = (s or "").strip()
    s = re.sub(r"\s+Subscribe to MarketBeat.*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s+Visit MarketBeat.*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s+\|\s*MarketBeat.*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def _parse_money(s: str) -> Optional[float]:
    s = s.strip()
    m = re.search(r"(-?\d+(?:\.\d+)?)", s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _split_arrow(s: str) -> Tuple[Optional[str], Optional[str]]:
    s = _clean_text(s)
    if not s:
        return None, None
    # normalize separators
    for sep in ["➝", "→", "»", "->"]:
        if sep in s:
            parts = [p.strip() for p in s.split(sep)]
            if len(parts) >= 2:
                return (parts[0] or None), (parts[1] or None)
    return (s or None), None


def _action_hu(kind: str, pt_from: float | None, pt_to: float | None) -> str:
    """Adjon magyar, nem-hibrid eseménytípust."""
    if kind == "upgrade":
        return "Felminősítés"
    if kind == "downgrade":
        return "Leminősítés"
    if kind == "pt_change":
        if pt_from is not None and pt_to is not None:
            if pt_to > pt_from:
                return "Célár emelés"
            if pt_to < pt_from:
                return "Célár csökkentés"
        return "Célár frissítés"
    return "Elemzői frissítés"



def _extract_events_from_ratings_page(
    html_text: str,
    kind: str,
    master_set: set,
    asof_date: str,
    source: str,
) -> List[AnalystEvent]:
    # NOTE: We intentionally avoid external deps to keep GH Actions lean.
    # MarketBeat ratings pages contain a sortable table with rows referencing tickers via:
    # - data-clean="TICKER|Company"
    # - or stock link /stocks/EXCHANGE/TICKER/
    #
    # We parse rows with regex + tag stripping. This is robust enough for our use.
    if not html_text:
        return []

    # Locate rows (best-effort)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not rows:
        return []

    def _strip_tags(s: str) -> str:
        s = re.sub(r"<script[\s\S]*?</script>", " ", s, flags=re.IGNORECASE)
        s = re.sub(r"<style[\s\S]*?</style>", " ", s, flags=re.IGNORECASE)
        s = re.sub(r"<[^>]+>", " ", s)
        s = html.unescape(s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    events: List[AnalystEvent] = []

    for row_html in rows:
        # Grab td cells
        tds = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.IGNORECASE | re.DOTALL)
        if len(tds) < 6:
            continue

        # Ticker: prefer data-clean attribute in the first cell
        ticker = ""
        m_dc = re.search(r'data-clean="(?P<t>[A-Z0-9\.\-]+)\|', tds[0], flags=re.IGNORECASE)
        if m_dc:
            ticker = m_dc.group("t").upper().strip()
        if not ticker:
            m2 = re.search(r"/stocks/[A-Z0-9]+/(?P<t>[A-Z0-9\.\-]+)/", row_html, flags=re.IGNORECASE)
            if m2:
                ticker = m2.group("t").upper().strip()

        if not ticker or ticker not in master_set:
            continue

        # Column order observed:
        # 0 Company, 1 Action, 2 Brokerage, 3 Analyst, 4 Current Price, 5 Price Target, 6 Rating, 7 Details (optional)
        company_cell = _strip_tags(tds[0])
        action_cell = _strip_tags(tds[1]) if len(tds) > 1 else ""
        brokerage_cell = _strip_tags(tds[2]) if len(tds) > 2 else ""
        firm = _clean_firm(_clean_text(brokerage_cell) or "—")

        cp_cell = _strip_tags(tds[4]) if len(tds) > 4 else ""
        current_price = _parse_money(cp_cell) if cp_cell else None

        pt_cell = _strip_tags(tds[5]) if len(tds) >= 6 else ""
        rating_cell = _strip_tags(tds[6]) if len(tds) >= 7 else ""

        # Parse PT (may be single value or arrow)
        pt_from = pt_to = None
        if pt_cell:
            pf, pt = _split_arrow(pt_cell)
            if pt is None:
                pt_to = _parse_money(pf or "")
            else:
                pt_from = _parse_money(pf or "")
                pt_to = _parse_money(pt or "")

        # Parse Rating (single or arrow)
        rating_from = rating_to = None
        if rating_cell:
            rf, rt = _split_arrow(rating_cell)
            if rt is None:
                rating_to = rf
            else:
                rating_from, rating_to = rf, rt

        # Source URL: prefer stock page if present
        stock_url = ""
        m_href = re.search(r'href="(?P<h>[^"]*/stocks/[^"]+)"', row_html, flags=re.IGNORECASE)
        if m_href:
            href = m_href.group("h")
            if href.startswith("http"):
                stock_url = href
            else:
                stock_url = BASE + href
        src_url = stock_url or source

        events.append(
            AnalystEvent(
                ticker=ticker,
                date=asof_date,
                firm=firm,
                action=_action_hu(kind, pt_from, pt_to),
                current_price=current_price,
                rating_from=rating_from,
                rating_to=rating_to,
                pt_from=pt_from,
                pt_to=pt_to,
                currency="USD",
                source=src_url,
            )
        )

    return events



def _date_candidates_for_ratings_url(base_url: str, ymd: str) -> List[str]:
    """Try multiple URL shapes for MarketBeat 'Reporting Date' filtering.
    MarketBeat sometimes changes parameter names; we brute-force safely.
    """
    sep = "&" if "?" in base_url else "?"
    candidates = [
        f"{base_url}{sep}date={ymd}",
        f"{base_url}{sep}reporting_date={ymd}",
        f"{base_url}{sep}reportingDate={ymd}",
        f"{base_url}{sep}reporting-date={ymd}",
        f"{base_url}{sep}report_date={ymd}",
        f"{base_url}{sep}reportDate={ymd}",
    ]
    # path-style (some sites use /YYYY-MM-DD/)
    base_slash = base_url.rstrip("/")
    candidates.append(f"{base_slash}/{ymd}/")
    return candidates


def _ratings_page_signature(html: str) -> str:
    """Extract a stable-ish signature to detect whether date filtering took effect."""
    m = re.search(r"<table[^>]*>.*?</table>", html, re.IGNORECASE | re.DOTALL)
    if not m:
        return html[:2000]
    table = m.group(0)
    # remove whitespace + digits that often vary (timestamps, cache-busters)
    table = re.sub(r"\s+", " ", table)
    return table[:2000]


def _page_mentions_date(html: str, ymd: str) -> bool:
    # common representations: 2026-01-30, 2026. 01. 30., 01/30/2026
    y, m, d = ymd.split("-")
    variants = {
        ymd,
        f"{y}.{m}.{d}",
        f"{y}. {m}. {d}.",
        f"{m}/{d}/{y}",
        f"{y}/{m}/{d}",
    }
    low = html.lower()
    return any(v.lower() in low for v in variants)


def _fetch_ratings_page_for_date(
    opener,
    base_url: str,
    ymd: str,
    timeout: int,
    debug_dir: Optional[Path],
    kind: str,
    baseline_sig: Optional[str],
) -> Tuple[int, str, str]:
    """Return (status, final_url, html) for a given reporting date.
    We try several URL patterns and pick the first that differs from baseline
    OR explicitly contains the requested date.
    """
    candidates = _date_candidates_for_ratings_url(base_url, ymd)
    last_status, last_html, last_url = 0, "", base_url

    for cand in candidates:
        status, html = _http_get(
            opener,
            cand,
            timeout=timeout,
            debug_dir=debug_dir,
            debug_tag=f"ratings_{kind}_{ymd}",
            allow_jinja_fallback=True,
            max_tries=3,
        )
        last_status, last_html, last_url = status, html, cand

        if status != 200:
            continue
        if _is_challenge_page(html):
            continue

        # Accept if we see the date explicitly, OR content differs from baseline.
        sig = _ratings_page_signature(html)
        if _page_mentions_date(html, ymd) or (baseline_sig is not None and sig != baseline_sig):
            return status, cand, html

    return last_status, last_url, last_html


def fetch_events_from_ratings_pages(
    tickers_set: set,
    days: int,
    debug_dir: Optional[Path] = None,
    timeout: int = 20,
) -> Tuple[List[AnalystEvent], Dict[str, Tuple[int, str]]]:
    """Fetch MarketBeat ratings pages for the last N calendar days.

    Important: The /ratings/* lists are date-filtered on the site ("Reporting Date").
    Previously we only fetched today's list and relied on first-seen cache, which meant
    you could miss older (but still in-window) events when today's list had no MASTER tickers.
    This function now iterates over the last N dates and brute-forces the date parameter.
    """
    opener = _build_opener()

    today = dt.date.today()
    days = max(1, int(days))
    date_list = [today - dt.timedelta(days=i) for i in range(days)]

    events: List[AnalystEvent] = []
    status_map: Dict[str, Tuple[int, str]] = {}

    # Pre-fetch baseline (today) signatures per kind/source so we can detect if date filtering works.
    baseline_sigs: Dict[str, str] = {}

    for src in RATINGS_SOURCES:
        kind = src["kind"]
        url = BASE_URL.rstrip("/") + src["path"]
        status, html = _http_get(
            opener,
            url,
            timeout=timeout,
            debug_dir=debug_dir,
            debug_tag=f"ratings_{kind}_baseline_{today.isoformat()}",
            allow_jinja_fallback=True,
            max_tries=3,
        )
        status_map[f"{kind}:{today.isoformat()}" ] = (status, url)
        if status == 200 and not _is_challenge_page(html):
            baseline_sigs[kind] = _ratings_page_signature(html)
            events.extend(_extract_from_ratings_page(html, tickers_set, today.isoformat(), kind))

    # Historical dates (yesterday..)
    for d in date_list[1:]:
        ymd = d.isoformat()
        for src in RATINGS_SOURCES:
            kind = src["kind"]
            base_url = BASE_URL.rstrip("/") + src["path"]
            baseline_sig = baseline_sigs.get(kind)
            status, final_url, html = _fetch_ratings_page_for_date(
                opener,
                base_url,
                ymd,
                timeout=timeout,
                debug_dir=debug_dir,
                kind=kind,
                baseline_sig=baseline_sig,
            )
            status_map[f"{kind}:{ymd}"] = (status, final_url)
            if status != 200:
                continue
            if _is_challenge_page(html):
                continue
            events.extend(_extract_from_ratings_page(html, tickers_set, ymd, kind))

    # Sort: newest report date first, then ticker
    def _sort_key(ev: AnalystEvent):
        return (ev.date, ev.ticker or "", ev.action or "", ev.firm or "")

    events.sort(key=_sort_key, reverse=True)
    return events, status_map
def read_master_tickers(master_csv: Path) -> List[str]:
    # Read CSV and return unique tickers (best-effort on column name).
    raw = master_csv.read_text(encoding="utf-8", errors="replace").splitlines()
    reader = csv.DictReader(raw)
    if not reader.fieldnames:
        return []
    # find likely ticker column
    cols = [c.strip() for c in reader.fieldnames if c]
    key = None
    for cand in ["ticker", "symbol", "TICKER", "Symbol", "Ticker"]:
        if cand in cols:
            key = cand
            break
    if key is None:
        # fallback: first column
        key = cols[0]
    tickers: List[str] = []
    for row in reader:
        t = (row.get(key) or "").strip().upper()
        # allow "PKN.WA" etc but you later filter out elsewhere if needed
        if t and re.fullmatch(r"[A-Z0-9\.\-]+", t):
            tickers.append(t)
    # uniq preserve order
    seen=set()
    out=[]
    for t in tickers:
        if t not in seen:
            seen.add(t); out.append(t)
    return out


def write_outputs(
    out_md: Path,
    out_json: Optional[Path],
    events: List[AnalystEvent],
    days: int,
    source_status: Dict[str, int],
) -> None:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = []
    lines.append(f"# Elemzői feed (fel/leminősítés + célár) — utolsó {days} naptári nap (első észlelés alapján)")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Generálva (UTC): {now}")
    lines.append("")

    any_fail = any((k.startswith("ratings_") and (v >= 400 or v == 0)) for k, v in source_status.items())
    if not events:
        if any_fail:
            # show clear source error, not "no events"
            parts=[]
            for k,v in sorted(source_status.items()):
                if k.startswith("ratings_"):
                    parts.append(f"{k.replace('ratings_','')}={v}")
            msg = ", ".join(parts) if parts else "unknown"
            lines.append(f"_MarketBeat forrás nem elérhető / blokkolva, ezért nem tudtam friss analyst eseményeket lekérni._ (HTTP: {msg})")
        else:
            lines.append(f"_Nincs friss (≤{days} naptári nap) fel/leminősítés vagy célár-frissítés a forrásban, vagy a forrás challenge-t ad (HTTP 200)._\n_(Ha challenge-re gyanakszol, nézd meg a debug HTML-eket.)_")
    else:
        by: Dict[str, List[AnalystEvent]] = {}
        for e in events:
            by.setdefault(e.ticker, []).append(e)
        for t in sorted(by.keys()):
            lines.append(f"## {t}")
            for e in sorted(by[t], key=lambda x: x.date, reverse=True):
                parts = [f"- {t} — {e.date} — {e.firm} — {e.action}"]
                if e.current_price is not None:
                    parts.append(f"Ár: {e.currency} {e.current_price:.2f}")
                if e.rating_from or e.rating_to:
                    rf = RATING_HU.get(e.rating_from, e.rating_from) if e.rating_from else "—"
                    rt = RATING_HU.get(e.rating_to, e.rating_to) if e.rating_to else "—"
                    parts.append(f"Ajánlás: {rf} → {rt}")
                if e.pt_from is not None or e.pt_to is not None:
                    if e.pt_from is not None and e.pt_to is not None:
                        parts.append(f"Célár: {e.currency} {e.pt_from:.2f} → {e.pt_to:.2f}")
                    elif e.pt_to is not None:
                        parts.append(f"Célár: {e.currency} {e.pt_to:.2f}")
                parts.append(f"Forrás: {e.source}")
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
            "source_status": source_status,
            "events": [asdict(e) for e in events],
        }
        out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-json", default="")
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug-dir", default="reports/debug_marketbeat")
    args = ap.parse_args()

    master = Path(args.master)
    if not master.exists():
        _log(f"ERROR: MASTER CSV not found: {master}")
        return 2

    debug_dir = Path(args.debug_dir) if args.debug else None
    out_md = Path(args.out_md)
    out_json = Path(args.out_json) if args.out_json else None

    # Seen-cache for assigning real (first-seen) dates, because ratings pages have no per-row timestamps
    seen_path = out_md.parent / "marketbeat_seen.json"
    seen_cache = load_seen_cache(seen_path)

    _log(f"START {VERSION} days={args.days} master={master} mode=ratings_pages")

    tickers = read_master_tickers(master)

    events, status_map = fetch_events_from_ratings_pages(
        tickers,
        days=args.days,
        timeout=args.timeout,
        sleep_s=max(0.2, float(args.sleep)),
        debug_dir=debug_dir,
    )

    today_iso = dt.datetime.utcnow().date().isoformat()
    events = apply_seen_dates_and_filter(events, args.days, seen_cache, today_iso)
    # merge duplicates across different MarketBeat lists
    events = dedup_merge_events(events)
    save_seen_cache(seen_path, seen_cache)

    write_outputs(out_md, out_json, events, args.days, status_map)

    _log(f"DONE events={len(events)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
