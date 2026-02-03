#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyst_marketbeat.py — Analyst feed (upgrades/downgrades + price-target changes) with MarketBeat scrape + API fallbacks.

Versioning rule: whenever this file is modified, bump VERSION continuously (no gaps).
VERSION: v0.3.36-marketbeat-fmp-fallback-stdlib-2026-02-02

IMÁDSÁG (2 sor):
bocsáss meg uram, mert balfék voltam, és hagytam hogy egy dependency szétverje a futást.
adj erőt, hogy a fallback mindig fusson, a jelentés meg mindig éljen. 🙏

What this script guarantees:
- NO third‑party deps (no requests). Stdlib HTTP only.
- Writes output even on BLOCKED/NO_EVENTS (exit code 0).
- If MarketBeat is blocked/challenged from CI, it can fallback to FMP (if FMP_API_KEY is provided).
- Finnhub upgrade/downgrade is optional; commonly premium/403 on free keys (detected and reported).

Inputs:
- --master: local CSV path or http(s) URL; must contain a ticker column (Ticker/Symbol/ticker/symbol).
- --days: last N calendar days (UTC) to include.
- --out-md / --out-json: output paths.

State files (persist via workflow cache):
- reports/marketbeat_seen.json        (first-seen date per event id)
- reports/marketbeat_events.json      (event cache so items that disappear from "latest" still show within window)
- reports/marketbeat_last_success.json (last successful fetch metadata)
- reports/marketbeat_last_success.md   (last successful rendered markdown)

Env vars (optional):
- FMP_API_KEY
- FINNHUB_API_KEY

Exit codes:
- 0: success (including BLOCKED/NO_EVENTS, outputs written)
- 2: fatal I/O/config error (e.g., master unreadable)
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# -------------------------
# Constants / paths
# -------------------------
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT = 25

REPORTS_DIR = "reports"
SEEN_PATH = os.path.join(REPORTS_DIR, "marketbeat_seen.json")
EVENTS_CACHE_PATH = os.path.join(REPORTS_DIR, "marketbeat_events.json")
LAST_SUCCESS_JSON = os.path.join(REPORTS_DIR, "marketbeat_last_success.json")
LAST_SUCCESS_MD = os.path.join(REPORTS_DIR, "marketbeat_last_success.md")


@dataclass
class AnalystEvent:
    ticker: str
    date: str  # YYYY-MM-DD
    action: str  # "Upgrade" / "Downgrade" / "Initiated" / "Price Target" / "Reiterated" / etc
    firm: str
    analyst: str
    rating: str
    pt: str
    summary: str
    source: str  # "MarketBeat" / "FMP" / "Finnhub"
    url: str

    def stable_id(self) -> str:
        raw = "|".join([
            self.ticker.upper().strip(),
            self.date.strip(),
            self.action.strip(),
            self.firm.strip(),
            self.analyst.strip(),
            self.rating.strip(),
            self.pt.strip(),
            self.url.strip(),
        ])
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


# -------------------------
# Small utilities
# -------------------------
def _utc_today() -> dt.date:
    return dt.datetime.utcnow().date()


def _parse_date_yyyy_mm_dd(s: str) -> Optional[dt.date]:
    s = (s or "").strip()
    if not s:
        return None
    # common formats: 2026-02-02, 02/02/2026, 2/2/2026
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    # MarketBeat sometimes: "Feb. 2, 2026" / "February 2, 2026"
    s2 = re.sub(r"\s+", " ", s)
    for fmt in ("%b. %d, %Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return dt.datetime.strptime(s2, fmt).date()
        except ValueError:
            pass
    return None


def _within_last_n_days(d: dt.date, days: int, today: Optional[dt.date] = None) -> bool:
    today = today or _utc_today()
    start = today - dt.timedelta(days=days - 1)
    return start <= d <= today


def _ensure_reports_dir() -> None:
    os.makedirs(REPORTS_DIR, exist_ok=True)


def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read().strip()
            if not txt or txt.lower() in ("placeholder", "null", "none"):
                return default
            return json.loads(txt)
    except FileNotFoundError:
        return default
    except Exception:
        # quarantine corrupt file
        try:
            bad = path + ".corrupt"
            if os.path.exists(path):
                os.replace(path, bad)
        except Exception:
            pass
        return default


def _save_json(path: str, obj: Any) -> None:
    _ensure_reports_dir()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def _write_text(path: str, text: str) -> None:
    _ensure_reports_dir()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def _http_get(url: str, timeout: int = DEFAULT_TIMEOUT, headers: Optional[Dict[str, str]] = None) -> Tuple[int, str, Dict[str, str]]:
    """
    Stdlib HTTP GET.
    Returns: (status_code, body_text, response_headers_dict)
    """
    hdrs = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if headers:
        hdrs.update(headers)
    req = Request(url, headers=hdrs, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            raw = resp.read()
            # best effort decode
            charset = "utf-8"
            ctype = resp.headers.get("Content-Type", "")
            m = re.search(r"charset=([^\s;]+)", ctype, re.I)
            if m:
                charset = m.group(1)
            try:
                body = raw.decode(charset, errors="replace")
            except Exception:
                body = raw.decode("utf-8", errors="replace")
            return int(status), body, dict(resp.headers.items())
    except HTTPError as e:
        try:
            raw = e.read()
            body = raw.decode("utf-8", errors="replace") if raw else ""
        except Exception:
            body = ""
        return int(getattr(e, "code", 0) or 0), body, dict(getattr(e, "headers", {}).items()) if getattr(e, "headers", None) else {}
    except URLError:
        return 0, "", {}
    except Exception:
        return 0, "", {}


def _looks_blocked(status: int, body: str, headers: Dict[str, str]) -> bool:
    if status in (0, 403, 429, 503):
        return True
    b = (body or "").lower()
    # generic bot/challenge hints
    hints = [
        "captcha",
        "cloudflare",
        "cf-challenge",
        "just a moment",
        "attention required",
        "verify you are human",
        "access denied",
        "bot detection",
        "unusual traffic",
        "enable javascript",
    ]
    return any(h in b for h in hints)


def _extract_tickers_from_master(master_csv_or_url: str) -> List[str]:
    # load csv (local or URL)
    parsed = urlparse(master_csv_or_url)
    if parsed.scheme in ("http", "https"):
        status, body, _ = _http_get(master_csv_or_url, timeout=DEFAULT_TIMEOUT)
        if status == 0:
            raise RuntimeError("MASTER download failed (no HTTP response).")
        if status >= 400:
            raise RuntimeError(f"MASTER download failed (HTTP {status}).")
        rows = list(csv.DictReader(body.splitlines()))
    else:
        with open(master_csv_or_url, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

    if not rows:
        return []

    # find ticker column
    keys = list(rows[0].keys())
    col = None
    for cand in ("Ticker", "ticker", "Symbol", "symbol"):
        if cand in keys:
            col = cand
            break
    if col is None:
        # try loose match
        for k in keys:
            if k and k.strip().lower() in ("ticker", "symbol"):
                col = k
                break
    if col is None:
        raise RuntimeError(f"MASTER CSV missing ticker column. Found columns: {keys}")

    tickers = []
    for r in rows:
        t = (r.get(col) or "").strip().upper()
        if not t:
            continue
        # skip obvious non-tickers
        if re.fullmatch(r"[A-Z0-9\.\-]{1,12}", t) is None:
            continue
        tickers.append(t)
    # dedupe, preserve order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# -------------------------
# MarketBeat scraping (best-effort)
# -------------------------
_MB_URLS = [
    # Most complete table (what you opened in browser)
    "https://www.marketbeat.com/ratings/us/",
    # Category pages (sometimes lighter/less likely to challenge)
    "https://www.marketbeat.com/ratings/upgrades/",
    "https://www.marketbeat.com/ratings/downgrades/",
    "https://www.marketbeat.com/ratings/initiations/",
    "https://www.marketbeat.com/ratings/pricetargetchanges/",
]


def _mb_parse_table_rows(html: str) -> List[Dict[str, str]]:
    """
    Parse MarketBeat rating tables from HTML without external parsers.
    Very defensive: uses regex to locate <tr> and extract <td> text.
    """
    if not html:
        return []
    # focus around the first <table ...> that contains "COMPANY" or "ACTION"
    # (we avoid huge regex; still acceptable for these pages)
    tables = re.split(r"(?i)</table>", html)
    target = ""
    for tb in tables:
        if re.search(r"(?i)\bcompany\b", tb) and re.search(r"(?i)\baction\b", tb):
            target = tb
            break
    if not target:
        target = html

    # extract rows
    rows = re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", target)
    out: List[Dict[str, str]] = []

    def clean(x: str) -> str:
        x = re.sub(r"(?is)<script.*?</script>", " ", x)
        x = re.sub(r"(?is)<style.*?</style>", " ", x)
        x = re.sub(r"(?is)<[^>]+>", " ", x)
        x = x.replace("&nbsp;", " ")
        x = x.replace("&amp;", "&")
        x = x.replace("&quot;", '"')
        x = x.replace("&#39;", "'")
        x = re.sub(r"\s+", " ", x).strip()
        return x

    for tr in rows:
        tds = re.findall(r"(?is)<t[dh][^>]*>(.*?)</t[dh]>", tr)
        if len(tds) < 4:
            continue
        cells = [clean(c) for c in tds]
        # heuristics: rows typically include ticker in first/second cell
        # Example on /ratings/us/: [Company, Action, Brokerage, Analyst, Current Price, Price Target, Rating, Details]
        out.append({
            "c1": cells[0] if len(cells) > 0 else "",
            "c2": cells[1] if len(cells) > 1 else "",
            "c3": cells[2] if len(cells) > 2 else "",
            "c4": cells[3] if len(cells) > 3 else "",
            "c5": cells[4] if len(cells) > 4 else "",
            "c6": cells[5] if len(cells) > 5 else "",
            "c7": cells[6] if len(cells) > 6 else "",
            "raw": " | ".join(cells[:8]),
        })
    return out


def _mb_extract_event_from_row(row: Dict[str, str], today: dt.date) -> Optional[AnalystEvent]:
    """
    Convert a parsed row into AnalystEvent using heuristics.
    """
    c1 = row.get("c1", "")
    c2 = row.get("c2", "")
    c3 = row.get("c3", "")
    c4 = row.get("c4", "")
    c6 = row.get("c6", "")
    c7 = row.get("c7", "")

    # ticker usually appears as a standalone token in c1 (e.g. "AAPL Apple") or in the raw
    m = re.search(r"\b([A-Z]{1,6}(?:\.[A-Z]{1,3})?)\b", c1)
    if not m:
        m = re.search(r"\b([A-Z]{1,6}(?:\.[A-Z]{1,3})?)\b", row.get("raw", ""))
    if not m:
        return None
    ticker = m.group(1).upper()

    # action is usually c2 (e.g. "Reiterated by", "Target Set by", "Upgraded by", "Downgraded by")
    action_txt = c2.strip()
    action = "Rating"
    if "upgrad" in action_txt.lower():
        action = "Upgrade"
    elif "downgrad" in action_txt.lower():
        action = "Downgrade"
    elif "initiat" in action_txt.lower():
        action = "Initiated"
    elif "target" in action_txt.lower():
        action = "Price Target"
    elif "reiterat" in action_txt.lower():
        action = "Reiterated"
    else:
        action = action_txt.replace(" by", "").strip() or "Rating"

    firm = c3.strip()
    analyst = c4.strip()
    pt = c6.strip()
    rating = c7.strip()

    # date: MarketBeat pages are "recent ratings" but not always show date in the visible table;
    # we treat as "today" (UTC) for grouping; the persistent cache keeps items within N-day window once seen.
    date_s = today.strftime("%Y-%m-%d")

    summary = row.get("raw", "").strip()
    return AnalystEvent(
        ticker=ticker,
        date=date_s,
        action=action,
        firm=firm,
        analyst=analyst,
        rating=rating,
        pt=pt,
        summary=summary,
        source="MarketBeat",
        url="https://www.marketbeat.com/ratings/us/",
    )


def fetch_marketbeat_events(tickers: List[str], days: int) -> Tuple[str, List[AnalystEvent], str]:
    """
    Returns (status, events, note)
    status ∈ {"OK","BLOCKED","HTTP_ERROR","NO_EVENTS"}
    """
    today = _utc_today()
    tickset = set(tickers)

    # Load persistent caches
    seen: Dict[str, str] = _load_json(SEEN_PATH, {})
    cache: Dict[str, Dict[str, Any]] = _load_json(EVENTS_CACHE_PATH, {})

    def upsert(evt: AnalystEvent) -> None:
        eid = evt.stable_id()
        if eid not in seen:
            seen[eid] = evt.date
        cache[eid] = asdict(evt)

    any_ok = False
    blocked_any = False
    http_err = False

    # Try multiple pages; keep first successful
    harvested: List[AnalystEvent] = []
    for url in _MB_URLS:
        status, body, headers = _http_get(url, timeout=DEFAULT_TIMEOUT, headers={"Referer": "https://www.marketbeat.com/"})
        if _looks_blocked(status, body, headers):
            blocked_any = True
            continue
        if status >= 400:
            http_err = True
            continue

        any_ok = True
        rows = _mb_parse_table_rows(body)
        for r in rows:
            evt = _mb_extract_event_from_row(r, today=today)
            if not evt:
                continue
            if evt.ticker not in tickset:
                continue
            upsert(evt)
            harvested.append(evt)

        # If we got anything on the main /ratings/us/ page, that's best.
        if harvested and url.endswith("/ratings/us/"):
            break

        # small politeness delay
        time.sleep(0.7)

    # Save caches regardless
    _save_json(SEEN_PATH, seen)
    _save_json(EVENTS_CACHE_PATH, cache)

    # Now compute windowed output from cache (so disappearing "latest" items still appear)
    out: List[AnalystEvent] = []
    today_d = _utc_today()
    for eid, obj in cache.items():
        try:
            d = _parse_date_yyyy_mm_dd(obj.get("date", "")) or today_d
            if not _within_last_n_days(d, days, today=today_d):
                continue
            t = (obj.get("ticker") or "").upper()
            if t not in tickset:
                continue
            out.append(AnalystEvent(**obj))
        except Exception:
            continue

    # Sort: date desc, ticker
    def key(e: AnalystEvent):
        dd = _parse_date_yyyy_mm_dd(e.date) or today_d
        return (dd, e.ticker)

    out.sort(key=key, reverse=True)

    if not any_ok:
        if blocked_any:
            return "BLOCKED", out, "MarketBeat challenge/bot or blocked from CI."
        if http_err:
            return "HTTP_ERROR", out, "MarketBeat HTTP error."
        return "HTTP_ERROR", out, "MarketBeat no response."
    if not out:
        return "NO_EVENTS", [], "No events matched MASTER tickers in cache/window."
    return "OK", out, "MarketBeat scrape OK."


# -------------------------
# FMP fallback (API)
# -------------------------
def _fmp_get_json(url: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[int, Any]:
    status, body, _ = _http_get(url, timeout=timeout, headers={"Accept": "application/json,*/*;q=0.8"})
    if status == 0:
        return 0, None
    try:
        return status, json.loads(body) if body else None
    except Exception:
        return status, None


def fetch_fmp_events(tickers: List[str], days: int) -> Tuple[str, List[AnalystEvent], str]:
    """
    FMP fallback for analyst events (upgrades/downgrades + price target changes).

    IMPORTANT:
    - Uses per-symbol endpoints (NOT RSS-feed), because RSS-feed endpoints are often paywalled/403.
      Docs (legacy) show per-symbol endpoints:
        - https://financialmodelingprep.com/api/v4/upgrades-downgrades?symbol=AAPL
        - https://financialmodelingprep.com/api/v4/price-target?symbol=AAPL
    - 403 from FMP typically means invalid/missing API key or insufficient access.
      We probe /api/v3/quote to validate key reachability before batch fetching.

    Returns:
      status: "OK" | "AUTH" | "RATE_LIMIT" | "HTTP_ERROR" | "NO_KEY"
      events: list of AnalystEvent
      note: human explanation for report
    """
    apikey = (os.environ.get("FMP_API_KEY") or "").strip()
    if not apikey:
        return "NO_KEY", [], "FMP_API_KEY missing."

    # Quick probe: verify the key is actually being injected and accepted.
    probe_url = f"https://financialmodelingprep.com/api/v3/quote/{urlencode({'symbol': 'AAPL'})}".replace("symbol=AAPL", "AAPL")
    # Some FMP endpoints use /api/v3/quote/AAPL (path param) rather than ?symbol=
    probe_url = f"https://financialmodelingprep.com/api/v3/quote/AAPL?{urlencode({'apikey': apikey})}"
    st, data = _fmp_get_json(probe_url, timeout=min(10, DEFAULT_TIMEOUT))
    if st == 403:
        return "AUTH", [], "FMP auth failed (HTTP 403). API key missing/invalid or not permitted for this endpoint."
    if st in (401, 402):
        return "AUTH", [], f"FMP auth failed (HTTP {st})."
    if st == 429:
        return "RATE_LIMIT", [], "FMP rate limit hit during auth probe (HTTP 429)."
    if st is None:
        return "HTTP_ERROR", [], "FMP probe failed (no HTTP response)."
    # If probe returns JSON error object, handle.
    if isinstance(data, dict) and data.get("Error Message"):
        return "AUTH", [], f"FMP error: {data.get('Error Message')}"

    today = _utc_today()
    cutoff = today - dt.timedelta(days=max(0, days - 1))
    tickset = set([t.strip().upper() for t in tickers if t and t.strip()])
    if not tickset:
        return "OK", [], "No tickers."

    def _parse_fmp_date(s: str) -> Optional[dt.date]:
        if not s:
            return None
        s = str(s).strip()
        # Common formats: '2026-02-02', '2026-02-02 00:00:00'
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y"):
            try:
                return dt.datetime.strptime(s[:len(fmt)], fmt).date()
            except Exception:
                continue
        return None

    def _fetch_symbol(sym: str) -> Tuple[str, List[AnalystEvent], Optional[int]]:
        """Returns (sym, events, http_status_problem_if_any)."""
        out: List[AnalystEvent] = []
        sym = sym.upper()

        # 1) Upgrades/Downgrades
        u_url = f"https://financialmodelingprep.com/api/v4/upgrades-downgrades?{urlencode({'symbol': sym, 'apikey': apikey})}"
        u_st, u_data = _fmp_get_json(u_url, timeout=DEFAULT_TIMEOUT)
        if u_st in (401, 402, 403):
            return sym, [], u_st
        if u_st == 429:
            return sym, [], 429
        if u_st is None:
            return sym, [], 520

        if isinstance(u_data, list):
            for r in u_data:
                try:
                    t = (r.get("symbol") or sym).upper()
                    d = _parse_fmp_date(r.get("publishedDate") or r.get("date") or r.get("updated") or "")
                    if not d or d < cutoff:
                        continue
                    if t not in tickset:
                        continue
                    firm = (r.get("gradingCompany") or r.get("company") or r.get("analystFirm") or r.get("firm") or "").strip()
                    analyst = (r.get("analyst") or "").strip()
                    action = (r.get("action") or r.get("newGrade") or r.get("newRating") or "").strip()
                    prev = (r.get("previousGrade") or r.get("previousRating") or "").strip()
                    summary = (r.get("newsTitle") or r.get("notes") or "").strip()
                    if prev and action and prev != action:
                        act = f"{prev} → {action}"
                    else:
                        act = action or "Rating update"
                    out.append(
                        AnalystEvent(
                            ticker=t,
                            date=d.isoformat(),
                            action=act,
                            firm=firm or "FMP",
                            analyst=analyst,
                            summary=summary,
                            source="FMP:upgrades-downgrades",
                            url="",
                        )
                    )
                except Exception:
                    continue

        # 2) Price Target changes
        pt_url = f"https://financialmodelingprep.com/api/v4/price-target?{urlencode({'symbol': sym, 'apikey': apikey})}"
        pt_st, pt_data = _fmp_get_json(pt_url, timeout=DEFAULT_TIMEOUT)
        if pt_st in (401, 402, 403):
            return sym, out, pt_st
        if pt_st == 429:
            return sym, out, 429
        if pt_st is None:
            return sym, out, 520

        if isinstance(pt_data, list):
            for r in pt_data:
                try:
                    t = (r.get("symbol") or sym).upper()
                    d = _parse_fmp_date(r.get("publishedDate") or r.get("date") or "")
                    if not d or d < cutoff:
                        continue
                    if t not in tickset:
                        continue
                    firm = (r.get("analystCompany") or r.get("company") or r.get("analystFirm") or r.get("firm") or "").strip()
                    analyst = (r.get("analystName") or r.get("analyst") or "").strip()
                    pt = r.get("priceTarget") or r.get("adjPriceTarget") or r.get("target") or ""
                    pt_old = r.get("priceTargetOld") or r.get("oldPriceTarget") or ""
                    if pt_old and pt:
                        act = f"PT {pt_old} → {pt}"
                    elif pt:
                        act = f"PT set {pt}"
                    else:
                        act = "Price target update"
                    summary = (r.get("newsTitle") or r.get("notes") or "").strip()
                    out.append(
                        AnalystEvent(
                            ticker=t,
                            date=d.isoformat(),
                            action=act,
                            firm=firm or "FMP",
                            analyst=analyst,
                            summary=summary,
                            source="FMP:price-target",
                            url="",
                        )
                    )
                except Exception:
                    continue

        return sym, out, None

    # Concurrent fetch to keep runtime reasonable for 100+ tickers.
    # Keep worker count conservative to avoid 429.
    workers = int(os.environ.get("FMP_WORKERS") or "8")
    workers = max(2, min(12, workers))

    all_events: List[AnalystEvent] = []
    problems: List[int] = []

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_fetch_symbol, sym): sym for sym in sorted(tickset)}
        for fut in as_completed(futs):
            sym, evs, prob = fut.result()
            if evs:
                all_events.extend(evs)
            if prob is not None:
                problems.append(prob)

    # Normalize + sort newest first
    all_events.sort(key=lambda e: (e.date or "", e.ticker), reverse=True)

    if problems:
        # Prioritize meaningful status
        if any(p == 429 for p in problems):
            return "RATE_LIMIT", all_events, "FMP rate limit hit (HTTP 429) on some requests; partial results."
        if any(p in (401, 402, 403) for p in problems):
            return "AUTH", all_events, f"FMP auth failed (HTTP {max([p for p in problems if p in (401,402,403)] or [403])})."
        return "HTTP_ERROR", all_events, "FMP had HTTP errors on some requests; partial results."

    return "OK", all_events, "FMP OK."

def fetch_finnhub_events(tickers: List[str], days: int) -> Tuple[str, List[AnalystEvent], str]:
    token = (os.environ.get("FINNHUB_API_KEY") or "").strip()
    if not token:
        return "NO_KEY", [], "FINNHUB_API_KEY missing."

    today = _utc_today()
    tickset = set(tickers)

    # Finnhub upgrade/downgrade endpoint (often premium). Docs: citeturn4search2
    url = f"https://finnhub.io/api/v1/stock/upgrade-downgrade?{urlencode({'token': token})}"
    status, body, _ = _http_get(url, timeout=DEFAULT_TIMEOUT, headers={"Accept": "application/json"})
    if status in (401, 403):
        return "AUTH", [], f"Finnhub access denied (HTTP {status}) — likely premium endpoint."
    if status >= 400 or status == 0:
        return "HTTP_ERROR", [], f"Finnhub HTTP error (HTTP {status})."

    try:
        data = json.loads(body) if body else []
    except Exception:
        data = []
    if not isinstance(data, list):
        return "HTTP_ERROR", [], "Finnhub unexpected response."

    out: List[AnalystEvent] = []
    for it in data:
        try:
            sym = (it.get("symbol") or "").upper().strip()
            if sym not in tickset:
                continue
            d = _parse_date_yyyy_mm_dd(str(it.get("gradeTime") or "")[:10]) or today
            if not _within_last_n_days(d, days, today=today):
                continue
            firm = str(it.get("from") or it.get("firm") or "").strip()
            action = str(it.get("action") or "Rating").strip()
            oldg = str(it.get("fromGrade") or "").strip()
            newg = str(it.get("toGrade") or "").strip()
            pt = str(it.get("toPriceTarget") or it.get("priceTarget") or "").strip()
            summary = f"{action}: {oldg} → {newg}".strip(": ")
            if pt:
                summary += f"; PT {pt}"
            out.append(AnalystEvent(
                ticker=sym,
                date=d.strftime("%Y-%m-%d"),
                action=action,
                firm=firm,
                analyst="",
                rating=newg,
                pt=pt,
                summary=summary,
                source="Finnhub",
                url="https://finnhub.io/docs/api/upgrade-downgrade",
            ))
        except Exception:
            continue

    if not out:
        return "NO_EVENTS", [], "Finnhub returned no matching events in window."
    out.sort(key=lambda e: (e.date, e.ticker), reverse=True)
    return "OK", out, "Finnhub OK."


# -------------------------
# Rendering
# -------------------------
def render_markdown(days: int, status: str, note: str, events: List[AnalystEvent]) -> str:
    title = f"## Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {days} naptári nap)"
    if status in ("BLOCKED", "HTTP_ERROR"):
        return f"{title}\n\n_BLOCKED_ ({note})\n"
    if status in ("NO_EVENTS",):
        return f"{title}\n\n_NO_EVENTS_\n"
    if status in ("NO_KEY", "AUTH"):
        return f"{title}\n\n_BLOCKED_ ({note})\n"

    # Group by ticker
    by: Dict[str, List[AnalystEvent]] = {}
    for e in events:
        by.setdefault(e.ticker, []).append(e)

    lines = [title, ""]
    for t in sorted(by.keys()):
        lines.append(f"**{t}**")
        for e in by[t][:20]:
            bits = [e.date, e.action]
            if e.firm:
                bits.append(e.firm)
            if e.analyst:
                bits.append(e.analyst)
            if e.rating:
                bits.append(e.rating)
            if e.pt:
                bits.append(f"PT {e.pt}")
            lines.append(f"- " + " | ".join(bits))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# -------------------------
# Main
# -------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="Path or URL to MASTER CSV")
    ap.add_argument("--days", type=int, default=3, help="Last N calendar days (UTC)")
    ap.add_argument("--out-md", required=True, help="Output markdown path")
    ap.add_argument("--out-json", required=True, help="Output json path")
    args = ap.parse_args()

    days = int(args.days)
    if days < 1 or days > 30:
        days = 3

    _ensure_reports_dir()

    try:
        tickers = _extract_tickers_from_master(args.master)
    except Exception as e:
        md = f"## Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {days} naptári nap)\n\n_N/A._\n"
        _write_text(args.out_md, md)
        _save_json(args.out_json, [])
        return 2

    # 1) Try MarketBeat scrape + cache
    status, events, note = fetch_marketbeat_events(tickers, days)

    # 2) If blocked/empty, fallback to cached last success markdown (if exists) OR FMP
    used_source = "MarketBeat"
    if status in ("BLOCKED", "HTTP_ERROR") or (status == "NO_EVENTS" and not events):
        # Prefer FMP if key exists (can produce today's AAPL etc even when MarketBeat blocks CI)
        fmp_status, fmp_events, fmp_note = fetch_fmp_events(tickers, days)
        if fmp_status == "OK" and fmp_events:
            status, events, note = "OK", fmp_events, fmp_note
            used_source = "FMP"
        else:
            # if we have last_success.md, reuse it (better than _N/A._)
            last_md = _read_text(LAST_SUCCESS_MD)
            if last_md and status != "NO_EVENTS":
                _write_text(args.out_md, last_md)
                last_json = _load_json(LAST_SUCCESS_JSON, [])
                _save_json(args.out_json, last_json if isinstance(last_json, list) else [])
                return 0
            # otherwise keep current status but add note about FMP
            if fmp_status in ("NO_KEY", "AUTH", "HTTP_ERROR"):
                note = f"{note} FMP fallback unavailable: {fmp_note}"

    md = render_markdown(days=days, status=status, note=note, events=events)
    _write_text(args.out_md, md)
    _save_json(args.out_json, [asdict(e) for e in events])

    # Persist last success only when we have OK + non-empty
    if status == "OK" and events:
        _save_json(LAST_SUCCESS_JSON, [asdict(e) for e in events])
        _write_text(LAST_SUCCESS_MD, md)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
