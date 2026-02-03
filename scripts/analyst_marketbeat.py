#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyst_marketbeat.py — Analyst feed (upgrades/downgrades + price-target changes)

VERSION: v0.3.37-marketbeat-v0316-strategy-jina-firstseen-2026-02-03
Versioning rule: whenever this file is modified, bump VERSION continuously (no gaps).

IMÁDSÁG (2 sor):
bocsáss meg uram, mert balfék voltam, és nem azt a stratégiát használtam, ami már egyszer működött.
adj erőt, hogy a Jina-retry és a cache mindig életben tartsa az analyst feedet. 🙏

Key changes (requested): revert to the previously-working MarketBeat strategy (v0.3.16-style)
- Primary sources: lightweight category pages:
  /ratings/upgrades/ , /ratings/downgrades/ , /ratings/pricetargetchanges/ (plus initiations)
- Challenge detection even on HTTP 200 + retry via r.jina.ai proxy
- No per-row timestamp parsing; uses first_seen/last_seen cache (UTC) to keep items inside N-day windows
- Stdlib only (no requests); always writes outputs (no silent N/A due to exceptions)
- Optional fallback: FMP (if FMP_API_KEY) using per-symbol endpoints (v4 upgrades-downgrades, v4 price-target)
- Optional fallback: last_success snapshot (if MarketBeat blocked and FMP unavailable)

Exit codes:
- 0: outputs written (including BLOCKED/NO_EVENTS)
- 2: fatal config/I/O error (e.g., master unreadable)
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import time
import hashlib
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

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

_MB_LIGHT_PAGES = [
    "https://www.marketbeat.com/ratings/upgrades/",
    "https://www.marketbeat.com/ratings/downgrades/",
    "https://www.marketbeat.com/ratings/pricetargetchanges/",
    "https://www.marketbeat.com/ratings/initiations/",
]

@dataclass
class AnalystEvent:
    ticker: str
    date: str   # YYYY-MM-DD (we display last_seen UTC)
    action: str
    firm: str
    analyst: str
    rating: str
    pt: str
    summary: str
    source: str
    url: str

    def stable_id(self) -> str:
        raw = "|".join([
            self.ticker.upper().strip(),
            self.action.strip(),
            self.firm.strip(),
            self.analyst.strip(),
            self.rating.strip(),
            self.pt.strip(),
            self.url.strip(),
        ])
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

def _utc_today() -> dt.date:
    return dt.datetime.utcnow().date()

def _ensure_reports_dir() -> None:
    os.makedirs(REPORTS_DIR, exist_ok=True)

def _parse_date_yyyy_mm_dd(s: str) -> Optional[dt.date]:
    s = (s or "").strip()
    try:
        return dt.datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None

def _within_last_n_days(d: dt.date, days: int, today: dt.date) -> bool:
    start = today - dt.timedelta(days=days - 1)
    return start <= d <= today

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
        # quarantine corrupt
        try:
            if os.path.exists(path):
                os.replace(path, path + ".corrupt")
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
            status = int(getattr(resp, "status", 200))
            raw = resp.read()
            ctype = resp.headers.get("Content-Type", "") or ""
            enc = "utf-8"
            m = re.search(r"charset=([^\s;]+)", ctype, re.I)
            if m:
                enc = m.group(1)
            try:
                body = raw.decode(enc, errors="replace")
            except Exception:
                body = raw.decode("utf-8", errors="replace")
            return status, body, dict(resp.headers.items())
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

def _looks_blocked(status: int, body: str) -> bool:
    if status in (0, 403, 429, 503):
        return True
    b = (body or "").lower()
    for h in (
        "captcha", "cloudflare", "cf-challenge", "just a moment",
        "verify you are human", "attention required", "access denied",
        "enable javascript", "bot detection", "unusual traffic"
    ):
        if h in b:
            return True
    return False

def _jina_url(url: str) -> str:
    return f"https://r.jina.ai/{url}"

def _extract_tickers_from_master(master_csv_or_url: str) -> List[str]:
    parsed = urlparse(master_csv_or_url)
    if parsed.scheme in ("http", "https"):
        st, body, _ = _http_get(master_csv_or_url, timeout=DEFAULT_TIMEOUT)
        if st == 0 or st >= 400:
            raise RuntimeError(f"MASTER download failed (HTTP {st}).")
        rows = list(csv.DictReader(body.splitlines()))
    else:
        with open(master_csv_or_url, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    if not rows:
        return []
    keys = list(rows[0].keys())
    col = None
    for cand in ("Ticker", "ticker", "Symbol", "symbol"):
        if cand in keys:
            col = cand
            break
    if col is None:
        raise RuntimeError(f"MASTER CSV missing ticker column. Found columns: {keys}")
    out: List[str] = []
    seen = set()
    for r in rows:
        t = (r.get(col) or "").strip().upper()
        if not t:
            continue
        if re.fullmatch(r"[A-Z0-9\.\-]{1,12}", t) is None:
            continue
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

def _mb_parse_rows(html: str) -> List[List[str]]:
    if not html:
        return []
    # try to isolate first table with headers
    chunks = re.split(r"(?i)</table>", html)
    target = ""
    for ch in chunks:
        if re.search(r"(?i)\bcompany\b", ch) and re.search(r"(?i)\baction\b", ch):
            target = ch
            break
    if not target:
        target = html
    rows = re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", target)

    def clean(x: str) -> str:
        x = re.sub(r"(?is)<script.*?</script>", " ", x)
        x = re.sub(r"(?is)<style.*?</style>", " ", x)
        x = re.sub(r"(?is)<[^>]+>", " ", x)
        x = x.replace("&nbsp;", " ").replace("&amp;", "&").replace("&quot;", '"').replace("&#39;", "'")
        x = re.sub(r"\s+", " ", x).strip()
        return x

    out: List[List[str]] = []
    for tr in rows:
        cells = re.findall(r"(?is)<t[dh][^>]*>(.*?)</t[dh]>", tr)
        if len(cells) < 4:
            continue
        c = [clean(v) for v in cells]
        j = " ".join(c).lower()
        if "company" in j and "action" in j and "brokerage" in j:
            continue
        out.append(c)
    return out

def _mb_event_from_cells(cells: List[str], page_url: str, today: dt.date) -> Optional[AnalystEvent]:
    if not cells:
        return None
    c0 = cells[0] if len(cells) > 0 else ""
    c1 = cells[1] if len(cells) > 1 else ""
    c2 = cells[2] if len(cells) > 2 else ""
    c3 = cells[3] if len(cells) > 3 else ""
    rest = " | ".join(cells[:8])

    m = re.search(r"\b([A-Z]{1,6}(?:\.[A-Z]{1,3})?)\b", c0)
    if not m:
        m = re.search(r"\b([A-Z]{1,6}(?:\.[A-Z]{1,3})?)\b", rest)
    if not m:
        return None
    ticker = m.group(1).upper()

    action_txt = (c1 or "").strip()
    low = action_txt.lower()
    if "upgrad" in low:
        action = "Upgrade"
    elif "downgrad" in low:
        action = "Downgrade"
    elif "initiat" in low:
        action = "Initiated"
    elif "target" in low or "price target" in low:
        action = "Price Target"
    elif "reiterat" in low:
        action = "Reiterated"
    else:
        action = action_txt.replace(" by", "").strip() or "Rating"

    firm = (c2 or "").strip()
    analyst = (c3 or "").strip()

    rating = ""
    pt = ""

    # best-effort: search later cells for rating keywords or PT-like numbers
    for x in cells[4:9]:
        if not x:
            continue
        if not rating and re.fullmatch(r"(buy|hold|sell|overweight|underweight|neutral|outperform|underperform|market perform|sector perform)", x.strip(), re.I):
            rating = x.strip()
        if not pt and re.search(r"\b\d+(\.\d+)?\b", x):
            pt = x.strip()

    summary = rest[:400].strip()
    date_s = today.strftime("%Y-%m-%d")

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
        url=page_url,
    )

def fetch_marketbeat_events_v0316(tickers: List[str], days: int) -> Tuple[str, List[AnalystEvent], str]:
    today = _utc_today()
    tickset = set(tickers)

    first_seen: Dict[str, str] = _load_json(SEEN_PATH, {})
    cache: Dict[str, Dict[str, Any]] = _load_json(EVENTS_CACHE_PATH, {})

    def upsert(evt: AnalystEvent) -> None:
        eid = evt.stable_id()
        if eid not in first_seen:
            first_seen[eid] = today.isoformat()
        obj = asdict(evt)
        obj["first_seen"] = first_seen[eid]
        obj["last_seen"] = today.isoformat()
        cache[eid] = obj

    any_ok = False
    blocked_any = False
    http_err_any = False
    matched_rows = 0

    for page in _MB_LIGHT_PAGES:
        st, body, _ = _http_get(page, timeout=DEFAULT_TIMEOUT, headers={"Referer": "https://www.marketbeat.com/"})
        if _looks_blocked(st, body):
            st2, body2, _ = _http_get(_jina_url(page), timeout=DEFAULT_TIMEOUT, headers={"Referer": "https://r.jina.ai/"})
            if _looks_blocked(st2, body2) or st2 >= 400 or st2 == 0:
                blocked_any = True
                continue
            any_ok = True
            body = body2
        else:
            if st >= 400:
                http_err_any = True
                continue
            any_ok = True

        rows = _mb_parse_rows(body)
        for cells in rows:
            evt = _mb_event_from_cells(cells, page_url=page, today=today)
            if not evt:
                continue
            if evt.ticker not in tickset:
                continue
            upsert(evt)
            matched_rows += 1

        time.sleep(0.35)

    _save_json(SEEN_PATH, first_seen)
    _save_json(EVENTS_CACHE_PATH, cache)

    out: List[AnalystEvent] = []
    for eid, obj in cache.items():
        try:
            t = (obj.get("ticker") or "").upper()
            if t not in tickset:
                continue
            last_seen_s = str(obj.get("last_seen") or obj.get("date") or "")
            d = _parse_date_yyyy_mm_dd(last_seen_s) or today
            if not _within_last_n_days(d, days, today=today):
                continue
            obj2 = dict(obj)
            obj2["date"] = d.isoformat()
            obj2.pop("first_seen", None)
            obj2.pop("last_seen", None)
            out.append(AnalystEvent(**obj2))
        except Exception:
            continue

    out.sort(key=lambda e: (e.date, e.ticker), reverse=True)

    if not any_ok:
        if blocked_any:
            return "BLOCKED", out, "MarketBeat challenge/bot or blocked from CI (even after Jina retry)."
        if http_err_any:
            return "HTTP_ERROR", out, "MarketBeat HTTP error."
        return "HTTP_ERROR", out, "MarketBeat no response."
    if not out:
        return "NO_EVENTS", [], "No events matched MASTER tickers in cache/window."
    return "OK", out, f"MarketBeat OK (matched rows: {matched_rows})."

# ---- FMP fallback (per-symbol endpoints) ----
def _fmp_get_json(url: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[int, Any]:
    st, body, _ = _http_get(url, timeout=timeout, headers={"Accept": "application/json,*/*;q=0.8"})
    if st == 0:
        return 0, None
    try:
        return st, json.loads(body) if body else None
    except Exception:
        return st, None

def fetch_fmp_events(tickers: List[str], days: int) -> Tuple[str, List[AnalystEvent], str]:
    apikey = (os.environ.get("FMP_API_KEY") or "").strip()
    if not apikey:
        return "NO_KEY", [], "FMP_API_KEY missing."

    probe_url = f"https://financialmodelingprep.com/api/v3/quote/AAPL?{urlencode({'apikey': apikey})}"
    st, data = _fmp_get_json(probe_url, timeout=min(10, DEFAULT_TIMEOUT))
    if st in (401, 402, 403):
        return "AUTH", [], f"FMP auth failed (HTTP {st})."
    if st == 429:
        return "RATE_LIMIT", [], "FMP rate limit hit (HTTP 429)."
    if st == 0 or st >= 400:
        return "HTTP_ERROR", [], f"FMP probe HTTP error (HTTP {st})."
    if isinstance(data, dict) and data.get("Error Message"):
        return "AUTH", [], f"FMP error: {data.get('Error Message')}"

    today = _utc_today()
    cutoff = today - dt.timedelta(days=max(0, days - 1))
    tickset = set([t.strip().upper() for t in tickers if t and t.strip()])

    def _parse_fmp_date(s: str) -> Optional[dt.date]:
        if not s:
            return None
        s = str(s).strip()
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y"):
            try:
                return dt.datetime.strptime(s[:len(fmt)], fmt).date()
            except Exception:
                continue
        try:
            return dt.datetime.fromisoformat(s.replace("Z", "")[:19]).date()
        except Exception:
            return None

    def _fetch_symbol(sym: str) -> Tuple[List[AnalystEvent], Optional[int]]:
        sym = sym.upper()
        out: List[AnalystEvent] = []

        u_url = f"https://financialmodelingprep.com/api/v4/upgrades-downgrades?{urlencode({'symbol': sym, 'apikey': apikey})}"
        u_st, u_data = _fmp_get_json(u_url)
        if u_st in (401, 402, 403):
            return out, u_st
        if u_st in (429, 0) or u_st >= 400:
            return out, u_st or 520

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
                    newg = (r.get("newGrade") or r.get("newRating") or r.get("toGrade") or r.get("action") or "").strip()
                    oldg = (r.get("previousGrade") or r.get("previousRating") or r.get("fromGrade") or "").strip()
                    act = f"{oldg} → {newg}".strip(" →") if (oldg or newg) else "Rating update"
                    summary = (r.get("newsTitle") or r.get("notes") or "").strip()
                    out.append(AnalystEvent(
                        ticker=t, date=d.isoformat(), action=act,
                        firm=firm or "FMP", analyst=analyst, rating=newg, pt="",
                        summary=summary, source="FMP:upgrades-downgrades", url=""
                    ))
                except Exception:
                    continue

        pt_url = f"https://financialmodelingprep.com/api/v4/price-target?{urlencode({'symbol': sym, 'apikey': apikey})}"
        pt_st, pt_data = _fmp_get_json(pt_url)
        if pt_st in (401, 402, 403):
            return out, pt_st
        if pt_st in (429, 0) or pt_st >= 400:
            return out, pt_st or 520

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
                    pt = str(r.get("priceTarget") or r.get("adjPriceTarget") or r.get("target") or "").strip()
                    pt_old = str(r.get("priceTargetOld") or r.get("oldPriceTarget") or "").strip()
                    act = f"PT {pt_old} → {pt}".strip(" →") if (pt_old or pt) else "Price target update"
                    summary = (r.get("newsTitle") or r.get("notes") or "").strip()
                    out.append(AnalystEvent(
                        ticker=t, date=d.isoformat(), action=act,
                        firm=firm or "FMP", analyst=analyst, rating="", pt=pt,
                        summary=summary, source="FMP:price-target", url=""
                    ))
                except Exception:
                    continue

        return out, None

    # small concurrency
    from concurrent.futures import ThreadPoolExecutor, as_completed
    workers = int(os.environ.get("FMP_WORKERS") or "8")
    workers = max(2, min(12, workers))

    all_events: List[AnalystEvent] = []
    probs: List[int] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_fetch_symbol, sym): sym for sym in sorted(tickset)}
        for fut in as_completed(futs):
            evs, prob = fut.result()
            if evs:
                all_events.extend(evs)
            if prob is not None:
                probs.append(prob)

    all_events.sort(key=lambda e: (e.date, e.ticker), reverse=True)
    if probs:
        if any(p == 429 for p in probs):
            return "RATE_LIMIT", all_events, "FMP rate limit (partial)."
        if any(p in (401, 402, 403) for p in probs):
            return "AUTH", all_events, "FMP auth/plan issue (partial)."
        return "HTTP_ERROR", all_events, "FMP HTTP errors (partial)."

    return ("OK" if all_events else "NO_EVENTS"), all_events, ("FMP OK." if all_events else "FMP no events in window.")

def render_markdown(days: int, status: str, note: str, events: List[AnalystEvent]) -> str:
    title = f"## Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {days} naptári nap)"
    if status in ("BLOCKED", "HTTP_ERROR"):
        return f"{title}\n\n_BLOCKED_ ({note})\n"
    if status in ("NO_EVENTS",):
        return f"{title}\n\n_NO_EVENTS_\n"
    if status in ("NO_KEY", "AUTH", "RATE_LIMIT"):
        return f"{title}\n\n_BLOCKED_ ({note})\n"

    by: Dict[str, List[AnalystEvent]] = {}
    for e in events:
        by.setdefault(e.ticker, []).append(e)

    lines = [title, ""]
    for t in sorted(by.keys()):
        lines.append(f"**{t}**")
        for e in by[t][:20]:
            bits = [e.date, e.action]
            if e.firm: bits.append(e.firm)
            if e.analyst: bits.append(e.analyst)
            if e.rating: bits.append(e.rating)
            if e.pt: bits.append(f"PT {e.pt}")
            if e.source and e.source != "MarketBeat": bits.append(e.source)
            lines.append("- " + " | ".join(bits))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-json", required=True)
    args = ap.parse_args()

    days = int(args.days)
    if days < 1 or days > 30:
        days = 3

    _ensure_reports_dir()

    try:
        tickers = _extract_tickers_from_master(args.master)
    except Exception:
        md = f"## Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {days} naptári nap)\n\n_N/A._\n"
        _write_text(args.out_md, md)
        _save_json(args.out_json, [])
        return 2

    status, events, note = fetch_marketbeat_events_v0316(tickers, days)

    # fallback: FMP if MarketBeat blocked or empty
    if status in ("BLOCKED", "HTTP_ERROR") or (status == "NO_EVENTS" and not events):
        fmp_status, fmp_events, fmp_note = fetch_fmp_events(tickers, days)
        if fmp_status == "OK" and fmp_events:
            status, events, note = "OK", fmp_events, fmp_note
        else:
            last_md = _read_text(LAST_SUCCESS_MD)
            if last_md and status != "NO_EVENTS":
                _write_text(args.out_md, last_md)
                last_json = _load_json(LAST_SUCCESS_JSON, [])
                _save_json(args.out_json, last_json if isinstance(last_json, list) else [])
                return 0
            if fmp_status in ("NO_KEY", "AUTH", "HTTP_ERROR", "RATE_LIMIT"):
                note = f"{note} FMP fallback unavailable: {fmp_note}"

    md = render_markdown(days, status, note, events)
    _write_text(args.out_md, md)
    _save_json(args.out_json, [asdict(e) for e in events])

    if status == "OK" and events:
        _save_json(LAST_SUCCESS_JSON, [asdict(e) for e in events])
        _write_text(LAST_SUCCESS_MD, md)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
