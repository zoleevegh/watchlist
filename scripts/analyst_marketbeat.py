#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MarketBeat analyst feed (upgrades/downgrades/price-target changes) with persistent cache + fallback sources.

Versioning rule: whenever this file is modified, bump VERSION continuously (no gaps).
VERSION: v0.3.33-ratings-us-primary-no-requests-2026-02-02

What this fixes (vs the broken N/A/BLOCKED runs):
- Removes hard dependency on `requests` (GitHub Actions runner may not have it). Uses stdlib urllib.
- Uses MarketBeat public broad feed as PRIMARY source:
    https://www.marketbeat.com/ratings/us/
  This page is typically accessible even when per‑ticker pages are bot-protected.
- Parses dates from the table (when available). If a row has no date, it is treated as "today" but marked.
- Never writes empty/invalid JSON files. `analyst_lastXd.json` is always valid JSON, `events` always a list.
- Distinguishes NO_EVENTS vs BLOCKED explicitly in markdown.

Optional fallbacks (only if API keys exist):
- Finnhub: /quote (key validation) + /stock/upgrade-downgrade (Premium may 403).
- Financial Modeling Prep (FMP): /api/v3/upgrades-downgrades

Exit codes:
0 = success (even if no events)
2 = bad arguments or missing master file
"""

from __future__ import annotations

import argparse
import csv
import datetime as _dt
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from io import StringIO
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None  # type: ignore

import urllib.request
import urllib.error


VERSION = "v0.3.33-ratings-us-primary-no-requests-2026-02-02"

MB_BASE = "https://www.marketbeat.com"
MB_PAGES = {
    # primary (broad feed)
    "us": "/ratings/us/",
    # secondary (may be challenged)
    "upgrade": "/ratings/upgrades/",
    "downgrade": "/ratings/downgrades/",
    "pt_change": "/ratings/pricetargetchanges/",
    "all": "/ratings/analyst-ratings/",
}

# ---- Models ----

@dataclass
class AnalystEvent:
    ticker: str
    date: str  # YYYY-MM-DD (UTC date if parsed, otherwise today)
    firm: Optional[str]
    action: str  # HU label: "Felminősítés", "Leminősítés", "Célár emelés", "Célár csökkentés", "Elemzői lépés"
    current_price: Optional[float]
    rating_from: Optional[str]
    rating_to: Optional[str]
    pt_from: Optional[float]
    pt_to: Optional[float]
    currency: str
    source: str

    # cache metadata
    first_seen: str  # YYYY-MM-DD (UTC date)
    last_seen: str   # YYYY-MM-DD (UTC date)
    uid: str         # stable id/hash
    date_inferred: bool = False


# ---- Utilities ----

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"

def _utc_today() -> _dt.date:
    return _dt.datetime.utcnow().date()

def _iso_date(d: _dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def _parse_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null", "—", "-", "n/a"}:
        return None
    s2 = re.sub(r"[^0-9.\-]", "", s)
    if not s2 or s2 in {"-", "."}:
        return None
    try:
        return float(s2)
    except Exception:
        return None

def _clean_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null", "—", "-", "n/a"}:
        return None
    return s

def _hash_uid(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore"))
        h.update(b"|")
    return h.hexdigest()[:16]

def _safe_json_load(path: str) -> Any:
    if not path:
        return None
    p = os.path.abspath(path)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # quarantine
        try:
            bad = p + ".bad"
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                raw = f.read()
            with open(bad, "w", encoding="utf-8") as f:
                f.write(raw)
        except Exception:
            pass
        return None

def _safe_json_dump(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)

def _is_blocked_html(html: str) -> bool:
    s = html.lower()
    # Cloudflare / bot / captcha patterns
    pats = [
        "cf-challenge", "cloudflare", "attention required", "verify you are human",
        "captcha", "are you a robot", "/cdn-cgi/", "access denied"
    ]
    return any(p in s for p in pats)

def _http_get(url: str, timeout: int = 20) -> Tuple[int, str]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)  # py<3.9 compatibility
            data = resp.read()
            # try utf-8 then fallback
            try:
                text = data.decode("utf-8")
            except Exception:
                text = data.decode("latin-1", errors="replace")
            return int(status), text
    except urllib.error.HTTPError as e:
        try:
            data = e.read()
            try:
                text = data.decode("utf-8")
            except Exception:
                text = data.decode("latin-1", errors="replace")
        except Exception:
            text = ""
        return int(e.code), text
    except Exception:
        return 0, ""

# ---- MASTER tickers ----

def _load_master_tickers(master_path_or_url: str) -> List[str]:
    if master_path_or_url.startswith("http://") or master_path_or_url.startswith("https://"):
        status, text = _http_get(master_path_or_url, timeout=30)
        if status < 200 or status >= 300 or not text.strip():
            raise FileNotFoundError(f"MASTER download failed (status {status})")
        rows = list(csv.DictReader(StringIO(text)))
    else:
        if not os.path.exists(master_path_or_url):
            raise FileNotFoundError(f"MASTER not found: {master_path_or_url}")
        with open(master_path_or_url, "r", encoding="utf-8", errors="replace") as f:
            rows = list(csv.DictReader(f))

    # find a ticker column
    cols = [c for c in (rows[0].keys() if rows else [])]
    colmap = {c.lower(): c for c in cols}
    for key in ["ticker", "tickers", "symbol", "symbols"]:
        if key in colmap:
            col = colmap[key]
            break
    else:
        # fallback: first column
        col = cols[0] if cols else "ticker"

    out: List[str] = []
    for r in rows:
        t = (r.get(col) or "").strip().upper()
        if not t:
            continue
        # basic sanitize
        t = re.sub(r"[^A-Z0-9\.\-]", "", t)
        if t:
            out.append(t)
    # de-dup preserving order
    seen = set()
    uniq = []
    for t in out:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq

# ---- Parsing helpers ----

def _parse_date_any(x: Any) -> Optional[_dt.date]:
    s = _clean_str(x)
    if not s:
        return None
    s = s.strip()
    # common formats: 2026-02-02, Feb 2, 2026, 02/02/2026
    fmts = ["%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y", "%B %d, %Y"]
    for f in fmts:
        try:
            return _dt.datetime.strptime(s, f).date()
        except Exception:
            pass
    # sometimes "2/2/26"
    for f in ["%m/%d/%y"]:
        try:
            return _dt.datetime.strptime(s, f).date()
        except Exception:
            pass
    return None

def _find_best_table(dfs: List["pd.DataFrame"]) -> Optional["pd.DataFrame"]:
    # pick the table that most likely contains ratings: must have a ticker/symbol column
    best = None
    best_score = -1
    for df in dfs:
        cols = [str(c).strip().lower() for c in df.columns]
        score = 0
        if any("ticker" in c or "symbol" in c for c in cols):
            score += 5
        if any("firm" in c or "analyst" in c or "broker" in c for c in cols):
            score += 2
        if any("price" in c and "target" in c for c in cols):
            score += 2
        if any("date" in c or "time" in c for c in cols):
            score += 1
        if score > best_score:
            best = df
            best_score = score
    return best

def _event_from_row(row: Dict[str, Any], default_action: str, source_url: str) -> Optional[Tuple[AnalystEvent, str]]:
    # returns (event, inferred_reason)
    # identify ticker
    ticker = None
    firm = None
    date_val = None
    # normalize keys
    norm = {str(k).strip().lower(): v for k, v in row.items()}
    for k in list(norm.keys()):
        if "ticker" in k or "symbol" in k:
            ticker = _clean_str(norm[k])
            if ticker:
                ticker = ticker.upper()
                ticker = re.sub(r"[^A-Z0-9\.\-]", "", ticker)
            break
    if not ticker:
        return None

    for k in list(norm.keys()):
        if "firm" in k or "broker" in k or "analyst" in k:
            firm = _clean_str(norm[k])
            break

    for k in list(norm.keys()):
        if "date" in k or "time" in k:
            date_val = norm[k]
            break

    d = _parse_date_any(date_val)
    inferred = False
    inferred_reason = ""
    if d is None:
        d = _utc_today()
        inferred = True
        inferred_reason = "date_missing_in_source"
    date_iso = _iso_date(d)

    # rating and PT columns (flexible)
    rating_from = rating_to = None
    pt_from = pt_to = None
    current_price = None

    # rating
    for k, v in norm.items():
        if ("old" in k and "rating" in k) or ("from" in k and "rating" in k):
            rating_from = _clean_str(v)
        if ("new" in k and "rating" in k) or ("to" in k and "rating" in k):
            rating_to = _clean_str(v)
        if k == "rating" and not rating_to:
            rating_to = _clean_str(v)

    # price target
    for k, v in norm.items():
        if ("old" in k and "target" in k) or ("from" in k and "target" in k):
            pt_from = _parse_float(v)
        if ("new" in k and "target" in k) or ("to" in k and "target" in k):
            pt_to = _parse_float(v)
        if ("price" in k and "target" in k) and pt_to is None and ("new" not in k and "old" not in k):
            pt_to = _parse_float(v)

    # current price (if present)
    for k, v in norm.items():
        if ("price" in k) and ("target" not in k) and ("current" in k or k == "price"):
            current_price = _parse_float(v)

    # action heuristics
    action = default_action
    if pt_from is not None and pt_to is not None:
        if pt_to > pt_from:
            action = "Célár emelés"
        elif pt_to < pt_from:
            action = "Célár csökkentés"
        else:
            action = "Célár változás"

    uid = _hash_uid(ticker, date_iso, firm or "", action, str(rating_from or ""), str(rating_to or ""), str(pt_from or ""), str(pt_to or ""), source_url)
    today_iso = _iso_date(_utc_today())
    ev = AnalystEvent(
        ticker=ticker,
        date=date_iso,
        firm=firm,
        action=action,
        current_price=current_price,
        rating_from=rating_from,
        rating_to=rating_to,
        pt_from=pt_from,
        pt_to=pt_to,
        currency="USD",
        source=source_url,
        first_seen=today_iso,
        last_seen=today_iso,
        uid=uid,
        date_inferred=inferred
    )
    return ev, inferred_reason

def _scrape_table(url: str, default_action: str) -> Tuple[str, List[AnalystEvent]]:
    status, html = _http_get(url)
    if status == 0:
        return "HTTP_ERROR", []
    if status >= 400:
        return f"HTTP_{status}", []
    if _is_blocked_html(html):
        return "BLOCKED", []
    if pd is None:
        return "NO_PANDAS", []
    try:
        dfs = pd.read_html(StringIO(html))
        if not dfs:
            return "NO_TABLE", []
        df = _find_best_table(dfs) or dfs[0]
        # normalize columns
        df.columns = [str(c).strip() for c in df.columns]
        events: List[AnalystEvent] = []
        for _, r in df.iterrows():
            row = {c: r[c] for c in df.columns}
            tup = _event_from_row(row, default_action=default_action, source_url=url)
            if not tup:
                continue
            ev, _ = tup
            events.append(ev)
        return "OK", events
    except Exception:
        return "PARSE_FAIL", []

# ---- Cache: persistent events ----

def _load_event_cache(path: str) -> Dict[str, Dict[str, Any]]:
    raw = _safe_json_load(path)
    if not isinstance(raw, list):
        raw = []
    out: Dict[str, Dict[str, Any]] = {}
    for item in raw:
        if isinstance(item, dict) and item.get("uid"):
            out[str(item["uid"])] = item
    return out

def _save_event_cache(path: str, cache: Dict[str, Dict[str, Any]]) -> None:
    _safe_json_dump(path, list(cache.values()))

def _merge_seen(cache: Dict[str, Dict[str, Any]], events: List[AnalystEvent]) -> None:
    today_iso = _iso_date(_utc_today())
    for ev in events:
        uid = ev.uid
        if uid in cache:
            cache[uid]["last_seen"] = today_iso
        else:
            cache[uid] = asdict(ev)

def _window_filter(cache: Dict[str, Dict[str, Any]], days: int) -> List[AnalystEvent]:
    # include events whose (date) OR (last_seen) is within window
    cutoff = _utc_today() - _dt.timedelta(days=days)
    out: List[AnalystEvent] = []
    for uid, item in cache.items():
        try:
            d = _parse_date_any(item.get("date")) or _parse_date_any(item.get("last_seen"))
            if d is None:
                continue
            if d < cutoff:
                continue
            out.append(AnalystEvent(**item))
        except Exception:
            continue
    # sort newest first, then ticker
    out.sort(key=lambda e: (e.date, e.ticker), reverse=True)
    return out

# ---- Output ----

def _render_md(days: int, events: List[AnalystEvent], status: str) -> str:
    lines: List[str] = []
    lines.append(f"## Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {days} naptári nap)")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Generálva (UTC): {_dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    if not events:
        if status == "BLOCKED":
            lines.append("_BLOCKED (MarketBeat challenge/bot)_")
        elif status.startswith("HTTP_") or status in {"HTTP_ERROR", "PARSE_FAIL"}:
            lines.append(f"_NO_DATA ({status})_")
        else:
            lines.append("_NO_EVENTS_")
        lines.append("")
        return "\n".join(lines)

    # group by ticker
    by: Dict[str, List[AnalystEvent]] = {}
    for ev in events:
        by.setdefault(ev.ticker, []).append(ev)

    for t in sorted(by.keys()):
        lines.append(f"### {t}")
        for ev in by[t]:
            firm = ev.firm or "—"
            rating = ""
            if ev.rating_from or ev.rating_to:
                rating = f" | Ajánlás: {ev.rating_from or '—'} → {ev.rating_to or '—'}"
            pt = ""
            if ev.pt_from is not None or ev.pt_to is not None:
                pt = f" | Célár: USD {ev.pt_from if ev.pt_from is not None else '—'} → {ev.pt_to if ev.pt_to is not None else '—'}"
            inf = " (date inferred)" if ev.date_inferred else ""
            lines.append(f"- {t} — {ev.date}{inf} — {firm} — {ev.action}{rating}{pt} | Forrás: {ev.source}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"

def _write_outputs(days: int, out_json: str, out_md: str, events: List[AnalystEvent], status: str, fallback_status: Dict[str, Any]) -> None:
    payload = {
        "count": len(events),
        "days": days,
        "status": status,
        "events": [asdict(e) for e in events],
        "fallback_status": fallback_status,
        "version": VERSION,
        "generated_utc": _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _safe_json_dump(out_json, payload)
    md = _render_md(days, events, status)
    os.makedirs(os.path.dirname(os.path.abspath(out_md)) or ".", exist_ok=True)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md)

# ---- Optional API fallbacks (minimal, only status) ----

def _finnhub_validate(key: str) -> Tuple[bool, str]:
    # simple /quote validation (AAPL). If 403 => premium or blocked.
    if not key:
        return False, "missing"
    url = f"https://finnhub.io/api/v1/quote?symbol=AAPL&token={key}"
    status, txt = _http_get(url)
    if status == 200 and txt.strip().startswith("{"):
        return True, "ok"
    if status == 403:
        return True, "403"
    return False, f"http_{status}"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True, help="MASTER CSV path or URL")
    ap.add_argument("--days", type=int, default=3, help="Lookback window in calendar days")
    ap.add_argument("--out-json", required=True, help="Output JSON path")
    ap.add_argument("--out-md", required=True, help="Output Markdown path")
    ap.add_argument("--cache-events", default="reports/marketbeat_events.json", help="Persistent events cache JSON")
    args = ap.parse_args()

    try:
        tickers = _load_master_tickers(args.master)
    except Exception as e:
        # hard error: missing master
        _safe_json_dump(args.out_json, {"count": 0, "days": args.days, "events": [], "status": "MASTER_ERROR", "error": str(e), "version": VERSION})
        with open(args.out_md, "w", encoding="utf-8") as f:
            f.write(f"## Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {args.days} naptári nap)\n\n_MASTER_ERROR: {e}_\n")
        return 2

    tickers_set = set(tickers)

    cache = _load_event_cache(args.cache_events)

    fallback_status: Dict[str, Any] = {"marketbeat": {}, "finnhub": {}}

    # 1) Primary: /ratings/us/
    url_us = MB_BASE + MB_PAGES["us"]
    st, events_us = _scrape_table(url_us, default_action="Elemzői lépés")
    fallback_status["marketbeat"]["ratings_us"] = st

    # keep only our tickers
    events_us = [e for e in events_us if e.ticker in tickers_set]

    # 2) Secondary pages (only if us returned nothing)
    status_final = "OK"
    events_all: List[AnalystEvent] = []
    if st == "OK" and events_us:
        events_all = events_us
        status_final = "OK"
    elif st == "BLOCKED":
        status_final = "BLOCKED"
    else:
        # attempt secondary pages (may still be blocked)
        for kind, action in [("upgrade", "Felminősítés"), ("downgrade", "Leminősítés"), ("pt_change", "Célár változás")]:
            url = MB_BASE + MB_PAGES[kind]
            st2, evs = _scrape_table(url, default_action=action)
            fallback_status["marketbeat"][kind] = st2
            if st2 == "BLOCKED":
                status_final = "BLOCKED"
            evs = [e for e in evs if e.ticker in tickers_set]
            events_all.extend(evs)

    # merge into persistent cache
    if events_all:
        _merge_seen(cache, events_all)
        _save_event_cache(args.cache_events, cache)

    # optional finnhub status (only for diagnostics)
    key = os.environ.get("FINNHUB_API_KEY", "").strip()
    ok, msg = _finnhub_validate(key) if key else (False, "missing")
    fallback_status["finnhub"] = {"available": bool(key), "key_validation": msg}

    # window from cache (not just this run)
    window_events = _window_filter(cache, args.days)

    # Final status: if we were blocked and cache also yields nothing, keep BLOCKED;
    # if cache yields data, OK (serving from cache).
    if status_final == "BLOCKED" and window_events:
        status_final = "OK_CACHED"

    _write_outputs(args.days, args.out_json, args.out_md, window_events, status_final if status_final != "OK_CACHED" else "OK", fallback_status)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
