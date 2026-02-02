#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MarketBeat analyst feed (upgrades/downgrades/price-target changes) with persistent cache + API fallbacks.

Versioning rule: whenever this file is modified, bump VERSION continuously (no gaps).
VERSION: v0.3.28-marketbeat-masterurl-parsefallback-2026-02-02

Key fixes vs v0.3.27:
- Robust cache loading: 'placeholder' / invalid JSON no longer crashes (auto-resets + .bad copy).
- Persistent event cache (marketbeat_events.json) stores full events with first_seen/last_seen so older events
  that disappear from MarketBeat's "latest" pages can still be reported within the last N calendar days.
- Challenge / robot-protection detection: HTTP 200 with bot page is treated as blocked.
- Output always includes ticker header; never exits non‑zero when data is simply empty.

Additional fixes in v0.3.28:
- MASTER can be a local CSV path OR an http(s) URL (workflow may pass URL when download fails).
- If MarketBeat parsing fails due to missing lxml/pandas issues, fallbacks can still run (FMP/Finnhub).

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
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

try:
    import pandas as pd
except Exception as e:
    pd = None  # type: ignore


VERSION = "v0.3.28-marketbeat-masterurl-parsefallback-2026-02-02"

MB_BASE = "https://www.marketbeat.com"
MB_PAGES = {
    "upgrade": "/ratings/upgrades/",
    "downgrade": "/ratings/downgrades/",
    "pt_change": "/ratings/pricetargetchanges/",
    "all": "/ratings/analyst-ratings/",
}

# ---- Models ----

@dataclass
class AnalystEvent:
    ticker: str
    date: Optional[str]  # YYYY-MM-DD (announcement/effective date if parsed)
    firm: Optional[str]
    action: str  # HU label, e.g. "Felminősítés", "Leminősítés", "Célár emelés", "Célár csökkentés", "Célár változás"
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


# ---- Utilities ----

def _utc_today() -> _dt.date:
    return _dt.datetime.utcnow().date()

def _parse_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null", "—", "-", "n/a"}:
        return None
    # keep digits, dot, minus
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

def _iso_date(d: _dt.date) -> str:
    return d.strftime("%Y-%m-%d")

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
        # keep a forensic copy
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
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)

def _read_master_tickers(master_csv_path: str) -> List[str]:
    """Read tickers from a local CSV file OR from an http(s) CSV URL.

    Workflow safety: if MASTER download fails, the workflow passes the URL directly.
    The older versions crashed in this case because they only handled local paths.
    """

    csv_text: Optional[str] = None
    if re.match(r"^https?://", (master_csv_path or ""), flags=re.IGNORECASE):
        # Download CSV
        try:
            r = requests.get(master_csv_path, timeout=25)
            if r.status_code != 200 or not (r.text or "").strip():
                raise FileNotFoundError(master_csv_path)
            csv_text = r.text
        except Exception:
            raise FileNotFoundError(master_csv_path)
    else:
        if not os.path.exists(master_csv_path):
            raise FileNotFoundError(master_csv_path)
        with open(master_csv_path, "r", encoding="utf-8", errors="ignore") as f0:
            csv_text = f0.read()

    assert csv_text is not None
    # Use DictReader on an in-memory stream for both cases
    import io
    f = io.StringIO(csv_text)
    with f:
        # sniff header
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample) if sample.strip() else csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        # common header names
        candidates = ["ticker", "Ticker", "TICKER", "Symbol", "symbol"]
        col = None
        for c in candidates:
            if c in headers:
                col = c
                break
        if col is None:
            # fallback: first column
            col = headers[0] if headers else None
        if col is None:
            return []
        out = []
        for row in reader:
            t = (row.get(col) or "").strip()
            if not t:
                continue
            t = t.upper()
            # keep common formats like PKN.WA; exclude weird
            out.append(t)
        return sorted(set(out))

def _make_uid(ev: Dict[str, Any]) -> str:
    key = "|".join([
        (ev.get("ticker") or "").upper(),
        str(ev.get("date") or ""),
        str(ev.get("firm") or ""),
        str(ev.get("action") or ""),
        str(ev.get("rating_from") or ""),
        str(ev.get("rating_to") or ""),
        str(ev.get("pt_from") or ""),
        str(ev.get("pt_to") or ""),
        str(ev.get("currency") or "USD"),
        str(ev.get("source") or ""),
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def _parse_ticker_from_text(s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    # (AAPL) at end
    m = re.search(r"\(([A-Z0-9.\-]{1,12})\)\s*$", s)
    if m:
        return m.group(1)
    # standalone ticker in first token
    m = re.match(r"^([A-Z0-9.\-]{1,12})\b", s)
    if m and len(m.group(1)) <= 8:
        return m.group(1)
    return None

def _detect_bot_page(html: str) -> bool:
    if not html:
        return True
    low = html.lower()
    needles = [
        "checking your browser",
        "please verify you are a human",
        "enable javascript",
        "cloudflare",
        "cf-chl",
        "captcha",
        "access denied",
        "unusual traffic",
    ]
    return any(n in low for n in needles)

def _http_get(session: requests.Session, url: str, timeout: int = 20) -> Tuple[int, str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "close",
    }
    r = session.get(url, headers=headers, timeout=timeout)
    return r.status_code, r.text or ""

def _pd_read_tables(html: str) -> List[Any]:
    if pd is None:
        return []
    try:
        # MarketBeat sometimes has multiple tables; first "Ratings" table is enough.
        return pd.read_html(html)
    except Exception:
        return []

def _parse_date_any(x: Any) -> Optional[str]:
    s = _clean_str(x)
    if not s:
        return None
    # common formats: "Jan 27, 2026" or "2026-01-27"
    s = s.replace("\u00a0", " ").strip()
    for fmt in ("%Y-%m-%d", "%b %d, %Y", "%B %d, %Y", "%m/%d/%Y"):
        try:
            d = _dt.datetime.strptime(s, fmt).date()
            return _iso_date(d)
        except Exception:
            pass
    # last resort: regex YYYY-MM-DD
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    return None

def _action_hu_from_page(page_key: str, row: Dict[str, Any]) -> str:
    # upgrade/downgrade pages are clear; PT changes derive direction if old/new exist.
    if page_key == "upgrade":
        return "Felminősítés"
    if page_key == "downgrade":
        return "Leminősítés"
    if page_key == "pt_change":
        # determine direction
        pf = _parse_float(row.get("pt_from"))
        pt = _parse_float(row.get("pt_to"))
        if pf is not None and pt is not None:
            if pt > pf:
                return "Célár emelés"
            if pt < pf:
                return "Célár csökkentés"
        return "Célár változás"
    return "Elemzői frissítés"

def _extract_from_table(table, page_key: str, source_url: str) -> List[Dict[str, Any]]:
    """
    Heuristic extraction that tolerates column name changes.
    Returns raw dict events (without cache metadata).
    """
    events: List[Dict[str, Any]] = []
    if table is None:
        return events

    # Normalize columns
    cols = [str(c).strip() for c in list(getattr(table, "columns", []))]
    # Build row dicts
    for _, r in table.iterrows():
        row = {str(k).strip(): r[k] for k in cols}

        # ticker
        ticker = None
        for k in cols:
            lk = k.lower()
            if lk in {"ticker", "symbol"}:
                ticker = _clean_str(row.get(k))
                break
        if not ticker:
            # sometimes in "Company" / "Stock" column
            for k in cols:
                lk = k.lower()
                if "company" in lk or "stock" in lk or "security" in lk:
                    ticker = _parse_ticker_from_text(_clean_str(row.get(k)) or "")
                    if ticker:
                        break
        if not ticker:
            continue
        ticker = ticker.upper()

        # date
        date = None
        for k in cols:
            lk = k.lower()
            if "date" in lk:
                date = _parse_date_any(row.get(k))
                if date:
                    break

        # firm / analyst
        firm = None
        for k in cols:
            lk = k.lower()
            if "firm" in lk or "analyst" in lk or "broker" in lk:
                firm = _clean_str(row.get(k))
                if firm:
                    break

        # ratings
        rating_from = None
        rating_to = None
        for k in cols:
            lk = k.lower()
            if "old rating" in lk or "from rating" in lk or lk == "from":
                rating_from = _clean_str(row.get(k))
            if "new rating" in lk or "to rating" in lk or lk == "to":
                rating_to = _clean_str(row.get(k))
        # combined rating change column
        if not (rating_from or rating_to):
            for k in cols:
                lk = k.lower()
                if "rating" in lk and "change" in lk:
                    s = _clean_str(row.get(k)) or ""
                    m = re.search(r"(.+?)\s*(?:→|->)\s*(.+)", s)
                    if m:
                        rating_from = _clean_str(m.group(1))
                        rating_to = _clean_str(m.group(2))
                        break

        # price target
        pt_from = None
        pt_to = None
        for k in cols:
            lk = k.lower()
            if ("old" in lk and "target" in lk) or ("from" in lk and "target" in lk) or lk in {"old pt", "old price target"}:
                pt_from = _parse_float(row.get(k))
            if ("new" in lk and "target" in lk) or ("to" in lk and "target" in lk) or lk in {"new pt", "new price target"}:
                pt_to = _parse_float(row.get(k))
        if pt_from is None or pt_to is None:
            for k in cols:
                lk = k.lower()
                if "price target" in lk and ("change" in lk or "target" == lk):
                    s = _clean_str(row.get(k)) or ""
                    # examples: "$100.00 → $140.00" or "100 to 140"
                    m = re.search(r"([0-9][0-9,\.]*)\s*(?:→|->|to)\s*([0-9][0-9,\.]*)", s)
                    if m:
                        pt_from = _parse_float(m.group(1))
                        pt_to = _parse_float(m.group(2))
                        break

        # current price
        current_price = None
        for k in cols:
            lk = k.lower()
            if "current price" in lk or lk == "price":
                current_price = _parse_float(row.get(k))
                if current_price is not None:
                    break

        action = _action_hu_from_page(page_key, {"pt_from": pt_from, "pt_to": pt_to})

        events.append({
            "ticker": ticker,
            "date": date,
            "firm": firm,
            "action": action,
            "current_price": current_price,
            "rating_from": rating_from,
            "rating_to": rating_to,
            "pt_from": pt_from,
            "pt_to": pt_to,
            "currency": "USD",
            "source": source_url,
        })
    return events

def _fetch_marketbeat_events(tickers: set, max_pages_each: int = 2) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Scrape MarketBeat rating pages.
    Returns (events_for_tickers, status_dict).
    """
    status: Dict[str, Any] = {}
    all_events: List[Dict[str, Any]] = []

    any_table_parsed = False
    if pd is None:
        status["pandas_unavailable"] = True

    with requests.Session() as s:
        # warmup
        st, html = _http_get(s, MB_BASE + "/", timeout=15)
        status["warmup_home"] = st

        for key, path in [("ratings_upgrade", MB_PAGES["upgrade"]),
                          ("ratings_downgrade", MB_PAGES["downgrade"]),
                          ("ratings_pt_change", MB_PAGES["pt_change"])]:
            page_key = "upgrade" if "upgrade" in key else ("downgrade" if "downgrade" in key else "pt_change")
            url0 = MB_BASE + path
            st0, html0 = _http_get(s, url0, timeout=20)
            status[key] = st0

            if st0 != 200 or _detect_bot_page(html0):
                status[key + "_blocked"] = True
                continue

            # pagination: MarketBeat uses ?p=2 in some pages; also /?page=2 in others. We'll try both.
            html_pages = [html0]
            for p in range(2, max_pages_each + 1):
                url = url0 + f"?p={p}"
                stp, hp = _http_get(s, url, timeout=20)
                if stp != 200 or _detect_bot_page(hp):
                    break
                html_pages.append(hp)

            for hp in html_pages:
                tables = _pd_read_tables(hp)
                if not tables:
                    continue
                any_table_parsed = True
                # find the most "ratings-like" table: has a ticker/symbol column or company column.
                chosen = None
                for t in tables:
                    cols = [str(c).lower() for c in getattr(t, "columns", [])]
                    if any(c in {"ticker", "symbol"} for c in cols) or any("company" in c for c in cols):
                        chosen = t
                        break
                if chosen is None:
                    chosen = tables[0]
                evs = _extract_from_table(chosen, page_key=page_key, source_url=url0)
                for ev in evs:
                    if ev["ticker"] in tickers:
                        all_events.append(ev)

    if not any_table_parsed and not any(status.get(k + "_blocked") for k in ["ratings_upgrade", "ratings_downgrade", "ratings_pt_change"]):
        # Common CI issue: pandas.read_html can't parse because lxml isn't installed.
        status["parse_failed"] = True

    # de-dup raw events
    uniq = {}
    for ev in all_events:
        uid = _make_uid(ev)
        if uid not in uniq:
            uniq[uid] = ev
    return list(uniq.values()), status

# ---- Fallbacks ----

def _finnhub_validate_key(token: str) -> Tuple[bool, str]:
    try:
        url = "https://finnhub.io/api/v1/quote"
        r = requests.get(url, params={"symbol": "AAPL", "token": token}, timeout=15)
        if r.status_code == 200:
            js = r.json()
            # valid keys return dict with 'c' current price key
            if isinstance(js, dict) and "c" in js:
                return True, "ok"
        if r.status_code in (401, 403):
            return False, f"auth_{r.status_code}"
        return False, f"http_{r.status_code}"
    except Exception as e:
        return False, "error"

def _finnhub_updown(token: str, tickers: Iterable[str], days: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    status: Dict[str, Any] = {"provider": "Finnhub"}
    ok, msg = _finnhub_validate_key(token)
    status["key_validation"] = msg
    if not ok:
        status["available"] = False
        return [], status
    status["available"] = True

    cutoff = _utc_today() - _dt.timedelta(days=days)
    out: List[Dict[str, Any]] = []
    for t in tickers:
        try:
            url = "https://finnhub.io/api/v1/stock/upgrade-downgrade"
            r = requests.get(url, params={"symbol": t, "token": token}, timeout=20)
            status.setdefault("per_ticker_http", {})[t] = r.status_code
            if r.status_code == 403:
                # premium gating (common)
                status["premium_or_blocked"] = True
                continue
            if r.status_code != 200:
                continue
            js = r.json()
            if not isinstance(js, list):
                continue
            for item in js:
                # item keys: grade, fromGrade, toGrade, action, firm, pt, fromPT, toPT, date etc. (varies)
                d = _parse_date_any(item.get("date"))
                if not d:
                    continue
                try:
                    dd = _dt.datetime.strptime(d, "%Y-%m-%d").date()
                except Exception:
                    continue
                if dd < cutoff:
                    continue
                action_raw = (item.get("action") or "").lower()
                action = "Elemzői frissítés"
                if "up" in action_raw:
                    action = "Felminősítés"
                elif "down" in action_raw:
                    action = "Leminősítés"
                ev = {
                    "ticker": t,
                    "date": d,
                    "firm": _clean_str(item.get("firm")),
                    "action": action,
                    "current_price": None,
                    "rating_from": _clean_str(item.get("fromGrade")),
                    "rating_to": _clean_str(item.get("toGrade")),
                    "pt_from": _parse_float(item.get("fromPT")),
                    "pt_to": _parse_float(item.get("toPT")),
                    "currency": "USD",
                    "source": "https://finnhub.io/docs/api/upgrade-downgrade",
                }
                out.append(ev)
        except Exception:
            continue

    # de-dup
    uniq = {}
    for ev in out:
        uid = _make_uid(ev)
        if uid not in uniq:
            uniq[uid] = ev
    return list(uniq.values()), status

def _fmp_updown(apikey: str, tickers: Iterable[str], days: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    status: Dict[str, Any] = {"provider": "FMP"}
    url = "https://financialmodelingprep.com/api/v3/upgrades-downgrades"
    try:
        r = requests.get(url, params={"apikey": apikey}, timeout=25)
        status["http"] = r.status_code
        if r.status_code != 200:
            return [], status
        js = r.json()
        if not isinstance(js, list):
            return [], status
    except Exception:
        status["error"] = True
        return [], status

    wanted = set(tickers)
    cutoff = _utc_today() - _dt.timedelta(days=days)
    out: List[Dict[str, Any]] = []
    for item in js:
        try:
            t = (item.get("symbol") or item.get("ticker") or "").upper().strip()
            if not t or t not in wanted:
                continue
            d = _parse_date_any(item.get("publishedDate") or item.get("date"))
            if not d:
                continue
            dd = _dt.datetime.strptime(d, "%Y-%m-%d").date()
            if dd < cutoff:
                continue
            action_raw = (item.get("action") or "").lower()
            action = "Elemzői frissítés"
            if "upgrade" in action_raw:
                action = "Felminősítés"
            elif "downgrade" in action_raw:
                action = "Leminősítés"
            # price target
            pf = _parse_float(item.get("oldPriceTarget") or item.get("pt_from"))
            pt = _parse_float(item.get("newPriceTarget") or item.get("pt_to"))
            if pf is not None and pt is not None:
                if pt > pf:
                    action = "Célár emelés"
                elif pt < pf:
                    action = "Célár csökkentés"
                else:
                    action = "Célár változás"

            ev = {
                "ticker": t,
                "date": d,
                "firm": _clean_str(item.get("analystFirm") or item.get("firm")),
                "action": action,
                "current_price": None,
                "rating_from": _clean_str(item.get("oldRating") or item.get("fromGrade")),
                "rating_to": _clean_str(item.get("newRating") or item.get("toGrade")),
                "pt_from": pf,
                "pt_to": pt,
                "currency": "USD",
                "source": "https://financialmodelingprep.com/developer/docs/#Upgrades-and-Downgrades",
            }
            out.append(ev)
        except Exception:
            continue

    uniq = {}
    for ev in out:
        uid = _make_uid(ev)
        if uid not in uniq:
            uniq[uid] = ev
    return list(uniq.values()), status

# ---- Cache handling ----

def _load_event_cache(path: str) -> Dict[str, Any]:
    obj = _safe_json_load(path)
    if isinstance(obj, dict):
        # expected: {"events": {...}} or flat map uid->event
        return obj
    return {}

def _normalize_event_cache(obj: Dict[str, Any]) -> Dict[str, Any]:
    if "events" in obj and isinstance(obj["events"], dict):
        return obj
    # allow flat map uid->event
    if obj and all(isinstance(v, dict) for v in obj.values()):
        return {"events": obj}
    return {"events": {}}

def _merge_events_into_cache(cache: Dict[str, Any], fresh_events: List[Dict[str, Any]], seen_date: str) -> None:
    cache = _normalize_event_cache(cache)
    events = cache["events"]
    assert isinstance(events, dict)
    for ev in fresh_events:
        uid = _make_uid(ev)
        if uid not in events or not isinstance(events.get(uid), dict):
            # new
            ev2 = dict(ev)
            ev2["first_seen"] = seen_date
            ev2["last_seen"] = seen_date
            ev2["uid"] = uid
            events[uid] = ev2
        else:
            # update last_seen + best-effort fill missing fields
            cur = events[uid]
            cur["last_seen"] = seen_date
            for k, v in ev.items():
                if cur.get(k) in (None, "", "—") and v not in (None, "", "—"):
                    cur[k] = v

def _events_within_days(cache: Dict[str, Any], tickers: set, days: int, today: _dt.date) -> List[Dict[str, Any]]:
    cache = _normalize_event_cache(cache)
    events = cache["events"]
    if not isinstance(events, dict):
        return []
    cutoff = today - _dt.timedelta(days=days)
    out = []
    for uid, ev in events.items():
        try:
            if not isinstance(ev, dict):
                continue
            t = (ev.get("ticker") or "").upper()
            if not t or t not in tickers:
                continue

            # effective date: prefer announced date; fallback to last_seen; fallback first_seen
            d = ev.get("date") or ev.get("last_seen") or ev.get("first_seen")
            d_iso = _parse_date_any(d) or None
            if not d_iso:
                continue
            dd = _dt.datetime.strptime(d_iso, "%Y-%m-%d").date()
            if dd < cutoff:
                continue
            out.append(ev)
        except Exception:
            continue

    # sort newest first by effective date
    def _key(ev: Dict[str, Any]) -> Tuple[str, str]:
        d = ev.get("date") or ev.get("last_seen") or ev.get("first_seen") or "0000-00-00"
        return (str(d), str(ev.get("ticker") or ""))

    out.sort(key=_key, reverse=True)
    return out

# ---- Rendering ----

def _render_md(events: List[Dict[str, Any]], days: int, generated_utc: str) -> str:
    lines = []
    lines.append(f"## Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {days} naptári nap)\n")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Generálva (UTC): {generated_utc}\n")

    if not events:
        lines.append("_N/A._\n")
        return "\n".join(lines)

    # group by ticker
    by: Dict[str, List[Dict[str, Any]]] = {}
    for ev in events:
        t = (ev.get("ticker") or "").upper()
        if not t:
            continue
        by.setdefault(t, []).append(ev)

    for t in sorted(by.keys()):
        lines.append(f"## {t}")
        for ev in by[t]:
            d = ev.get("date") or ev.get("last_seen") or ev.get("first_seen") or "—"
            firm = ev.get("firm") or "—"
            action = ev.get("action") or "—"
            cp = ev.get("current_price")
            cp_s = f"USD {cp:.2f}" if isinstance(cp, (int, float)) else "—"
            rf = ev.get("rating_from") or "—"
            rt = ev.get("rating_to") or "—"
            pf = ev.get("pt_from")
            pt = ev.get("pt_to")
            if isinstance(pf, (int, float)) and isinstance(pt, (int, float)):
                pt_s = f"USD {pf:.2f} → {pt:.2f}"
            elif isinstance(pt, (int, float)):
                pt_s = f"→ USD {pt:.2f}"
            else:
                pt_s = "—"
            src = ev.get("source") or "—"
            lines.append(f"- {t} — {d} — {firm} — {action} | Ár: {cp_s} | Ajánlás: {rf} → {rt} | Célár: {pt_s} | Forrás: {src}")
        lines.append("")  # blank line

    return "\n".join(lines).rstrip() + "\n"

# ---- Main ----

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="MarketBeat analyst feed scraper with cache + fallbacks.")
    ap.add_argument("--master", required=True, help="Path to master.csv (must contain tickers)")
    ap.add_argument("--days", type=int, default=3, help="Lookback window in calendar days (default 3)")
    ap.add_argument("--out-json", required=True, help="Output JSON path")
    ap.add_argument("--out-md", required=True, help="Output Markdown path")
    ap.add_argument("--cache-events", default="reports/marketbeat_events.json", help="Persistent events cache JSON")
    ap.add_argument("--cache-seen", default="reports/marketbeat_seen.json", help="Seen uid->last_seen map (legacy)")
    ap.add_argument("--max-pages", type=int, default=2, help="Max pages per MarketBeat category to scrape")
    args = ap.parse_args(argv)

    if args.days < 1 or args.days > 60:
        print("Invalid --days (1..60)", file=sys.stderr)
        return 2

    try:
        tickers = set(_read_master_tickers(args.master))
    except FileNotFoundError:
        print(f"MASTER not found: {args.master}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"MASTER read error: {e}", file=sys.stderr)
        return 2

    today = _utc_today()
    seen_date = _iso_date(today)
    generated_utc = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # load caches
    cache_obj = _load_event_cache(args.cache_events)
    cache_obj = _normalize_event_cache(cache_obj)
    seen_map = _safe_json_load(args.cache_seen)
    if not isinstance(seen_map, dict):
        seen_map = {}

    # scrape MarketBeat
    fresh, status = _fetch_marketbeat_events(tickers=tickers, max_pages_each=args.max_pages)

    # If MarketBeat yielded nothing, try fallbacks (best effort).
    # Reasons:
    # - blocked/challenge (HTTP 200 bot page)
    # - CI parsing failure (no lxml -> pandas.read_html returns nothing)
    # - pandas missing
    fallback_status = {}
    need_fallback = (not fresh) and (
        any(status.get(k + "_blocked") for k in ["ratings_upgrade", "ratings_downgrade", "ratings_pt_change"]) or
        bool(status.get("parse_failed")) or
        bool(status.get("pandas_unavailable"))
    )

    if need_fallback:
        # prefer FMP (if key exists), then Finnhub
        fmp_key = os.getenv("FMP_API_KEY", "").strip()
        finnhub_key = os.getenv("FINNHUB_API_KEY", "").strip()
        if fmp_key:
            fmp_events, fmp_status = _fmp_updown(fmp_key, tickers, days=args.days)
            fallback_status["fmp"] = fmp_status
            if fmp_events:
                fresh = fmp_events
        if (not fresh) and finnhub_key:
            fh_events, fh_status = _finnhub_updown(finnhub_key, tickers, days=args.days)
            fallback_status["finnhub"] = fh_status
            if fh_events:
                fresh = fh_events

    # merge into persistent cache
    _merge_events_into_cache(cache_obj, fresh_events=fresh, seen_date=seen_date)

    # update legacy seen map for compatibility
    for ev in fresh:
        uid = _make_uid(ev)
        seen_map[uid] = seen_date

    # select window
    window_events = _events_within_days(cache_obj, tickers=tickers, days=args.days, today=today)

    # output json payload
    payload = {
        "version": VERSION,
        "generated_utc": generated_utc,
        "days": args.days,
        "count": len(window_events),
        "source_status": status,
        "fallback_status": fallback_status,
        "events": window_events,
    }

    _safe_json_dump(args.out_json, payload)
    md = _render_md(window_events, days=args.days, generated_utc=generated_utc)
    os.makedirs(os.path.dirname(os.path.abspath(args.out_md)), exist_ok=True)
    with open(args.out_md, "w", encoding="utf-8") as f:
        f.write(md)

    # persist caches
    _safe_json_dump(args.cache_events, cache_obj)
    _safe_json_dump(args.cache_seen, seen_map)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
