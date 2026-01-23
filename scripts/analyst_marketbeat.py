#!/usr/bin/env python3
# analyst_marketbeat.py – v0.3.35-marketbeat-fmp-primary-finnhub-quotecheck-cli-alias-2026-01-23
# NOTE: Increment version on every modification (strictly monotonic).

"""
Analyst feed module for Price Engine (#1): upgrades/downgrades/price-target changes.

Primary: MarketBeat ratings pages (may be blocked by Cloudflare "HTTP 200 challenge")
Fallback: Financial Modeling Prep (FMP) upgrades/downgrades RSS feed (recommended)
Optional diagnostic: Finnhub quote API key validation + premium detection for upgrade/downgrade endpoint.

Outputs:
- Markdown snippet (reports/analyst_last2d.md by default)
- JSON events (reports/analyst_last2d.json by default)

Two-line "ima" for hotfix bundles:
Bocsáss meg Uram, mert balfék voltam, és NameError-ral felrobbantottam a futást.
Adj türelmet és jó logot, hogy legközelebb elsőre stabil legyen.
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
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


VERSION = "v0.3.35-marketbeat-fmp-primary-finnhub-quotecheck-cli-alias-2026-01-23"

DEFAULT_OUT_MD = "reports/analyst_last2d.md"
DEFAULT_OUT_JSON = "reports/analyst_last2d.json"
DEFAULT_SEEN_PATH = "reports/marketbeat_seen.json"
DEFAULT_LAST_SUCCESS_PATH = "reports/marketbeat_last_success.json"

# Conservative UA to avoid being flagged too aggressively; do NOT rotate aggressively in GH Actions.
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"

MARKETBEAT_URLS = {
    "upgrade": "https://www.marketbeat.com/ratings/upgrades/",
    "downgrade": "https://www.marketbeat.com/ratings/downgrades/",
    "pt_change": "https://www.marketbeat.com/ratings/price-target-changes/",
}

FMP_RSS_URL = "https://financialmodelingprep.com/api/v4/upgrades-downgrades-rss-feed?page={page}"
FMP_COMPANY_URL = "https://financialmodelingprep.com/api/v4/upgrades-downgrades?symbol={symbol}"

FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote?symbol={symbol}&token={token}"
FINNHUB_UPDOWN_URL = "https://finnhub.io/api/v1/stock/upgrade-downgrade?symbol={symbol}&token={token}"


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(tz=_dt.timezone.utc)


def _log(msg: str) -> None:
    ts = _utc_now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] {msg}", flush=True)


def _safe_mkdir_for(path: str) -> None:
    p = os.path.dirname(path)
    if p and not os.path.exists(p):
        os.makedirs(p, exist_ok=True)


def _read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return None


def _write_text(path: str, content: str) -> None:
    _safe_mkdir_for(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _read_json(path: str) -> Optional[Any]:
    raw = _read_text(path)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _write_json(path: str, obj: Any) -> None:
    _safe_mkdir_for(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)


def _http_get(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 20, retries: int = 2) -> Tuple[int, str, bytes]:
    hdr = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    if headers:
        hdr.update(headers)
    last_err: Optional[str] = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=hdr, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status = getattr(resp, "status", 200)
                final_url = resp.geturl()
                body = resp.read()
                return int(status), final_url, body
        except urllib.error.HTTPError as e:
            last_err = f"HTTPError {e.code}"
            # Don't retry 4xx except 429
            if e.code == 429 and attempt < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < retries:
                time.sleep(1.0 * (attempt + 1))
                continue
            raise
    raise RuntimeError(last_err or "HTTP failed")


def _looks_like_marketbeat_challenge(text: str) -> bool:
    t = text.lower()
    # Common Cloudflare/challenge markers
    markers = [
        "cf-chl", "cloudflare", "just a moment", "attention required", "captcha",
        "/cdn-cgi/", "checking your browser", "verify you are human",
    ]
    return any(m in t for m in markers)


def _parse_marketbeat_table(html: str, kind: str) -> List[Dict[str, Any]]:
    """
    Best-effort parse. MarketBeat HTML changes often; we keep this minimal and robust.
    We only try to extract: date, symbol, company, analyst, action/from/to, pt.
    """
    rows: List[Dict[str, Any]] = []
    # Find table rows (very forgiving)
    for m in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, flags=re.S | re.I):
        tr = m.group(1)
        # Symbol often appears as /stocks/NASDAQ/AAPL/ or /stocks/NYSE/XYZ/
        sm = re.search(r"/stocks/(?:nasdaq|nyse|amex|otc|nysemkt)/([a-z0-9\.\-]+)/", tr, flags=re.I)
        if not sm:
            continue
        symbol = sm.group(1).upper()

        # Date often in <td>Jan 23, 2026</td>
        dm = re.search(r"<td[^>]*>\s*([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\s*</td>", tr)
        date_s = dm.group(1) if dm else ""

        # Analyst firm: look for first <td> with letters and spaces after symbol cell
        # This is heuristic; accept missing.
        analyst = ""
        # Company name might be in title or link text
        comp = ""
        cm = re.search(r'title="([^"]+)"', tr)
        if cm:
            comp = re.sub(r"\s+", " ", cm.group(1)).strip()

        # Rating/action: attempt to capture "Upgraded" or "Downgraded" or similar
        action = kind
        fm = re.search(r"(upgraded|downgraded|initiated|reiterated|maintained|resumed)", tr, flags=re.I)
        if fm:
            action = fm.group(1).lower()

        from_grade = ""
        to_grade = ""
        gm = re.search(r"from\s+([A-Za-z ]+?)\s+to\s+([A-Za-z ]+?)(?:<|$)", tr, flags=re.I)
        if gm:
            from_grade = gm.group(1).strip()
            to_grade = gm.group(2).strip()

        pt = None
        # Price target often like $150.00
        pm = re.search(r"\$\s*([0-9]+(?:\.[0-9]+)?)", tr)
        if pm:
            try:
                pt = float(pm.group(1))
            except Exception:
                pt = None

        rows.append({
            "source": "marketbeat",
            "kind": kind,
            "date": date_s,
            "symbol": symbol,
            "company": comp,
            "analyst": analyst,
            "action": action,
            "from": from_grade,
            "to": to_grade,
            "pt": pt,
        })
    return rows


def _parse_date_any(s: str) -> Optional[_dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # ISO
    try:
        if s.endswith("Z"):
            return _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return _dt.datetime.fromisoformat(s)
    except Exception:
        pass
    # "Jan 23, 2026"
    try:
        dt = _dt.datetime.strptime(s, "%b %d, %Y")
        return dt.replace(tzinfo=_dt.timezone.utc)
    except Exception:
        pass
    # "2026-01-23 12:34:56"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = _dt.datetime.strptime(s, fmt)
            return dt.replace(tzinfo=_dt.timezone.utc)
        except Exception:
            continue
    return None


def _event_id(ev: Dict[str, Any]) -> str:
    key = json.dumps({
        "source": ev.get("source"),
        "symbol": ev.get("symbol"),
        "date": ev.get("date"),
        "analyst": ev.get("analyst"),
        "action": ev.get("action"),
        "from": ev.get("from"),
        "to": ev.get("to"),
        "pt": ev.get("pt"),
        "pt_old": ev.get("pt_old"),
        "pt_new": ev.get("pt_new"),
    }, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _load_seen_db(path: str) -> Dict[str, str]:
    obj = _read_json(path)
    if isinstance(obj, dict):
        # id -> first_seen_iso
        return {str(k): str(v) for k, v in obj.items()}
    return {}


def _save_seen_db(path: str, seen: Dict[str, str]) -> None:
    _write_json(path, seen)


def _within_days(ev_dt: Optional[_dt.datetime], days: int, now: _dt.datetime) -> bool:
    if ev_dt is None:
        return False
    return (now - ev_dt) <= _dt.timedelta(days=days)


def _load_master_tickers(master_csv: str) -> List[str]:
    if not os.path.exists(master_csv):
        raise FileNotFoundError(f"MASTER not found: {master_csv}")
    with open(master_csv, "r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        fields = [c.lower() for c in (rdr.fieldnames or [])]
        # pick a likely column
        col = None
        for candidate in ("ticker", "symbol", "tickers"):
            if candidate in fields:
                col = (rdr.fieldnames or [])[fields.index(candidate)]
                break
        tickers: List[str] = []
        if col is None:
            # fallback: first column
            first = (rdr.fieldnames or [""])[0]
            col = first
        for row in rdr:
            raw = (row.get(col) or "").strip()
            if not raw:
                continue
            t = raw.upper()
            # skip non-US / weird
            if " " in t:
                t = t.split()[0]
            tickers.append(t)
    # unique, preserve order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _fmp_fetch_rss(fmp_key: str, max_pages: int = 3) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Returns (events, error_message). Events in unified schema.
    """
    if not fmp_key:
        return [], "FMP_API_KEY hiányzik"
    events: List[Dict[str, Any]] = []
    for page in range(max_pages):
        url = FMP_RSS_URL.format(page=page)
        # FMP typically requires apikey param
        url = url + "&apikey=" + urllib.parse.quote(fmp_key)
        try:
            status, final_url, body = _http_get(url, headers={"Accept": "application/json"})
        except urllib.error.HTTPError as e:
            return [], f"FMP RSS HTTP {e.code}"
        except Exception as e:
            return [], f"FMP RSS hiba: {e}"
        txt = body.decode("utf-8", errors="replace").strip()
        # JSON expected, but handle XML just in case
        page_items: List[Dict[str, Any]] = []
        if txt.startswith("<"):
            try:
                root = ET.fromstring(txt)
                # naive RSS parsing
                for item in root.findall(".//item"):
                    title = (item.findtext("title") or "").strip()
                    pub = (item.findtext("pubDate") or "").strip()
                    desc = (item.findtext("description") or "").strip()
                    page_items.append({"title": title, "pubDate": pub, "description": desc})
            except Exception:
                return [], "FMP RSS: nem értelmezhető XML"
            # Convert RSS item to event heuristically (best effort)
            for it in page_items:
                sym = ""
                m = re.search(r"\b([A-Z]{1,5}(?:\.[A-Z])?)\b", it.get("title",""))
                if m:
                    sym = m.group(1)
                events.append({
                    "source": "fmp",
                    "kind": "rss",
                    "date": it.get("pubDate",""),
                    "symbol": sym,
                    "company": "",
                    "analyst": "",
                    "action": "",
                    "from": "",
                    "to": "",
                    "pt": None,
                    "raw": it,
                })
            continue
        try:
            data = json.loads(txt)
        except Exception:
            return [], "FMP RSS: nem értelmezhető JSON"
        if isinstance(data, dict) and data.get("Error Message"):
            return [], f"FMP RSS error: {data.get('Error Message')}"
        if not isinstance(data, list):
            # sometimes wrapped
            if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
                data = data["items"]
            else:
                return [], "FMP RSS: váratlan válaszformátum"
        for it in data:
            if not isinstance(it, dict):
                continue
            # field names can vary; handle common ones
            sym = (it.get("symbol") or it.get("ticker") or "").upper()
            dt = it.get("publishedDate") or it.get("date") or it.get("updated") or it.get("time") or ""
            firm = it.get("gradingCompany") or it.get("analystCompany") or it.get("firm") or it.get("company") or ""
            action = it.get("action") or it.get("newGrade") or it.get("rating") or it.get("grade") or ""
            fromg = it.get("previousGrade") or it.get("oldGrade") or it.get("fromGrade") or ""
            tog = it.get("newGrade") or it.get("toGrade") or it.get("rating") or ""
            # price target
            pt_old = it.get("previousPriceTarget") or it.get("oldPriceTarget")
            pt_new = it.get("newPriceTarget") or it.get("priceTarget")
            # normalize floats
            def to_float(x):
                try:
                    return float(x)
                except Exception:
                    return None
            pt_old_f = to_float(pt_old)
            pt_new_f = to_float(pt_new)
            ev = {
                "source": "fmp",
                "kind": "rating",
                "date": str(dt),
                "symbol": sym,
                "company": "",
                "analyst": str(firm),
                "action": str(action).lower(),
                "from": str(fromg),
                "to": str(tog),
                "pt_old": pt_old_f,
                "pt_new": pt_new_f,
                "pt": pt_new_f if pt_new_f is not None else None,
                "raw": it,
            }
            events.append(ev)
        # If we got some, no need for more pages unless days window is large.
        if events and page >= 1:
            break
    return events, None


def _finnhub_quote_check(token: str) -> Tuple[bool, str]:
    """
    Validate token by calling /quote on a liquid symbol. This does not require premium.
    """
    if not token:
        return False, "FINNHUB_API_KEY hiányzik"
    url = FINNHUB_QUOTE_URL.format(symbol="AAPL", token=urllib.parse.quote(token))
    try:
        status, _, body = _http_get(url, headers={"Accept": "application/json"}, timeout=15, retries=1)
        txt = body.decode("utf-8", errors="replace")
        data = json.loads(txt)
        if isinstance(data, dict) and "c" in data:
            return True, "OK"
        return False, "Váratlan válasz"
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}"
    except Exception as e:
        return False, f"Hiba: {e}"


def _finnhub_updown_premium_probe(token: str) -> Tuple[bool, str]:
    """
    Probe /stock/upgrade-downgrade. Free keys often get 403 (premium required).
    """
    if not token:
        return False, "FINNHUB_API_KEY hiányzik"
    url = FINNHUB_UPDOWN_URL.format(symbol="", token=urllib.parse.quote(token))
    # Some servers require symbol omitted; keep empty symbol param.
    try:
        status, _, body = _http_get(url, headers={"Accept": "application/json"}, timeout=15, retries=0)
        txt = body.decode("utf-8", errors="replace")
        data = json.loads(txt)
        if isinstance(data, list):
            return True, "OK"
        if isinstance(data, dict) and data.get("error"):
            return False, f"API error: {data.get('error')}"
        return False, "Váratlan válasz"
    except urllib.error.HTTPError as e:
        if e.code == 403:
            return False, "HTTP 403 (Premium endpoint / tiltás)"
        return False, f"HTTP {e.code}"
    except Exception as e:
        return False, f"Hiba: {e}"


def _marketbeat_fetch_all() -> Tuple[Dict[str, List[Dict[str, Any]]], bool]:
    """
    Return (rows_by_kind, blocked).
    """
    rows_by_kind: Dict[str, List[Dict[str, Any]]] = {"upgrade": [], "downgrade": [], "pt_change": []}
    blocked_any = False
    for kind, url in MARKETBEAT_URLS.items():
        try:
            status, final_url, body = _http_get(url, headers={"Accept": "text/html"}, timeout=20, retries=1)
            html = body.decode("utf-8", errors="replace")
            if _looks_like_marketbeat_challenge(html):
                blocked_any = True
                rows_by_kind[kind] = []
                continue
            rows_by_kind[kind] = _parse_marketbeat_table(html, kind=kind)
        except Exception:
            blocked_any = True
            rows_by_kind[kind] = []
    return rows_by_kind, blocked_any


def _marketbeat_to_events(rows_by_kind: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    evs: List[Dict[str, Any]] = []
    for kind, rows in rows_by_kind.items():
        for r in rows:
            r = dict(r)
            r["kind"] = kind
            evs.append(r)
    return evs


def _normalize_events(events: List[Dict[str, Any]], master_set: set, days: int, now: _dt.datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for ev in events:
        sym = (ev.get("symbol") or "").upper().strip()
        if not sym or sym not in master_set:
            continue
        dt = _parse_date_any(str(ev.get("date") or ""))
        if not _within_days(dt, days, now):
            continue
        ev = dict(ev)
        ev["symbol"] = sym
        ev["dt_utc"] = dt.isoformat().replace("+00:00", "Z") if dt else ""
        out.append(ev)
    # Sort by datetime desc, then symbol
    def keyf(e):
        d = _parse_date_any(e.get("dt_utc","")) or _dt.datetime(1970,1,1,tzinfo=_dt.timezone.utc)
        return (d, e.get("symbol",""))
    out.sort(key=keyf, reverse=True)
    return out


def _apply_seen_filter(events: List[Dict[str, Any]], seen_db: Dict[str, str], now: _dt.datetime) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    new_events: List[Dict[str, Any]] = []
    for ev in events:
        eid = _event_id(ev)
        if eid not in seen_db:
            seen_db[eid] = now.isoformat().replace("+00:00", "Z")
        # We still include events even if seen earlier; the report is "last N days",
        # but we want stable output. For "new-only" mode you'd filter here.
        new_events.append(ev)
    return new_events, seen_db


def _render_md(days: int,
               events: List[Dict[str, Any]],
               marketbeat_blocked: bool,
               mb_used_cache: bool,
               fmp_status: Optional[str],
               finnhub_quote_status: Optional[str],
               finnhub_premium_status: Optional[str]) -> str:
    lines: List[str] = []
    lines.append(f"## Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {days} naptári nap)")
    lines.append("")
    if not events:
        if marketbeat_blocked:
            # explain based on fallback statuses
            msg = "- MarketBeat blokkolás mellett jelenleg nincs megjeleníthető elemzői esemény"
            if fmp_status:
                msg += f" (FMP fallback: {fmp_status})."
            else:
                msg += " (FMP fallback üres vagy nem elérhető)."
            lines.append(msg)
            if finnhub_premium_status:
                lines.append(f"- Finnhub fallback státusz: {finnhub_premium_status}.")
            if finnhub_quote_status:
                lines.append(f"- Finnhub API ellenőrzés: {finnhub_quote_status}.")
            lines.append("")
            lines.append("_Megjegyzés: MarketBeat blokkolás / robotvédelem (a feed nem megbízhatóan elérhető)._")
        else:
            lines.append("_Nincs friss fel/leminősítés vagy célár-változás az elmúlt időablakban._")
        lines.append("")
        return "\n".join(lines)

    # Summarize events as bullets
    for ev in events[:80]:  # cap
        sym = ev.get("symbol","")
        dt = ev.get("date","") or ev.get("dt_utc","")
        firm = ev.get("analyst","") or ev.get("gradingCompany","") or ""
        action = (ev.get("action") or ev.get("kind") or "").strip()
        fromg = (ev.get("from") or "").strip()
        tog = (ev.get("to") or "").strip()
        pt_old = ev.get("pt_old")
        pt_new = ev.get("pt_new")
        pt = ev.get("pt")
        parts = [f"**{sym}**"]
        if dt:
            parts.append(str(dt))
        if firm:
            parts.append(str(firm))
        if action:
            parts.append(str(action))
        if fromg or tog:
            parts.append(f"{fromg} → {tog}".strip())
        if pt_old is not None or pt_new is not None:
            parts.append(f"PT: {pt_old} → {pt_new}")
        elif pt is not None:
            parts.append(f"PT: {pt}")
        lines.append("- " + " | ".join([p for p in parts if p]))

    lines.append("")
    if marketbeat_blocked:
        lines.append("_Megjegyzés: MarketBeat blokkolás / robotvédelem (a feed nem megbízhatóan elérhető). Fallback forrás: FMP._")
    return "\n".join(lines)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--days", type=int, default=2, help="Lookback window in calendar days (default: 2)")
    p.add_argument("--master", type=str, default="reports/master.csv", help="MASTER CSV path")
    p.add_argument("--mode", type=str, default="ratings_pages", help="Compatibility arg (ignored, but kept)")
    p.add_argument("--seen_path", type=str, default=DEFAULT_SEEN_PATH, help="Seen DB json path")
    p.add_argument("--last_success_path", type=str, default=DEFAULT_LAST_SUCCESS_PATH, help="Last-success cache path")
    p.add_argument("--fmp_api_key", type=str, default=os.environ.get("FMP_API_KEY",""), help="FMP API key (recommended fallback)")
    p.add_argument("--finnhub_api_key", type=str, default=os.environ.get("FINNHUB_API_KEY",""), help="Finnhub API key (diagnostic/premium probe)")
    # Output path aliases (underscore and dash forms)
    p.add_argument("--out_md", dest="out_md", type=str, default=DEFAULT_OUT_MD, help="Output markdown path")
    p.add_argument("--out-md", dest="out_md", type=str, help=argparse.SUPPRESS)
    p.add_argument("--out_json", dest="out_json", type=str, default=DEFAULT_OUT_JSON, help="Output json path")
    p.add_argument("--out-json", dest="out_json", type=str, help=argparse.SUPPRESS)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    now = _utc_now()

    _log(f"START {VERSION} days={args.days} master={args.master} mode={args.mode}")

    # Load master
    try:
        tickers = _load_master_tickers(args.master)
    except Exception as e:
        _log(f"ERROR master load: {e}")
        # still write minimal output
        md = _render_md(args.days, [], marketbeat_blocked=True, mb_used_cache=False,
                        fmp_status="MASTER hiba", finnhub_quote_status=None, finnhub_premium_status=None)
        _write_text(args.out_md, md)
        _write_json(args.out_json, {"version": VERSION, "events": [], "error": "master load failed"})
        return 2

    master_set = set(tickers)

    seen_db = _load_seen_db(args.seen_path)

    # MarketBeat attempt
    rows_by_kind, mb_blocked = _marketbeat_fetch_all()
    mb_events_raw = _marketbeat_to_events(rows_by_kind)
    mb_events = _normalize_events(mb_events_raw, master_set, args.days, now)

    mb_used_cache = False
    last_success = _read_json(args.last_success_path) if args.last_success_path else None

    events: List[Dict[str, Any]] = []
    fmp_status: Optional[str] = None

    if mb_events:
        events = mb_events
        # Save last success cache
        try:
            _write_json(args.last_success_path, {"ts": now.isoformat().replace("+00:00","Z"), "events": mb_events_raw})
        except Exception:
            pass
    else:
        # if MarketBeat blocked and we have last_success cache, use it to avoid empty
        if mb_blocked and isinstance(last_success, dict) and isinstance(last_success.get("events"), list):
            mb_used_cache = True
            cached = last_success.get("events") or []
            cached_norm = _normalize_events(list(cached), master_set, args.days, now)
            if cached_norm:
                events = cached_norm

        # If still empty, use FMP fallback (preferred)
        if not events:
            fmp_key = (args.fmp_api_key or "").strip()
            fmp_events_raw, fmp_err = _fmp_fetch_rss(fmp_key=fmp_key, max_pages=3)
            if fmp_err:
                fmp_status = fmp_err
            else:
                fmp_status = "OK"
            fmp_events = _normalize_events(fmp_events_raw, master_set, args.days, now)
            if fmp_events:
                events = fmp_events

    # Seen DB update (keeps stability)
    events, seen_db = _apply_seen_filter(events, seen_db, now)
    try:
        _save_seen_db(args.seen_path, seen_db)
    except Exception:
        pass

    # Finnhub diagnostics (quote check + premium probe)
    finnhub_quote_status = None
    finnhub_premium_status = None
    if (args.finnhub_api_key or "").strip():
        ok, msg = _finnhub_quote_check(args.finnhub_api_key.strip())
        finnhub_quote_status = ("sikeres" if ok else "sikertelen") + f" ({msg})"
        ok2, msg2 = _finnhub_updown_premium_probe(args.finnhub_api_key.strip())
        finnhub_premium_status = ("elérhető" if ok2 else "nem elérhető") + f" ({msg2})"

    md = _render_md(
        days=args.days,
        events=events,
        marketbeat_blocked=mb_blocked,
        mb_used_cache=mb_used_cache,
        fmp_status=fmp_status,
        finnhub_quote_status=finnhub_quote_status,
        finnhub_premium_status=finnhub_premium_status,
    )

    _write_text(args.out_md, md)
    _write_json(args.out_json, {
        "version": VERSION,
        "generated_at_utc": now.isoformat().replace("+00:00","Z"),
        "days": args.days,
        "master_rows": len(tickers),
        "marketbeat_blocked": mb_blocked,
        "marketbeat_used_cache": mb_used_cache,
        "fmp_status": fmp_status,
        "finnhub_quote_status": finnhub_quote_status,
        "finnhub_premium_status": finnhub_premium_status,
        "events": events,
    })

    _log(f"DONE events={len(events)} wrote={args.out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
