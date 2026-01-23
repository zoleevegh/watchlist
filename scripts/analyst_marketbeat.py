#!/usr/bin/env python3
# Ima (v0.3.36): bocsáss meg uram, ha megint mellényúltam;
# add, hogy a robotvédelem ellenére is legyen tiszta fallback és korrekt üzenet.
# Version: v0.3.37-marketbeat-fmp-stable-grades-historical-2026-01-23
"""
analyst_marketbeat.py

Purpose (PRICE ENGINE #1):
- Produce a short "Analyst feed" block for the last N calendar days:
  upgrades / downgrades (+ minimal context) and (if available) price-target changes.

Reality check:
- MarketBeat frequently returns HTTP 200 with a bot-challenge page. That is treated as "blocked/challenge".
  We DO NOT try to bypass robot protection.
- Fallback: Financial Modeling Prep (FMP) "Historical Stock Grades API" (grades-historical).
  This yields upgrades/downgrades/reiterations (grades) but typically does NOT include explicit price-target changes.

Outputs:
- Markdown: reports/analyst_last2d.md (default)
- JSON:     reports/analyst_last2d.json (default)

Exit behavior:
- Always write outputs and exit 0 so the workflow can continue.
- Prints "ANALYST_EXIT=1" for degraded mode (blocked/empty/fallback issues), "ANALYST_EXIT=0" for OK.

CLI compatibility:
- Keeps legacy args used by the workflow: --days --master --mode --seen_path --last_success_path
  and both --out_md / --out-md, --out_json / --out-json.
"""

import argparse
import csv
import hashlib
import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

VERSION = "v0.3.37-marketbeat-fmp-stable-grades-historical-2026-01-23"

DEFAULT_OUT_MD = "reports/analyst_last2d.md"
DEFAULT_OUT_JSON = "reports/analyst_last2d.json"
DEFAULT_SEEN_PATH = "reports/marketbeat_seen.json"
DEFAULT_LAST_SUCCESS_PATH = "reports/marketbeat_last_success.json"

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"

# MarketBeat pages (kept for a quick "blocked/challenge" detection only)
MB_UPGRADE_URL = "https://www.marketbeat.com/ratings/upgrades/"
MB_DOWNGRADE_URL = "https://www.marketbeat.com/ratings/downgrades/"
MB_PT_URL = "https://www.marketbeat.com/ratings/price-target-changes/"

# FMP stable "Historical Stock Grades API"
# Doc example: https://financialmodelingprep.com/stable/grades-historical?page=0&limit=10&apikey=YOUR_API_KEY
FMP_GRADES_HIST_URL = "https://financialmodelingprep.com/stable/grades-historical"
FMP_QUOTE_SHORT_URL = "https://financialmodelingprep.com/stable/quote-short"

# Finnhub (diagnostic only): validate API key via /api/v1/quote (free-tier compatible)
FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _log(msg: str) -> None:
    ts = _utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


def _read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return None


def _write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


def _read_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception:
        return default


def _write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _load_master_tickers(master_csv_path: str) -> List[str]:
    tickers: List[str] = []
    with open(master_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Accept common header variants
        possible_cols = ["ticker", "Ticker", "TICKER", "symbol", "Symbol", "SYMBOL"]
        col = None
        for c in possible_cols:
            if c in reader.fieldnames:
                col = c
                break
        if not col:
            raise ValueError(f"MASTER CSV has no ticker column. Columns: {reader.fieldnames}")
        for row in reader:
            t = (row.get(col) or "").strip()
            if t:
                tickers.append(t.upper())
    # Unique, stable order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out


def _http_get(url: str, timeout: int = 20, headers: Optional[Dict[str, str]] = None) -> Tuple[int, str]:
    h = {"User-Agent": USER_AGENT, "Accept": "text/html,application/json;q=0.9,*/*;q=0.8"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 200))
            body = resp.read().decode("utf-8", errors="replace")
            return status, body
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return int(e.code), body
    except Exception:
        return 0, ""


def _detect_marketbeat_block(status: int, body: str) -> bool:
    """
    MarketBeat can return HTTP 200 while serving a bot-challenge page.
    We treat the following as "blocked/challenge":
    - explicit "blocked" / "challenge" markers
    - Cloudflare / bot management hints
    - HTML that looks like a challenge rather than the ratings page
    """
    if status != 200:
        return True
    b = (body or "").lower()
    markers = [
        "blocked", "challenge", "cf-challenge", "cloudflare",
        "robot", "captcha", "verify you are human", "checking your browser",
        "access denied", "attention required"
    ]
    if any(m in b for m in markers):
        return True
    # Very small body is suspicious for these pages
    if len(b) < 800:
        return True
    return False


def _validate_finnhub_key(api_key: str) -> Tuple[bool, str]:
    """
    Only a key validity check against /quote (free-tier compatible).
    Returns (ok, note).
    """
    if not api_key:
        return False, "Finnhub API key hiányzik"
    url = f"{FINNHUB_QUOTE_URL}?symbol=AAPL&token={urllib.parse.quote(api_key)}"
    status, body = _http_get(url, timeout=15, headers={"Accept": "application/json"})
    if status == 200 and body.strip().startswith("{"):
        return True, "Finnhub API ellenőrzés: sikeres (OK)"
    if status == 401 or status == 403:
        return False, f"Finnhub API ellenőrzés: sikertelen (HTTP {status})"
    if status == 0:
        return False, "Finnhub API ellenőrzés: hálózati hiba / timeout"
    return False, f"Finnhub API ellenőrzés: ismeretlen válasz (HTTP {status})"



def _validate_fmp_key(api_key: str) -> Tuple[bool, str]:
    """Minimal key check against FMP stable quote-short."""
    if not api_key:
        return False, "FMP API key hiányzik"
    url = f"{FMP_QUOTE_SHORT_URL}?symbol=AAPL&apikey={urllib.parse.quote(api_key)}"
    status, body = _http_get(url, timeout=15, headers={"Accept": "application/json"})
    if status == 200 and body.strip().startswith("["):
        return True, "FMP API ellenőrzés: sikeres (OK)"
    if status in (401, 403):
        return False, f"FMP API ellenőrzés: sikertelen (HTTP {status})"
    if status == 402:
        return False, "FMP API ellenőrzés: Payment Required (HTTP 402) – a csomagod/hozzáférés nem engedi ezt az endpointot"
    if status == 0:
        return False, "FMP API ellenőrzés: hálózati hiba / timeout"
    return False, f"FMP API ellenőrzés: ismeretlen válasz (HTTP {status})"
def _parse_iso_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        # Example: 2025-02-04T19:18:04.000Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _event_key(e: Dict[str, Any]) -> str:
    """
    Stable key for dedup / first-seen.
    """
    raw = "|".join([
        str(e.get("source", "")),
        str(e.get("symbol", "")),
        str(e.get("publishedDate", "")),
        str(e.get("gradingCompany", "")),
        str(e.get("action", "")),
        str(e.get("previousGrade", "")),
        str(e.get("newGrade", "")),
    ])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _fetch_fmp_grades_historical(api_key: str, days: int, tickers_set: set, max_symbols: int = 200) -> Tuple[List[Dict[str, Any]], str]:
    """
    Fallback analyst feed via FMP Stable 'Historical Stock Grades' per-symbol endpoint.
    Filters for the last `days` calendar days; keeps upgrade/downgrade actions only.
    """
    if not api_key:
        return [], "FMP fallback: FMP_API_KEY hiányzik"

    ok, chk = _validate_fmp_key(api_key)
    if not ok:
        return [], f"FMP fallback: {chk}"

    since = _utc_now() - timedelta(days=max(1, int(days)))
    api_key_q = urllib.parse.quote(api_key)
    events: List[Dict[str, Any]] = []

    symbols = sorted([s for s in tickers_set if isinstance(s, str) and s.strip()])[:max_symbols]
    for sym in symbols:
        sym_q = urllib.parse.quote(sym)
        url = f"{FMP_GRADES_HIST_URL}?symbol={sym_q}&apikey={api_key_q}"
        status, body = _http_get(url, timeout=20, headers={"Accept": "application/json"})

        if status == 200:
            try:
                data = json.loads(body) if body else []
            except Exception:
                data = []
            if not isinstance(data, list):
                continue
            for r in data:
                if not isinstance(r, dict):
                    continue
                dt = _parse_iso_dt(r.get("date") or r.get("publishedDate") or "")
                if not dt or dt < since:
                    continue
                action = (r.get("action") or "").lower().strip()
                if action not in ("upgrade", "downgrade"):
                    continue
                events.append({
                    "source": "FMP",
                    "symbol": sym,
                    "publishedDate": r.get("date") or r.get("publishedDate") or "",
                    "gradingCompany": r.get("gradingCompany") or r.get("firm") or "",
                    "action": action,
                    "previousGrade": r.get("previousGrade") or "",
                    "newGrade": r.get("newGrade") or "",
                    "newsTitle": r.get("newsTitle") or r.get("title") or "",
                    "newsURL": r.get("newsURL") or r.get("url") or "",
                })
            continue

        if status == 429:
            return events, "FMP fallback: rate limit (HTTP 429) – részleges eredmény"
        if status == 402:
            return [], "FMP fallback: Payment Required (HTTP 402) – a csomagod/hozzáférés nem engedi az analyst/grades endpointot"
        if status in (401, 403):
            return [], f"FMP fallback: nem engedélyezett (HTTP {status})"
        if status == 0:
            return events, "FMP fallback: hálózati hiba/timeout – részleges eredmény"
        # otherwise continue

    if events:
        return events, f"FMP fallback: grades-historical OK ({len(events)} esemény)"
    return [], "FMP fallback: nincs friss upgrade/downgrade az időablakban"

def _render_markdown(days: int,
                     mb_blocked: bool,
                     mb_note: str,
                     fmp_note: str,
                     finnhub_note: str,
                     events: List[Dict[str, Any]],
                     seen_db: Dict[str, Any],
                     now: datetime) -> str:
    title = f"## Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {days} naptári nap)"
    lines: List[str] = [title, ""]
    if events:
        # Sort newest first
        def key_dt(e: Dict[str, Any]) -> float:
            dt = _parse_iso_dt(e.get("publishedDate") or "")
            return dt.timestamp() if dt else 0.0
        events_sorted = sorted(events, key=key_dt, reverse=True)

        # Compact list
        for e in events_sorted[:50]:
            dt = _parse_iso_dt(e.get("publishedDate") or "")
            dt_s = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if dt else (e.get("publishedDate") or "")
            sym = e.get("symbol", "")
            action = e.get("action", "").lower()
            firm = e.get("gradingCompany", "").strip()
            prevg = e.get("previousGrade", "").strip()
            newg = e.get("newGrade", "").strip()
            title2 = (e.get("newsTitle") or "").strip()
            url = (e.get("newsURL") or "").strip()

            # First-seen tagging (optional)
            k = _event_key(e)
            first_seen = seen_db.get(k)
            if not first_seen:
                seen_db[k] = now.isoformat()
                first_seen = now.isoformat()
            # keep it simple in MD (no noisy first_seen)
            grade_part = ""
            if prevg or newg:
                grade_part = f"{prevg} → {newg}".strip(" →")
            parts = [f"- {dt_s} | {sym} | {action.upper()}"]
            if firm:
                parts.append(f"| {firm}")
            if grade_part:
                parts.append(f"| {grade_part}")
            if title2:
                parts.append(f"| {title2[:120]}")
            if url:
                parts.append(f"| {url}")
            lines.append(" ".join(parts))

        lines.append("")
        # Status notes
        if mb_blocked:
            lines.append(f"- MarketBeat státusz: {mb_note}.")
        if fmp_note:
            lines.append(f"- {fmp_note}.")
        if finnhub_note:
            lines.append(f"- {finnhub_note}.")
        lines.append("")
        lines.append("_Megjegyzés: MarketBeat blokkolás / robotvédelem (a feed nem megbízhatóan elérhető)._")
        return "\n".join(lines)

    # No events
    lines.append(f"- MarketBeat státusz: {mb_note}.")
    if fmp_note:
        lines.append(f"- {fmp_note}.")
    if finnhub_note:
        lines.append(f"- {finnhub_note}.")
    lines.append("")
    lines.append("_Megjegyzés: MarketBeat blokkolás / robotvédelem (a feed nem megbízhatóan elérhető)._")
    return "\n".join(lines)


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--days", type=int, default=2, help="Lookback window in calendar days (default: 2)")
    p.add_argument("--master", type=str, default="reports/master.csv", help="MASTER CSV path")
    p.add_argument("--mode", type=str, default="ratings_pages", help="Compatibility arg (ignored, but kept)")
    p.add_argument("--seen_path", type=str, default=DEFAULT_SEEN_PATH, help="Seen DB json path")
    p.add_argument("--last_success_path", type=str, default=DEFAULT_LAST_SUCCESS_PATH, help="Last-success cache path")
    p.add_argument("--fmp_api_key", type=str, default=os.environ.get("FMP_API_KEY",""), help="FMP API key (recommended fallback)")
    p.add_argument("--finnhub_api_key", type=str, default=os.environ.get("FINNHUB_API_KEY",""), help="Finnhub API key (diagnostic only)")

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

    # Load master tickers
    degraded = False
    try:
        tickers = _load_master_tickers(args.master)
    except Exception as e:
        tickers = []
        degraded = True
        _log(f"ERROR master load: {e}")

    tickers_set = set(tickers)

    # Seen DB (used for first-seen tracking)
    seen_db = _read_json(args.seen_path, default={})
    if not isinstance(seen_db, dict):
        seen_db = {}

    # MarketBeat quick probe (no scraping; only detect block)
    mb_blocked = True
    mb_note = "MarketBeat blokkolás / robotvédelem (challenge)"
    for url in (MB_UPGRADE_URL, MB_DOWNGRADE_URL, MB_PT_URL):
        st, body = _http_get(url, timeout=15)
        if not _detect_marketbeat_block(st, body):
            mb_blocked = False
            mb_note = "MarketBeat elérhető (de scrape nincs engedélyezve ebben a módban)"
            break
    if mb_blocked:
        degraded = True

    # Finnhub key check (diagnostic only; no premium endpoints)
    finnhub_ok, finnhub_note = _validate_finnhub_key(args.finnhub_api_key)
    if not finnhub_ok:
        # Not fatal; but keep as note
        degraded = True

    # FMP fallback
    events: List[Dict[str, Any]] = []
    fmp_note = ""
    ev, fmp_note = _fetch_fmp_grades_historical(args.fmp_api_key, args.days, tickers_set)
    if ev:
        events = ev
    else:
        degraded = True

    md = _render_markdown(
        days=args.days,
        mb_blocked=mb_blocked,
        mb_note=mb_note,
        fmp_note=fmp_note,
        finnhub_note=finnhub_note,
        events=events,
        seen_db=seen_db,
        now=now,
    )

    out_json_obj = {
        "version": VERSION,
        "generated_utc": now.isoformat(),
        "days": args.days,
        "master": args.master,
        "marketbeat": {"blocked": mb_blocked, "note": mb_note},
        "fmp": {"note": fmp_note, "events": events},
        "finnhub": {"note": finnhub_note, "ok": finnhub_ok, "used": False},
        "tickers_count": len(tickers),
    }

    _write_text(args.out_md, md)
    _write_json(args.out_json, out_json_obj)
    _write_json(args.seen_path, seen_db)
    _write_json(args.last_success_path, {"generated_utc": now.isoformat(), "events": len(events)})

    if degraded:
        print("ANALYST_EXIT=1")
    else:
        print("ANALYST_EXIT=0")
    _log(f"DONE events={len(events)} out_md={args.out_md} out_json={args.out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
