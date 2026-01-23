#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Ima (v0.3.26): bocsáss meg uram, ha megint mellényúltam;
# adj stabil fallback‑ot, ha a Cloudflare csuklózik, és legyen adat, ne N/A.
"""
analyst_marketbeat.py — MarketBeat (FREE) "ratings pages" scraper (Solution #2)
- No per-ticker search (avoids mass HTTP 403)
- Pulls central ratings lists once and filters to MASTER tickers
- Persists a small "first_seen" cache (reports/marketbeat_seen.json) so the "last N calendar days"
  window is stable even if MarketBeat is temporarily blocked (HTTP 403) on a given run.
- Hungarian output.

Versioning: keep bumping VERSION for every change.
"""

from __future__ import annotations

import argparse
import calendar
# IMPORTANT: do not alias datetime as "dt" in this project.
# Multiple merges previously introduced bugs like dt.dt.utcnow().
from datetime import datetime, timedelta, date
import hashlib
import http.cookiejar
import json
import os
import re
import time
import urllib.error
import urllib.request
import urllib.parse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


VERSION="v0.3.29-marketbeat-finnhub-fallback-clean-2026-01-23"

# ima (2 sor) – hiba utáni kötelező kiegészítés
# bocsáss meg uram mert balfék voltam, az API fallbackot rossz kulcsra kötöttem.
# add, hogy a Finnhub fallback éljen, és MarketBeat challenge esetén se legyen N/A.

BASE = "https://www.marketbeat.com"
DEFAULT_SEEN_FILE = "reports/marketbeat_seen.json"
LAST_SUCCESS_JSON = "reports/marketbeat_last_success.json"
LAST_SUCCESS_MD = "reports/marketbeat_last_success.md"

# Finnhub fallback (official API) for upgrades/downgrades when MarketBeat is blocked/challenged.
# Uses /stock/upgrade-downgrade with from/to window and filters to MASTER tickers.
# Docs: https://finnhub.io/docs/api/upgrade-downgrade
FINNHUB_ENDPOINT = "https://finnhub.io/api/v1/stock/upgrade-downgrade"


def _utc_midnight_unix(d: date) -> int:
    # UNIX timestamp for 00:00:00 UTC of the given date (naive -> UTC)
    return int(calendar.timegm(datetime(d.year, d.month, d.day, 0, 0, 0).timetuple()))


def fetch_events_from_finnhub(
    master_tickers: List[str],
    days: int,
    api_key: str,
    timeout: int = 20,
) -> Tuple[List["AnalystEvent"], Dict[str, str]]:
    """Fallback: pull Finnhub Upgrade/Downgrade API and filter to MASTER tickers + last N calendar days.

    Notes:
    - Finnhub's upgrade/downgrade endpoint may not include explicit price target changes; PT fields remain empty.
    - We attempt a single 'global' request (no symbol) first; if Finnhub requires symbol, we degrade gracefully.
    """
    statuses: Dict[str, str] = {}
    out: List[AnalystEvent] = []
    key = (api_key or "").strip()
    if not key:
        statuses["finnhub"] = "missing_key"
        return out, statuses

    master_set = set(t.upper() for t in master_tickers)
    today = datetime.utcnow().date()
    cutoff = today - timedelta(days=days - 1)

    from_ts = _utc_midnight_unix(cutoff)
    to_ts = int(calendar.timegm(datetime.utcnow().timetuple()))

    params = {"from": str(from_ts), "to": str(to_ts), "token": key}
    url = FINNHUB_ENDPOINT + "?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT_POOL[0],
            "Accept": "application/json",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(raw)
            except Exception:
                statuses["finnhub"] = "bad_json"
                return out, statuses
    except urllib.error.HTTPError as e:
        statuses["finnhub"] = f"http_{e.code}"
        return out, statuses
    except Exception:
        statuses["finnhub"] = "network_error"
        return out, statuses

    # Finnhub may return {'error': '...'} on invalid request
    if isinstance(data, dict) and data.get("error"):
        statuses["finnhub"] = f"error:{str(data.get('error'))[:60]}"
        return out, statuses

    if not isinstance(data, list):
        statuses["finnhub"] = "unexpected_payload"
        return out, statuses

    for it in data:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or it.get("ticker") or "").upper().strip()
        if not sym or sym not in master_set:
            continue

        # time field may be unix seconds; fallback to date string fields if present
        dt_iso = ""
        tval = it.get("time") or it.get("datetime") or it.get("timestamp")
        if isinstance(tval, (int, float)) and tval > 0:
            try:
                dt_iso = datetime.utcfromtimestamp(int(tval)).date().isoformat()
            except Exception:
                dt_iso = ""
        if not dt_iso:
            dt_iso = str(it.get("date") or it.get("eventDate") or it.get("publishedDate") or "").strip()
            # normalize to YYYY-MM-DD if it contains time
            if dt_iso and len(dt_iso) >= 10:
                dt_iso = dt_iso[:10]
        if not dt_iso:
            continue
        if dt_iso < cutoff.isoformat():
            continue

        firm = str(it.get("analyst") or it.get("firm") or it.get("company") or "Finnhub").strip()

        action_raw = str(it.get("action") or "").strip().lower()
        if action_raw in ("up", "upgrade", "upgraded"):
            action = "upgrade"
        elif action_raw in ("down", "downgrade", "downgraded"):
            action = "downgrade"
        else:
            action = action_raw or "rating"

        rating_from = it.get("previousGrade") or it.get("fromGrade") or it.get("oldGrade") or it.get("prevGrade")
        rating_to = it.get("grade") or it.get("toGrade") or it.get("newGrade") or it.get("newGrade")

        ev = AnalystEvent(
            ticker=sym,
            date=dt_iso,
            firm=firm,
            action=action,
            rating_from=str(rating_from).strip() if rating_from not in ("", None) else None,
            rating_to=str(rating_to).strip() if rating_to not in ("", None) else None,
            pt_from=None,
            pt_to=None,
            currency="USD",
            source_url="",
        )
        out.append(ev)

    statuses["finnhub"] = f"ok n={len(out)}"
    return out, statuses


def fetch_events_from_ratings_pages(
    master_tickers: List[str],
    days: int,
    timeout: int,
    sleep_s: float,
    debug_dir: Optional[Path],
) -> Tuple[List[AnalystEvent], Dict[str, str], bool, Optional[str]]:
    today = datetime.utcnow().date()
    cutoff = today - timedelta(days=days - 1)
    master_set = {t.upper() for t in master_tickers}

    statuses: Dict[str, str] = {}
    parse_issue = False
    fetch_ok = False

    # Session-like opener with cookies + realistic UA; try UA fallbacks if blocked
    opener = _build_opener(UA_POOL[0])

    # Warmup home to get cookies (some runs require it)
    _http_get(opener, BASE + "/", timeout, debug_dir, "warmup_home", referer=None)

    out: List[AnalystEvent] = []
    seen: set = set()

    for kind, path in RATINGS_SOURCES:
        url = BASE + path
        status, page_html = _http_get(opener, url, timeout, debug_dir, f"ratings_{kind}", referer=BASE + '/')
        statuses[kind] = f"HTTP {status}" if status else "HTTP ?"

        # If blocked, retry once with alternate UA (new cookie jar)
        if status == 403 and len(UA_POOL) > 1:
            opener = _build_opener(UA_POOL[1])
            _http_get(opener, BASE + "/", timeout, debug_dir, "warmup_home_alt", referer=None)
            status, page_html = _http_get(opener, url, timeout, debug_dir, f"ratings_{kind}_alt", referer=BASE + "/")
            statuses[kind] = f"HTTP {status}" if status else "HTTP ?"

        if status >= 400 or status == 0:
            _log(f"RATINGS {kind}: {statuses[kind]} (skip)")
            time.sleep(sleep_s)
            continue

        # Some bot protections return HTTP 200 with a challenge page.
        if _looks_like_block_page(page_html):
            statuses[kind] = f"HTTP {status} (blocked/challenge)"
            _log(f"RATINGS {kind}: {statuses[kind]} (skip)")
            time.sleep(sleep_s)
            continue

        # Another failure mode: HTTP 200 but a non-rating payload (empty shell / JS challenge)
        # that does NOT contain our marker strings. Don't parse it.
        if not _looks_like_ratings_payload(page_html, kind):
            statuses[kind] = f"HTTP {status} (unexpected html)"
            _log(f"RATINGS {kind}: {statuses[kind]} (skip)")
            time.sleep(sleep_s)
            continue

        fetch_ok = True

        # Iterate <tr> blocks
        for tr in re.findall(r"<tr[^>]*>.*?</tr>", page_html, flags=re.I | re.S):
            tds = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.I | re.S)
            if not tds:
                continue
            cells = [_clean_text(re.sub(r"<[^>]+>", " ", c)) for c in tds]
            if not cells:
                continue

            # Date (usually first cell)
            date_obj = _parse_date_any(cells[0]) if cells else None
            if not date_obj:
                # If missing, treat as today so it can be cached; still filtered later via first_seen
                date_obj = today

            if date_obj < cutoff or date_obj > today:
                continue

            ticker = _extract_ticker_from_row(tr, cells)
            if not ticker or ticker.upper() not in master_set:
                continue

            urls = _extract_urls_from_row_html(tr)
            stock_url = urls.get("stock", "")
            details_url = urls.get("details", "")
            source_url = details_url or stock_url or url

            joined = " ".join([c for c in cells if c]).strip()
            firm = _guess_firm(cells, joined)
            action = _kind_to_action(kind, joined)

            rating_from, rating_to = _extract_rating_change(joined)
            pt_from, pt_to, currency = _extract_pt_change(joined)

            # For PT change decide direction label
            if kind == "pt_change":
                if pt_from is not None and pt_to is not None:
                    if pt_to > pt_from:
                        action = "Célár emelés"
                    elif pt_to < pt_from:
                        action = "Célár csökkentés"
                    else:
                        action = "Célár változatlan"
                else:
                    action = "Célár változás"

            e = AnalystEvent(
                ticker=ticker.upper(),
                date=date_obj.isoformat(),  # will be replaced by first_seen on output
                firm=firm,
                action=action,
                rating_from=rating_from,
                rating_to=rating_to,
                pt_from=pt_from,
                pt_to=pt_to,
                currency=currency,
                source_url=source_url,
            )
            key = (
                e.ticker,
                e.firm,
                e.action,
                e.rating_from,
                e.rating_to,
                e.pt_from,
                e.pt_to,
                e.source_url,
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(e)

        time.sleep(sleep_s)

    note = None
    if not fetch_ok:
        # If everything is blocked, expose that clearly (we will still output cache if available)
        if any("403" in v for v in statuses.values()):
            note = None
        else:
            note = None

    return out, statuses, fetch_ok, note


def write_outputs(
    out_md: Path,
    out_json: Optional[Path],
    events: List[AnalystEvent],
    days: int,
    fetch_ok: bool = True,
    cache_based: bool = False,
    cache_has_history: bool = False,
    cache_is_empty: bool = False,
    statuses: Optional[Dict[str, str]] = None,
    note: Optional[str] = None,
) -> str:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = []
    lines.append(f"# Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó {days} naptári nap) (első észlelés alapján)")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Generálva (UTC): {now}")



    lines.append("")

    if not events:
        if fetch_ok:
            # True empty (source reachable but no items)
            lines.append(f"_Nincs új fel/leminősítés vagy célár‑változás az elmúlt {days} naptári napban (MarketBeat / szűrés)._")
        else:
            # Blocked/error: if we have any previously-seen/cache state, show a cache-based "no events" line.
            if cache_based:
                if cache_is_empty:
                    lines.append(f"_Nincs friss fel/leminősítés vagy célár‑változás az elmúlt {days} naptári napban (cache még üres / még nem volt sikeres lekérés)._")
                else:
                    lines.append(f"_Nincs friss fel/leminősítés vagy célár‑változás az elmúlt {days} naptári napban (utolsó ismert cache alapján)._")
            else:
                # If MarketBeat is blocked and we have no cache yet, "N/A" is misleading.
                if cache_is_empty:
                    lines.append(f"_MarketBeat blokkolás / robotvédelem mellett a cache még üres, ezért most nincs megjeleníthető elemzői esemény (állíts be FINNHUB_API_KEY-t a fallbackhoz, vagy várd meg az első sikeres MarketBeat lekérést)._" )
                else:
                    lines.append("_N/A._")
    else:
        by: Dict[str, List[AnalystEvent]] = {}
        for e in events:
            by.setdefault(e.ticker, []).append(e)
        for t in sorted(by.keys()):
            lines.append(f"## {t}")
            for e in sorted(by[t], key=lambda x: x.date, reverse=True):
                parts = [f"- {e.date} — {e.firm} — {e.action}"]
                if e.rating_from or e.rating_to:
                    parts.append(f"Ajánlás: {e.rating_from or '—'} → {e.rating_to or '—'}")
                if e.pt_from is not None or e.pt_to is not None:
                    if e.pt_from is not None and e.pt_to is not None:
                        parts.append(f"Célár: {e.currency} {e.pt_from:.2f} → {e.pt_to:.2f}")
                    elif e.pt_to is not None:
                        parts.append(f"Célár: {e.currency} {e.pt_to:.2f}")
                parts.append(f"Forrás: {e.source_url}")
                lines.append(" | ".join(parts))
            lines.append("")

    if note:
        # Keep it short and non-invasive: just one italic line.
        if lines and lines[-1] != "":
            lines.append("")
        lines.append(f"_{note}_")

    md_text = "\n".join(lines).rstrip() + "\n"
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(md_text, encoding="utf-8")

    if out_json:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": VERSION,
            "generated_utc": now,
            "days": days,
            "count": len(events),
            "events": [asdict(e) for e in events],
        }
        out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    return md_text


def read_master_tickers(master_csv: Path) -> List[str]:
    # CSV exported from Google Sheets: contains "Ticker" column (or similar)
    txt = master_csv.read_text(encoding="utf-8", errors="replace")
    lines = [l for l in txt.splitlines() if l.strip()]
    if not lines:
        return []
    header = [h.strip().strip('"') for h in lines[0].split(",")]
    # Try common column names
    idx = None
    for cand in ("Ticker", "ticker", "TICKER", "Symbol", "symbol"):
        if cand in header:
            idx = header.index(cand)
            break
    if idx is None:
        # Fallback: first column
        idx = 0
    out: List[str] = []
    for l in lines[1:]:
        cols = [c.strip().strip('"') for c in l.split(",")]
        if idx < len(cols):
            t = cols[idx].strip().upper()
            if t and re.fullmatch(r"[A-Z0-9.\-]{1,12}", t):
                out.append(t)
    return sorted(set(out))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-json", default="")
    ap.add_argument("--seen-file", default=DEFAULT_SEEN_FILE)
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument(
        "--finnhub_api_key",
        default=os.environ.get("FINNHUB_API_KEY", ""),
        help="Finnhub API key (optional) for analyst feed fallback when MarketBeat is blocked",
    )
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug-dir", default="reports/debug_marketbeat")
    args = ap.parse_args()

    master = Path(args.master)
    if not master.exists():
        _log(f"ERROR: MASTER CSV not found: {master}")
        # Still write a small md so the main report doesn't break
        out_md = Path(args.out_md)
        write_outputs(out_md, None, [], args.days, fetch_ok=False, statuses={"master": "missing"}, note=None)
        return 0

    debug_dir = Path(args.debug_dir) if args.debug else None

    _log(f"START {VERSION} days={args.days} master={master} mode=ratings_pages")

    today_iso = datetime.utcnow().date().isoformat()
    cutoff_iso = (datetime.utcnow().date() - timedelta(days=args.days - 1)).isoformat()

    tickers = read_master_tickers(master)

    seen_path = Path(args.seen_file)
    seen_db = _load_seen_db(seen_path)

    # Fetch today's items (best effort)
    events_today: List[AnalystEvent] = []
    statuses: Dict[str, str] = {}
    fetch_ok = False

    note: Optional[str] = None

    try:
        events_today, statuses, fetch_ok, note = fetch_events_from_ratings_pages(
            master_tickers=tickers,
            days=args.days,
            timeout=args.timeout,
            sleep_s=max(args.sleep, 0.35),
            debug_dir=debug_dir,
        )
    except Exception as e:
        fetch_ok = False
        note = None
        _log(f"WARN: exception during fetch: {e}")

    if fetch_ok and events_today:
        _update_seen_with_today(seen_db, events_today, today_iso)
        _save_seen_db(seen_path, seen_db)
    elif fetch_ok:
        # Source ok but no events: still update db 'last_seen' is irrelevant; keep db as-is
        _save_seen_db(seen_path, seen_db)
    else:
        # Source not ok: do not overwrite cache; keep db untouched
        pass

    # Default output: from seen-db window (first_seen-based)
    events_out = _fresh_events_from_seen(seen_db, cutoff_iso)


    # Finnhub fallback: if MarketBeat is blocked/challenged and we have no events in the seen-window,
    # try pulling an official upgrades/downgrades API and filter to our MASTER.
    finnhub_key = (args.finnhub_api_key or "").strip()
    blocked_hint = " ".join(statuses.values()).lower()
    if (not events_out) and finnhub_key and ("blocked" in blocked_hint or "challenge" in blocked_hint or not fetch_ok):
        _log("INFO: MarketBeat blocked/challenge -> trying Finnhub upgrade/downgrade API fallback")
        fh_events, fh_statuses = fetch_events_from_finnhub(
            master_tickers=tickers,
            days=args.days,
            api_key=finnhub_key,
            timeout=args.timeout,
        )
        for k, v in fh_statuses.items():
            statuses[f"_{k}"] = v
        if fh_events:
            events_out = fh_events
            note = (note or "Megjegyzés: MarketBeat blokkolás / robotvédelem (a feed nem megbízhatóan elérhető).") + " Finnhub fallback használva."
        else:
            _log("INFO: Finnhub fallback returned 0 events (or not reachable)")
    elif (not events_out) and ("blocked" in blocked_hint or "challenge" in blocked_hint or not fetch_ok) and (not finnhub_key):
        _log("INFO: MarketBeat blocked/challenge but FINNHUB_API_KEY is not set -> no API fallback")



    # If the source is not OK and the seen-db window is empty, fallback to last successful payload.
    if (not fetch_ok or statuses.get("_note") == "parse_issue") and not events_out:
        last_payload = _load_last_success(Path(LAST_SUCCESS_JSON))
        if last_payload and isinstance(last_payload.get("events"), list):
            gen = str(last_payload.get("generated_utc") or "")
            cached: List[AnalystEvent] = []
            for d in last_payload.get("events", []):
                try:
                    # last_success payload is saved from this script, so field names match AnalystEvent.
                    ev = AnalystEvent(
                        ticker=str(d.get("ticker") or "").upper(),
                        date=str(d.get("date") or ""),
                        firm=str(d.get("firm") or ""),
                        action=str(d.get("action") or ""),
                        rating_from=(d.get("rating_from") if d.get("rating_from") not in ("", None) else None),
                        rating_to=(d.get("rating_to") if d.get("rating_to") not in ("", None) else None),
                        pt_from=(float(d["pt_from"]) if d.get("pt_from") not in ("", None) else None),
                        pt_to=(float(d["pt_to"]) if d.get("pt_to") not in ("", None) else None),
                        currency=str(d.get("currency") or "USD"),
                        source_url=str(d.get("source_url") or ""),
                    )
                    if ev.date and ev.date >= cutoff_iso:
                        cached.append(ev)
                except Exception:
                    continue
            if cached:
                events_out = cached
                note = f"Megjegyzés: MarketBeat most blokkolt/hibás; a legutóbbi sikeres mentett eredmény látható (UTC: {gen})."

    events_out.sort(key=lambda e: (e.date, e.ticker), reverse=True)

    out_md = Path(args.out_md)
    out_json = Path(args.out_json) if args.out_json else None

    # Add a short note on failures.
    if not fetch_ok and not note:
        st = " ".join(statuses.values()).lower()
        if "blocked" in st or "http 403" in st or "http 429" in st:
            note = "Megjegyzés: MarketBeat blokkolás / robotvédelem (a feed nem megbízhatóan elérhető)."
        elif statuses.get("_note") == "parse_issue":
            note = "Megjegyzés: MarketBeat oldal szerkezete változhatott (parse 0 sor)."
        else:
            note = "Megjegyzés: MarketBeat forráshiba."

    def _clean_status(s: str) -> bool:
        ss = (s or "").lower()
        return ss.startswith("http 200") and ("blocked" not in ss) and ("unexpected" not in ss)

    fetch_ok_all = all(_clean_status(statuses.get(k, "")) for k in ("upgrade", "downgrade", "pt_change"))

    def _has_last_success_events(p: Path) -> bool:
        try:
            if not p.exists():
                return False
            d = json.loads(p.read_text(encoding='utf-8', errors='replace'))
            ev = d.get('events')
            return isinstance(ev, list) and len(ev) > 0
        except Exception:
            return False

    cache_has_history = bool(seen_db)
    cache_has_last_success = _has_last_success_events(Path(LAST_SUCCESS_JSON))
    cache_any = cache_has_history or cache_has_last_success
    cache_is_empty = (not cache_has_history) and (not cache_has_last_success)

    md_text = write_outputs(
        out_md,
        out_json,
        events_out,
        args.days,
        fetch_ok=fetch_ok,
        cache_based=((not fetch_ok) and cache_any),
        cache_has_history=cache_has_history,
        cache_is_empty=cache_is_empty,
        statuses=statuses,
        note=note,
    )

    if fetch_ok_all:
        payload = {
            "generated_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "days": args.days,
            "statuses": statuses,
            "events": [asdict(e) for e in events_out],
        }
        _save_last_success(Path(LAST_SUCCESS_JSON), Path(LAST_SUCCESS_MD), payload, md_text)

    _log(f"DONE events={len(events_out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
