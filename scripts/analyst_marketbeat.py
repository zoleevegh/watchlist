#!/usr/bin/env python3
# analyst_marketbeat.py — v0.3.4-marketbeat-free-2026-01-21
#
# FREE analyst feed (upgrade/downgrade + PT change) using MarketBeat HTML.
# Goal: last N calendar days (default 2) for tickers in MASTER CSV.
#
# Notes:
# - Uses only stdlib (urllib, html, re). No API keys.
# - Tries MarketBeat search to resolve ticker -> stock page URL.
# - Parses "Analyst Upgrades and Downgrades" style rows when present.
#
# Exit codes:
# - 0 OK (even if no events found)
# - 2 bad args / missing master
# - 3 master parse error
# - 4 network errors (fatal)
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import html
import http.cookiejar
import json
import random
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Dict, Tuple

VERSION = "v0.3.4-marketbeat-free-2026-01-21"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
BASE = "https://www.marketbeat.com"

# Cookie-aware opener (MarketBeat sometimes blocks requests without a session).
_COOKIEJAR = http.cookiejar.CookieJar()
_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_COOKIEJAR))


@dataclass
class AnalystEvent:
    ticker: str
    date: str  # ISO date YYYY-MM-DD
    firm: str
    action: str
    rating_from: Optional[str]
    rating_to: Optional[str]
    pt_from: Optional[float]
    pt_to: Optional[float]
    currency: str
    source_url: str


def _log(msg: str) -> None:
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


def _warmup_session(timeout: int, debug_dir: Optional[Path]) -> None:
    """Best-effort warmup to obtain cookies/session."""
    try:
        status, _ = _http_get_raw(f"{BASE}/", timeout, debug_dir, "warmup_home")
        _log(f"WARMUP home: HTTP {status}")
    except Exception as e:
        _log(f"WARMUP home: ERROR {e}")


def _http_get_raw(url: str, timeout: int, debug_dir: Optional[Path], debug_tag: str) -> Tuple[int, str]:
    """Low-level HTTP GET with cookie-aware opener."""
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip",
            "Connection": "close",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
        method="GET",
    )
    try:
        with _OPENER.open(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            raw = resp.read()
            enc = ""
            try:
                enc = (resp.headers.get("Content-Encoding") or "").lower()
            except Exception:
                enc = ""
            if enc == "gzip":
                try:
                    raw = gzip.decompress(raw)
                except Exception:
                    pass
            text = raw.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        status = e.code
        try:
            raw = e.read()
            # HTTPError also has headers sometimes
            enc = ""
            try:
                enc = (e.headers.get("Content-Encoding") or "").lower()
            except Exception:
                enc = ""
            if enc == "gzip":
                try:
                    raw = gzip.decompress(raw)
                except Exception:
                    pass
            text = raw.decode("utf-8", errors="replace")
        except Exception:
            text = ""
    except Exception as e:
        raise RuntimeError(f"Network error for {url}: {e}") from e

    if debug_dir:
        debug_dir.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", debug_tag)[:120]
        (debug_dir / f"{safe}.status.txt").write_text(str(status), encoding="utf-8")
        (debug_dir / f"{safe}.html").write_text(text, encoding="utf-8")

    return status, text


def _jina_wrap(url: str) -> str:
    if url.startswith("https://"):
        return "https://r.jina.ai/https://" + url[len("https://"):]
    if url.startswith("http://"):
        return "https://r.jina.ai/http://" + url[len("http://"):]
    return "https://r.jina.ai/" + url


def _http_get_best(
    url: str,
    timeout: int,
    debug_dir: Optional[Path],
    debug_tag: str,
    allow_jina: bool = True,
    max_attempts: int = 3,
) -> Tuple[int, str, str, int]:
    """Best-effort GET: retry with jitter; optionally fallback via r.jina.ai.

    Returns: (status, text, via, direct_status)
    """
    direct_status = 0
    last_status = 0
    last_text = ""

    # Direct attempts first
    for i in range(max_attempts):
        status, text = _http_get_raw(url, timeout, debug_dir, f"{debug_tag}_direct_{i+1}")
        direct_status = status if i == 0 else direct_status
        last_status, last_text = status, text
        if status < 400:
            return status, text, "direct", direct_status
        # retry on 403/429/5xx
        if status in (403, 429) or status >= 500:
            time.sleep(0.6 + random.random() * 0.8)
            continue
        break

    if not allow_jina:
        return last_status, last_text, "direct", direct_status

    # Fallback via Jina proxy
    jurl = _jina_wrap(url)
    for i in range(max_attempts):
        status, text = _http_get_raw(jurl, timeout, debug_dir, f"{debug_tag}_jina_{i+1}")
        last_status, last_text = status, text
        if status < 400:
            return status, text, "jina", direct_status
        if status in (403, 429) or status >= 500:
            time.sleep(0.6 + random.random() * 0.8)
            continue
        break

    return last_status, last_text, "jina", direct_status


def _extract_first_stock_url_from_search(html_text: str) -> Optional[str]:
    m = re.search(r'href="(/stocks/[A-Z]+/[A-Z0-9.\-]+/)"', html_text)
    if m:
        return urllib.parse.urljoin(BASE, m.group(1))
    m2 = re.search(r'<link[^>]+rel="canonical"[^>]+href="([^"]+)"', html_text, re.I)
    if m2 and "/stocks/" in m2.group(1):
        return m2.group(1)
    return None


def _resolve_ticker_to_stock_url(ticker: str, timeout: int, debug_dir: Optional[Path]) -> Optional[str]:
    q = urllib.parse.urlencode({"Symbol": ticker})
    url = f"{BASE}/stocks/?{q}"
    status, text = _http_get_raw(url, timeout, debug_dir, f"{ticker}_search")
    if status >= 400:
        _log(f"RESOLVE {ticker}: HTTP {status} on search page")
        return None
    return _extract_first_stock_url_from_search(text)


_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

def _parse_date_any(s: str) -> Optional[dt.date]:
    s = s.strip()
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", s)
    if m:
        mon = _MONTHS.get(m.group(1).lower())
        if mon:
            return dt.date(int(m.group(3)), mon, int(m.group(2)))
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return None


def _clean_text(s: str) -> str:
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_money(s: str) -> Optional[float]:
    s = _clean_text(s)
    if not s:
        return None
    s2 = re.sub(r"[^0-9.\-]", "", s)
    if not s2 or s2 in ("-", "."):
        return None
    try:
        return float(s2)
    except Exception:
        return None


def _extract_upgrade_rows(stock_html: str) -> List[List[str]]:
    """Backward-compatible row extractor.
    MarketBeat ratings pages and stock pages both typically render HTML tables.
    We extract <tr> rows and convert <td>/<th> cells into plain text.
    """
    rows: List[List[str]] = []
    for tr in re.findall(r"<tr[^>]*>.*?</tr>", stock_html, flags=re.I | re.S):
        tds = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.I | re.S)
        if not tds:
            continue
        cells = [_clean_text(re.sub(r"<[^>]+>", " ", c)) for c in tds]
        # keep only rows that contain at least one parseable date cell
        if not any(_parse_date_any(c) for c in cells[:2]):
            continue
        rows.append(cells)
    return rows


RATINGS_SOURCES: List[Tuple[str, str]] = [
    ("upgrade", "/ratings/upgrades/"),
    ("downgrade", "/ratings/downgrades/"),
    ("pt_change", "/ratings/pricetargetchanges/"),
]


def _extract_urls_from_row_html(tr_html: str) -> Dict[str, str]:
    """Extract likely useful URLs (details + stock) from a <tr> HTML chunk."""
    out: Dict[str, str] = {}
    # stock page URLs like /stocks/NASDAQ/AAPL/
    m = re.search(r'href="(?P<url>/stocks/[A-Z0-9]+/[A-Z0-9\.\-]+/)"', tr_html, flags=re.I)
    if m:
        out["stock"] = BASE + m.group("url")
    # details page (often /ratings/ or /ratings/by-issuer/ or similar)
    m2 = re.search(r'href="(?P<url>/ratings/[^"]+/)"', tr_html, flags=re.I)
    if m2:
        out["details"] = BASE + m2.group("url")
    return out


def _extract_ticker_from_row(tr_html: str, cells: List[str]) -> Optional[str]:
    # Prefer stock URL because it is unambiguous.
    m = re.search(r"/stocks/[A-Z0-9]+/(?P<t>[A-Z0-9\.\-]+)/", tr_html, flags=re.I)
    if m:
        return m.group("t").upper()
    # Fallback: scan cells for a ticker-like token.
    for c in cells:
        c2 = re.sub(r"[^A-Z0-9\.\- ]", " ", c.upper()).strip()
        for tok in c2.split():
            if 1 <= len(tok) <= 6 and re.fullmatch(r"[A-Z][A-Z0-9\.\-]{0,5}", tok):
                return tok
    return None


def _extract_rating_change(text: str) -> Tuple[Optional[str], Optional[str]]:
    # Handles: "Hold ➝ Buy", "Hold → Buy", "Hold -> Buy"
    arrow_pat = r"(Strong Buy|Buy|Hold|Sell|Overweight|Underweight|Outperform|Underperform|Market Perform|Sector Perform)"
    m = re.search(rf"\b{arrow_pat}\b\s*(?:➝|→|->|to)\s*\b{arrow_pat}\b", text, flags=re.I)
    if not m:
        return None, None
    # m.group(0) contains full; use findall to get both
    vals = re.findall(arrow_pat, m.group(0), flags=re.I)
    if len(vals) >= 2:
        return vals[0].title(), vals[1].title()
    return None, None


def _extract_pt_change(text: str) -> Tuple[Optional[float], Optional[float], str]:
    # Prefer explicit "from X to Y"
    m = re.search(r"(?:from|From)\s*\$?([0-9]{1,6}(?:\.[0-9]{1,2})?)\s*(?:to|To)\s*\$?([0-9]{1,6}(?:\.[0-9]{1,2})?)", text)
    if m:
        return float(m.group(1)), float(m.group(2)), "$"
    # Otherwise, we may only have a single price target in the row. Heuristic: last $ amount.
    amts = [float(x) for x in re.findall(r"\$\s*([0-9]{1,6}(?:\.[0-9]{1,2})?)", text)]
    if amts:
        return None, amts[-1], "$"
    return None, None, "$"


def _guess_firm(cells: List[str], joined: str) -> str:
    # Brokerage is usually present as a short token (e.g., "HSBC", "TD Cowen").
    # Pick the first "name-like" cell that isn't a date, action, rating, or pure number.
    blacklist = {"UPGRADED", "DOWNGRADED", "UPGRADED BY", "DOWNGRADED BY", "RAISED", "LOWERED", "PRICE", "TARGET", "RATING", "DETAILS", "ACTION", "BROKERAGE", "ANALYST", "COMPANY", "CURRENT"}
    for c in cells:
        cu = c.strip()
        if not cu:
            continue
        if _parse_date_any(cu):
            continue
        if cu.upper() in blacklist:
            continue
        if re.search(r"\b(Upgraded|Downgraded)\b", cu, re.I):
            continue
        if re.fullmatch(r"\$?[0-9,\.]+%?", cu.replace(" ", "")):
            continue
        if re.search(r"\b(Strong Buy|Buy|Hold|Sell|Overweight|Underweight|Outperform|Underperform|Market Perform|Sector Perform)\b", cu, re.I):
            continue
        # likely a brokerage name
        if 2 <= len(cu) <= 40:
            return cu
    return "—"


def _kind_to_action(kind: str, joined: str) -> str:
    if kind == "upgrade":
        return "Upgrade"
    if kind == "downgrade":
        return "Downgrade"
    if kind == "pt_change":
        # if text hints direction, keep it
        if re.search(r"\braised\b|\bincreased\b", joined, re.I):
            return "Price target raised"
        if re.search(r"\blowered\b|\bdecreased\b|\bcut\b", joined, re.I):
            return "Price target lowered"
        return "Price target change"
    return kind


def fetch_events_from_ratings_pages(
    master_tickers: List[str],
    days: int,
    timeout: int,
    sleep_s: float,
    debug_dir: Optional[Path],
) -> Tuple[List[AnalystEvent], Dict[str, str], bool]:
    today = dt.datetime.utcnow().date()
    cutoff = today - dt.timedelta(days=days - 1)
    master_set = {t.upper() for t in master_tickers}

    out: List[AnalystEvent] = []
    seen: set = set()

    statuses: Dict[str, str] = {}
    pages_ok = 0

    for kind, path in RATINGS_SOURCES:
        url = BASE + path
        status, page_html, via, direct_status = _http_get_best(
            url, timeout, debug_dir, f"ratings_{kind}", allow_jina=True, max_attempts=3
        )
        if via == "direct":
            statuses[kind] = str(status)
        else:
            statuses[kind] = f"{direct_status}->{status}({via})"

        if status >= 400:
            _log(f"RATINGS {kind}: HTTP {statuses[kind]} (skip)")
            time.sleep(sleep_s)
            continue

        # Basic sanity: require some table/row signal.
        if "<tr" not in page_html.lower():
            time.sleep(sleep_s)
            continue
        pages_ok += 1

        # Iterate <tr> blocks; this is robust for MarketBeat ratings pages.
        for tr in re.findall(r"<tr[^>]*>.*?</tr>", page_html, flags=re.I | re.S):
            tds = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.I | re.S)
            if not tds:
                continue
            cells = [_clean_text(re.sub(r"<[^>]+>", " ", c)) for c in tds]
            # Find date cell (usually first)
            date_obj = None
            date_raw = None
            for c in cells[:3]:
                d = _parse_date_any(c)
                if d:
                    date_obj = d
                    date_raw = d.isoformat()
                    break
            if not date_obj:
                continue
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

            e = AnalystEvent(
                ticker=ticker.upper(),
                date=date_raw,
                firm=firm,
                action=action,
                rating_from=rating_from,
                rating_to=rating_to,
                pt_from=pt_from,
                pt_to=pt_to,
                currency=currency,
                source_url=source_url,
            )
            key = (e.ticker, e.date, e.firm, e.action, e.rating_from, e.rating_to, e.pt_from, e.pt_to, e.source_url)
            if key in seen:
                continue
            seen.add(key)
            out.append(e)

        time.sleep(sleep_s)

    fetch_ok = pages_ok > 0
    return out, statuses, fetch_ok


def _rows_to_events(ticker: str, stock_url: str, rows: List[List[str]], days: int) -> List[AnalystEvent]:
    today = dt.datetime.utcnow().date()
    cutoff = today - dt.timedelta(days=days - 1)
    out: List[AnalystEvent] = []
    for cells in rows:
        d = _parse_date_any(cells[0])
        if not d or d < cutoff:
            continue

        firm = cells[1] if len(cells) > 1 else ""
        action = cells[2] if len(cells) > 2 else ""

        rating_from = None
        rating_to = None
        pt_from = None
        pt_to = None
        currency = "USD"

        joined = " | ".join(cells)

        if len(cells) >= 5:
            if re.search(r"(Buy|Hold|Sell|Overweight|Underweight|Neutral|Outperform|Underperform|Market Perform|Sector Perform|Strong Buy)", cells[3], re.I):
                rating_from = cells[3] or None
            if re.search(r"(Buy|Hold|Sell|Overweight|Underweight|Neutral|Outperform|Underperform|Market Perform|Sector Perform|Strong Buy)", cells[4], re.I):
                rating_to = cells[4] or None

        mpt = re.search(r"(?:from|From)\s*\$?([0-9]{1,5}(?:\.[0-9]{1,2})?)\s*(?:to|To)\s*\$?([0-9]{1,5}(?:\.[0-9]{1,2})?)", joined)
        if mpt:
            pt_from = float(mpt.group(1))
            pt_to = float(mpt.group(2))
        else:
            v1 = _parse_money(cells[-2]) if len(cells) >= 2 else None
            v2 = _parse_money(cells[-1])
            if v1 is not None and v2 is not None:
                pt_from, pt_to = v1, v2
            elif v2 is not None:
                pt_to = v2

        if not firm and not action:
            continue

        out.append(AnalystEvent(
            ticker=ticker,
            date=d.isoformat(),
            firm=firm.strip(),
            action=action.strip(),
            rating_from=rating_from,
            rating_to=rating_to,
            pt_from=pt_from,
            pt_to=pt_to,
            currency=currency,
            source_url=stock_url,
        ))
    return out


def read_master_tickers(master_csv: Path) -> List[str]:
    with master_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return []
    header = [c.strip().lower() for c in rows[0]]
    ticker_idx = 0
    for i, c in enumerate(header):
        if c in ("ticker", "symbol"):
            ticker_idx = i
            break
    tickers: List[str] = []
    for r in rows[1:]:
        if len(r) <= ticker_idx:
            continue
        t = r[ticker_idx].strip().upper()
        if t and not t.startswith("#"):
            tickers.append(t)
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def write_outputs(
    out_md: Path,
    out_json: Optional[Path],
    events: List[AnalystEvent],
    days: int,
    fetch_ok: bool,
    statuses: Optional[Dict[str, str]] = None,
) -> None:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = []
    lines.append(f"# Analyst feed (upgrade/downgrade + PT) — last {days} calendar days")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Generálva (UTC): {now}")
    lines.append("")
    if not events:
        # IMPORTANT: distinguish between "no fresh events" and "source unreachable / blocked".
        if not fetch_ok:
            s_txt = ""
            if statuses:
                parts = [f"{k}={v}" for k, v in sorted(statuses.items())]
                s_txt = " (HTTP: " + ", ".join(parts) + ")"
            lines.append(f"_MarketBeat forrás nem elérhető / blokkolva, ezért nem tudtam friss analyst eseményeket lekérni._{s_txt}")
        else:
            # Pages loaded successfully, but nothing matched within the requested window.
            lines.append(f"_Nincs friss (≤{days} naptári nap) fel/leminősítés vagy célár-frissítés a forrásban._")
    else:
        by: Dict[str, List[AnalystEvent]] = {}
        for e in events:
            by.setdefault(e.ticker, []).append(e)
        for t in sorted(by.keys()):
            lines.append(f"## {t}")
            for e in sorted(by[t], key=lambda x: x.date, reverse=True):
                parts = [f"- {e.date} — {e.firm} — {e.action}"]
                if e.rating_from or e.rating_to:
                    parts.append(f"Rating: {e.rating_from or '—'} → {e.rating_to or '—'}")
                if e.pt_from is not None or e.pt_to is not None:
                    if e.pt_from is not None and e.pt_to is not None:
                        parts.append(f"PT: {e.currency} {e.pt_from:.2f} → {e.pt_to:.2f}")
                    elif e.pt_to is not None:
                        parts.append(f"PT: {e.currency} {e.pt_to:.2f}")
                parts.append(f"Forrás: {e.source_url}")
                lines.append("  ".join(parts))
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
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug-dir", default="reports/debug_marketbeat")
    args = ap.parse_args()

    master = Path(args.master)
    if not master.exists():
        _log(f"ERROR: MASTER CSV not found: {master}")
        return 2

    debug_dir = Path(args.debug_dir) if args.debug else None

    _log(f"START {VERSION} days={args.days} master={master} mode=ratings_pages")

    tickers = read_master_tickers(master)

    # Warm up cookies/session (best-effort).
    _warmup_session(timeout=args.timeout, debug_dir=debug_dir)

    # Solution #2: pull the central MarketBeat ratings lists once,
    # then filter to MASTER tickers (avoids per-ticker search that triggers HTTP 403).
    events, statuses, fetch_ok = fetch_events_from_ratings_pages(
        master_tickers=tickers,
        days=args.days,
        timeout=args.timeout,
        sleep_s=max(args.sleep, 0.35),
        debug_dir=debug_dir,
    )

    events.sort(key=lambda e: (e.date, e.ticker), reverse=True)
    out_md = Path(args.out_md)
    out_json = Path(args.out_json) if args.out_json else None
    write_outputs(out_md, out_json, events, args.days, fetch_ok=fetch_ok, statuses=statuses)
    _log(f"DONE events={len(events)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
