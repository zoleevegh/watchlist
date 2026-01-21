#!/usr/bin/env python3
# analyst_marketbeat.py — v0.3.0-marketbeat-free-2026-01-15
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
import html
import json
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional, Dict, Tuple

VERSION = "v0.3.0-marketbeat-free-2026-01-15"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
BASE = "https://www.marketbeat.com"


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


def _http_get(url: str, timeout: int, debug_dir: Optional[Path], debug_tag: str) -> Tuple[int, str]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "close",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            raw = resp.read()
            text = raw.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        status = e.code
        try:
            text = e.read().decode("utf-8", errors="replace")
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


def _extract_first_stock_url_from_search(html_text: str) -> Optional[str]:
    m = re.search(r'href="(/stocks/[A-Z]+/[A-Z0-9.\-]+/)"', html_text)
    if m:
        return urllib.parse.urljoin(BASE, m.group(1))
    m2 = re.search(r'<link[^>]+rel="canonical"[^>]+href="([^"]+)"', html_text, re.I)
    if m2 and "/stocks/" in m2.group(1):
        return m2.group(1)
    return None


EXCHANGE_TRIES = ["NASDAQ", "NYSE", "AMEX", "NYSEARCA", "OTCMKTS", "OTCBB"]

def _resolve_ticker_to_stock_url(ticker: str, timeout: int, debug_dir: Optional[Path]) -> Optional[str]:
    # 1) próbáljuk direkt a stock oldalt (kerüli a 403-at adó search endpointot)
    for ex in EXCHANGE_TRIES:
        url = f"{BASE}/stocks/{ex}/{ticker}/"
        status, text = _http_get(url, timeout, debug_dir, f"{ticker}_{ex}")
        if status == 200 and (f"/stocks/{ex}/" in text or f"/stocks/{ex}/{ticker}/" in text):
            return url
        time.sleep(0.15)  # pici jitter is elég itt

    # 2) fallback: ha nagyon muszáj, próbáld a search-öt (de lehet 403)
    q = urllib.parse.urlencode({"Symbol": ticker})
    url = f"{BASE}/stocks/?{q}"
    status, text = _http_get(url, timeout, debug_dir, f"{ticker}_search")
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
    rows: List[List[str]] = []
    for tr in re.findall(r"<tr[^>]*>.*?</tr>", stock_html, flags=re.I | re.S):
        tds = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.I | re.S)
        if not tds:
            continue
        cells = [_clean_text(re.sub(r"<[^>]+>", " ", c)) for c in tds]
        if not _parse_date_any(cells[0]):
            continue
        rows.append(cells)
    return rows


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


def write_outputs(out_md: Path, out_json: Optional[Path], events: List[AnalystEvent], days: int) -> None:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = []
    lines.append(f"# Analyst feed (upgrade/downgrade + PT) — last {days} calendar days")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Generálva (UTC): {now}")
    lines.append("")
    if not events:
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

    _log(f"START {VERSION} days={args.days} master={master}")

    tickers = read_master_tickers(master)
    events: List[AnalystEvent] = []

    for t in tickers:
        try:
            stock_url = _resolve_ticker_to_stock_url(t, args.timeout, debug_dir)
            if not stock_url:
                _log(f"{t}: not resolved (skip)")
                time.sleep(args.sleep)
                continue
            status, stock_html = _http_get(stock_url, args.timeout, debug_dir, f"{t}_stock")
            if status >= 400:
                _log(f"{t}: HTTP {status} stock page (skip)")
                time.sleep(args.sleep)
                continue
            rows = _extract_upgrade_rows(stock_html)
            evs = _rows_to_events(t, stock_url, rows, args.days)
            if evs:
                _log(f"{t}: {len(evs)} event(s)")
                events.extend(evs)
        except Exception as e:
            _log(f"{t}: ERROR {e}")
        time.sleep(args.sleep)

    events.sort(key=lambda e: (e.date, e.ticker), reverse=True)
    out_md = Path(args.out_md)
    out_json = Path(args.out_json) if args.out_json else None
    write_outputs(out_md, out_json, events, args.days)
    _log(f"DONE events={len(events)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
