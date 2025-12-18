#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
yahoo_analyst_events_fetcher.py – v4.0.0 (A: MarketBeat analyst ratings tracker)

Kimenet:
  - reports/yahoo_analyst_{report}.json

Mit csinál:
  - MarketBeat \"ratings\" tracker oldaláról kiolvassa a friss broker-akciókat
    (upgrade/downgrade/initiations/price target változások), és csak a report univerzum tickereire szűr.
  - Targets snapshot: MarketBeat forecast oldalról (consensus / hi / low) best-effort.

Miért:
  - A Yahoo root.App.main / QuoteSummaryStore szerkezet folyamatosan változik,
    ezért az eddigi Yahoo-s parserek gyakran 0 itemet adtak és tele voltak hibával.

Időablak:
  - lookbackDays (default 14) – ez illeszkedik a high-conv logikához is.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
from urllib.request import Request, urlopen

VERSION = "4.0.0"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

EXCHANGES = ["NASDAQ", "NYSE", "AMEX", "OTCMKTS", "TSX", "TSXV"]

MB_RATINGS_URL = "https://www.marketbeat.com/ratings/us/"

def _http_get(url: str, timeout: int = 25) -> str:
    req = Request(url, headers={"User-Agent": UA, "Accept": "text/html,*/*"})
    with urlopen(req, timeout=timeout) as r:
        raw = r.read()
    for enc in ("utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="replace")


def _load_universe(report: str) -> List[str]:
    latest = Path("reports") / f"latest_{report}.json"
    if latest.exists():
        try:
            data = json.loads(latest.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("tickers"), list):
                return [str(x).strip().upper() for x in data["tickers"] if str(x).strip()]
            if isinstance(data, list):
                out = []
                for row in data:
                    if isinstance(row, dict):
                        t = row.get("ticker") or row.get("Ticker") or row.get("symbol")
                        if t:
                            out.append(str(t).strip().upper())
                    elif isinstance(row, str):
                        out.append(row.strip().upper())
                return [t for t in out if t]
        except Exception:
            pass

    master = Path("reports") / "master.csv"
    if master.exists():
        out = []
        with master.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
        if not rows:
            return []
        header = [h.strip().lower() for h in rows[0]]
        ticker_idx = 0
        for i, h in enumerate(header):
            if h in ("ticker", "symbol"):
                ticker_idx = i
                break
        for r in rows[1:]:
            if len(r) > ticker_idx:
                t = r[ticker_idx].strip().upper()
                if t:
                    out.append(t)
        return out

    return []


def _parse_mb_date(s: str, now_utc: datetime) -> Optional[datetime]:
    """
    MarketBeat ratings tracker tipikusan 'Dec 18, 2025' vagy '12/18/2025' formátumokkal jön.
    """
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    # sometimes 'Dec. 18, 2025'
    s2 = s.replace(".", "")
    for fmt in ("%b %d, %Y",):
        try:
            return datetime.strptime(s2, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _strip_tags(html: str) -> str:
    html = re.sub(r"<script[\\s\\S]*?</script>", " ", html, flags=re.I)
    html = re.sub(r"<style[\\s\\S]*?</style>", " ", html, flags=re.I)
    html = re.sub(r"<[^>]+>", " ", html)
    html = re.sub(r"\\s+", " ", html).strip()
    return html


def _extract_ticker_from_cell(cell_html: str) -> Optional[str]:
    """
    ratings trackerben a ticker sokszor zárójelben szerepel: 'Apple (AAPL)'
    """
    txt = _strip_tags(cell_html)
    m = re.search(r"\\((?P<t>[A-Z0-9\\.\\-]{1,10})\\)", txt)
    if m:
        return m.group("t").upper()
    # fallback: data-ticker attr
    m2 = re.search(r'data-ticker\\s*=\\s*\"([^\"]+)\"', cell_html, flags=re.I)
    if m2:
        return m2.group(1).upper()
    return None


def _parse_ratings_table(html: str) -> List[Dict]:
    """
    Best-effort HTML table parser: a ratings/us oldalon a fő táblázat sorait kivesszük.
    """
    rows = []
    # find table rows
    for m in re.finditer(r"<tr[^>]*>(?P<tr>[\\s\\S]*?)</tr>", html, flags=re.I):
        tr = m.group("tr")
        cells = re.findall(r"<t[dh][^>]*>([\\s\\S]*?)</t[dh]>", tr, flags=re.I)
        if len(cells) < 4:
            continue
        # Heurisztika a tipikus oszlopokra: Date | Company | Action | Firm | Rating | PT (nem mindig ugyanaz)
        date_txt = _strip_tags(cells[0])
        ticker = _extract_ticker_from_cell(cells[1]) or _extract_ticker_from_cell(tr)
        action_txt = _strip_tags(cells[2])
        firm_txt = _strip_tags(cells[3]) if len(cells) > 3 else ""
        rating_txt = _strip_tags(cells[4]) if len(cells) > 4 else ""
        pt_txt = _strip_tags(cells[5]) if len(cells) > 5 else ""

        if not date_txt or not ticker:
            continue

        rows.append(
            {
                "date": date_txt,
                "ticker": ticker,
                "action": action_txt,
                "firm": firm_txt,
                "rating": rating_txt,
                "priceTargetRaw": pt_txt,
            }
        )
    return rows


def _mb_forecast_url(ticker: str, exch: str) -> str:
    return f"https://www.marketbeat.com/stocks/{exch}/{quote(ticker)}/forecast/"


def _extract_targets_snapshot(html: str) -> Dict:
    """
    A forecast oldalon tipikusan szerepel:
      - Consensus Price Target
      - highest / lowest price target
    """
    txt = _strip_tags(html)
    snap = {}

    # consensus price target: often like '$283.92'
    m = re.search(r"Consensus Price Target\\s+\\$?([0-9]+(?:\\.[0-9]+)?)", txt, flags=re.I)
    if m:
        snap["consensus"] = float(m.group(1))

    m = re.search(r"highest price target\\s+is\\s+\\$?([0-9]+(?:\\.[0-9]+)?)", txt, flags=re.I)
    if m:
        snap["high"] = float(m.group(1))

    m = re.search(r"lowest price target\\s+is\\s+\\$?([0-9]+(?:\\.[0-9]+)?)", txt, flags=re.I)
    if m:
        snap["low"] = float(m.group(1))

    # consensus rating label (Moderate Buy etc.)
    m = re.search(r"Consensus Rating\\s+([A-Za-z ]{3,25})", txt, flags=re.I)
    if m:
        snap["consensusRating"] = m.group(1).strip()

    return snap


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="1")
    ap.add_argument("--lookback-days", type=int, default=14)
    ap.add_argument("--sleep", type=float, default=0.6)
    args = ap.parse_args()

    report = str(args.report)
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=int(args.lookback_days))

    universe = set([t for t in _load_universe(report) if t and "." not in t and "/" not in t])

    items: List[Dict] = []
    errors_ratings: List[str] = []
    errors_targets: List[str] = []
    targets_snapshot: Dict[str, Dict] = {}

    # --- 1) Ratings tracker ---
    try:
        html = _http_get(MB_RATINGS_URL)
        rows = _parse_ratings_table(html)
        for r in rows:
            t = r["ticker"]
            if t not in universe:
                continue
            dt = _parse_mb_date(r["date"], now_utc)
            if not dt:
                continue
            if not (start_utc <= dt <= now_utc):
                continue

            # try parse PT number if present
            pt = None
            if r.get("priceTargetRaw"):
                m = re.search(r"\\$?([0-9]+(?:\\.[0-9]+)?)", r["priceTargetRaw"])
                if m:
                    try:
                        pt = float(m.group(1))
                    except Exception:
                        pt = None

            items.append(
                {
                    "ticker": t,
                    "dateUtc": dt.isoformat(),
                    "source": "marketbeat_ratings_tracker",
                    "action": r.get("action") or "",
                    "firm": r.get("firm") or "",
                    "rating": r.get("rating") or "",
                    "priceTarget": pt,
                    "priceTargetRaw": r.get("priceTargetRaw") or "",
                }
            )
    except Exception as e:
        errors_ratings.append(f"ratings-tracker: {type(e).__name__}: {e}")

    # --- 2) Targets snapshot (per ticker, best-effort) ---
    for t in sorted(universe):
        got = False
        last_err = None
        for exch in EXCHANGES:
            url = _mb_forecast_url(t, exch)
            try:
                html = _http_get(url)
                snap = _extract_targets_snapshot(html)
                if snap:
                    targets_snapshot[t] = {"source": "marketbeat_forecast", "exchange": exch, **snap}
                    got = True
                    break
                last_err = f"{t}: snapshot-empty ({exch})"
            except Exception as e:
                last_err = f"{t}: fetch-error ({exch}): {type(e).__name__}: {e}"
            finally:
                if args.sleep:
                    time.sleep(float(args.sleep))
        if not got and last_err:
            errors_targets.append(last_err)

    payload = {
        "ok": True,
        "type": "yahoo_analyst_events",
        "version": VERSION,
        "report": report,
        "generatedAt": now_utc.isoformat(),
        "window": {"lookbackDays": int(args.lookback_days)},
        "count": len(items),
        "items": sorted(items, key=lambda x: (x.get("dateUtc", ""), x.get("ticker", ""))),
        "targetsSnapshot": targets_snapshot,
        "sources": {
            "marketbeat_ratings_tracker": {
                "ok": len(errors_ratings) == 0,
                "count": len([i for i in items if i.get("source") == "marketbeat_ratings_tracker"]),
                "errors": errors_ratings[:50],
                "url": MB_RATINGS_URL,
            },
            "marketbeat_forecast_targets_snapshot": {
                "ok": len(targets_snapshot) > 0,
                "count": len(targets_snapshot),
                "errors": errors_targets[:200],
            },
        },
    }

    out_path = Path("reports") / f"yahoo_analyst_{report}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
