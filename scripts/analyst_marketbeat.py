#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# analyst_marketbeat.py — v0.5.0-nasdaq-api-2026-02-05
#
# PURPOSE
# - Replace brittle MarketBeat scraping (Cloudflare/challenge) with Nasdaq public JSON APIs.
# - Emit the same artifacts expected by PRICE ENGINE:
#     reports/analyst_last2d.md
#     reports/analyst_last2d.json
#
# CLI (compatible)
#   --master <csv>
#   --days N              (calendar days, UTC; default: 2)
#   --out-md <path>
#   --out-json <path>
#   --debug               (write extra diagnostics to reports/nasdaq_api_debug.json)
#
# NOTES
# - Nasdaq endpoints used are public JSON but require browser-like headers.
# - If Nasdaq returns empty/no-data for a symbol, we keep a per-run cache to avoid false "no events".
#
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

DEFAULT_DAYS = 2

NASDAQ_ENDPOINT_CANDIDATES = [
    # These endpoints are known to exist for some symbols (community-documented).
    # We try several, because Nasdaq has changed schemas over time.
    ("ratings", "https://api.nasdaq.com/api/analyst/{sym}/ratings"),
    ("recommendations", "https://api.nasdaq.com/api/analyst/{sym}/recommendations"),
    ("targetprice", "https://api.nasdaq.com/api/analyst/{sym}/targetprice"),
]

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_date_any(s: Any) -> Optional[dt.datetime]:
    if not s:
        return None
    s = str(s).strip()
    # Common formats observed across Nasdaq JSON
    # Examples: "02/05/2026", "2026-02-05", "2026-02-05T00:00:00.000Z"
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            d = dt.datetime.strptime(s, fmt)
            if d.tzinfo is None:
                d = d.replace(tzinfo=dt.timezone.utc)
            return d.astimezone(dt.timezone.utc)
        except Exception:
            pass
    # Try to extract yyyy-mm-dd
    m = re.search(r"(20\d{2})[-/](\d{2})[-/](\d{2})", s)
    if m:
        try:
            return dt.datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=dt.timezone.utc)
        except Exception:
            return None
    return None


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace("$", "").replace(",", "")
    if s == "" or s.lower() in {"n/a", "na", "null", "none", "-"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _headers(referer: str) -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "Origin": "https://www.nasdaq.com",
        "Connection": "keep-alive",
    }


@dataclass
class AnalystEvent:
    ticker: str
    date: str  # YYYY-MM-DD (UTC)
    brokerage: str
    action: str               # e.g., Upgrade/Downgrade/Reiterated/Initiated/PT Change
    rating_from: str
    rating_to: str
    pt_from: Optional[float]
    pt_to: Optional[float]
    source: str               # URL


def _read_master_tickers(master_csv: str) -> List[str]:
    tickers: List[str] = []
    with open(master_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        candidates = [c for c in (reader.fieldnames or [])]

        def pick_field() -> str:
            for k in candidates:
                if k and k.lower() in {"ticker", "symbol"}:
                    return k
            return candidates[0] if candidates else "ticker"

        field = pick_field()
        for row in reader:
            t = (row.get(field) or "").strip().upper()
            if not t or t.startswith("#"):
                continue
            # Project rule: omit PKN.WA unless explicitly asked
            if t == "PKN.WA":
                continue
            tickers.append(t)

    seen = set()
    out: List[str] = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _http_get_json(url: str, referer: str, timeout: int = 20) -> Tuple[Optional[Dict[str, Any]], int, str]:
    try:
        r = requests.get(url, headers=_headers(referer), timeout=timeout)
        status = r.status_code
        if status != 200:
            return None, status, r.text[:5000]
        try:
            return r.json(), status, ""
        except Exception:
            return None, status, r.text[:5000]
    except requests.RequestException as e:
        return None, 0, str(e)


def _extract_events_from_payload(ticker: str, payload: Dict[str, Any], source_url: str) -> List[AnalystEvent]:
    """
    Nasdaq schemas vary. This function tries multiple patterns.
    We look for dict nodes that contain a date and any of:
      - firm/brokerage, action, rating changes, price target changes.
    """
    events: List[AnalystEvent] = []

    def walk(obj: Any) -> None:
        if isinstance(obj, list):
            for it in obj:
                walk(it)
            return
        if not isinstance(obj, dict):
            return

        # Find a date field
        date_val: Optional[dt.datetime] = None
        for k, v in obj.items():
            if "date" in str(k).lower():
                d = _parse_date_any(v)
                if d:
                    date_val = d
                    break

        brokerage = str(
            obj.get("brokerage")
            or obj.get("firm")
            or obj.get("analystFirm")
            or obj.get("broker")
            or obj.get("source")
            or ""
        ).strip()

        action = str(obj.get("action") or obj.get("ratingAction") or obj.get("type") or obj.get("eventType") or "").strip()

        rating_from = str(
            obj.get("fromRating") or obj.get("previousRating") or obj.get("priorRating") or obj.get("oldRating") or obj.get("ratingFrom") or ""
        ).strip()
        rating_to = str(
            obj.get("toRating") or obj.get("newRating") or obj.get("currentRating") or obj.get("ratingTo") or obj.get("rating") or ""
        ).strip()

        pt_from = _safe_float(obj.get("fromPriceTarget") or obj.get("priorPriceTarget") or obj.get("oldPriceTarget") or obj.get("priceTargetFrom"))
        pt_to = _safe_float(obj.get("toPriceTarget") or obj.get("newPriceTarget") or obj.get("priceTarget") or obj.get("priceTargetTo"))

        # Skip pure consensus nodes (they are not events)
        if "consensusOverview" in obj and isinstance(obj.get("consensusOverview"), dict):
            for v in obj.values():
                walk(v)
            return

        if date_val and (action or brokerage or rating_from or rating_to or pt_from is not None or pt_to is not None):
            if not action and (pt_from is not None or pt_to is not None):
                action = "Price Target Change"
            if not action:
                action = "Rating Update"
            if not brokerage:
                brokerage = "Unknown"

            events.append(
                AnalystEvent(
                    ticker=ticker,
                    date=date_val.date().isoformat(),
                    brokerage=brokerage,
                    action=action,
                    rating_from=rating_from or "-",
                    rating_to=rating_to or "-",
                    pt_from=pt_from,
                    pt_to=pt_to,
                    source=source_url,
                )
            )

        for v in obj.values():
            walk(v)

    root: Any = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
    walk(root)

    # Dedup
    seen = set()
    uniq: List[AnalystEvent] = []
    for e in events:
        key = (e.ticker, e.date, e.brokerage.lower(), e.action.lower(), e.rating_from, e.rating_to, e.pt_from, e.pt_to)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)
    return uniq


def _load_cache(cache_path: Path) -> Dict[str, Any]:
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_cache(cache_path: Path, data: Dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _within_window(d: dt.datetime, days: int, now: dt.datetime) -> bool:
    start = (now - dt.timedelta(days=days)).date()
    return d.date() >= start


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS)
    ap.add_argument("--out-md", default="reports/analyst_last2d.md")
    ap.add_argument("--out-json", default="reports/analyst_last2d.json")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    out_md = Path(args.out_md)
    out_json = Path(args.out_json)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    tickers = _read_master_tickers(args.master)
    now = _now_utc()

    cache_path = Path("reports/nasdaq_events_cache.json")
    cache = _load_cache(cache_path)

    debug_items: Dict[str, Any] = {"run_utc": now.isoformat(), "days": args.days, "tickers": len(tickers), "by_ticker": {}}

    all_events: List[AnalystEvent] = []
    source_status: Dict[str, Dict[str, Any]] = {k: {"ok": 0, "fail": 0, "last_status": None} for k, _ in NASDAQ_ENDPOINT_CANDIDATES}

    for idx, t in enumerate(tickers, 1):
        per_ticker_debug: Dict[str, Any] = {"attempts": []}
        sym = t.lower()

        # Light throttle (Nasdaq can rate-limit)
        if idx > 1:
            time.sleep(0.15)

        ticker_events: List[AnalystEvent] = []

        for name, tpl in NASDAQ_ENDPOINT_CANDIDATES:
            url = tpl.format(sym=sym)
            referer = f"https://www.nasdaq.com/market-activity/stocks/{sym}/analyst-research"

            payload, status, err = _http_get_json(url, referer=referer)
            per_ticker_debug["attempts"].append(
                {"endpoint": name, "url": url, "status": status, "err_sample": (err[:300] if err else "")}
            )
            source_status[name]["last_status"] = status

            if payload is None:
                source_status[name]["fail"] += 1
                continue

            source_status[name]["ok"] += 1

            extracted = _extract_events_from_payload(t, payload, url)

            in_window: List[AnalystEvent] = []
            for e in extracted:
                d = _parse_date_any(e.date)
                if d and _within_window(d, args.days, now):
                    in_window.append(e)

            if in_window:
                ticker_events.extend(in_window)
                # First endpoint with concrete events wins
                break

        # If none found, try cache (persistent)
        if not ticker_events:
            cached = cache.get(t, [])
            kept = []
            for item in cached:
                d = _parse_date_any(item.get("date"))
                if d and _within_window(d, args.days, now):
                    kept.append(item)
            if kept:
                ticker_events = [AnalystEvent(**item) for item in kept]
                per_ticker_debug["cache_used"] = True
            else:
                per_ticker_debug["cache_used"] = False

        # Update cache with newly found events (keep last 14 days)
        if ticker_events:
            existing = cache.get(t, [])
            merged = existing + [asdict(e) for e in ticker_events]
            seen = set()
            out_list = []
            for it in sorted(
                merged,
                key=lambda x: (x.get("date", ""), x.get("brokerage", ""), x.get("action", "")),
                reverse=True,
            ):
                key = (
                    it.get("date"),
                    it.get("brokerage"),
                    it.get("action"),
                    it.get("rating_from"),
                    it.get("rating_to"),
                    it.get("pt_from"),
                    it.get("pt_to"),
                )
                if key in seen:
                    continue
                seen.add(key)
                out_list.append(it)

            trimmed = []
            for it in out_list:
                d = _parse_date_any(it.get("date"))
                if d and _within_window(d, 14, now):
                    trimmed.append(it)
            cache[t] = trimmed

        all_events.extend(ticker_events)
        debug_items["by_ticker"][t] = per_ticker_debug

    _save_cache(cache_path, cache)

    all_events_sorted = sorted(all_events, key=lambda e: (e.date, e.ticker, e.brokerage), reverse=True)

    lines: List[str] = []
    lines.append(f"## Elemzői feed (Nasdaq API) — fel/leminősítések + célár (utolsó {args.days} naptári nap)")
    ss_parts = []
    for name in ["ratings", "recommendations", "targetprice"]:
        st = source_status[name]
        ss_parts.append(f"{name}:ok={st['ok']},fail={st['fail']},last={st['last_status']}")
    lines.append(f"_forrás státusz: {' | '.join(ss_parts)}_")
    lines.append("")

    by_ticker: Dict[str, List[AnalystEvent]] = {}
    for e in all_events_sorted:
        by_ticker.setdefault(e.ticker, []).append(e)

    if not by_ticker:
        lines.append("_Nincs Nasdaq-API esemény a megadott ablakban; cache sem adott vissza találatot._")
    else:
        for t in sorted(by_ticker.keys()):
            lines.append(f"## {t}")
            for e in by_ticker[t]:
                pt_part = ""
                if e.pt_from is not None or e.pt_to is not None:
                    pf = "-" if e.pt_from is None else f"{e.pt_from:.2f}"
                    pt = "-" if e.pt_to is None else f"{e.pt_to:.2f}"
                    pt_part = f" | Célár: USD {pf} → {pt}"
                r_part = ""
                if (e.rating_from and e.rating_from != "-") or (e.rating_to and e.rating_to != "-"):
                    r_part = f" | Ajánlás: {e.rating_from} → {e.rating_to}"
                lines.append(f"- {e.date} — {e.brokerage} — {e.action}{r_part}{pt_part} | Forrás: {e.source}")
            lines.append("")

    out_md.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    payload_out = {
        "meta": {
            "engine": "nasdaq_api",
            "version": "0.5.0",
            "run_utc": now.isoformat(),
            "days": args.days,
            "tickers_total": len(tickers),
            "events_total": len(all_events_sorted),
            "source_status": source_status,
            "cache_path": str(cache_path),
        },
        "events": [asdict(e) for e in all_events_sorted],
    }
    out_json.write_text(json.dumps(payload_out, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.debug:
        Path("reports").mkdir(exist_ok=True)
        Path("reports/nasdaq_api_debug.json").write_text(json.dumps(debug_items, ensure_ascii=False, indent=2), encoding="utf-8")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except FileNotFoundError as e:
        print(f"ANALYST_ERROR_TAIL: master file missing: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"ANALYST_ERROR_TAIL: unexpected: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(3)
