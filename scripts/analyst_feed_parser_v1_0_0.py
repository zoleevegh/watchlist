#!/usr/bin/env python3
"""Részvények Projekt – analyst_feed_parser
Verzió: v1.0.0-yahoo-bbg-mb-reuters-ir

Feladat:
- Egységes „analyst” és „catalyst” feed lehúzása egy Apps Script webapp-ról.
- Forráspprioritás az elemzői / hír eseményekhez:
    1) Yahoo Finance
    2) Bloomberg
    3) MarketBeat
    4) Reuters / AP / hivatalos IR (Investor Relations)
- Kimenet:
    * reports/analyst_1.json
    * reports/catalysts_1.json

A script NEM magát a web-scrapinget végzi, hanem egy már normalizált
Apps Script JSON feedet dolgoz fel. A webapp URL-je(i) környezeti
változókból jönnek:

- ANALYST_FEED_URL   → ?type=analyst endpoint
- CATALYST_FEED_URL  → ?type=catalyst endpoint

Várt, „best-effort” JSON layout:

- a gyökér lehet lista vagy objektum {"items": [...]} / {"events": [...]}.
- egy esemény tipikus mezői (nem mind kötelező):
    ticker / symbol
    source          (pl. "Yahoo Finance", "Bloomberg", "MarketBeat",
                     "Reuters", "AP", "Company IR")
    headline
    summary
    datetime / date
    rating_action / action       (Upgrade/Downgrade/Initiation/...)
    from_rating / old_rating
    to_rating / new_rating
    pt_from / old_pt
    pt_to / new_pt
    url
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import requests

__version__ = "1.0.0-yahoo-bbg-mb-reuters-ir"


# ---- Segédfüggvények ----------------------------------------------------


def debug(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def _load_items_from_response(data: Any) -> List[Dict[str, Any]]:
    """Elfogad lista vagy dict+items/events layoutot; csak dict eventeket tart meg."""
    if isinstance(data, dict):
        items = data.get("items") or data.get("events") or data.get("data") or []
    else:
        items = data
    out: List[Dict[str, Any]] = []
    if isinstance(items, dict):
        # ha véletlenül dict jön, benne szintén lehet 'items'
        inner = items.get("items") or items.get("events") or []
        items = inner
    if not isinstance(items, Iterable):
        return out
    for ev in items:
        if isinstance(ev, dict):
            out.append(ev)
    return out


def _norm_source(src: str) -> str:
    return (src or "").strip().lower()


def _source_rank(src: str) -> int:
    """Forráspprioritás numerikus formában (kisebb = jobb).

    Canonikus sorrend (a biblia szerint):
        1) Yahoo Finance
        2) Bloomberg
        3) MarketBeat
        4) Reuters / AP / IR
        9) egyéb, ismeretlen

    A sztringet best-effort módon normalizáljuk.
    """
    s = _norm_source(src)
    if not s:
        return 9
    if "yahoo" in s:
        return 1
    if "bloomberg" in s or "bbg" in s:
        return 2
    if "marketbeat" in s:
        return 3
    if "reuters" in s or "ap " in s or "associated press" in s or " investor relations" in s or s == "ir":
        return 4
    return 9


def _ensure_ticker(ev: Dict[str, Any]) -> str:
    t = (ev.get("ticker") or ev.get("symbol") or ev.get("name") or "").strip().upper()
    return t


def _ensure_iso_datetime(ev: Dict[str, Any]) -> str:
    # Itt nem erőltetünk szigorú datetime parse-t; a feedben már legyen
    # ISO- vagy ISO-szerű formátum. Ha több kulcs is van, az első nem üreset vesszük.
    for key in ("datetime", "timestamp", "date", "time"):
        val = ev.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


# ---- Feed fetcherek -----------------------------------------------------


def fetch_feed(url: str, feed_type: str) -> List[Dict[str, Any]]:
    if not url:
        debug(f"[analyst_feed_parser] {feed_type}: URL nem definiált (üres).")
        return []
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        debug(f"[analyst_feed_parser] {feed_type}: HTTP hiba: {e!r}")
        return []
    try:
        data = resp.json()
    except Exception as e:
        debug(f"[analyst_feed_parser] {feed_type}: JSON parse hiba: {e!r}")
        return []
    items = _load_items_from_response(data)
    for ev in items:
        ev.setdefault("feed_type", feed_type)
    return items


# ---- Csoportosítás, priorizálás ----------------------------------------


def group_and_rank(events: List[Dict[str, Any]], feed_type: str) -> Dict[str, List[Dict[str, Any]]]:
    """Tickerenként csoportosít, és hozzáadja a forrás-prioritás metaadatot.

    - ticker: ticker/symbol/name alapján
    - source_rank: a _source_rank() szerint
    - datetime_norm: best-effort időbélyeg
    - is_primary: a *legmagasabb* prioritású / legfrissebb esemény jelölése
    """
    per_ticker: Dict[str, List[Dict[str, Any]]] = {}
    for ev in events:
        t = _ensure_ticker(ev)
        if not t:
            continue
        src = str(ev.get("source") or "")
        ev["source_rank"] = _source_rank(src)
        ev["source_normalized"] = _norm_source(src)
        ev["datetime_norm"] = _ensure_iso_datetime(ev)
        ev.setdefault("feed_type", feed_type)
        per_ticker.setdefault(t, []).append(ev)

    # is_primary jelölés ticker-szinten
    for t, evs in per_ticker.items():
        evs.sort(key=lambda e: (e.get("source_rank", 9), e.get("datetime_norm", "")), reverse=False)
        if evs:
            # első: legjobb forrásprió + legrégebbi/legkorábbi időbélyeg
            evs[0]["is_primary"] = True
            # a többinél explicit false, hogy a downstream egyszerűen tudjon szűrni
            for e in evs[1:]:
                e["is_primary"] = False
    return per_ticker


def flatten_grouped(grouped: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t, evs in sorted(grouped.items()):
        out.extend(evs)
    return out


# ---- Fő workflow --------------------------------------------------------


def build_analyst_and_catalyst_feeds(
    analyst_url: str,
    catalyst_url: str,
    analyst_out: Path,
    catalysts_out: Path,
) -> None:
    # 1) Analyst feed
    analyst_raw = fetch_feed(analyst_url, feed_type="analyst")
    analyst_grouped = group_and_rank(analyst_raw, feed_type="analyst")
    analyst_flat = flatten_grouped(analyst_grouped)

    analyst_out.parent.mkdir(parents=True, exist_ok=True)
    analyst_out.write_text(
        json.dumps(analyst_flat, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    debug(f"[analyst_feed_parser] analyst_1.json írása kész: {analyst_out}")

    # 2) Catalyst feed
    catalyst_raw = fetch_feed(catalyst_url, feed_type="catalyst")
    catalyst_grouped = group_and_rank(catalyst_raw, feed_type="catalyst")
    catalyst_flat = flatten_grouped(catalyst_grouped)

    catalysts_out.parent.mkdir(parents=True, exist_ok=True)
    catalysts_out.write_text(
        json.dumps(catalyst_flat, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    debug(f"[analyst_feed_parser] catalysts_1.json írása kész: {catalysts_out}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Egységes analyst/catalyst feed parser a Részvények projekthez "
            f"(verzió: {__version__})"
        )
    )
    parser.add_argument(
        "--analyst-url",
        help="Analyst feed URL (Apps Script webapp, alap: $ANALYST_FEED_URL)",
    )
    parser.add_argument(
        "--catalyst-url",
        help="Catalyst feed URL (Apps Script webapp, alap: $CATALYST_FEED_URL)",
    )
    parser.add_argument(
        "--analyst-out",
        default="reports/analyst_1.json",
        help="Analyst JSON kimenet (alap: reports/analyst_1.json)",
    )
    parser.add_argument(
        "--catalysts-out",
        default="reports/catalysts_1.json",
        help="Catalysts JSON kimenet (alap: reports/catalysts_1.json)",
    )
    args = parser.parse_args()

    analyst_url = (args.analyst_url or os.environ.get("ANALYST_FEED_URL", "")).strip()
    catalyst_url = (args.catalyst_url or os.environ.get("CATALYST_FEED_URL", "")).strip()

    analyst_out = Path(args.analyst_out)
    catalysts_out = Path(args.catalysts_out)

    build_analyst_and_catalyst_feeds(
        analyst_url=analyst_url,
        catalyst_url=catalyst_url,
        analyst_out=analyst_out,
        catalysts_out=catalysts_out,
    )


if __name__ == "__main__":
    main()
