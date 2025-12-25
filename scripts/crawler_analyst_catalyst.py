#!/usr/bin/env python3
"""
Analyst & catalyst "crawler" skeleton
Version: v1.0.1

Cél:
- A #1/#2/#3 jelentésekhez tartozó *nyers* eseményfájlokat legenerálni:
    reports/raw_analyst_{report}.json
    reports/raw_catalysts_{report}.json

Fontos:
- Ez NEM "web-scraper" konkrét oldalra drótozva.
- Úgy van megírva, hogy bármilyen *saját* JSON/CSV endpointot vagy exportot
  rá tudj kötni (pl. Google Apps Script, saját MarketBeat-export, egyéb API).
- A valódi, netről adatot gyűjtő logika a Te oldaladon futhat (pl. Apps Script),
  ez a script csak "letölti és normalizálja".

Források (prioritás logika a biblia szerint a Te scriptedben dől el):
- analyst feed:   JSON vagy CSV (fel/lemínősítés, PT változás, stb.)
- catalysts feed: JSON vagy CSV (earnings dátum, guide, M&A, buyback, stb.)

Várt input-formátumok:

1) JSON (ajánlott)
   [
     {
       "ticker": "NVDA",
       "event_type": "upgrade",
       "headline": "Morgan Stanley felminősítette az NVDA-t, PT 120→145",
       "summary": "AI-kereslet továbbra is erős...",
       "source": "MarketBeat",
       "ts": "2025-11-27T08:30:00Z"
     },
     ...
   ]

2) CSV (UTF-8, fejléccel)
   ticker,event_type,headline,summary,source,ts
   NVDA,upgrade,"Morgan Stanley felminősítette az NVDA-t, PT 120→145","AI-kereslet...","MarketBeat","2025-11-27T08:30:00Z"

CLI használat:

    python crawler_analyst_catalyst.py \
        --report 1 \
        --analyst-url "$ANALYST_FEED_URL" \
        --catalyst-url "$CATALYST_FEED_URL"

Ha nem adsz meg URL-t paraméterben, a script az alábbi env változókat próbálja:

    ANALYST_FEED_URL_1 / 2 / 3
    CATALYST_FEED_URL_1 / 2 / 3

Kimenet:

    reports/raw_analyst_{report}.json
    reports/raw_catalysts_{report}.json

Ezeket olvassa majd az events_fetcher.py, ami tovább szűr:
    → reports/analyst_{report}.json
    → reports/catalysts_{report}.json
"""
import argparse
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import json
import csv

try:
    import requests  # type: ignore
except ImportError:
    requests = None


def _debug(msg: str) -> None:
    print(f"[crawler] {msg}", file=sys.stderr)


def fetch_url(url: str) -> str:
    if requests is None:
        raise RuntimeError("A 'requests' nincs telepítve. Add hozzá a requirements.txt-hez: requests")
    _debug(f"Letöltés: {url}")
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.text


def guess_format_from_url_or_content(url: str, content: str) -> str:
    """
    Próbáljuk kitalálni, hogy JSON vagy CSV.
    - Ha .json / ?format=json / application/json header → json
    - Ha .csv / 'ticker,' a headerben → csv
    """
    lu = url.lower()
    if lu.endswith(".json") or "format=json" in lu:
        return "json"
    if lu.endswith(".csv") or "output=csv" in lu:
        return "csv"

    # nagyon primitív heuristics
    stripped = content.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        return "json"
    first_line = stripped.splitlines()[0]
    if "ticker" in first_line and "," in first_line:
        return "csv"
    return "json"  # default


def parse_json_events(text: str) -> List[Dict[str, Any]]:
    """Rugalmas JSON parser.

    Elfogad:
    - root lista: [ {...}, ... ]
    - dict lista-kulccsal: events/data/items/content
    - Apps Script tipikus válasz: {status/type/report/generated_at, events:[...]} vagy {.., content:[...]}
    - ha nincs benne lista → üres lista (nem dob hibát)
    """
    data = json.loads(text)

    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]

    if isinstance(data, dict):
        for key in ("events", "data", "items", "content"):
            v = data.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]

        # fallback: ha maga a dict egyetlen eventnek néz ki
        if any(k in data for k in ("ticker", "symbol", "headline", "title", "event_type")):
            return [data]  # type: ignore[list-item]

        return []

    return []

def parse_csv_events(text: str) -> List[Dict[str, Any]]:
    lines = text.splitlines()
    reader = csv.DictReader(lines)
    return [row for row in reader]


def normalise_event(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bemenet: tetszőleges dict a fenti kulcsok valamelyik kombinációjával.
    Kimenet: egységesített kulcsok a raw_*.json-hoz.
    """
    # ticker
    ticker = (
        item.get("ticker")
        or item.get("symbol")
        or item.get("TICKER")
        or item.get("SYMBOL")
        or ""
    )

    # event_type
    event_type = (
        item.get("event_type")
        or item.get("type")
        or item.get("eventType")
        or ""
    )

    # headline / summary
    headline = (
        item.get("headline")
        or item.get("title")
        or item.get("HEADLINE")
        or item.get("TITLE")
        or ""
    )
    summary = (
        item.get("summary")
        or item.get("desc")
        or item.get("description")
        or ""
    )

    # source
    source = (
        item.get("source")
        or item.get("SOURCE")
        or item.get("provider")
        or ""
    )

    # timestamp
    ts = (
        item.get("ts")
        or item.get("time")
        or item.get("datetime")
        or item.get("date")
        or ""
    )

    return {
        "ticker": str(ticker).upper().strip(),
        "event_type": str(event_type).strip(),
        "headline": str(headline).strip(),
        "summary": str(summary).strip(),
        "source": str(source).strip(),
        "ts": str(ts).strip(),
    }


def load_events_from_url(url: str) -> List[Dict[str, Any]]:
    raw = fetch_url(url)
    fmt = guess_format_from_url_or_content(url, raw)
    _debug(f"Formátum detektálva: {fmt}")

    if fmt == "csv":
        rows = parse_csv_events(raw)
    else:
        rows = parse_json_events(raw)

    events: List[Dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        norm = normalise_event(item)
        # ticker nélkül is engedjük (szöveg-only), majd events_fetcher tud vele mit kezdeni
        if not any(norm.values()):
            continue
        events.append(norm)
    _debug(f"{len(events)} esemény beolvasva {url}-ről")
    return events


def resolve_url_from_env_or_arg(kind: str, report: int, arg_url: Optional[str]) -> Optional[str]:
    """
    kind: 'analyst' vagy 'catalyst'
    report: 1/2/3
    """
    if arg_url:
        return arg_url
    base_name = f"{kind.upper()}_FEED_URL_{report}"
    env_val = os.getenv(base_name)
    if env_val:
        return env_val
    # fallback: ANALYST_FEED_URL, CATALYST_FEED_URL
    base2 = f"{kind.upper()}_FEED_URL"
    env_val2 = os.getenv(base2)
    return env_val2


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Analyst & catalyst raw events crawler (JSON/CSV → raw_*.json)"
    )
    ap.add_argument(
        "--report",
        type=int,
        required=True,
        choices=[1, 2, 3],
        help="Report # (1/2/3)",
    )
    ap.add_argument(
        "--analyst-url",
        help="Analyst feed URL (JSON/CSV). Ha nincs megadva, ANALYST_FEED_URL_{report} / ANALYST_FEED_URL env kerül használatra.",
    )
    ap.add_argument(
        "--catalyst-url",
        help="Catalyst feed URL (JSON/CSV). Ha nincs megadva, CATALYST_FEED_URL_{report} / CATALYST_FEED_URL env kerül használatra.",
    )
    ap.add_argument(
        "--out-dir",
        default="reports",
        help="Kimeneti könyvtár (default: reports)",
    )
    args = ap.parse_args()

    report = args.report
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    analyst_url = resolve_url_from_env_or_arg("analyst", report, args.analyst_url)
    catalyst_url = resolve_url_from_env_or_arg("catalyst", report, args.catalyst_url)

    raw_analyst: List[Dict[str, Any]] = []
    raw_catalysts: List[Dict[str, Any]] = []

    if analyst_url:
        try:
            raw_analyst = load_events_from_url(analyst_url)
        except Exception as e:
            _debug(f"Analyst feed letöltési/parzolási hiba: {e!r}")
    else:
        _debug("Nincs analyst URL megadva/env-ben → raw_analyst üres marad.")

    if catalyst_url:
        try:
            raw_catalysts = load_events_from_url(catalyst_url)
        except Exception as e:
            _debug(f"Catalyst feed letöltési/parzolási hiba: {e!r}")
    else:
        _debug("Nincs catalyst URL megadva/env-ben → raw_catalysts üres marad.")

    # Írás
    raw_analyst_path = out_dir / f"raw_analyst_{report}.json"
    raw_catalysts_path = out_dir / f"raw_catalysts_{report}.json"

    with raw_analyst_path.open("w", encoding="utf-8") as f:
        json.dump(raw_analyst, f, ensure_ascii=False, indent=2)
    with raw_catalysts_path.open("w", encoding="utf-8") as f:
        json.dump(raw_catalysts, f, ensure_ascii=False, indent=2)

    _debug(f"Mentve: {raw_analyst_path} ({len(raw_analyst)} elem)")
    _debug(f"Mentve: {raw_catalysts_path} ({len(raw_catalysts)} elem)")


if __name__ == "__main__":
    main()
