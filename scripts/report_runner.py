#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import math
import os
import sys
from typing import Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Python <3.9 fallback – treat all timestamps as UTC-equivalent
    class ZoneInfo:  # type: ignore
        def __init__(self, name: str) -> None:
            self.name = name


import requests

# Biblia checklist helper (placeholder).
# These functions will later hold the canonical #1/#2/#3 reporting rules.
try:
    from biblia_helper import (
        get_report1_checklist,
        get_report2_checklist,
        get_report3_checklist,
    )  # noqa: F401
except ImportError:
    # Optional helper; the script works without this file present.
    pass



SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
)

DEFAULT_K = 3.0
DEFAULT_SCRIPT_VERSION = "2.1.9-biblia-yahoo-us-time-chart-meta-prevclose-helper"


def debug(msg: str) -> None:
    """Simple stderr logger so the MD remains clean."""
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def find_col(headers: List[str], candidates: List[str]) -> Optional[str]:
    lower = {h.strip().lower(): h for h in headers if h}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def load_positions(path: Optional[str]) -> Dict[str, Dict]:
    positions: Dict[str, Dict] = {}
    if not path or not os.path.exists(path):
        debug(f"[WARN] Positions file not found: {path}")
        return positions

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return positions
        headers = [h.strip() for h in reader.fieldnames]
        ticker_col = find_col(headers, ["ticker", "symbol", "szimbólum"])
        qty_col = find_col(headers, ["shares", "quantity", "qty", "darabszám", "db"])

        if not ticker_col:
            debug("[WARN] No ticker column found in positions.")
            return positions

        for row in reader:
            sym = (row.get(ticker_col) or "").strip().upper()
            if not sym:
                continue
            qty_raw = row.get(qty_col) if qty_col else None
            try:
                qty = float(qty_raw) if qty_raw not in (None, "") else 0.0
            except ValueError:
                qty = 0.0
            positions[sym] = {
                "ticker": sym,
                "quantity": qty,
            }
    return positions


def load_watchlist(path: Optional[str]) -> Dict[str, Dict]:
    watch: Dict[str, Dict] = {}
    if not path or not os.path.exists(path):
        debug(f"[WARN] Watchlist file not found: {path}")
        return watch

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return watch
        headers = [h.strip() for h in reader.fieldnames]
        ticker_col = find_col(headers, ["ticker", "symbol", "szimbólum"])
        k_col = find_col(headers, ["k", "k_move", "k_threshold"])

        if not ticker_col:
            debug("[WARN] No ticker column found in watchlist.")
            return watch

        for row in reader:
            sym = (row.get(ticker_col) or "").strip().upper()
            if not sym:
                continue
            k_raw = row.get(k_col) if k_col else None
            try:
                k_val = float(k_raw) if k_raw not in (None, "") else None
            except ValueError:
                k_val = None
            watch[sym] = {
                "ticker": sym,
                "k": k_val,
            }
    return watch


def fetch_chart(symbol: str) -> Tuple[dict, List[int], List[Optional[float]]]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "2d", "interval": "5m", "includePrePost": "true"}
    resp = SESSION.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    chart = data.get("chart", {})
    error = chart.get("error")
    if error:
        raise RuntimeError(f"chart_error: {error}")
    result = chart.get("result")
    if not result:
        raise RuntimeError("no_result")
    res0 = result[0]
    meta = res0.get("meta") or {}
    ts = res0.get("timestamp") or []
    indicators = res0.get("indicators") or {}
    quotes = indicators.get("quote") or [{}]
    closes = quotes[0].get("close") or []
    return meta, ts, closes


def compute_ah_pm_move(
    meta: dict, timestamps: List[int], closes: List[Optional[float]]
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Visszaadja: (rth_close_price, ah_pct, pm_pct)

    - RTH: 09:30–16:00 exchange time
    - AH:  RTH záró utáni 16:00–20:00, ugyanazon a napon
    - PM:  RTH záró utáni 04:00–09:30 (következő kereskedési nap)
    """
    if not timestamps or not closes:
        return None, None, None

    tz_name = meta.get("exchangeTimezoneName") or "America/New_York"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("America/New_York")

    dts = [dt.datetime.fromtimestamp(t, tz) for t in timestamps]

    # RTH: 09:30–16:00
    rth_points: List[Tuple[dt.datetime, float]] = []
    for d, c in zip(dts, closes):
        if c is None:
            continue
        if (d.hour > 9 or (d.hour == 9 and d.minute >= 30)) and d.hour < 16:
            rth_points.append((d, float(c)))

    if not rth_points:
        return None, None, None

    last_rth_dt, rth_close_price = rth_points[-1]

    # After-hours: ugyanaz a nap, >16:00–20:00
    ah_points: List[Tuple[dt.datetime, float]] = []
    for d, c in zip(dts, closes):
        if c is None:
            continue
        if d.date() == last_rth_dt.date() and d > last_rth_dt and d.hour <= 20:
            ah_points.append((d, float(c)))

    if ah_points:
        ah_last_price = ah_points[-1][1]
        ah_pct = (ah_last_price - rth_close_price) / rth_close_price * 100.0
    else:
        ah_pct = None

    # Premarket: >RTH záró, 04:00–09:30
    pm_points: List[Tuple[dt.datetime, float]] = []
    for d, c in zip(dts, closes):
        if c is None:
            continue
        if d <= last_rth_dt:
            continue
        if d.hour < 4:
            continue
        if d.hour > 9 or (d.hour == 9 and d.minute > 30):
            continue
        pm_points.append((d, float(c)))

    if pm_points:
        pm_last_price = pm_points[-1][1]
        pm_pct = (pm_last_price - rth_close_price) / rth_close_price * 100.0
    else:
        pm_pct = None

    return rth_close_price, ah_pct, pm_pct


def fmt_pct(value: Optional[float]) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    return f"{value:+.2f}%"


def generate_model_report(
    positions_path: Optional[str],
    watchlist_path: Optional[str],
    script_version: str,
    k_default: float,
    output_md: str,
    output_json: str,
) -> str:
    positions = load_positions(positions_path)
    watch = load_watchlist(watchlist_path)

    # <<< Itt volt a bug, javítva >>>
    all_symbols = sorted(set(watch.keys()) | set(positions.keys()))

    missing: Dict[str, str] = {}
    darab_results: List[dict] = []
    watch_results: List[dict] = []

    for sym in all_symbols:
        try:
            meta, ts, closes = fetch_chart(sym)
            _, ah_pct, pm_pct = compute_ah_pm_move(meta, ts, closes)
            if ah_pct is None and pm_pct is None:
                missing[sym] = "no_prepost_data"
                continue
        except Exception as e:
            missing[sym] = str(e)
            continue

        is_position = sym in positions and positions[sym].get("quantity", 0) > 0
        k_val = watch.get(sym, {}).get("k") or k_default

        max_move = max(
            abs(ah_pct or 0.0),
            abs(pm_pct or 0.0),
        )

        entry = {
            "ticker": sym,
            "ah_pct": ah_pct,
            "pm_pct": pm_pct,
            "is_position": is_position,
            "k": k_val,
        }

        if is_position:
            darab_results.append(entry)
        elif max_move >= k_val:
            watch_results.append(entry)

    if not missing:
        coverage_line = "Lefedettség: TELJES"
    else:
        tickers_str = ", ".join(sorted(missing.keys()))
        coverage_line = (
            "Lefedettség: HIÁNYOS – nem elérhető ticker(ek): "
            + tickers_str
            + " (oka: lásd belső logot / forráshibát)"
        )

    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=1)))

    header_lines = [
        "#1 – After-hours (22:00–02:00) + Premarket (10:00–15:30) — CEST",
        "",
        f"Script verzió: {script_version}",
        "",
        "Vizsgált ablakok (CEST): AH – előző kereskedési nap 22:00 → 02:00, "
        "PM – aktuális nap 10:00 → 15:30",
        "",
        "Árforrás: Yahoo Finance chart (v8 – 2d/5m, includePrePost; "
        "utolsó RTH záró → AH/PM utolsó ár alapján számolt % mozgás)",
        "",
        coverage_line,
        "Politika/FED / Trump-napihír",
        "",
        "(Auto mód még nincs implementálva – add meg a makró összefoglalót a workflow 'macro' mezőjében.)",
    ]

    lines: List[str] = []
    lines.extend(header_lines)
    lines.append("Darabszámos tickerek – After-hours & Premarket mozgások")
    lines.append("")

    for entry in sorted(darab_results, key=lambda x: x["ticker"]):
        sym = entry["ticker"]
        ah_pct = entry["ah_pct"]
        pm_pct = entry["pm_pct"]
        line = (
            f"{sym} — AH {fmt_pct(ah_pct)} | PM {fmt_pct(pm_pct)} — "
            "Egyelőre nincs küszöb feletti AH/PM elmozdulás."
        )
        lines.append(line)

    if watch_results:
        lines.append("Watchlist – After-hours & Premarket mozgások (csak ha ≥K)")
        lines.append("")
        for entry in sorted(watch_results, key=lambda x: x["ticker"]):
            sym = entry["ticker"]
            ah_pct = entry["ah_pct"]
            pm_pct = entry["pm_pct"]
            k_val = entry["k"]
            line = (
                f"{sym} — AH {fmt_pct(ah_pct)} | PM {fmt_pct(pm_pct)} — "
                f"Watchlisten is érdemi AH/PM elmozdulás (≥K={k_val:g}) az utolsó RTH záróhoz képest."
            )
            lines.append(line)

    lines.append(f"Job summary generated at run-time ({now.isoformat(timespec='minutes')})")

    md_text = "\n".join(lines)

    os.makedirs(os.path.dirname(output_md), exist_ok=True)
    with open(output_md, "w", encoding="utf-8") as f:
        f.write(md_text)

    payload = {
        "generated_at": now.isoformat(),
        "script_version": script_version,
        "coverage_missing": missing,
        "positions": darab_results,
        "watchlist_moves": watch_results,
    }
    os.makedirs(os.path.dirname(output_json), exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return md_text


def main() -> None:
    parser = argparse.ArgumentParser(description="Automatikus napi jelentés (#1/#2/#3)")

    # ÚJ interfész
    parser.add_argument("--mode", type=int, choices=[1, 2, 3], help="1/2/3-as jelentés mód")

    parser.add_argument(
        "--positions",
        help="Pozíciók CSV (darabszámos tickerek)",
        default="reports/positions.csv",
    )
    parser.add_argument(
        "--watchlist",
        help="MASTER / watchlist CSV",
        default="reports/master.csv",
    )
    parser.add_argument(
        "--k-default",
        type=float,
        default=DEFAULT_K,
        help="Alapértelmezett K küszöb, ha a watchlist cella üres/érvénytelen",
    )
    parser.add_argument(
        "--script-version",
        default=DEFAULT_SCRIPT_VERSION,
        help="Verzió-string, ami a report elejére kerül",
    )

    # LEGACY kompatibilitás – régi workflow ne haljon el
    parser.add_argument("--report", type=int, choices=[1, 2, 3], help="Alias of --mode (legacy)")
    parser.add_argument("--csv", help="Alias of --watchlist (legacy)")
    parser.add_argument("--summary", help="Kimeneti summary path (legacy, opcionális)")
    parser.add_argument("--macro", help="Makró szöveg (legacy, jelenleg ignorált)")

    args = parser.parse_args()

    mode = args.mode or args.report or 1
    watchlist_path = args.watchlist or args.csv or "reports/master.csv"
    positions_path = args.positions or "reports/positions.csv"
    script_version = args.script_version or DEFAULT_SCRIPT_VERSION
    k_default = args.k_default or DEFAULT_K

    if mode == 1:
        summary_path = args.summary or "reports/summary_report_1.md"
        json_path = "reports/latest_1.json"
        text = generate_model_report(
            positions_path=positions_path,
            watchlist_path=watchlist_path,
            script_version=script_version,
            k_default=k_default,
            output_md=summary_path,
            output_json=json_path,
        )
        print(text)
    else:
        msg = f"# Report mód {mode} még nincs implementálva ebben a verzióban."
        print(msg)


if __name__ == "__main__":
    main()
