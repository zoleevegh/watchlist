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
        fetch_yahoo_macro_news,
        format_macro_block,
        fetch_analyst_events,
        format_analyst_block,
        fetch_catalyst_events,
        format_catalyst_block,
        fetch_highconviction_events,
        format_highconviction_block,
    )  # noqa: F401
except ImportError:
    # Optional helper; a script működik helper nélkül is, de a makró/elemző/katalizátor/high-conviction blokkok ilyenkor üresek maradnak.
    def fetch_yahoo_macro_news():
        return []

    def format_macro_block(macro_text, yahoo_news):
        return ""

    def fetch_analyst_events(path=None):
        return []

    def format_analyst_block(events):
        return []

    def fetch_catalyst_events(path=None):
        return []

    def format_catalyst_block(events):
        return []

    def fetch_highconviction_events(path=None):
        return []

    def format_highconviction_block(events):
        return []


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
DEFAULT_SCRIPT_VERSION = "2.2.6-biblia-yahoo-us-time-chart-meta-prevclose-helper-macro-analyst-catalyst-hc-hiconv"

WATCHLIST_DEFAULT_PATH = "reports/master.csv"
ANALYST_EVENTS_PATH = "reports/analyst_1.json"
CATALYST_EVENTS_PATH = "reports/catalysts_1.json"
HIGHCONV_EVENTS_PATH = "reports/highconviction_1.json"



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


def infer_positions_from_watchlist(path: Optional[str]) -> Dict[str, Dict]:
    """Darabszámos pozíciók kinyerése a MASTER / watchlist CSV-ből.

    Logika:
    - ticker oszlop: ticker/symbol/szimbólum/Ticker
    - quantity oszlop: shares/quantity/qty/darabszám/darabszam/db/Darabszam
    - csak >0 értékű sorok kerülnek be pozícióként.
    - ha ugyanaz a ticker többször szerepel, összegezzük a darabszámot.
    """
    positions: Dict[str, Dict] = {}
    if not path or not os.path.exists(path):
        debug(f"[WARN] Cannot infer positions – watchlist file not found: {path}")
        return positions

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return positions
        headers = [h.strip() for h in reader.fieldnames]
        ticker_col = find_col(headers, ["ticker", "symbol", "szimbólum", "Ticker"])
        qty_col = find_col(
            headers,
            ["shares", "quantity", "qty", "darabszám", "darabszam", "db", "Darabszam"],
        )

        if not ticker_col or not qty_col:
            debug("[INFO] Cannot infer positions from watchlist – ticker or quantity column missing.")
            return positions

        for row in reader:
            sym = (row.get(ticker_col) or "").strip().upper()
            if not sym:
                continue
            qty_raw = row.get(qty_col)
            try:
                qty = float(qty_raw.replace(",", ".")) if qty_raw not in (None, "") else 0.0
            except Exception:
                qty = 0.0
            if qty <= 0:
                continue
            prev = positions.get(sym, {}).get("quantity", 0.0)
            positions[sym] = {
                "ticker": sym,
                "quantity": prev + qty,
            }

    if positions:
        debug(f"[INFO] Inferred {len(positions)} darabszámos pozíció a watchlist/master CSV-ből.")
    else:
        debug("[INFO] No darabszámos pozíció inferred from watchlist.")
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
        ticker_col = find_col(headers, ["ticker", "symbol", "szimbólum", "Ticker"])
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
    """Visszaadja: (rth_close_price, ah_pct, pm_pct)

    Haladó, bíblia-kompatibilis logika #1-hez:

    - A Yahoo 2d/5m + includePrePost sorozatából külön gyűjtjük:
      * RTH: 09:30–16:00
      * After-hours: 16:00–20:00
      * Premarket: 04:00–09:30
    - Megkeressük az IDŐBEN LEGHAMARABB érkező pre/post (AH vagy PM) gyertyát.
    - Az ehhez képest UTOLSÓ MEGELŐZŐ RTH gyertya záróára a bázis
      (utolsó teljes RTH záró).
    - AH és PM mozgást ehhez a bázishoz viszonyítjuk, függetlenül attól,
      hogy mikor fut a script (nyitás előtt, közben, után).
    """
    if not timestamps or not closes:
        return None, None, None

    tz_name = meta.get("exchangeTimezoneName") or "America/New_York"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("America/New_York")

    dts = [dt.datetime.fromtimestamp(t, tz) for t in timestamps]

    rth_points: List[Tuple[dt.datetime, float]] = []
    ah_points: List[Tuple[dt.datetime, float]] = []
    pm_points: List[Tuple[dt.datetime, float]] = []

    for d, c in zip(dts, closes):
        if c is None:
            continue
        c = float(c)
        time = d.time()

        # Regular trading hours: 09:30–16:00
        if (time.hour > 9 or (time.hour == 9 and time.minute >= 30)) and time.hour < 16:
            rth_points.append((d, c))
        # After-hours: 16:00–20:00
        elif time.hour >= 16 and time.hour <= 20:
            ah_points.append((d, c))
        # Premarket: 04:00–09:30
        elif (time.hour > 4 or (time.hour == 4 and time.minute >= 0)) and (
            time.hour < 9 or (time.hour == 9 and time.minute <= 30)
        ):
            pm_points.append((d, c))

    if not rth_points:
        return None, None, None

    # Ha nincs egyáltalán pre/post adat, akkor tényleg nincs mit jelenteni
    if not ah_points and not pm_points:
        return None, None, None

    # 1) Keressük meg az IDŐBEN LEGHAMARABB érkező pre/post gyertyát
    first_prepost_dt: Optional[dt.datetime] = None
    if ah_points:
        first_prepost_dt = ah_points[0][0]
    if pm_points and (first_prepost_dt is None or pm_points[0][0] < first_prepost_dt):
        first_prepost_dt = pm_points[0][0]

    # 2) Ehhez képest az utolsó megelőző RTH-gyertya a bázis
    base_candidates = [p for p in rth_points if p[0] < first_prepost_dt] if first_prepost_dt else rth_points
    if not base_candidates:
        base_candidates = rth_points  # fallback: összes közül az utolsó
    last_rth_dt, rth_close_price = base_candidates[-1]

    base_date = last_rth_dt.date()

    # 3) After-hours: ugyanarra a napra, a záró utáni 16:00–20:00 tartományból
    ah_for_base = [p for p in ah_points if p[0].date() == base_date and p[0] > last_rth_dt]
    if ah_for_base:
        ah_last_price = ah_for_base[-1][1]
        ah_pct = (ah_last_price - rth_close_price) / rth_close_price * 100.0
    else:
        ah_pct = None

    # 4) Premarket: a bázis nap utáni napra eső 04:00–09:30 közötti gyertyák
    pm_for_base = [p for p in pm_points if p[0] > last_rth_dt]
    if pm_for_base:
        pm_last_price = pm_for_base[-1][1]
        pm_pct = (pm_last_price - rth_close_price) / rth_close_price * 100.0
    else:
        pm_pct = None

    return rth_close_price, ah_pct, pm_pct


def fmt_pct(value: Optional[float]) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    return f"{value:+.2f}%"


def generate_model_report(
    watchlist_path: Optional[str],
    script_version: str,
    k_default: float,
    output_md: str,
    output_json: str,
    macro_text: Optional[str] = None,
) -> str:
    # Pozíciók: a master/watchlist alapján inferálva (nincs külön positions.csv)
    positions = infer_positions_from_watchlist(watchlist_path)
    watch = load_watchlist(watchlist_path)

    all_symbols = sorted(set(watch.keys()) | set(positions.keys()))

    missing: Dict[str, str] = {}
    darab_results: List[dict] = []
    watch_results: List[dict] = []

    for sym in all_symbols:
        ah_pct: Optional[float] = None
        pm_pct: Optional[float] = None

        try:
            meta, ts, closes = fetch_chart(sym)
            _, ah_pct, pm_pct = compute_ah_pm_move(meta, ts, closes)
        except Exception as e:
            # Valódi forráshiba / HTTP hiba / stb. – ez lefedettség-hiba
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
            "max_move": max_move,
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
    ]

        # Politika/FED / Piaci hangulat + Elemzői lépések + Közeli katalizátorok + High-conviction blokkok
    yahoo_macro_news = fetch_yahoo_macro_news()
    macro_block = format_macro_block(macro_text or "", yahoo_macro_news)

    analyst_events = fetch_analyst_events(ANALYST_EVENTS_PATH)
    analyst_block = format_analyst_block(analyst_events)

    catalyst_events = fetch_catalyst_events(CATALYST_EVENTS_PATH)
    catalyst_block = format_catalyst_block(catalyst_events)

    highconv_events = fetch_highconviction_events(HIGHCONV_EVENTS_PATH)
    highconv_block = format_highconviction_block(highconv_events)
    lines: List[str] = []
    lines.extend(header_lines)

    if macro_block:
        lines.append("")
        lines.append(macro_block)

    if analyst_block:
        lines.append("")
        lines.append(analyst_block)

    if catalyst_block:
        lines.append("")
        lines.append(catalyst_block)

    if highconv_block:
        lines.append("")
        lines.append(highconv_block)

    lines.append("")
    lines.append("Darabszámos tickerek – After-hours & Premarket mozgások")
    lines.append("")

    # Darabszámosok rendezése max abs mozgás szerint (csökkenő)
    darab_sorted = sorted(
        darab_results,
        key=lambda x: x.get("max_move", 0.0),
        reverse=True,
    )

    for entry in darab_sorted:
        sym = entry["ticker"]
        ah_pct = entry["ah_pct"]
        pm_pct = entry["pm_pct"]
        line = (
            f"{sym} — AH {fmt_pct(ah_pct)} | PM {fmt_pct(pm_pct)} — "
            "Egyelőre nincs küszöb feletti AH/PM elmozdulás."
        )
        lines.append(line)

    # Watchlist – max(|AH|,|PM|) szerint csökkenő
    watch_sorted = sorted(
        watch_results,
        key=lambda x: x.get("max_move", 0.0),
        reverse=True,
    )

    if watch_sorted:
        lines.append("")
        lines.append("Watchlist – After-hours & Premarket mozgások (csak ha ≥K)")
        lines.append("")
        for entry in watch_sorted:
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
        "positions": darab_sorted,
        "watchlist_moves": watch_sorted,
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
    script_version = args.script_version or DEFAULT_SCRIPT_VERSION
    k_default = args.k_default or DEFAULT_K

    if mode == 1:
        summary_path = args.summary or "reports/summary_report_1.md"
        json_path = "reports/latest_1.json"
        text = generate_model_report(
            watchlist_path=watchlist_path,
            script_version=script_version,
            k_default=k_default,
            output_md=summary_path,
            output_json=json_path,
            macro_text=args.macro,
        )
        print(text)
    else:
        msg = f"# Report mód {mode} még nincs implementálva ebben a verzióban."
        print(msg)


if __name__ == "__main__":
    main()
