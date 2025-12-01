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
    def fetch_yahoo_macro_news(*args, **kwargs):
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

# Makró feed helper (Apps Script webapp – opcionális).
try:
    from scripts.macro_fetcher import fetch_macro_text
except ImportError:  # pragma: no cover - optional helper
    def fetch_macro_text(*args, **kwargs):  # type: ignore
        return ""




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
DEFAULT_SCRIPT_VERSION = "2.3.5-biblia-yahoo-us-time-quote-spark-meta-prevclose-helper-macro-analyst-catalyst-hc-hiconv-auto-r2r3finom-spark-ahpm-bstyle"
AH_PM_MODE = "spark"  # alapértelmezett: Yahoo quote/spark alapú AH/PM


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



def fetch_yahoo_quote_batch(symbols: List[str]) -> Dict[str, Tuple[Optional[float], Optional[float], Optional[float]]]:
    """Batch-ben lehúzza a Yahoo quote (spark) feedet AH/PM-hez.

    Visszatér: {ticker: (regular_prev_close, ah_pct, pm_pct)}

    - AH: postMarketChangePercent
    - PM: preMarketChangePercent
    - Ha valamelyik mező hiányzik, None marad.
    """
    if not symbols:
        return {}

    # Yahoo quote endpoint – ez hajtja a webes portfólió UI-t is.
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    joined = ",".join(sorted(set(symbols)))
    params = {"symbols": joined}

    try:
        resp = SESSION.get(url, params=params, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        debug(f"[YF-QUOTE] batch fetch error: {e}")
        return {}

    try:
        data = resp.json()
    except Exception as e:
        debug(f"[YF-QUOTE] JSON parse error: {e}")
        return {}

    result_list = data.get("quoteResponse", {}).get("result", []) or []
    out: Dict[str, Tuple[Optional[float], Optional[float], Optional[float]]] = {}

    for item in result_list:
        sym = item.get("symbol")
        if not sym:
            continue
        prev_close = item.get("regularMarketPreviousClose")
        # Ezek már %-ban érkeznek (nem 0–1 frakcióban)
        ah_pct = item.get("postMarketChangePercent")
        pm_pct = item.get("preMarketChangePercent")
        # Biztonság kedvéért float() konverzió – ha nem konvertálható, None marad
        def _to_float(x):
            try:
                return float(x)
            except Exception:
                return None
        prev_close_f = _to_float(prev_close)
        ah_pct_f = _to_float(ah_pct)
        pm_pct_f = _to_float(pm_pct)
        out[sym.upper()] = (prev_close_f, ah_pct_f, pm_pct_f)

    return out


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



def fetch_marketwatch_premarket_pct(symbol: str, rth_close: Optional[float]) -> Optional[float]:
    """Best-effort MarketWatch premarket fallback.

    Ha a Yahoo 2d/5m chart nem ad a bázis RTH UTÁN premarket gyertyát,
    és még nincs következő napi RTH sem, utolsó esélyként megpróbáljuk
    a MarketWatch "Premarket" árát beolvasni.

    - URL: https://www.marketwatch.com/investing/stock/{symbol.lower()}
    - User-agent headerrel kérjük le a HTML-t.
    - Szövegben megkeressük a "Premarket" blokkot és az utána következő
      "${ár}" mintát.

    Hiba vagy hiányzó adat esetén None-t ad vissza, és nem dobja el a futást.
    """
    if not rth_close:
        return None

    url = f"https://www.marketwatch.com/investing/stock/{symbol.lower()}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        )
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:  # pragma: no cover - best-effort
        debug(f"[MW] {symbol}: MarketWatch request error: {e}")
        return None

    try:
        pre_idx = html.lower().find("premarket")
        if pre_idx == -1:
            return None
        window = html[pre_idx : pre_idx + 2000]
        m_price = re.search(r"\$\s*([0-9]+(?:\.[0-9]+)?)", window)
        if not m_price:
            return None
        price_str = m_price.group(1).replace(",", "")
        pm_price = float(price_str)
    except Exception as e:  # pragma: no cover - best-effort
        debug(f"[MW] {symbol}: parse error: {e}")
        return None

    try:
        pm_pct = (pm_price - float(rth_close)) / float(rth_close) * 100.0
        return pm_pct
    except Exception as e:  # pragma: no cover - best-effort
        debug(f"[MW] {symbol}: pct calc error: {e}")
        return None


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

    ah_pm_mode = os.environ.get("AH_PM_MODE", AH_PM_MODE).lower()
    quote_map: Dict[str, Tuple[Optional[float], Optional[float], Optional[float]]] = {}
    if ah_pm_mode == "spark":
        quote_map = fetch_yahoo_quote_batch(all_symbols)


    missing: Dict[str, str] = {}
    darab_results: List[dict] = []
    watch_results: List[dict] = []

    for sym in all_symbols:
        ah_pct: Optional[float] = None
        pm_pct: Optional[float] = None
        rth_close: Optional[float] = None

        try:
            # 1) Elsődlegesen Yahoo quote/spark feed (UI-kompatibilis AH/PM)
            if ah_pm_mode == "spark":
                qt = quote_map.get(sym)
                if qt:
                    rth_close, ah_pct, pm_pct = qt

            # 2) Ha nincs értelmezhető adat a quote feedből, próbáljuk meg a chartot
            if rth_close is None and ah_pct is None and pm_pct is None:
                meta, ts, closes = fetch_chart(sym)
                rth_close_chart, ah_from_chart, pm_from_chart = compute_ah_pm_move(meta, ts, closes)
                rth_close = rth_close_chart
                if ah_pct is None:
                    ah_pct = ah_from_chart
                if pm_pct is None:
                    pm_pct = pm_from_chart

            # 3) Premarket fallback MarketWatch-ról – csak ha még mindig nincs PM, de van bázisár
            if pm_pct is None and rth_close is not None:
                try:
                    pm_from_mw = fetch_marketwatch_premarket_pct(sym, rth_close)
                    if pm_from_mw is not None:
                        pm_pct = pm_from_mw
                except Exception as mw_e:  # pragma: no cover - best-effort
                    debug(f"[MW] {sym}: fallback error: {mw_e}")
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
        "## After-hours & Premarket – #1 jelentés",
        "",
        f"**Script verzió:** {script_version}",
        f"**Futás ideje:** {now.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        "",
        "**Időablakok (CEST)**",
        "- AH: előző kereskedési nap 22:00–02:00",
        "- PM: aktuális nap 10:00–15:30",
        "",
        "**Árforrás:** Yahoo Finance chart (v8 – 2d/5m, includePrePost; "
        "utolsó RTH záró → AH/PM utolsó ár alapján számolt % mozgás)",
        "",
        coverage_line,
    ]



    # Makró / FED / piaci hangulat blokk (#1)
    if macro_text and macro_text.strip():
        macro_text_final = macro_text
    else:
        macro_text_final = fetch_macro_text(
            report=1,
            out_path="reports/macro_1.txt",
            base_url_env="MACRO_FEED_URL_1",
        )

    if macro_text_final:
        # A Yahoo-makró híreket itt nem keverjük hozzá, a webapp már tartalmazza az összefoglalót.
        macro_block = format_macro_block(macro_text_final, [])
    else:
        macro_block = ""

        # Elemzői lépések / közeli katalizátorok / high-conviction események (5/6/7. blokk)
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

    # 3–4. blokk: ármozgások (darabszámos + watchlist)
    lines.append("")
    lines.append("### 📊 Darabszámos tickerek – After-hours & Premarket mozgások")
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
        max_move = entry.get("max_move")
        base = f"{sym} — AH {fmt_pct(ah_pct)} | PM {fmt_pct(pm_pct)}"
        comment = ""

        if max_move is None:
            comment = "Nincs AH/PM adat az adott ablakokra."
        else:
            abs_mv = abs(max_move)
            if abs_mv >= 3.0:
                comment = "Küszöb feletti AH/PM elmozdulás (≥3%)."
            elif abs_mv >= 1.0:
                comment = "Küszöb alatti AH/PM elmozdulás (<3%)."

        line = base if not comment else f"{base} — {comment}"
        lines.append(line)

    # Watchlist – max(|AH|,|PM|) szerint csökkenő
    watch_sorted = sorted(
        watch_results,
        key=lambda x: x.get("max_move", 0.0),
        reverse=True,
    )

    if watch_sorted:
        lines.append("")
        lines.append("### 🔍 Watchlist – After-hours & Premarket mozgások (csak ha ≥K)")
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

    # 5) Bejelentések & elemzői fel/lemínősítések
    if analyst_block:
        lines.append("")
        lines.append(analyst_block)

    # 6) Közeli katalizátorok (3–12 hónap)
    if catalyst_block:
        lines.append("")
        lines.append(catalyst_block)

    # 7) High-conviction (3–12 hónapos, listán kívüli jelöltek)
    if highconv_block:
        lines.append("")
        lines.append(highconv_block)

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




def generate_report2_macro_only(
    script_version: str,
    output_md: str,
    output_json: str,
    macro_text: Optional[str] = None,
) -> str:
    """#2 – Tegnapi nyitástól zárásig – makró/elemző/katalizátor/high-conviction váz."""
    coverage_line = (
        "Lefedettség: HIÁNYOS – ticker-szintű #2 modul még fejlesztés alatt ebben a verzióban."
    )

    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=1)))

    header_lines = [
        "#2 – Előző kereskedési nap: nyitástól zárásig (15:30–22:00) — CEST",
        "",
        f"Script verzió: {script_version}",
        "",
        "Időablak (CEST): előző kereskedési nap 15:30 → 22:00 (US RTH Open→Close).",
        "",
        coverage_line,
    ]

    yahoo_macro_news = fetch_yahoo_macro_news(report_type=2, now_cet=now)
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

    text = "\n".join(lines)

    # JSON váz – makró + event-listák
    payload = {
        "mode": 2,
        "script_version": script_version,
        "coverage": coverage_line,
        "macro_text": macro_text or "",
        "analyst_events": analyst_events,
        "catalyst_events": catalyst_events,
        "highconviction_events": highconv_events,
    }
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    with open(output_md, "w", encoding="utf-8") as f:
        f.write(text)

    return text


def generate_report3_macro_only(
    script_version: str,
    output_md: str,
    output_json: str,
    macro_text: Optional[str] = None,
) -> str:
    """#3 – Ma nyitástól mostanáig – makró/elemző/katalizátor/high-conviction váz."""
    coverage_line = (
        "Lefedettség: HIÁNYOS – ticker-szintű #3 modul még fejlesztés alatt ebben a verzióban."
    )

    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=1)))

    header_lines = [
        "#3 – Mai kereskedési nap: nyitástól mostanáig (15:30-tól) — CEST",
        "",
        f"Script verzió: {script_version}",
        "",
        "Időablak (CEST): mai kereskedési nap 15:30 → mostanáig (US RTH Open→Most).",
        "",
        coverage_line,
    ]

    yahoo_macro_news = fetch_yahoo_macro_news(report_type=3, now_cet=now)
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

    text = "\n".join(lines)

    payload = {
        "mode": 3,
        "script_version": script_version,
        "coverage": coverage_line,
        "macro_text": macro_text or "",
        "analyst_events": analyst_events,
        "catalyst_events": catalyst_events,
        "highconviction_events": highconv_events,
    }
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    with open(output_md, "w", encoding="utf-8") as f:
        f.write(text)

    return text

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
    parser.add_argument("--macro", help="Makró szöveg Politika/FED/piaci hangulat blokkokhoz")

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
    elif mode == 2:
        summary_path = args.summary or "reports/summary_report_2.md"
        json_path = "reports/latest_2.json"
        text = generate_report2_macro_only(
            script_version=script_version,
            output_md=summary_path,
            output_json=json_path,
            macro_text=args.macro,
        )
        print(text)
    elif mode == 3:
        summary_path = args.summary or "reports/summary_report_3.md"
        json_path = "reports/latest_3.json"
        text = generate_report3_macro_only(
            script_version=script_version,
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
