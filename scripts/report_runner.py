#!/usr/bin/env python3
"""report_runner.py – v3.0.35-biblia-ahpm-sessionfix-5d

Megjegyzés: Ez a verzió a korábbi teljes runner logikát megtartja.
A bibliás formátum finomhangolása külön lépésekben történik.
"""
# IMÁDSÁG (hibajavítás után)
# Bocsáss meg uram, mert balfék voltam, és elcsúszott az indent.
# Add uram, hogy ez a módosítás most hibátlanul fusson.
# bocsáss meg Uram, hogy eddig két pipeline ütközött.
# add, hogy most az egyetlen kanonikus út stabilan fusson. Ámen.



import argparse
import csv
import datetime as dt
import json

def load_macro_from_json(path: str) -> str:
    """Load macro output written by the GAS macro feed.

    We accept multiple shapes because the webapp can evolve:
    - dict with 'narrative' (list of strings)
    - dict with 'items' (list of dicts)
    - dict with 'events' (list of dicts)
    - list of dicts (treated as items/events)
    - plain string (already a narrative text)

    Returns a single text blob (max ~3–6 lines is enforced upstream).
    """
    try:
        import os

        if not path or (not os.path.exists(path)):
            return ""

        raw = open(path, "r", encoding="utf-8", errors="replace").read().strip()
        if not raw:
            return ""

        # Try JSON first; if it fails, treat as plain text.
        try:
            data = json.loads(raw)
        except Exception:
            return raw

        # Normalize to a list of line strings.
        lines: list[str] = []

        if isinstance(data, str):
            return data.strip()

        # If the top-level is a list -> treat as items
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            # Preferred: narrative array
            if isinstance(data.get("narrative"), list):
                lines = [str(x).strip() for x in data.get("narrative") if str(x).strip()]
                return "\n".join(lines).strip()
            # Common: items/events arrays
            items = data.get("items")
            if not isinstance(items, list):
                items = data.get("events")
            if not isinstance(items, list):
                # Some implementations may store 'text'
                txt = data.get("text") or data.get("message") or ""
                return str(txt).strip()
        else:
            return ""

        # items/events list of dicts (or strings)
        for it in items:
            if isinstance(it, str):
                s = it.strip()
                if s:
                    lines.append(s)
                continue
            if not isinstance(it, dict):
                continue
            title = (it.get("headline") or it.get("title") or it.get("text") or "").strip()
            src = (it.get("source") or "").strip()
            if title and src:
                lines.append(f"{title} ({src})")
            elif title:
                lines.append(title)

        return "\n".join([x for x in lines if x]).strip()

    except Exception as e:
        print(f"[MACRO_JSON] error: {e}")
        return ""


def _get_requests_session():
    """Lazy requests.Session + alap header (Yahoo 429 ellen)"""
    global SESSION
    if SESSION is None:
        SESSION = requests.Session()
        SESSION.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9,hu;q=0.8",
            "Connection": "keep-alive",
        })
    return SESSION

def _get_json_with_retries(url: str, params: dict, timeout: int = 12, max_tries: int = 4):
    """GET JSON retries (főleg Yahoo 429 / 5xx)."""
    sess = _get_requests_session()
    backoffs = [1.5, 3.0, 6.0, 12.0]
    last_err = None
    for i in range(max_tries):
        try:
            resp = sess.get(url, params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                # backoff + jitter
                import time, random
                wait = backoffs[min(i, len(backoffs)-1)] + random.random()
                time.sleep(wait)
                last_err = f"HTTP_{resp.status_code}"
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = str(e)
            try:
                import time, random
                wait = backoffs[min(i, len(backoffs)-1)] + random.random()
                time.sleep(wait)
            except Exception:
                pass
            continue
    raise RuntimeError(f"GET JSON failed after retries: {last_err}")



DEFAULT_K = 3.0

def _prev_close_bud(now_bud: dt.datetime) -> dt.datetime:
    """Previous US trading day close expressed in Budapest time at 22:00.
    Rule of thumb (biblia #1 macro block): previous close 22:00 CEST -> now.
    """
    # normalize tz-aware
    if now_bud.tzinfo is None:
        now_bud = now_bud.replace(tzinfo=ZoneInfo("Europe/Budapest"))
    close_t = dt.time(22, 0)
    wd = now_bud.weekday()  # Mon=0 ... Sun=6
    # If weekend, go back to Friday
    if wd == 5:  # Sat
        base = now_bud - dt.timedelta(days=1)
        base = base.replace(hour=22, minute=0, second=0, microsecond=0)
        return base - dt.timedelta(days=base.weekday() - 4) if base.weekday() != 4 else base
    if wd == 6:  # Sun
        base = now_bud - dt.timedelta(days=2)
        return base.replace(hour=22, minute=0, second=0, microsecond=0)
    # Weekday: if before 22:00, previous day's close (Mon -> Fri)
    if now_bud.time() < close_t:
        if wd == 0:
            base = now_bud - dt.timedelta(days=3)
        else:
            base = now_bud - dt.timedelta(days=1)
        return base.replace(hour=22, minute=0, second=0, microsecond=0)
    # After close: today's close
    return now_bud.replace(hour=22, minute=0, second=0, microsecond=0)


def _sanitize_macro_lines(macro_text: str) -> List[str]:
    """Keep only real lines; no headings/boilerplate. Never invent content."""
    out: List[str] = []
    for raw in (macro_text or "").splitlines():
        s = raw.strip()
        if not s:
            continue
        if s.startswith("#"):
            continue
        if s.lower().startswith("időablak"):
            continue
        if s.lower().startswith("time window"):
            continue
        out.append(s)
    return out


def build_macro_block_report1(macro_text: str, now_bud: dt.datetime) -> str:
    """Biblia-style #1 macro block:
    - Always prints time window line at top
    - If no news: single 'no material news' line (no filler)
    - If news: 3–6 lines max (we do not pad to 3; we never invent)
    """
    start_bud = _prev_close_bud(now_bud)
    tzname = now_bud.tzname() or "CEST"
    window_line = f"**Időablak:** {start_bud.strftime('%Y-%m-%d %H:%M')} {tzname} → {now_bud.strftime('%Y-%m-%d %H:%M')} {tzname}"

    lines = ["### 🧭 Makró / FED / Politika", window_line, ""]
    items = _sanitize_macro_lines(macro_text)

    if not items:
        lines.append("Az előző piaczárás óta nem érkezett a piac egészét érdemben befolyásoló makró, FED vagy politikai hír.")
        return "\n".join(lines)

    # Keep up to 6 lines; never invent missing lines.
    items = items[:6]
    lines.extend(items)
    return "\n".join(lines)


def run_analyst_catalyst_builder(report: int, reports_dir: str = 'reports') -> None:
    """Build analyst/catalyst artifacts via scripts/analyst_catalyst_builder.py (noblock)."""
    builder = os.path.join('scripts', 'analyst_catalyst_builder.py')
    if not os.path.exists(builder):
        print(f"[WARN] missing: {builder} (skipping analyst/catalyst build)")
        return
    cmd = [sys.executable, builder, '--report', str(report), '--reports-dir', reports_dir]
    try:
        r = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if r.stdout.strip():
            print(r.stdout.strip())
        if r.stderr.strip():
            print(r.stderr.strip())
        if r.returncode != 0:
            print(f"[WARN] analyst_catalyst_builder exit={r.returncode} (continuing)")
    except Exception as e:
        print(f"[WARN] analyst_catalyst_builder crashed: {e}")

DEFAULT_SCRIPT_VERSION = "v3.0.45"
AH_PM_MODE = "chart"  # alapértelmezett: Yahoo quote/spark alapú AH/PM


WATCHLIST_DEFAULT_PATH = "reports/master.csv"
ANALYST_EVENTS_PATH_TEMPLATE = "reports/analyst_{report}.json"
CATALYST_EVENTS_PATH_TEMPLATE = "reports/catalysts_{report}.json"

# Optional overrides (set from CLI args). If set, report number formatting is ignored.
ANALYST_EVENTS_PATH_OVERRIDE = None
CATALYST_EVENTS_PATH_OVERRIDE = None

def get_analyst_events_path(report: str) -> str:
    """Path to analyst-events JSON used by the report.

    Convention: write/read under reports/ (NOT reports/<report>/).
    Allow override via --analyst-events-path.
    """
    if ANALYST_EVENTS_PATH_OVERRIDE:
        return ANALYST_EVENTS_PATH_OVERRIDE
    return f"reports/analyst_{report}.json"


def get_catalyst_events_path(report: str) -> str:
    """Path to catalyst-events JSON used by the report.

    Convention: write/read under reports/ (NOT reports/<report>/).
    Allow override via --catalyst-events-path.
    """
    if CATALYST_EVENTS_PATH_OVERRIDE:
        return CATALYST_EVENTS_PATH_OVERRIDE
    return f"reports/catalysts_{report}.json"


HIGHCONV_EVENTS_PATH = "reports/high_conv_1.json"


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

    rows_raw = []  # raw CSV rows for lot/broker notes (Option A)
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
            rows_raw.append(dict(row))
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

        # Build informational lot/broker notes for ticker-unique reporting (Option A)
    global POSITION_LOT_NOTES
    lot_map: Dict[str, Dict[str, object]] = {}
    for _row in rows_raw:
        _t = str(_row.get('ticker','') or _row.get('Ticker','') or '').strip().upper()
        if not _t:
            continue
        _q = _row.get('quantity') if 'quantity' in _row else _row.get('qty')
        try:
            _qty = float(str(_q).replace(',', '.')) if _q not in (None, '') else 0.0
        except Exception:
            _qty = 0.0
        if _qty <= 0:
            continue
        _broker = str(_row.get('broker','') or _row.get('Broker','') or _row.get('bróker','') or '').strip()
        if _t not in lot_map:
            lot_map[_t] = {'lots': 0, 'brokers': set()}
        lot_map[_t]['lots'] = int(lot_map[_t]['lots']) + 1
        if _broker:
            lot_map[_t]['brokers'].add(_broker)
    POSITION_LOT_NOTES = {}
    for _t, _info in lot_map.items():
        _lots = int(_info.get('lots', 0))
        if _lots >= 2:
            _brokers = _info.get('brokers') or set()
            _btxt = '+'.join(sorted(list(_brokers))) if _brokers else 'multi'
            POSITION_LOT_NOTES[_t] = f" ({_lots} lot: {_btxt})"

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
        resp = _get_requests_session().get(url, params=params, timeout=10)
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



def fetch_yahoo_quote_single(symbol: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Single-symbol Yahoo quote fallback (prev_close, ah_pct, pm_pct). Best-effort."""
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": symbol}
    try:
        data = _get_json_with_retries(url, params=params, timeout=12, max_tries=4)
        items = (data.get("quoteResponse", {}) or {}).get("result", []) or []
        if not items:
            return (None, None, None)
        item = items[0] or {}
        def _to_float(x):
            try:
                return float(x)
            except Exception:
                return None
        prev_close = _to_float(item.get("regularMarketPreviousClose"))
        ah_pct = _to_float(item.get("postMarketChangePercent"))
        pm_pct = _to_float(item.get("preMarketChangePercent"))
        return (prev_close, ah_pct, pm_pct)
    except Exception:
        return (None, None, None)


def _chart_get(host: str, symbol: str, params: dict):
    url = f"https://{host}/v8/finance/chart/{symbol}"
    return _get_requests_session().get(url, params=params, timeout=15)


def fetch_chart(symbol: str) -> Tuple[dict, List[int], List[Optional[float]]]:
    """Yahoo chart v8 with retries + host fallback (query1 -> query2)."""
    ny_now = dt.datetime.now(ZoneInfo("America/New_York"))
    rng = "5d" if ny_now.weekday() in (0, 5, 6) else "2d"
    params = {"range": rng, "interval": "5m", "includePrePost": "true"}

    last_err = None
    for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
        for _ in range(3):
            try:
                resp = _chart_get(host, symbol, params)
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
            except Exception as e:
                last_err = e
                continue
    raise RuntimeError(str(last_err) if last_err else "chart_failed")


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
    report: int = 1,
) -> str:
    # Pozíciók: a master/watchlist alapján inferálva (nincs külön positions.csv)
    positions = infer_positions_from_watchlist(watchlist_path)
    watch = load_watchlist(watchlist_path)
    all_symbols = sorted(set(watch.keys()) | set(positions.keys()))

    ah_pm_mode = "chart"
    quote_map: Dict[str, Tuple[Optional[float], Optional[float], Optional[float]]] = {}

    # AH/PM mód csak #1 riportnál értelmezett
    if report == 1:
        ah_pm_mode = os.environ.get("AH_PM_MODE", AH_PM_MODE).lower()
        if ah_pm_mode == "spark":
            quote_map = fetch_yahoo_quote_batch(all_symbols)
            if all_symbols and not quote_map:
                debug("[WARN] Yahoo quote batch üres/blocked – chart fallback kényszerítve minden tickerre.")
                ah_pm_mode = "chart"

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
            if rth_close is None or ah_pct is None or pm_pct is None:
                meta, ts, closes = fetch_chart(sym)
                rth_close_chart, ah_from_chart, pm_from_chart = compute_ah_pm_move(meta, ts, closes)
                rth_close = rth_close_chart
                if ah_pct is None:
                    ah_pct = ah_from_chart
                if pm_pct is None:
                    pm_pct = pm_from_chart
        except Exception as e:
            # Chart hiba esetén próbáljuk meg a single-quote fallbackot (ha elérhető)
            rth_close_q, ah_q, pm_q = fetch_yahoo_quote_single(sym)
            if rth_close is None:
                rth_close = rth_close_q
            if ah_pct is None:
                ah_pct = ah_q
            if pm_pct is None:
                pm_pct = pm_q
            if rth_close is None and ah_pct is None and pm_pct is None:
                missing[sym] = str(e)
                INTERNAL_LOG.append({'stage': 'price_fetch', 'ticker': sym, 'error': str(e)})
                continue

        # Ha sem bázisár, sem AH/PM % nem állt elő, ez lefedettségi hiba (ne fusson át csendben).
        if rth_close is None and ah_pct is None and pm_pct is None:
            missing[sym] = "nincs RTH/AH/PM adat (Yahoo quote+chart nem adott értelmezhető értéket)"
            INTERNAL_LOG.append({'stage': 'price_fetch', 'ticker': sym, 'error': missing.get(sym)})
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
        "**Árforrás:** Yahoo Finance chart (v8 – 2d/5m; hétfő/hétvége: 5d/5m; includePrePost; "
        "utolsó RTH záró → AH/PM utolsó ár alapján számolt % mozgás)",
        "",
        coverage_line,
    ]


    # Makró / FED / piaci hangulat blokk (#1)
    macro_text_json = load_macro_from_json("reports/macro_news_1.json")
    if macro_text_json:
        macro_text_final = macro_text_json
    elif macro_text and macro_text.strip():
        macro_text_final = macro_text
    else:
        macro_text_final = fetch_macro_text(
            report=1,
            out_path="reports/macro_1.txt",
            base_url_env="MACRO_FEED_URL_1",
        )

    if macro_text_final:
        # A Yahoo-makró híreket itt nem keverjük hozzá, a webapp már tartalmazza az összefoglalót.
        macro_block = build_macro_block_report1(macro_text_final, now_bud=dt.datetime.now(ZoneInfo("Europe/Budapest")))
    else:
        macro_block = build_macro_block_report1("", now_bud=dt.datetime.now(ZoneInfo("Europe/Budapest")))

        # Elemzői lépések / közeli katalizátorok / high-conviction események (5/6/7. blokk)
    analyst_events = fetch_analyst_events(get_analyst_events_path(report))
    analyst_block = format_analyst_block(analyst_events)

    catalyst_events = fetch_catalyst_events(get_catalyst_events_path(report))
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
        note = POSITION_LOT_NOTES.get(sym, "")
        base = f"{sym}{note} — AH {fmt_pct(ah_pct)} | PM {fmt_pct(pm_pct)}"
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


    # belső log (hibaokok) mentése – ha van
    if INTERNAL_LOG:
        internal_log_path = os.path.join(os.path.dirname(output_md), f"internal_log_{report}.txt")
        with open(internal_log_path, "w", encoding="utf-8") as lf:
            for rec in INTERNAL_LOG:
                lf.write(json.dumps(rec, ensure_ascii=False) + "\n")

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

    analyst_events = fetch_analyst_events(get_analyst_events_path(report))
    analyst_block = format_analyst_block(analyst_events)

    catalyst_events = fetch_catalyst_events(get_catalyst_events_path(report))
    catalyst_block = format_catalyst_block(catalyst_events)

    highconv_events = fetch_highconviction_events(HIGHCONV_EVENTS_PATH)
    highconv_block = format_highconviction_block(highconv_events)

    lines: List[str] = []
    lines.extend(header_lines)

    if macro_block:
        lines.append("")
        lines.append(macro_block)

    
    # 5) Bejelentések & elemzői fel/lemínősítések (KÖTELEZŐ blokk – null-blokk is)
    lines.append("")
    if analyst_block:
        lines.append(analyst_block)
    else:
        lines.append("### 🧩 Bejelentések & fel/lemínősítések")
        lines.append("- Nincs új, anyagilag lényeges vállalati bejelentés / fel- vagy leminősítés az AH/PM sávban.")

    # 6) Közelgő katalizátorok (KÖTELEZŐ blokk – null-blokk is)
    lines.append("")
    if catalyst_block:
        lines.append(catalyst_block)
    else:
        lines.append("### ⏳ Közelgő katalizátorok")
        lines.append("- Nincs közzétett, rövid távon (napok–hetek) esedékes katalizátor a vizsgált AH/PM sávban.")

    # 7) Listán kívüli, 3–12 hónapos high-conviction jelöltek (FELTÉTELES, de explicit üres blokk kell)
    lines.append("")
    if highconv_block:
        lines.append(highconv_block)
    else:
        lines.append("### 🚀 Listán kívüli, 3–12 hónapos high-conviction jelöltek")
        lines.append("- Nincs új, ismételt erős jelzés a vizsgált időablakban.")
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

    analyst_events = fetch_analyst_events(get_analyst_events_path(report))
    analyst_block = format_analyst_block(analyst_events)

    catalyst_events = fetch_catalyst_events(get_catalyst_events_path(report))
    catalyst_block = format_catalyst_block(catalyst_events)

    highconv_events = fetch_highconviction_events(HIGHCONV_EVENTS_PATH)
    highconv_block = format_highconviction_block(highconv_events)

    lines: List[str] = []
    lines.extend(header_lines)

    if macro_block:
        lines.append("")
        lines.append(macro_block)

    
    # 5) Bejelentések & elemzői fel/lemínősítések (KÖTELEZŐ blokk – null-blokk is)
    lines.append("")
    if analyst_block:
        lines.append(analyst_block)
    else:
        lines.append("### 🧩 Bejelentések & fel/lemínősítések")
        lines.append("- Nincs új, anyagilag lényeges vállalati bejelentés / fel- vagy leminősítés az AH/PM sávban.")

    # 6) Közelgő katalizátorok (KÖTELEZŐ blokk – null-blokk is)
    lines.append("")
    if catalyst_block:
        lines.append(catalyst_block)
    else:
        lines.append("### ⏳ Közelgő katalizátorok")
        lines.append("- Nincs közzétett, rövid távon (napok–hetek) esedékes katalizátor a vizsgált AH/PM sávban.")

    # 7) Listán kívüli, 3–12 hónapos high-conviction jelöltek (FELTÉTELES, de explicit üres blokk kell)
    lines.append("")
    if highconv_block:
        lines.append(highconv_block)
    else:
        lines.append("### 🚀 Listán kívüli, 3–12 hónapos high-conviction jelöltek")
        lines.append("- Nincs új, ismételt erős jelzés a vizsgált időablakban.")
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
    parser.add_argument("--analyst-out", dest="analyst_out", default="", help="(optional) Analyst events JSON input path (overrides default reports/analyst_{report}.json)")
    parser.add_argument("--catalyst-out", dest="catalyst_out", default="", help="(optional) Catalyst events JSON input path (overrides default reports/catalysts_{report}.json)")

    args = parser.parse_args()

    # Wire optional input paths (keeps workflow backwards compatible if it passes these flags).
    global ANALYST_EVENTS_PATH_OVERRIDE, CATALYST_EVENTS_PATH_OVERRIDE
    if getattr(args, 'analyst_out', ''):
        ANALYST_EVENTS_PATH_OVERRIDE = args.analyst_out
    if getattr(args, 'catalyst_out', ''):
        CATALYST_EVENTS_PATH_OVERRIDE = args.catalyst_out


    mode = args.mode or args.report or 1
    watchlist_path = args.watchlist or args.csv or "reports/master.csv"
    script_version = args.script_version or DEFAULT_SCRIPT_VERSION
    k_default = args.k_default or DEFAULT_K


    # Build analyst/catalyst JSON via noblock builder (RSS + Yahoo).
    run_analyst_catalyst_builder(report=mode, reports_dir='reports')

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
            report=mode,
        )
        print(text)

        # Post-process #1 report (BIBLIA blokk-sorrend és null-blokkok biztosítása)
        try:
            import subprocess
            this_dir = os.path.dirname(os.path.abspath(__file__))
            pp_path = os.path.join(this_dir, "postprocess_report.py")
            subprocess.run(
                [sys.executable, pp_path, "--md", summary_path, "--bundle-dir", "reports"],
                check=False,
            )
        except Exception as e:  # pragma: no cover - postprocess best-effort
            debug(f"Postprocess hiba: {e!r}")

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
