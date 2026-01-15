\
#!/usr/bin/env python3
# analyst.py — v0.1.0-finnhub-updown-2026-01-15
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.
#
# Cél:
# - Ingyenes, script-biztos forrás: Finnhub (FREE tier; API key kell, de ingyenes)
# - 2 naptári napon belüli upgrade/downgrade + (ha van) célárváltozás (from → to)
#
# Használat:
#   python scripts/analyst.py --master reports/master.csv --days 2 --out-md reports/analyst_last2d.md --out-json reports/analyst_last2d.json
#
# Secret:
#   FINNHUB_API_KEY (GitHub Actions → Settings → Secrets and variables → Actions)

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

VERSION = "v0.1.0-finnhub-updown-2026-01-15"

TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,14}$")


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyst upgrades/downgrades + PT (Finnhub) — last N calendar days.")
    p.add_argument("--master", default="reports/master.csv", help="MASTER CSV path (default: reports/master.csv)")
    p.add_argument("--days", type=int, default=2, help="Lookback in calendar days (default: 2)")
    p.add_argument("--tickers", default="", help="Optional override tickers, comma-separated (e.g. NVDA,TSM,AMD)")
    p.add_argument("--out-md", default="reports/analyst_last2d.md", help="Output markdown path")
    p.add_argument("--out-json", default="reports/analyst_last2d.json", help="Output json path")
    p.add_argument("--max-tickers", type=int, default=250, help="Safety limit")
    p.add_argument("--sleep", type=float, default=0.25, help="Sleep between API calls (seconds)")
    p.add_argument("--debug", action="store_true", help="Verbose debug to stderr")
    return p.parse_args()


def _read_master_tickers(master_path: str, max_tickers: int, debug: bool = False) -> List[str]:
    tickers: List[str] = []
    if not os.path.exists(master_path):
        return tickers

    with open(master_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            # scan row for first plausible ticker
            cand = None
            for cell in row[:5]:
                if cell is None:
                    continue
                s = str(cell).strip().upper()
                if not s:
                    continue
                if s in ("TICKER", "SYMBOL", "RIC", "NAME"):
                    continue
                if TICKER_RE.match(s):
                    cand = s
                    break
            if cand and cand not in tickers:
                tickers.append(cand)
            if len(tickers) >= max_tickers:
                break

    if debug:
        print(f"[DEBUG] master tickers: {len(tickers)}", file=sys.stderr)
    return tickers


def _http_json(url: str, timeout: int = 20) -> Any:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; price-engine/analyst; +https://github.com/)",
            "Accept": "application/json,text/plain,*/*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8", errors="replace"))


def _start_of_day_utc(d: dt.date) -> dt.datetime:
    return dt.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=dt.timezone.utc)


def _within_last_calendar_days(ts: dt.datetime, days: int) -> bool:
    # "calendar days" UTC-based: include today and (days-1) previous dates
    now = _utc_now()
    start_date = (now.date() - dt.timedelta(days=days - 1))
    start = _start_of_day_utc(start_date)
    return ts >= start and ts <= now


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _as_ts(item: Dict[str, Any]) -> Optional[dt.datetime]:
    # Finnhub upgrade-downgrade items often have 'time' (unix seconds) or 'timestamp' or 'date'
    for k in ("time", "timestamp"):
        if k in item:
            v = item.get(k)
            try:
                if v is None:
                    continue
                return dt.datetime.fromtimestamp(int(v), tz=dt.timezone.utc)
            except Exception:
                pass
    if "date" in item:
        v = item.get("date")
        if isinstance(v, str) and v.strip():
            # try YYYY-MM-DD
            try:
                d = dt.date.fromisoformat(v.strip()[:10])
                return _start_of_day_utc(d)
            except Exception:
                return None
    return None


def _fetch_finnhub_updown(ticker: str, api_key: str, debug: bool = False) -> List[Dict[str, Any]]:
    # Endpoint: https://finnhub.io/docs/api/upgrade-downgrade
    url = f"https://finnhub.io/api/v1/stock/upgrade-downgrade?symbol={ticker}&token={api_key}"
    try:
        data = _http_json(url)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "error" in data:
            if debug:
                print(f"[DEBUG] Finnhub error {ticker}: {data.get('error')}", file=sys.stderr)
            return []
        return []
    except Exception as e:
        if debug:
            print(f"[DEBUG] Finnhub fetch failed {ticker}: {e}", file=sys.stderr)
        return []


def _normalize_event(ticker: str, item: Dict[str, Any]) -> Dict[str, Any]:
    ts = _as_ts(item)
    # common fields (best-effort)
    brokerage = item.get("brokerage") or item.get("analyst") or item.get("firm") or ""
    action = item.get("action") or ""  # upgrade/downgrade/reiterated/initiated etc.
    from_grade = item.get("fromGrade") or item.get("from_grade") or item.get("from") or ""
    to_grade = item.get("toGrade") or item.get("to_grade") or item.get("to") or ""
    # price targets may appear as floats or strings
    from_pt = _as_float(item.get("fromPT") or item.get("fromPt") or item.get("from_pt"))
    to_pt = _as_float(item.get("toPT") or item.get("toPt") or item.get("to_pt") or item.get("pt") or item.get("priceTarget"))
    return {
        "ticker": ticker,
        "timestamp": ts.isoformat() if ts else None,
        "brokerage": str(brokerage).strip(),
        "action": str(action).strip(),
        "from_grade": str(from_grade).strip(),
        "to_grade": str(to_grade).strip(),
        "from_pt": from_pt,
        "to_pt": to_pt,
        "raw": item,
    }


def _format_pt(from_pt: Optional[float], to_pt: Optional[float]) -> str:
    if to_pt is None and from_pt is None:
        return "PT: n/a"
    if to_pt is not None and from_pt is not None:
        return f"PT: {from_pt:g} → {to_pt:g}"
    if to_pt is not None:
        return f"PT: új {to_pt:g} (előző nem közölt)"
    return "PT: n/a"


def _format_rating(action: str, from_grade: str, to_grade: str) -> str:
    a = action.strip()
    fg = from_grade.strip()
    tg = to_grade.strip()
    if fg or tg:
        if fg and tg:
            return f"{a}: {fg} → {tg}".strip(": ")
        if tg:
            return f"{a}: → {tg}".strip(": ")
        return f"{a}: {fg} →".strip(": ")
    return a or "rating: n/a"


def _write_outputs(events: List[Dict[str, Any]], out_md: str, out_json: str, days: int) -> None:
    os.makedirs(os.path.dirname(out_md) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(out_json) or ".", exist_ok=True)

    payload = {
        "version": VERSION,
        "generated_utc": _utc_now().isoformat(),
        "lookback_calendar_days": days,
        "count": len(events),
        "events": events,
    }
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    lines: List[str] = []
    lines.append(f"# Analyst feed (upgrade/downgrade + PT) — last {days} calendar days")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Generálva (UTC): {_utc_now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    if not events:
        lines.append("_Nincs friss (≤2 naptári nap) fel/lemínősítés vagy célár-frissítés a forrásban._")
        lines.append("")
    else:
        # group by date
        for ev in events:
            ts = ev.get("timestamp") or ""
            ticker = ev.get("ticker", "")
            brokerage = ev.get("brokerage", "")
            rating = _format_rating(ev.get("action",""), ev.get("from_grade",""), ev.get("to_grade",""))
            pt = _format_pt(ev.get("from_pt"), ev.get("to_pt"))
            lines.append(f"- **{ticker}** — {ts} — {brokerage} — {rating} — {pt}")

    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")


def main() -> int:
    args = _parse_args()

    api_key = os.environ.get("FINNHUB_API_KEY", "").strip()

    if args.tickers.strip():
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = _read_master_tickers(args.master, args.max_tickers, debug=args.debug)

    # If no key or no tickers, still produce outputs with explicit note
    if not api_key:
        _write_outputs([], args.out_md, args.out_json, args.days)
        if args.debug:
            print("[DEBUG] FINNHUB_API_KEY missing -> empty output", file=sys.stderr)
        return 0

    if not tickers:
        _write_outputs([], args.out_md, args.out_json, args.days)
        if args.debug:
            print("[DEBUG] No tickers found -> empty output", file=sys.stderr)
        return 0

    events_norm: List[Dict[str, Any]] = []

    for i, t in enumerate(tickers, start=1):
        raw_list = _fetch_finnhub_updown(t, api_key, debug=args.debug)
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            ts = _as_ts(item)
            if ts is None:
                continue
            if _within_last_calendar_days(ts, args.days):
                events_norm.append(_normalize_event(t, item))
        if args.sleep > 0:
            time.sleep(args.sleep)

    # sort: newest first, then ticker
    def _sort_key(ev: Dict[str, Any]) -> Tuple[str, str]:
        ts = ev.get("timestamp") or ""
        # reverse by using negative isn't easy for strings; sort later with reverse
        return (ts, ev.get("ticker") or "")

    events_norm.sort(key=_sort_key, reverse=True)

    _write_outputs(events_norm, args.out_md, args.out_json, args.days)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
