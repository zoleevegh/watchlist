#!/usr/bin/env python3
# earnings.py – v0.2.5-earnings-next7d-nasdaq-calendar-master-2026-01-10
#
# Cél:
# - Következő 7 nap (lokális naptári nap, Europe/Budapest)
# - Nasdaq earnings CALENDAR (date-based) -> kevés kérés, stabilabb mint ticker-szintű
# - MASTER (CSV) tickerek szűrése
# - Kimenet: reports/earnings_next7d.md (csak ez)
#
# NEM:
# - ticker-szintű API
# - Yahoo / Investing
# - HTML scrape
# - UTC-trükközés (csak a napi dátumokhoz ISO YYYY-MM-DD)

from __future__ import annotations

import csv
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


TZ_NAME = "Europe/Budapest"
OUT_MD = Path("reports/earnings_next7d.md")
DEFAULT_MASTER_LOCAL = Path("reports/master.csv")


@dataclass(frozen=True)
class Row:
    d: str
    t: str
    ticker: str
    eps: str
    rev: str
    source: str


def _now_local() -> datetime:
    if ZoneInfo is None:
        return datetime.now()
    return datetime.now(ZoneInfo(TZ_NAME))


def _read_master_tickers(master_path: Path) -> set[str]:
    if not master_path.exists():
        raise FileNotFoundError(f"MASTER CSV not found: {master_path}")

    tickers: set[str] = set()
    with master_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        header = [h.strip() for h in (reader.fieldnames or [])]
        ticker_col = None
        for cand in ("Ticker", "ticker", "TICKER", "Symbol", "symbol"):
            if cand in header:
                ticker_col = cand
                break
        if ticker_col is None:
            ticker_col = header[0] if header else None
        if ticker_col is None:
            return tickers

        for row in reader:
            v = (row.get(ticker_col) or "").strip()
            if not v:
                continue
            tickers.add(v.upper())
    return tickers


def _http_get_json(url: str, timeout: int = 25) -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nasdaq.com/",
        "Connection": "close",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8", errors="replace"))


def _map_time(v: str) -> str:
    s = (v or "").strip().lower()
    if not s:
        return "time-not-supplied"
    if "before" in s and "open" in s:
        return "time-pre-market"
    if "after" in s and ("close" in s or "market" in s):
        return "time-after-hours"
    if "not" in s and "supplied" in s:
        return "time-not-supplied"
    return s.replace(" ", "-")


def _fmt_money(x) -> str:
    if x is None:
        return ""
    if isinstance(x, (int, float)):
        return f"${x:.2f}"
    s = str(x).strip()
    if not s:
        return ""
    if s.startswith("$"):
        return s
    try:
        fv = float(s.replace(",", ""))
        return f"${fv:.2f}"
    except Exception:
        return s


def _extract_rows_for_date(day: date) -> list[dict]:
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={day.isoformat()}"
    j = _http_get_json(url)
    data = (j.get("data") or {})
    rows = data.get("rows") or []
    return [r for r in rows if isinstance(r, dict)]


def _build_report(master_tickers: set[str], start_day: date, days: int = 7) -> tuple[list[Row], dict]:
    found: list[Row] = []
    stats = {
        "days_queried": 0,
        "nasdaq_calendar_ok_days": 0,
        "nasdaq_calendar_block_days": 0,
        "nasdaq_calendar_other_fail_days": 0,
    }

    for i in range(days):
        d = start_day + timedelta(days=i)
        stats["days_queried"] += 1
        try:
            rows = _extract_rows_for_date(d)
            stats["nasdaq_calendar_ok_days"] += 1
        except urllib.error.HTTPError as e:
            code = int(getattr(e, "code", 0) or 0)
            if code in (401, 403, 429):
                stats["nasdaq_calendar_block_days"] += 1
            else:
                stats["nasdaq_calendar_other_fail_days"] += 1
            continue
        except Exception:
            stats["nasdaq_calendar_other_fail_days"] += 1
            continue

        for r in rows:
            sym = (r.get("symbol") or "").strip().upper()
            if not sym or sym not in master_tickers:
                continue
            t = _map_time(r.get("time") or "")
            eps = _fmt_money(r.get("epsForecast"))
            rev = _fmt_money(r.get("revenueForecast"))
            found.append(Row(d=d.isoformat(), t=t, ticker=sym, eps=eps, rev=rev, source="nasdaq"))

        time.sleep(0.2)

    found.sort(key=lambda x: (x.d, x.t, x.ticker))
    return found, stats


def _write_md(rows: list[Row], master_count: int, start_day: date, end_day: date, run_local: datetime, stats: dict) -> None:
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Earnings – következő 7 nap (MASTER szűrés)")
    lines.append("")
    lines.append(f"Verzió: v0.2.5-earnings-next7d-nasdaq-calendar-master-2026-01-10 | Futás: {run_local:%Y-%m-%d %H:%M} ({TZ_NAME})")
    lines.append(f"Időablak: {start_day.isoformat()} → {end_day.isoformat()} (lokális naptári nap)")
    lines.append(f"Találat: {len(rows)} / {master_count} ticker")
    lines.append("")
    lines.append("## Lefedettség (Nasdaq calendar)")
    lines.append(f"- Napok lekérdezve: {stats.get('days_queried', 0)}")
    lines.append(f"- Sikeres napok: {stats.get('nasdaq_calendar_ok_days', 0)}")
    lines.append(f"- Blokkolt/limit napok (401/403/429): {stats.get('nasdaq_calendar_block_days', 0)}")
    lines.append(f"- Egyéb hiba napok: {stats.get('nasdaq_calendar_other_fail_days', 0)}")
    lines.append("")

    if rows:
        lines.append("| Dátum | Idő | Ticker | EPS forecast | Revenue forecast | Forrás |")
        lines.append("|---|---|---:|---:|---:|---|")
        for r in rows:
            lines.append(f"| {r.d} | {r.t} | {r.ticker} | {r.eps or ''} | {r.rev or ''} | {r.source} |")
    else:
        lines.append("Nincs találat a következő 7 napban a MASTER tickerek között **vagy** a Nasdaq calendar forrás blokkolt/hibás.")

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    master_path = DEFAULT_MASTER_LOCAL
    if "--master" in argv:
        idx = argv.index("--master")
        if idx + 1 < len(argv):
            master_path = Path(argv[idx + 1])

    run_local = _now_local()
    start_day = run_local.date()
    end_day = start_day + timedelta(days=7)

    master_tickers = _read_master_tickers(master_path)
    rows, stats = _build_report(master_tickers, start_day, days=7)
    _write_md(rows, master_count=len(master_tickers), start_day=start_day, end_day=end_day, run_local=run_local, stats=stats)

    print(f"OK: wrote {OUT_MD} (rows={len(rows)} master={len(master_tickers)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
