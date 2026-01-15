#!/usr/bin/env python3
# analyst.py — v0.2.0-finnhub-ubdown-debug-2026-01-15
#
# Cél: 2 naptári napon belüli fel-/leminősítések + célár (PT) változások Finnhub-ból.
# Forrás: Finnhub Upgrade/Downgrade endpoint (API key szükséges).
#
# Debug:
#   - --debug: reports/debug_analyst/ alá menti a nyers Finnhub válaszokat ticker-enként
#   - valamint reports/analyst_debug.log-ba is logol (UTC időbélyeggel)
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

VERSION = "v0.2.1-finnhub-ubdown-2026-01-15"


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def _log(msg: str, log_path: Optional[str]) -> None:
    ts = _utc_now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts} UTC] {msg}"
    print(line)
    if log_path:
        try:
            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass


def _http_get(url: str, timeout: int, debug: bool, debug_dir: str, save_as: str, log_path: Optional[str]) -> Tuple[int, Dict[str, str], str]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (watchlist; analyst feed)",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status = getattr(r, "status", 200)
            headers = {k.lower(): v for k, v in dict(r.headers).items()}
            body = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        _log(f"HTTP ERROR {type(e).__name__}: {e} | url={url}", log_path)
        return 0, {}, ""

    if debug:
        _log(f"HTTP {status} bytes={len(body)} url={url}", log_path)
        for hk in ["x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset", "retry-after"]:
            if hk in headers:
                _log(f"HEADER {hk}: {headers[hk]}", log_path)
        try:
            _ensure_dir(debug_dir)
            raw_path = Path(debug_dir) / f"{save_as}.json"
            raw_path.write_text(body[:200_000], encoding="utf-8")
        except Exception:
            pass

    return status, headers, body


def _read_master_tickers(master_csv: str) -> List[str]:
    tickers: List[str] = []
    with open(master_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return tickers

    header = [c.strip().lower() for c in rows[0]]
    ticker_idx = None
    for i, c in enumerate(header):
        if c in ("ticker", "symbol"):
            ticker_idx = i
            break

    start_row = 1 if ticker_idx is not None else 0
    if ticker_idx is None:
        ticker_idx = 0

    for r in rows[start_row:]:
        if not r or ticker_idx >= len(r):
            continue
        t = r[ticker_idx].strip().upper()
        if not t or t.startswith("#"):
            continue
        # minimál sanity
        if any(ch.isspace() for ch in t):
            t = t.split()[0]
        tickers.append(t)

    # dedup preserving order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _fetch_finnhub_updown(ticker: str, api_key: str, timeout: int, debug: bool, debug_dir: str, log_path: Optional[str]) -> List[Dict[str, Any]]:
    qs = {
        "symbol": ticker,
        "token": api_key,
    }
    url = "https://finnhub.io/api/v1/stock/upgrade-downgrade?" + urllib.parse.urlencode(qs)
    status, headers, body = _http_get(url, timeout=timeout, debug=debug, debug_dir=debug_dir, save_as=f"finnhub_{ticker}", log_path=log_path)
    if status == 0:
        return []
    if status == 401 or status == 403:
        _log(f"AUTH: Finnhub {status} — valószínű rossz/hiányzó API key. ticker={ticker}", log_path)
        return []
    if status == 429:
        _log(f"RATE_LIMIT: Finnhub 429 — limit. ticker={ticker}", log_path)
        return []
    try:
        data = json.loads(body) if body else None
    except Exception as e:
        _log(f"JSON parse error ticker={ticker}: {e}", log_path)
        return []

    if isinstance(data, dict) and "error" in data:
        _log(f"Finnhub error ticker={ticker}: {data.get('error')}", log_path)
        return []

    if not isinstance(data, list):
        _log(f"Unexpected Finnhub response type ticker={ticker}: {type(data).__name__}", log_path)
        return []

    return data


def _ts_to_utc(ts: Any) -> Optional[dt.datetime]:
    try:
        ts_i = int(ts)
        return dt.datetime.fromtimestamp(ts_i, tz=dt.timezone.utc)
    except Exception:
        return None


def _within_last_days_utc(event_dt: Optional[dt.datetime], days: int, now_utc: dt.datetime) -> bool:
    if event_dt is None:
        return False
    # naptári nap: most UTC dátum - days (inclusive)
    start_date = (now_utc.date() - dt.timedelta(days=days))
    return event_dt.date() >= start_date


def _fmt_pt(old_pt: Any, new_pt: Any) -> str:
    def _num(x: Any) -> Optional[float]:
        try:
            if x is None or x == "":
                return None
            return float(x)
        except Exception:
            return None
    o = _num(old_pt)
    n = _num(new_pt)
    if o is None and n is None:
        return ""
    if o is None and n is not None:
        return f"PT: n/a → {n:g}"
    if o is not None and n is None:
        return f"PT: {o:g} → n/a"
    assert o is not None and n is not None
    delta = (n - o) / o * 100.0 if o != 0 else None
    if delta is None:
        return f"PT: {o:g} → {n:g}"
    sign = "+" if delta >= 0 else ""
    return f"PT: {o:g} → {n:g} ({sign}{delta:.1f}%)"


def _render_md(events: List[Dict[str, Any]], days: int, generated_utc: dt.datetime) -> str:
    out: List[str] = []
    out.append(f"# Analyst feed (upgrade/downgrade + PT) — last {days} calendar days")
    out.append("")
    out.append(f"Verzió: {VERSION}")
    out.append(f"Generálva (UTC): {generated_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    out.append("")

    if not events:
        out.append(f"_Nincs friss (≤{days} naptári nap) fel/leminősítés vagy célár-frissítés a forrásban._")
        out.append("")
        return "\n".join(out)

    # idő szerint desc
    events_sorted = sorted(events, key=lambda x: x.get("_time_ts", 0), reverse=True)

    out.append("## Talált események")
    out.append("")
    for e in events_sorted:
        t = e.get("symbol", "")
        when = e.get("_time_str", "")
        action = (e.get("action") or "").strip()
        analyst = (e.get("analyst") or "").strip()
        frm = (e.get("fromGrade") or "").strip()
        to = (e.get("toGrade") or "").strip()
        pt_old = e.get("ptOld")
        pt_new = e.get("ptNew")
        pt_txt = _fmt_pt(pt_old, pt_new)
        # egy sor, tömör
        parts = [f"- **{t}** — {when} — {action}"]
        if frm or to:
            parts.append(f"({frm} → {to})")
        if pt_txt:
            parts.append(f"— {pt_txt}")
        if analyst:
            parts.append(f"— {analyst}")
        out.append(" ".join([p for p in parts if p]))
    out.append("")
    return "\n".join(out)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyst feed (upgrade/downgrade + target price) – last N calendar days")
    p.add_argument("--master", required=True, help="MASTER CSV (ticker column)")
    p.add_argument("--days", type=int, default=2, help="Hány naptári napot nézzünk vissza (default: 2)")

    # Outputok:
    # - --out-md: markdown riport (fő output)
    # - --out-json: nyers események JSON-ban (opcionális)
    # - --out: legacy alias az --out-md-re (ne töröld, workflow-k miatt)
    p.add_argument("--out-md", dest="out_md", default=None, help="Markdown output (pl. reports/analyst_last2d.md)")
    p.add_argument("--out-json", dest="out_json", default=None, help="JSON output (pl. reports/analyst_last2d.json)")
    p.add_argument("--out", default=None, help="LEGACY alias: ugyanaz, mint --out-md")

    p.add_argument("--timeout", type=float, default=12.0, help="HTTP timeout (sec)")
    p.add_argument("--debug", action="store_true", help="Debug log runner.log-ba")
    p.add_argument("--debug-dir", default=None, help="Debug dump mappa (requests/resp)")
    p.add_argument("--selftest", action="store_true", help="Selftest – hívás + parsing sanity (nem kötelező)")
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    # Output path resolution (backward compatible)
    out_md = args.out_md or args.out or f"reports/analyst_last{args.days}d.md"
    out_json = args.out_json or None

    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    log_path = "reports/analyst_debug.log" if args.debug else None

    if not api_key:
        _log("ERROR: FINNHUB_API_KEY nincs beállítva.", log_path)
        Path(out_md).write_text(
            f"# Analyst feed (upgrade/downgrade + PT) — last {args.days} calendar days\n\nVerzió: {VERSION}\n\n_Nincs adat: FINNHUB_API_KEY hiányzik._\n",
            encoding="utf-8",
        )
        return 2

    now_utc = _utc_now()
    _log(f"START {VERSION} days={args.days} master={args.master} out={out_md} debug={args.debug}", log_path)
    _log(f"API key present: yes (len={len(api_key)})", log_path)

    tickers: List[str]
    if args.selftest:
        tickers = [args.selftest.strip().upper()]
    else:
        try:
            tickers = _read_master_tickers(args.master)
        except Exception as e:
            _log(f"ERROR: MASTER olvasás sikertelen: {e}", log_path)
            return 3

    if not tickers:
        _log("WARNING: nincs ticker a MASTER-ben.", log_path)
        Path(out_md).write_text(_render_md([], args.days, now_utc), encoding="utf-8")
        return 0

    events_out: List[Dict[str, Any]] = []
    for i, t in enumerate(tickers, start=1):
        data = _fetch_finnhub_updown(t, api_key=api_key, timeout=args.timeout, debug=args.debug, debug_dir=args.debug_dir, log_path=log_path)
        if args.debug:
            _log(f"TICKER {i}/{len(tickers)} {t}: raw_items={len(data)}", log_path)

        for item in data:
            # Finnhub mezők: time (unix), action, fromGrade, toGrade, ptOld, ptNew, analyst, symbol
            ev_dt = _ts_to_utc(item.get("time"))
            if not _within_last_days_utc(ev_dt, args.days, now_utc):
                continue

            # csak akkor vesszük fel, ha van action (upgrade/downgrade/maintain) vagy PT változás
            has_action = bool((item.get("action") or "").strip())
            has_pt = item.get("ptOld") not in (None, "", 0) or item.get("ptNew") not in (None, "", 0)
            if not (has_action or has_pt):
                continue

            out_item = dict(item)
            out_item["symbol"] = out_item.get("symbol") or t
            out_item["_time_ts"] = int(item.get("time") or 0)
            out_item["_time_str"] = ev_dt.strftime("%Y-%m-%d") if ev_dt else "n/a"
            events_out.append(out_item)

        # óvatos throttle, hogy ne üsd agyon a finnhub-ot
        time.sleep(0.12)

    md = _render_md(events_out, args.days, now_utc)
    Path(out_md).parent.mkdir(parents=True, exist_ok=True)
    Path(out_md).write_text(md, encoding="utf-8")

    if out_json:
        payload = {
            "version": VERSION,
            "generated_utc": now_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "days": args.days,
            "events": events_out,
        }
        Path(out_json).parent.mkdir(parents=True, exist_ok=True)
        Path(out_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.selftest:
        _log(f"SELFTEST done: events_in_window={len(events_out)} (lásd: {out_md})", log_path)

    _log(f"DONE events={len(events_out)} tickers={len(tickers)}", log_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
