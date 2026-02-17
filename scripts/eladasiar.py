#!/usr/bin/env python3
# eladasiar.py
# v1.0.7-snapshot-schema-fix-flags-2026-02-17
# Patch summary_report_1.md watchlist lines with SellRef deltas vs last sell price.
# Snapshot-first: uses reports/price_snapshot_1.json produced by runner; zero Yahoo calls when snapshot exists.

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
from typing import Dict, Optional, Tuple


def _now_budapest() -> dt.datetime:
    # Runner uses CEST/CET logic; we only need local wall clock. GitHub runners run UTC.
    # We'll infer Europe/Budapest by applying fixed offset based on current date (DST-aware not available without tz db).
    # Practical: user runs in PM window; still, we avoid tz dependency.
    # If TZ env is set in workflow, datetime.now() already reflects it.
    return dt.datetime.now()


def detect_session(now: Optional[dt.datetime] = None) -> str:
    """Return 'PM', 'AH', or 'NONE' based on CEST/CET wall clock windows.

    PM: 10:00–15:30
    AH: 22:00–02:00 (cross-midnight)
    """
    now = now or _now_budapest()
    h = now.hour
    m = now.minute
    minutes = h * 60 + m

    pm_start = 10 * 60
    pm_end = 15 * 60 + 30

    ah_start = 22 * 60
    ah_end = 2 * 60

    if pm_start <= minutes <= pm_end:
        return "PM"
    # AH crosses midnight
    if minutes >= ah_start or minutes <= ah_end:
        return "AH"
    return "NONE"


def parse_float(x: str) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    # HU decimal comma
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    # remove currency symbols
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in ("", "-", "."):
        return None
    try:
        return float(s)
    except Exception:
        return None


def load_sell_prices(master_csv_path: str) -> Tuple[Dict[str, float], str, int]:
    """Return {ticker: sell_price}, detected header name, and eligible count of non-empty sell prices."""
    sell_by_ticker: Dict[str, float] = {}
    header_used = ""
    eligible = 0

    with open(master_csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return sell_by_ticker, header_used, 0

    raw_headers = rows[0]
    headers = [h.strip() for h in raw_headers]

    # Find sell column index with flexible matching
    candidates = {
        "eladasi ar",
        "eladási ár",
        "eladasiar",
        "sell",
        "sell_price",
        "last_sell",
    }

    sell_idx = None
    for i, h in enumerate(headers):
        if h.lower().strip() in candidates:
            sell_idx = i
            header_used = raw_headers[i]
            break

    if sell_idx is None:
        # also match substring
        for i, h in enumerate(headers):
            if "eladas" in h.lower() and "ar" in h.lower():
                sell_idx = i
                header_used = raw_headers[i]
                break

    # ticker index
    ticker_idx = None
    for i, h in enumerate(headers):
        if h.lower() in ("ticker", "symbol"):
            ticker_idx = i
            break

    if sell_idx is None or ticker_idx is None:
        return sell_by_ticker, header_used or "", 0

    # rule: last non-empty sell price wins (iterate top->bottom overwriting)
    for r in rows[1:]:
        if ticker_idx >= len(r):
            continue
        t = (r[ticker_idx] or "").strip().upper()
        if not t:
            continue
        val = r[sell_idx] if sell_idx < len(r) else ""
        sp = parse_float(val)
        if sp is None:
            continue
        sell_by_ticker[t] = sp

    eligible = len(sell_by_ticker)
    return sell_by_ticker, (header_used or ""), eligible


def load_snapshot(snapshot_path: str) -> Dict[str, dict]:
    if not os.path.exists(snapshot_path):
        return {}
    try:
        with open(snapshot_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Runner schema (v4.6.10+):
        # {
        #   "generated_at": "...",
        #   "window": {"pm": "10:00-15:30", "ah": "22:00-02:00"},
        #   "prices": {"AAPL": {"pm_price": ..., "ah_price": ..., "prev_close": ...}, ...}
        # }
        # Older / alternative schemas may be a flat map {"AAPL": {...}, ...}.

        if not isinstance(data, dict):
            return {}

        prices = data.get("prices") if isinstance(data.get("prices"), dict) else data

        out: Dict[str, dict] = {}
        for k, v in prices.items():
            if not isinstance(v, dict):
                continue
            out[str(k).upper()] = v
        return out
    except Exception:
        return {}
    return {}


def pick_current_from_snapshot(snap: Dict[str, dict], ticker: str, session: str) -> Optional[float]:
    v = snap.get(ticker)
    if not v:
        return None

    def _get(*keys):
        for kk in keys:
            if kk in v and v[kk] is not None:
                try:
                    return float(v[kk])
                except Exception:
                    pass
        return None

    if session == "PM":
        return _get(
            # runner snapshot
            "pm_price",
            # other possible names
            "pm", "pre", "premarket", "preMarketPrice", "preMarket",
        ) or _get("regular", "regularMarketPrice")
    if session == "AH":
        return _get(
            # runner snapshot
            "ah_price",
            # other possible names
            "ah", "post", "afterhours", "postMarketPrice", "postMarket",
        ) or _get("regular", "regularMarketPrice")
    return None


def compute_delta_pct(current: float, sell: float) -> float:
    return (current / sell - 1.0) * 100.0


def flag_for_delta(delta_pct: float) -> str:
    if delta_pct >= 10.0:
        return " 🟢"
    if delta_pct <= -10.0:
        return " 🔴"
    return ""


def patch_report(report_path: str, sell_by_ticker: Dict[str, float], snapshot: Dict[str, dict], session: str) -> Tuple[int, int, int, str]:
    """Return (patched_count, eligible_lines, price_ok, source)."""
    if session not in ("PM", "AH"):
        return 0, 0, 0, "none"

    with open(report_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    in_watchlist = False
    patched = 0
    eligible_lines = 0
    price_ok = 0
    source = "snapshot" if snapshot else "none"

    out_lines = []
    for line in lines:
        # detect watchlist section
        if line.strip().startswith("## Watchlist"):
            in_watchlist = True
            out_lines.append(line)
            continue
        if line.strip().startswith("## ") and not line.strip().startswith("## Watchlist"):
            in_watchlist = False
            out_lines.append(line)
            continue

        if not in_watchlist:
            out_lines.append(line)
            continue

        # bullet line: "- TICKER — ..."  (note: your report uses en dash and pipes; we'll parse conservatively)
        m = re.match(r"^\s*-\s*([A-Z0-9.\-]+)\b(.*)$", line)
        if not m:
            out_lines.append(line)
            continue

        ticker = m.group(1).upper()
        rest = m.group(2)

        sell = sell_by_ticker.get(ticker)
        if sell is None:
            out_lines.append(line)
            continue

        eligible_lines += 1

        # avoid double patch
        if "SellRef:" in line:
            out_lines.append(line)
            continue

        cur = pick_current_from_snapshot(snapshot, ticker, session) if snapshot else None
        if cur is not None:
            price_ok += 1
            d = compute_delta_pct(cur, sell)
            fflag = flag_for_delta(d)
            # Keep existing line ending
            newline = "" if line.endswith("\n") else "\n"
            # Ensure line has newline
            base = line.rstrip("\n")
            patched_line = f"{base} | SellRef: ${sell:.2f} → Now({session}) ${cur:.2f} ({d:+.2f}%)" + fflag + "\n"
            out_lines.append(patched_line)
            patched += 1
        else:
            # couldn't get current price
            out_lines.append(line)

    if patched > 0:
        with open(report_path, "w", encoding="utf-8") as f:
            f.writelines(out_lines)

    return patched, eligible_lines, price_ok, source


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--report", required=True)
    ap.add_argument("--snapshot", default="reports/price_snapshot_1.json")
    args = ap.parse_args()

    session = detect_session()

    sell_by_ticker, header_used, sell_prices_n = load_sell_prices(args.master)
    snapshot = load_snapshot(args.snapshot)

    patched, eligible_lines, price_ok, source = patch_report(args.report, sell_by_ticker, snapshot, session)

    # Always log one line for GitHub Actions
    print(
        f"SellRef: patched={patched} eligible={eligible_lines} session={session} "
        f"sell_prices={sell_prices_n} header='{header_used}' price_ok={price_ok} source={source}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
