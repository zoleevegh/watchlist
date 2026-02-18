#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eladasiar.py — SellRef (Last Sell Price) delta patcher for summary_report_1.md

Versioning rule: ALWAYS increment version on every modification, without gaps.
VERSION: v1.0.7-snapshot-first-flag-2026-02-18

Behavior:
- Reads MASTER CSV (reports/master.csv) and extracts "Eladasi ar" (handles trailing spaces & HU decimal comma).
- Reads runner-produced price snapshot (reports/price_snapshot_1.json) and uses it as the *primary* price source.
- Determines session:
    PM: 10:00–15:30 (Europe/Budapest)
    AH: 22:00–10:00 (includes 02:00–10:00 "between AH and PM" as AH per user rule)
- Patches Watchlist lines in the report: appends
    " | SellRef: $X.XX → Now(PM/AH) $Y.YY (+/-Z.ZZ%) [🟢/🔴]"
  Flags:
    🟢 if Δ% >= +10.00
    🔴 if Δ% <= -10.00
- Adds SellRef version into report header line:
    "Verzió: ... | Futás ideje: ... | SellRef: v1.0.7..."
- Prints a single summary line to stdout for GitHub Actions logs.

Exit code: 0 (never fails the workflow).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, time
from typing import Dict, Optional, Tuple, Any

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

VERSION = "v1.0.7-snapshot-first-flag-2026-02-18"
TZ_NAME = "Europe/Budapest"


def _tznow() -> datetime:
    if ZoneInfo is None:
        return datetime.now()
    return datetime.now(ZoneInfo(TZ_NAME))


def _parse_float_maybe(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    # HU decimal comma -> dot
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    # keep only valid float characters
    m = re.match(r"^-?\d+(\.\d+)?$", s)
    if not m:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _read_sell_prices(master_csv: str) -> Tuple[Dict[str, float], str]:
    """
    Returns (sell_price_by_ticker, detected_header_name).
    Duplicate tickers: last non-empty sell price wins (later row overrides).
    """
    sell: Dict[str, float] = {}
    detected_header = ""

    with open(master_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return sell, detected_header

    raw_headers = rows[0]
    # Strip headers to avoid trailing spaces (e.g., "Eladasi ar ")
    headers = [h.strip() for h in raw_headers]

    # Map stripped header -> original index
    idx_map = {h.strip(): i for i, h in enumerate(raw_headers)}

    # Candidate column names for sell price
    candidates = [
        "Eladasi ar",
        "Eladási ár",
        "Eladasi_ar",
        "SellPrice",
        "Sell Price",
        "Eladasiar",
    ]

    sell_col = None
    for c in candidates:
        if c in idx_map:
            sell_col = idx_map[c]
            detected_header = raw_headers[sell_col]
            break

    # Ticker column
    ticker_col = None
    for tname in ["Ticker", "ticker", "TICKER"]:
        if tname in idx_map:
            ticker_col = idx_map[tname]
            break

    if sell_col is None or ticker_col is None:
        return sell, detected_header

    for r in rows[1:]:
        if ticker_col >= len(r):
            continue
        t = (r[ticker_col] or "").strip().upper()
        if not t:
            continue
        sp = None
        if sell_col < len(r):
            sp = _parse_float_maybe(r[sell_col])
        if sp is None:
            continue
        sell[t] = sp

    return sell, detected_header


@dataclass
class PricePack:
    prev_close: Optional[float] = None
    pm_price: Optional[float] = None
    ah_price: Optional[float] = None


def _read_snapshot(snapshot_path: str) -> Dict[str, PricePack]:
    """
    Accepts:
    - {"prices": {"AAPL": {"pm_price":..., "ah_price":..., "prev_close":...}, ...}, ...}
    - or flat {"AAPL": {...}, ...} (older experiments)
    """
    if not os.path.isfile(snapshot_path):
        return {}

    with open(snapshot_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    prices_obj: Any
    if isinstance(data, dict) and "prices" in data and isinstance(data["prices"], dict):
        prices_obj = data["prices"]
    elif isinstance(data, dict):
        prices_obj = data
    else:
        return {}

    out: Dict[str, PricePack] = {}
    for k, v in prices_obj.items():
        if not isinstance(k, str):
            continue
        t = k.strip().upper()
        if not t:
            continue
        if not isinstance(v, dict):
            continue

        # accept multiple key variants
        prev_close = _parse_float_maybe(v.get("prev_close")) if "prev_close" in v else _parse_float_maybe(v.get("prevClose") or v.get("previousClose") or "")
        pm_price = _parse_float_maybe(v.get("pm_price")) if "pm_price" in v else _parse_float_maybe(v.get("preMarket") or v.get("premarket") or v.get("pm") or "")
        ah_price = _parse_float_maybe(v.get("ah_price")) if "ah_price" in v else _parse_float_maybe(v.get("postMarket") or v.get("afterHours") or v.get("ah") or "")

        out[t] = PricePack(prev_close=prev_close, pm_price=pm_price, ah_price=ah_price)

    return out


def _detect_session(now: datetime) -> str:
    """
    PM: 10:00–15:30
    AH: 22:00–10:00 (wraps midnight; includes 02:00–10:00)
    Else (RTH): "RTH" (we will still compute using prev_close if needed, but user asked AH/PM only)
    """
    lt = now.timetz() if hasattr(now, "timetz") else now.time()
    # comparisons with tz-aware time objects can be tricky; compare hour/minute integers
    hm = (now.hour, now.minute)

    # PM 10:00-15:30
    if (hm >= (10, 0)) and (hm <= (15, 30)):
        return "PM"
    # AH 22:00-10:00 (wrap)
    if (hm >= (22, 0)) or (hm < (10, 0)):
        return "AH"
    return "RTH"


def _choose_now_price(pack: PricePack, session: str) -> Optional[float]:
    if session == "PM":
        return pack.pm_price or pack.prev_close
    if session == "AH":
        return pack.ah_price or pack.prev_close
    # RTH: we don't have RTH now in snapshot; fall back to prev_close
    return pack.prev_close


def _flag(delta_pct: float) -> str:
    if delta_pct >= 10.0:
        return " 🟢"
    if delta_pct <= -10.0:
        return " 🔴"
    return ""


def _format_sellref(sell: float, now_price: float, session: str) -> str:
    delta_pct = (now_price / sell - 1.0) * 100.0
    return f"SellRef: ${sell:.2f} \u2192 Now({session}) ${now_price:.2f} ({delta_pct:+.2f}%){_flag(delta_pct)}"


def _patch_report(report_path: str,
                  sell_prices: Dict[str, float],
                  snapshot: Dict[str, PricePack],
                  session: str) -> Tuple[int, int, int]:
    """
    Returns (patched_count, eligible_count, price_ok_count)
    eligible_count: tickers that have sell price and appear in report watchlist lines
    price_ok_count: eligible with a usable now_price
    """
    if not os.path.isfile(report_path):
        return 0, 0, 0

    with open(report_path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    # Add SellRef version into header (first "Verzió:" line)
    patched_header = False
    for i, line in enumerate(lines[:20]):
        if line.startswith("Verzió:"):
            if "| SellRef:" not in line:
                lines[i] = f"{line} | SellRef: {VERSION}"
                patched_header = True
            else:
                # update existing SellRef version
                lines[i] = re.sub(r"\|\s*SellRef:\s*[^|]+", f"| SellRef: {VERSION}", line)
                patched_header = True
            break

    in_watchlist = False
    patched = 0
    eligible = 0
    price_ok = 0

    # Regex: "- TICKER — ..." (ticker at start after "- ")
    ticker_re = re.compile(r"^\s*-\s*([A-Z0-9\.\-]+)\b")

    for idx in range(len(lines)):
        line = lines[idx]

        if line.strip().startswith("## Watchlist"):
            in_watchlist = True
            continue
        if line.strip().startswith("## ") and in_watchlist and not line.strip().startswith("## Watchlist"):
            # next section
            in_watchlist = False

        if not in_watchlist:
            continue

        m = ticker_re.match(line)
        if not m:
            continue
        t = m.group(1).strip().upper()
        if t not in sell_prices:
            continue

        eligible += 1

        pack = snapshot.get(t, PricePack())
        now_price = _choose_now_price(pack, session)
        if now_price is None:
            continue
        price_ok += 1

        # Remove any previous SellRef fragment (idempotent)
        line_clean = re.sub(r"\s*\|\s*SellRef:.*$", "", line).rstrip()
        sell = sell_prices[t]
        sellref = _format_sellref(sell, now_price, session)
        lines[idx] = f"{line_clean} | {sellref}"
        patched += 1

    # Only write file if any changes (including header)
    if patched > 0 or patched_header:
        with open(report_path, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(lines) + "\n")

    return patched, eligible, price_ok


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--master", required=True, help="Path to master CSV (e.g., reports/master.csv)")
    ap.add_argument("--report", required=True, help="Path to report markdown (e.g., reports/summary_report_1.md)")
    ap.add_argument("--snapshot", default="reports/price_snapshot_1.json", help="Path to price snapshot JSON")
    ap.add_argument("--version", action="store_true", help="Print version and exit")
    args = ap.parse_args()

    if args.version:
        print(VERSION)
        return 0

    now = _tznow()
    session = _detect_session(now)

    # Read sell prices
    sell_prices, header_name = _read_sell_prices(args.master)

    # Snapshot-first
    snapshot = _read_snapshot(args.snapshot)
    source = "snapshot" if snapshot else "none"

    patched, eligible, price_ok = _patch_report(
        report_path=args.report,
        sell_prices=sell_prices,
        snapshot=snapshot,
        session=session,
    )

    # Always one summary log line for Actions
    # Example: SellRef: version=v1... patched=12 eligible=28 session=AH sell_prices=30 header='Eladasi ar ' price_ok=12 source=snapshot
    print(
        f"SellRef: version={VERSION} patched={patched} eligible={eligible} session={session} "
        f"sell_prices={len(sell_prices)} header='{header_name}' price_ok={price_ok} source={source} now={now.strftime('%Y-%m-%d %H:%M %Z')}"
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:
        # Never fail workflow
        print(f"SellRef: ERROR {e.__class__.__name__}: {e}")
        raise SystemExit(0)
