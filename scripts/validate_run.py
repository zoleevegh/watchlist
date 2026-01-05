#!/usr/bin/env python3
# validate_run.py — v4.0.3-price-engine-sort-premarket-noszuro-2026-01-05

from __future__ import annotations

import argparse
import os
import sys

REQUIRED_TOKENS = [
    "Lefedettség:",
    "Verzió:",
    "Run:",
    "Top Premarket mozgások",
    "Top After-hours mozgások",
    "📊 Darabszámos tickerek",
    "👀 Watchlist",
    "Job summary generated at run-time",
]

MIN_LINES = 40


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1, choices=[1])
    ap.add_argument("--path", type=str, default="reports/summary_report_1.md")
    args = ap.parse_args()

    if not os.path.exists(args.path):
        print(f"ERROR: hiányzik a report: {args.path}", file=sys.stderr)
        return 2

    with open(args.path, "r", encoding="utf-8") as f:
        txt = f.read()

    lines = txt.splitlines()
    if len(lines) < MIN_LINES:
        print(f"ERROR: gyanúsan kevés sor ({len(lines)}). Valószínű a tördelés sérült.", file=sys.stderr)
        return 3

    missing = [t for t in REQUIRED_TOKENS if t not in txt]
    if missing:
        print("ERROR: hiányzó kötelező token(ek): " + ", ".join(missing), file=sys.stderr)
        return 4

    if "Lefedettség: HIÁNYOS" in txt:
        print("WARN: Lefedettség HIÁNYOS (árfeed/forrás hiba).", file=sys.stderr)

    print("OK: validate_run (price-engine) passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
