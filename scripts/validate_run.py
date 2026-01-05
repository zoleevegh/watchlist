#!/usr/bin/env python3
# validate_run.py — v4.1.0-price-engine-quotev7-fallback-2026-01-05
# Fail if the report is "all n/a" (inkább piros run, mint kamu zöld).

from __future__ import annotations

import argparse
import os
import sys


REQUIRED_TOKENS = [
    "Verzió:",
    "Lefedettség:",
    "Forrás-statisztika:",
    "## Lista",
    "Job summary generated at run-time",
]


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

    missing = [t for t in REQUIRED_TOKENS if t not in txt]
    if missing:
        print("ERROR: hiányzó token(ek): " + ", ".join(missing), file=sys.stderr)
        return 4

    lines = txt.splitlines()
    data_lines = [ln for ln in lines if ln.startswith("- ")]
    if data_lines:
        na_lines = [ln for ln in data_lines if "PM n/a" in ln and "AH n/a" in ln]
        if len(na_lines) == len(data_lines):
            print("ERROR: minden ticker n/a (Yahoo blokkolás / forráshiba valószínű).", file=sys.stderr)
            return 5

    print("OK: validate_run passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
