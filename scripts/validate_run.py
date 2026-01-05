#!/usr/bin/env python3
# validate_run.py — v4.1.2-price-engine-validate-min-2026-01-05
#
# Cél: legyen kőkeményen egyszerű és stabil.
# Ellenőrzések:
#  1) report fájl létezik és nem üres
#  2) van legalább 1 adat sor: "- TICKER — ..."
#  3) nem ALL N/A (minden adat sor "PM n/a | AH n/a")
#
# Exit code:
#  0 = OK
#  2 = hiányzik report
#  3 = üres report
#  4 = nincs adat sor
#  5 = all n/a (Yahoo/forrás blokk)
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

from __future__ import annotations

import argparse
import os
import sys


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

    if not txt.strip():
        print("ERROR: üres report.", file=sys.stderr)
        return 3

    lines = txt.splitlines()
    data_lines = [ln for ln in lines if ln.startswith("- ")]

    if not data_lines:
        print("ERROR: nincs adat sor ('- TICKER — ...').", file=sys.stderr)
        return 4

    na_lines = [ln for ln in data_lines if "PM n/a" in ln and "AH n/a" in ln]
    if len(na_lines) == len(data_lines):
        print("ERROR: minden ticker n/a (forráshiba / Yahoo blokk valószínű).", file=sys.stderr)
        return 5

    print("OK: validate_run passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
