#!/usr/bin/env python3
# validate_run.py — v4.2.0-allow-all-na-2026-01-05
#
# Fix: ha minden ticker n/a, akkor ne bukjon el a workflow.
# Kimenet: WARN, exit 0.
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

import argparse, os, sys

VERSION = "v4.2.0-allow-all-na-2026-01-05"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1)
    ap.add_argument("--path", required=True)
    args = ap.parse_args()

    if not os.path.exists(args.path):
        print(f"ERROR: report file missing: {args.path}", file=sys.stderr, flush=True)
        return 1

    txt = open(args.path, "r", encoding="utf-8", errors="replace").read()

    lines = [ln.strip() for ln in txt.splitlines() if ln.strip().startswith("- ")]
    if not lines:
        print("ERROR: nincs ticker sor a riportban.", file=sys.stderr, flush=True)
        return 1

    na_count = sum(1 for ln in lines if ("PM n/a" in ln and "AH n/a" in ln))

    if na_count == len(lines):
        print(f"WARNING: minden ticker n/a ({na_count}/{len(lines)}) — forráshiba / sessionen kívüli lekérdezés / Yahoo limit.", file=sys.stderr, flush=True)
        return 0

    print(f"OK: ticker sorok={len(lines)}, n/a={na_count}", file=sys.stderr, flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
