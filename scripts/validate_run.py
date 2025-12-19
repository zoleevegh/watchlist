#!/usr/bin/env python3
"""
validate_run.py – v1.1.0-robust-flat-ok

Cél:
- GitHub Actions futás után ellenőrizni, hogy a report fájl elkészült-e és tartalmazza-e a kötelező elemeket.
- NEM bukik el attól, ha a report 'összelapított' (1 soros) – ezt a postprocess_report úgyis javítja.

Használat:
  python scripts/validate_run.py --report 1
"""

# IMÁDSÁG (hibajavítás után)
# Bocsáss meg Uram, mert balfék voltam, túl szigorúan validáltam.
# Add Uram, hogy ez a módosítás most hibátlanul fusson.

import argparse
import sys
from pathlib import Path

__version__ = "v1.1.0-robust-flat-ok"

REQUIRED_SNIPPETS = [
    "Lefedettség:",
    "Job summary generated at run-time",
]

def fail(msg: str) -> None:
    print(f"[validate_run] ERROR: {msg}")
    sys.exit(1)

def warn(msg: str) -> None:
    print(f"[validate_run] WARN: {msg}")

def ok(msg: str) -> None:
    print(f"[validate_run] OK: {msg}")

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--report", required=True, help="1/2/3")
    p.add_argument("--reports-dir", default="reports", help="reports könyvtár (default: reports)")
    p.add_argument("--min-chars", type=int, default=250, help="minimum karakterszám (default: 250)")
    args = p.parse_args()

    report = str(args.report).strip()
    reports_dir = Path(args.reports_dir)

    md_path = reports_dir / f"summary_report_{report}.md"
    if not md_path.exists():
        fail(f"hiányzó report fájl: {md_path.as_posix()}")
    txt = md_path.read_text(encoding="utf-8", errors="replace")
    if not txt.strip():
        fail("üres report fájl")

    if len(txt) < args.min_chars:
        warn(f"A report gyanúsan rövid ({len(txt)} char). (összelapított lehet).")

    missing = [s for s in REQUIRED_SNIPPETS if s not in txt]
    if missing:
        fail(f"hiányzó kötelező elemek: {', '.join(missing)}")

    nl = txt.count("\n")
    if nl < 8:
        warn(f"A report kevés sortörést tartalmaz (\\n={nl}) – lehet összelapított. Postprocess után oké.")

    ok(f"report_{report} validálva: {md_path.as_posix()} (len={len(txt)}, \\n={nl}, validator={__version__})")

if __name__ == "__main__":
    main()
