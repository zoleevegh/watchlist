#!/usr/bin/env python3
"""
validate_run.py – v1.0.0-biblia-guard

CI-ellenőrzés: akkor is bukjon a workflow, ha "látszólag lefut", de a kimenet hibás/hiányos.

Ellenőrzések:
- reports/summary_report_{N}.md létezik és nem üres
- Kötelező blokkok megvannak (#1 esetén)
- Lefedettség sor megvan; opcionálisan bukjon HIÁNYOS esetén
- Job summary blokk megvan (ha a pipeline ezt mindig hozzáírja)
- Alap formátum: sok-soros (nem egyetlen összeolvasott sor)
"""

from __future__ import annotations
import argparse
import sys
import re
from pathlib import Path

REQUIRED_BLOCKS_1 = [
    "## After-hours & Premarket - #1 jelentés",
    "Lefedettség:",
    "### Makró / Politika / FED",
    "### Bejelentések & fel/lemínősítések",
    "### Közeli katalizátorok",
    "### Listán kívüli, 3–12 hónapos high-conviction jelöltek",
]

def die(msg: str, code: int = 1) -> None:
    print(f"[validate_run] ERROR: {msg}")
    sys.exit(code)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, choices=["1","2","3"])
    ap.add_argument("--md", default=None)
    ap.add_argument("--fail-on-missing", action="store_true", help="Bukjon, ha Lefedettség: HIÁNYOS.")
    ap.add_argument("--min-lines", type=int, default=40, help="Minimum sor a végső MD-ben (összelapítás detektálás).")
    args = ap.parse_args()

    report = args.report
    md_path = Path(args.md) if args.md else Path("reports") / f"summary_report_{report}.md"
    if not md_path.is_file():
        die(f"Hiányzik a kimeneti MD: {md_path}")
    txt = md_path.read_text(encoding="utf-8", errors="replace")
    if not txt.strip():
        die(f"Üres a kimeneti MD: {md_path}")

    # basic "lapítás" ellenőrzés
    line_count = len(txt.splitlines())
    if line_count < args.min_lines:
        die(f"Gyanúsan kevés sor ({line_count}) – valószínű össze van lapítva a jelentés. Fájl: {md_path}")

    # coverage
    m = re.search(r"Lefedettség:\s*(TELJES|HIÁNYOS)", txt)
    if not m:
        die("Hiányzik a 'Lefedettség:' sor.")
    if args.fail_on_missing and m.group(1) != "TELJES":
        die("Lefedettség: HIÁNYOS – fail-on-missing aktív.")

    # required blocks for #1
    if report == "1":
        for b in REQUIRED_BLOCKS_1:
            if b not in txt:
                die(f"Hiányzó kötelező blokk/token: {b}")

        # job summary (nem kötelező mindenkinél, de nálatok igen)
        if "Job summary generated at run-time" not in txt:
            die("Hiányzik a Job summary blokk (Job summary generated at run-time).")

    print(f"[validate_run] OK – report #{report} valid. ({md_path}, {line_count} lines)")
    sys.exit(0)

if __name__ == "__main__":
    main()
