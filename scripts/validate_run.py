#!/usr/bin/env python3
"""
validate_run.py – v1.0.1-biblia-guard

CI-ellenőrzés: bukjon a workflow, ha a kimenet hibás/hiányos szerkezetileg (blokkok, lapítás, üres fájl).
FONTOS: "Lefedettség: HIÁNYOS" NEM build-breaker alapból (csak figyelmeztetés),
mert ez adatforrás/market-data oldalról gyakran előfordul.

Ellenőrzések:
- reports/summary_report_{N}.md létezik és nem üres
- Kötelező blokkok megvannak (#1 esetén)
- Lefedettség sor megvan (TELJES vagy HIÁNYOS)
- Job summary blokk megvan (#1 esetén, nálatok kötelező)
- Alap formátum: sok-soros (nem egyetlen összeolvasott sor)

Opció:
--fail-on-coverage-missing : csak akkor bukjon HIÁNYOS lefedettségre (ha tényleg ezt akarod).
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

def warn(msg: str) -> None:
    print(f"[validate_run] WARN: {msg}")

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, choices=["1", "2", "3"])
    ap.add_argument("--md", default=None)
    ap.add_argument("--fail-on-coverage-missing", action="store_true",
                    help="Bukjon, ha Lefedettség: HIÁNYOS (alapból csak WARN).")
    ap.add_argument("--min-lines", type=int, default=40,
                    help="Minimum sor a végső MD-ben (összelapítás detektálás).")
    args = ap.parse_args()

    report = args.report
    md_path = Path(args.md) if args.md else Path("reports") / f"summary_report_{report}.md"
    if not md_path.is_file():
        die(f"Hiányzik a kimeneti MD: {md_path}")

    txt = md_path.read_text(encoding="utf-8", errors="replace")
    if not txt.strip():
        die(f"Üres a kimeneti MD: {md_path}")

    line_count = len(txt.splitlines())
    if line_count < args.min_lines:
        die(f"Gyanúsan kevés sor ({line_count}) – valószínű össze van lapítva a jelentés. Fájl: {md_path}")

    m = re.search(r"Lefedettség:\s*(TELJES|HIÁNYOS)", txt)
    if not m:
        die("Hiányzik a 'Lefedettség:' sor.")

    coverage = m.group(1)
    if coverage != "TELJES":
        warn("Lefedettség: HIÁNYOS – adatforrás/market-data oldali hiány lehet.")
        if args.fail_on_coverage_missing:
            die("Lefedettség: HIÁNYOS – fail-on-coverage-missing aktív.")

    if report == "1":
        for b in REQUIRED_BLOCKS_1:
            if b not in txt:
                die(f"Hiányzó kötelező blokk/token: {b}")
        if "Job summary generated at run-time" not in txt:
            die("Hiányzik a Job summary blokk (Job summary generated at run-time).")

    print(f"[validate_run] OK – report #{report} valid. ({md_path}, {line_count} lines, coverage={coverage})")
    sys.exit(0)

if __name__ == "__main__":
    main()
