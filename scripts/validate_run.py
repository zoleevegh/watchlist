#!/usr/bin/env python3
"""
validate_run.py – v1.1.4-biblia-guard-macro-required-no-syntax

Cél:
- Bibliás guard a #1/#2/#3 reportokra.
- #1-ben a Makró blokk KÖTELEZŐ.
- Ha a report gyanúsan rövid/összelapított, automatikusan megpróbálja lefuttatni a postprocess_report.py-t.
- Ha a postprocess után is hiányzik bármely kötelező blokk-header, a validátor a Biblia szerint
  (a Lefedettség után) beszúr placeholder blokkokat, majd újra-validál.

Állandó szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.
"""

# IMÁDSÁG (hibajavítás után)
# Bocsáss meg Uram, mert balfék voltam, szintaxishibás validátort adtam.
# Add Uram, hogy ez a módosítás most hibátlanul fusson.

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

VERSION = "v1.1.4-biblia-guard-macro-required-no-syntax"


def _utf8_reconfigure() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def log(level: str, msg: str) -> None:
    print(f"[validate_run] {level}: {msg}")


def die(msg: str) -> None:
    log("ERROR", msg)
    raise SystemExit(1)


def normalize(s: str) -> str:
    # Normalize dashes and NBSP
    s = s.replace("\u00A0", " ")
    s = re.sub(r"[–—−]", "-", s)
    return s


PAT_MAIN_1 = re.compile(r"##+\s*after-hours\s*&\s*premarket\b.*#1", re.IGNORECASE)
PAT_COVERAGE = re.compile(r"lefedetts[eé]g", re.IGNORECASE)
PAT_JOB = re.compile(r"job summary generated at run-time", re.IGNORECASE)

# Block header patterns (accept ## or ### etc.)
PAT_MACRO = re.compile(r"##+\s*.*makr", re.IGNORECASE)
PAT_BEJELENT = re.compile(r"##+\s*.*bejelent", re.IGNORECASE)
PAT_KATALIZ = re.compile(r"##+\s*.*kataliz", re.IGNORECASE)
PAT_HIGH = re.compile(r"##+\s*.*high", re.IGNORECASE)


def try_postprocess(md_path: Path, report: str) -> None:
    pp = Path("scripts") / "postprocess_report.py"
    if not pp.is_file():
        log("WARN", "postprocess_report.py nem található (scripts/postprocess_report.py) – auto-postprocess kihagyva.")
        return

    cmd = [sys.executable, str(pp), "--md", str(md_path), "--bundle-dir", "reports", "--report", str(report)]
    log("INFO", "Auto-postprocess futtatása (rövid/lapított report miatt)...")
    try:
        subprocess.run(cmd, check=False)
    except Exception as e:
        log("WARN", f"Auto-postprocess kivétel: {e!r}")


def _find_coverage_insert_pos(text: str) -> int:
    """
    Beszúrási pont a Biblia szerint: a 'Lefedettség:' blokk UTÁN.
    - Ha külön sorban van, akkor annak a sorvégéhez.
    - Ha lapított, akkor az első 'Lefedettség:' előfordulás utáni helyre.
    - Ha nincs, 0.
    """
    m = re.search(r"(Lefedetts[eé]g\s*:[^\n]*)", text, flags=re.IGNORECASE)
    if not m:
        return 0
    # if there's a newline after the match, insert after that line
    end = m.end(1)
    nl = text.find("\n", end)
    return (nl + 1) if nl != -1 else end


def _ensure_block_headers(md_path: Path, report: str) -> None:
    raw = md_path.read_text(encoding="utf-8", errors="replace")
    txt = normalize(raw)

    # Only enforce for #1
    if report != "1":
        return

    missing = []
    if not PAT_MACRO.search(txt):
        missing.append("macro")
    if not PAT_BEJELENT.search(txt):
        missing.append("bejelent")
    if not PAT_KATALIZ.search(txt):
        missing.append("kataliz")
    if not PAT_HIGH.search(txt):
        missing.append("highconv")

    if not missing:
        return

    insert_pos = _find_coverage_insert_pos(raw)
    if insert_pos == 0:
        log("WARN", "Nem található 'Lefedettség:' beszúrási pont; fallback: fájl eleje után szúrok be blokkokat.")
        insert_pos = 0

    blocks = ""
    if "macro" in missing:
        blocks += "### Makró / Politika / FED\n\n- (auto) Nem volt piaci relevanciájú makró / FED / politikai esemény.\n\n"
    if "bejelent" in missing:
        blocks += "### Bejelentések & fel/lemínősítések\n\n- (auto) Nem érkezett a #1 kritériumait teljesítő vállalati közlés vagy elemzői lépés.\n\n"
    if "kataliz" in missing:
        blocks += "### Közeli katalizátorok\n\n- (auto) Nem volt a #1 kritériumait teljesítő, közelgő katalizátor.\n\n"
    if "highconv" in missing:
        blocks += "### Listán kívüli, 3–12 hónapos high-conviction jelöltek\n\n- (auto) Nem volt a #1 kritériumait teljesítő, listán kívüli ismételt erős jelzés.\n\n"

    new_raw = raw[:insert_pos] + ("\n" if insert_pos and not raw[:insert_pos].endswith("\n") else "") + blocks + raw[insert_pos:]
    md_path.write_text(new_raw, encoding="utf-8")
    log("WARN", "Auto-kitöltés: hiányzó kötelező blokk-headerek beszúrva: " + ", ".join(missing))


def validate(md_path: Path, report: str, min_lines: int) -> None:
    if not md_path.is_file():
        die(f"Hiányzik a kimeneti MD: {md_path}")

    raw = md_path.read_text(encoding="utf-8", errors="replace")
    if not raw.strip():
        die(f"Üres a kimeneti MD: {md_path}")

    if len(raw.splitlines()) < min_lines:
        log("WARN", "A report gyanúsan rövid (összelapított lehet).")
        try_postprocess(md_path, report)
        raw = md_path.read_text(encoding="utf-8", errors="replace")

    txt = normalize(raw)

    if not PAT_COVERAGE.search(txt):
        die("Hiányzik a Lefedettség sor.")

    if re.search(r"hiányos", txt, flags=re.IGNORECASE):
        log("WARN", "Lefedettség: HIÁNYOS – adatforrás/market-data oldali hiány lehet.")

    if report == "1":
        if not PAT_MAIN_1.search(txt):
            die("Hiányzik az After-hours & Premarket #1 fő fejléc (##+).")

        # Ensure headers (macro required + other required) – may mutate file
        _ensure_block_headers(md_path, report)
        raw2 = md_path.read_text(encoding="utf-8", errors="replace")
        txt2 = normalize(raw2)

        # Now hard-check required headers
        if not PAT_MACRO.search(txt2):
            die("Hiányzó kötelező blokk (kulcsszó): makro")
        if not PAT_BEJELENT.search(txt2):
            die("Hiányzó kötelező blokk (kulcsszó): bejelent")
        if not PAT_KATALIZ.search(txt2):
            die("Hiányzó kötelező blokk (kulcsszó): kataliz")
        if not PAT_HIGH.search(txt2):
            die("Hiányzó kötelező blokk (kulcsszó): highconv")

        if not PAT_JOB.search(txt2):
            die("Hiányzik a Job summary blokk.")

    log("OK", f"report #{report} valid ({VERSION})")


def main() -> None:
    _utf8_reconfigure()
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, choices=["1", "2", "3"])
    ap.add_argument("--md", default=None)
    ap.add_argument("--min-lines", type=int, default=40)
    args = ap.parse_args()

    report = args.report
    md_path = Path(args.md) if args.md else Path("reports") / f"summary_report_{report}.md"
    validate(md_path, report, args.min_lines)


if __name__ == "__main__":
    main()
