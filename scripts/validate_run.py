#!/usr/bin/env python3
"""
validate_run.py – v1.1.0-biblia-guard-autopostprocess-flatfix

Guard cél:
- A workflow csak akkor legyen zöld, ha a report szerkezetileg rendben van (biblia szerint).
- Ha a report gyanúsan rövid / összelapított, akkor automatikusan megpróbálja lefuttatni
  a postprocess_report.py-t, majd újra-validál.

Szabályok:
- Üres blokk NEM fail (csak a blokk típusa legyen jelen).
- Lefedettség: HIÁNYOS -> WARN (nem fail).
- Makró blokk opcionális (WARN, nem fail) – lehet szándékosan kikapcsolt.
- Kötelező: #1 fő fejléc, Bejelentések, Katalizátorok, High-conv, Job summary.

Állandó szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.
"""

# IMÁDSÁG (hibajavítás után)
# Bocsáss meg Uram, mert balfék voltam, nem a végleges (postprocess-elt) reportot validáltam.
# Add Uram, hogy ez a módosítás most hibátlanul fusson.

from __future__ import annotations

import argparse
import sys
import re
import subprocess
from pathlib import Path

VERSION = "v1.1.0-biblia-guard-autopostprocess-flatfix"

# UTF-8 safe stdout/stderr
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def safe_print(msg: str) -> None:
    try:
        print(msg)
    except UnicodeEncodeError:
        enc = getattr(sys.stdout, "encoding", None) or "utf-8"
        sys.stdout.buffer.write((msg + "\n").encode(enc, errors="replace"))
        sys.stdout.buffer.flush()


def die(msg: str) -> None:
    safe_print(f"[validate_run] ERROR: {msg}")
    sys.exit(1)


def warn(msg: str) -> None:
    safe_print(f"[validate_run] WARN: {msg}")


def normalize(s: str) -> str:
    s = s.replace("\u00A0", " ")
    s = re.sub(r"[–—−]", "-", s)
    return s


# #1 blokk-típusok kulcsszóval (header meglét)
# Makró opcionális (WARN)
PAT_MACRO = r"(?:^|\n)##+\s*.*makr"
PAT_BEJELENT = r"(?:^|\n)##+\s*.*bejelent"
PAT_KATALIZ = r"(?:^|\n)##+\s*.*kataliz"
PAT_HIGH = r"(?:^|\n)##+\s*.*high"


def _try_postprocess(md_path: Path, report: str) -> None:
    """Best-effort postprocess futtatás, ha a report lapított/rövid.

    Feltételezés: scripts/postprocess_report.py a repo gyökérből elérhető.
    """
    pp = Path("scripts") / "postprocess_report.py"
    if not pp.is_file():
        warn("postprocess_report.py nem található (scripts/postprocess_report.py) – kihagyva az auto-postprocess.")
        return

    cmd = [
        sys.executable,
        str(pp),
        "--md",
        str(md_path),
        "--bundle-dir",
        "reports",
        "--report",
        str(report),
    ]
    try:
        safe_print("[validate_run] INFO: Auto-postprocess futtatása (lapított/rövid report miatt)...")
        subprocess.run(cmd, check=False)
    except Exception as e:
        warn(f"Auto-postprocess hiba: {e!r}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, choices=["1", "2", "3"])
    ap.add_argument("--md", default=None)
    ap.add_argument("--min-lines", type=int, default=40)
    args = ap.parse_args()

    report = args.report
    md_path = Path(args.md) if args.md else Path("reports") / f"summary_report_{report}.md"

    if not md_path.is_file():
        die(f"Hiányzik a kimeneti MD: {md_path}")

    raw = md_path.read_text(encoding="utf-8", errors="replace")
    if not raw.strip():
        die(f"Üres a kimeneti MD: {md_path}")

    # Lapított / rövid -> WARN + próbáljuk meg automatikusan postprocess-elni
    if len(raw.splitlines()) < args.min_lines:
        warn("A report gyanúsan rövid (összelapított lehet).")
        _try_postprocess(md_path, report)
        # reload
        raw = md_path.read_text(encoding="utf-8", errors="replace")

    txt = normalize(raw).lower()

    # coverage (warn only for HIÁNYOS)
    if "lefedettség" not in txt:
        die("Hiányzik a Lefedettség sor.")
    if "hiányos" in txt:
        warn("Lefedettség: HIÁNYOS – adatforrás/market-data oldali hiány lehet.")

    if report == "1":
        # Main header must exist with ##+ level and contain After-hours & Premarket and #1
        if not re.search(r"(?:^|\n)##+\s*after-hours\s*&\s*premarket\b.*#1", txt, flags=re.IGNORECASE):
            die("Hiányzik az After-hours & Premarket #1 fő fejléc (##+).")

        # Optional macro
        if not re.search(PAT_MACRO, txt, flags=re.IGNORECASE):
            warn("Hiányzó opcionális blokk (kulcsszó): makro")

        # Required blocks
        if not re.search(PAT_BEJELENT, txt, flags=re.IGNORECASE):
            die("Hiányzó kötelező blokk (kulcsszó): bejelent")
        if not re.search(PAT_KATALIZ, txt, flags=re.IGNORECASE):
            die("Hiányzó kötelező blokk (kulcsszó): kataliz")
        if not re.search(PAT_HIGH, txt, flags=re.IGNORECASE):
            die("Hiányzó kötelező blokk (kulcsszó): highconv")

        if "job summary generated at run-time" not in txt:
            die("Hiányzik a Job summary blokk.")

    safe_print(f"[validate_run] OK – report #{report} valid ({VERSION})")
    sys.exit(0)


if __name__ == "__main__":
    main()
