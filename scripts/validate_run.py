#!/usr/bin/env python3
"""
validate_run.py – v1.0.8-biblia-guard-soft-macro-soft-minlines

Változás:
- A 'Makró / Politika / FED' blokk hiánya nem FAIL többé (csak WARN),
  mert a #1-ben a makró blokk lehet szándékosan kikapcsolt / üres / külön feed-ről jön.
- A túl rövid / összelapított report továbbra is WARN (nem fail).
- A többi biblia-guard ellenőrzés marad: fő fejléc, lefedettség sor, kötelező blokkok (bejelentések/katalizátor/highconv), Job summary.

Állandó szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.
"""

# IMÁDSÁG (hibajavítás után)
# Bocsáss meg Uram, mert balfék voltam, túl szigorúan fogtam a makró blokkot.
# Add Uram, hogy ez a módosítás most hibátlanul fusson.

from __future__ import annotations
import argparse
import sys
import re
from pathlib import Path

VERSION = "v1.0.8-biblia-guard-soft-macro-soft-minlines"

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
# Makró: csak WARN, nem FAIL
KEYWORDS_1_OPTIONAL = {
    "makro": r"^###\s*.*makr",
}

KEYWORDS_1_REQUIRED = {
    "bejelent": r"^###\s*.*bejelent",
    "kataliz": r"^###\s*.*kataliz",
    "highconv": r"^###\s*.*high",
}


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

    if len(raw.splitlines()) < args.min_lines:
        warn("A report gyanúsan rövid (összelapított lehet).")

    txt = normalize(raw).lower()

    # coverage (warn only for HIÁNYOS)
    if "lefedettség" not in txt:
        die("Hiányzik a Lefedettség sor.")
    if "hiányos" in txt:
        warn("Lefedettség: HIÁNYOS – adatforrás/market-data oldali hiány lehet.")

    if report == "1":
        # Main header must exist with ##+ level and contain After-hours & Premarket and #1
        if not re.search(r"^##+\s*after-hours\s*&\s*premarket\b.*#1", txt, flags=re.IGNORECASE | re.MULTILINE):
            die("Hiányzik az After-hours & Premarket #1 fő fejléc (##+).")

        # Optional (WARN)
        for name, pat in KEYWORDS_1_OPTIONAL.items():
            if not re.search(pat, txt, flags=re.IGNORECASE | re.MULTILINE):
                warn(f"Hiányzó opcionális blokk (kulcsszó): {name}")

        # Required (FAIL)
        for name, pat in KEYWORDS_1_REQUIRED.items():
            if not re.search(pat, txt, flags=re.IGNORECASE | re.MULTILINE):
                die(f"Hiányzó kötelező blokk (kulcsszó): {name}")

        if "job summary generated at run-time" not in txt:
            die("Hiányzik a Job summary blokk.")

    safe_print(f"[validate_run] OK – report #{report} valid ({VERSION})")
    sys.exit(0)


if __name__ == "__main__":
    main()
