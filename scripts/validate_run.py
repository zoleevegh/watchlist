#!/usr/bin/env python3
"""
validate_run.py – v1.0.3-biblia-guard-flexheader

Fix: a #1 fejléc/blokk tokenek ne legyenek túl törékenyek (különböző kötőjel: -, –, —, non‑breaking hyphen).
- Normalizáljuk a szöveget (dash normalize + whitespace normalize)
- Kötelező elemeket REGEX-szel ellenőrizzük

Továbbra is FAIL:
- hiányzó/üres summary_report_{N}.md
- "lapított" output (túl kevés sor)
- hiányzó kötelező blokkok (#1)
- hiányzó Job summary token (#1)

Coverage: HIÁNYOS → WARN (nem fail) alapból.
"""

from __future__ import annotations
import argparse
import sys
import re
from pathlib import Path

VERSION = "v1.0.3-biblia-guard-flexheader"

# Make stdout/stderr UTF-8 safe on Windows runners
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

def die(msg: str, code: int = 1) -> None:
    safe_print(f"[validate_run] ERROR: {msg}")
    sys.exit(code)

def warn(msg: str) -> None:
    safe_print(f"[validate_run] WARN: {msg}")

DASH_CHARS = "\u2010\u2011\u2012\u2013\u2014\u2212"  # hyphen, nb-hyphen, figure dash, en dash, em dash, minus
_dash_re = re.compile(f"[{DASH_CHARS}]")

def normalize_text(s: str) -> str:
    # normalize unicode dashes to "-"
    s = _dash_re.sub("-", s)
    # normalize non-breaking space
    s = s.replace("\u00A0", " ")
    # collapse whitespace
    s = re.sub(r"[ \t]+", " ", s)
    return s

# Regex patterns that must be present in #1
REQUIRED_PATTERNS_1 = [
    r"##\s*After-hours\s*&\s*Premarket\s*-\s*#1\s*jelent[eé]s",
    r"Lefedetts[eé]g:\s*(TELJES|HI[ÁA]NYOS)",
    r"###\s*Makr[oó]\s*/\s*Politika\s*/\s*FED",
    r"###\s*Bejelent[eé]sek\s*&\s*fel/lem[ií]n[oó]s[ií]t[eé]sek",
    r"###\s*K[öo]zeli\s*kataliz[aá]torok",
    r"###\s*List[aá]n\s*k[ií]v[uü]li,\s*3-12\s*h[oó]napos\s*high-conviction\s*jel[öo]ltek",
    r"Job summary generated at run-time",
]

def must_match(pattern: str, txt: str) -> None:
    if not re.search(pattern, txt, flags=re.IGNORECASE | re.MULTILINE):
        die(f"Hiányzó kötelező blokk/token (regex): {pattern}")

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

    raw = md_path.read_text(encoding="utf-8", errors="replace")
    if not raw.strip():
        die(f"Üres a kimeneti MD: {md_path}")

    # line count on raw (before normalization)
    line_count = len(raw.splitlines())
    if line_count < args.min_lines:
        die(f"Gyanúsan kevés sor ({line_count}) – valószínű össze van lapítva a jelentés. Fájl: {md_path}")

    txt = normalize_text(raw)

    # coverage
    m = re.search(r"Lefedetts[eé]g:\s*(TELJES|HI[ÁA]NYOS)", txt, flags=re.IGNORECASE)
    if not m:
        die("Hiányzik a 'Lefedettség:' sor.")
    coverage = m.group(1).upper()
    if "HI" in coverage:  # HIÁNYOS
        warn("Lefedettség: HIÁNYOS – adatforrás/market-data oldali hiány lehet.")
        if args.fail_on_coverage_missing:
            die("Lefedettség: HIÁNYOS – fail-on-coverage-missing aktív.")

    # required blocks for #1
    if report == "1":
        for pat in REQUIRED_PATTERNS_1:
            must_match(pat, txt)

    safe_print(f"[validate_run] OK – report #{report} valid. ({md_path}, {line_count} lines, coverage={coverage}, {VERSION})")
    sys.exit(0)

if __name__ == "__main__":
    main()
