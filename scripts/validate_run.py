#!/usr/bin/env python3
"""
validate_run.py – v1.0.4-biblia-guard-headeronly

IMPROVEMENT:
- Kötelező blokkoknál CSAK a HEADER meglétét ellenőrzi (#/##/###),
  a tartalom hiánya NEM okoz FAIL-t.
- Adathiány (Lefedettség: HIÁNYOS) -> WARN, nem FAIL.
- Unicode / Windows CP1252 safe.
"""

from __future__ import annotations
import argparse
import sys
import re
from pathlib import Path

VERSION = "v1.0.4-biblia-guard-headeronly"

# UTF-8 safe stdout/stderr (Windows runner)
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

DASH_CHARS = "\u2010\u2011\u2012\u2013\u2014\u2212"
_dash_re = re.compile(f"[{DASH_CHARS}]")

def normalize_text(s: str) -> str:
    s = _dash_re.sub("-", s)
    s = s.replace("\u00A0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s

REQUIRED_HEADERS_1 = [
    r"##\s*After-hours\s*&\s*Premarket\s*-\s*#1\s*jelent[eé]s",
    r"Lefedetts[eé]g:\s*(TELJES|HI[ÁA]NYOS)",
    r"###\s*Makr[oó]\s*/\s*Politika\s*/\s*FED",
    r"###\s*Bejelent[eé]sek\s*&\s*fel/lem[ií]n[oó]s[ií]t[eé]sek",
    r"###\s*K[öo]zeli\s*kataliz[aá]torok",
    r"###\s*List[aá]n\s*k[ií]v[uü]li,\s*3-12\s*h[oó]napos\s*high-conviction\s*jel[öo]ltek",
    r"Job summary generated at run-time",
]

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
        die("A report gyanúsan rövid (összelapított lehet).")

    txt = normalize_text(raw)

    if report == "1":
        for pat in REQUIRED_HEADERS_1:
            if not re.search(pat, txt, flags=re.IGNORECASE | re.MULTILINE):
                die(f"Hiányzó kötelező header: {pat}")

    safe_print(f"[validate_run] OK – report #{report} valid ({VERSION})")
    sys.exit(0)

if __name__ == "__main__":
    main()
