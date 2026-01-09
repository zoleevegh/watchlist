#!/usr/bin/env python3
# validate_run.py — v4.2.1-robust-ticker-detect-2026-01-09
#
# Fix: ha minden ticker n/a, akkor ne bukjon el a workflow.
# Kimenet: WARN, exit 0.
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

import argparse, os, re, sys

VERSION = "v4.2.1-robust-ticker-detect-2026-01-09"

# Ticker-detekció: a report formátuma változhat (bullet, bold, emdash, stb.).
# Cél: ne bukjon el feleslegesen a workflow csak azért, mert a sorprefix változott.
# A validálás továbbra is bukik NO_DATA esetén.

TICKER_RE = re.compile(r"\b([A-Z][A-Z0-9]{0,6}(?:\.[A-Z]{1,3})?)\b")
MD_BOLD_TICKER_RE = re.compile(r"\*\*([A-Z][A-Z0-9]{0,6}(?:\.[A-Z]{1,3})?)\*\*")

def extract_ticker_lines(txt: str) -> list[str]:
    """Próbálja megtalálni a ticker-sorokat több formátum alapján.

    Elfogadott minták:
      - "- TICKER ..."
      - "- **TICKER** ..."
      - "**TICKER**: ..." (nem feltétlen bullet)
    """
    out: list[str] = []
    for raw in txt.splitlines():
        ln = raw.strip()
        if not ln:
            continue
        is_bullet = ln.startswith("- ")
        # 1) bullet sorok: csak akkor, ha tickerrel kezdődik
        if is_bullet:
            # - TICKER ...
            m1 = re.match(r"^-\s+([A-Z][A-Z0-9]{0,6}(?:\.[A-Z]{1,3})?)\b", ln)
            if m1:
                out.append(ln)
                continue
            # - **TICKER** ...
            m2 = re.match(r"^-\s+\*\*([A-Z][A-Z0-9]{0,6}(?:\.[A-Z]{1,3})?)\*\*", ln)
            if m2:
                out.append(ln)
                continue
        # 2) nem bullet, de tickerrel kezdődő bold: "**NVDA**"
        if ln.startswith("**"):
            m = MD_BOLD_TICKER_RE.match(ln)
            if m:
                out.append(ln)
    return out

def count_na(lines: list[str]) -> int:
    return sum(1 for ln in lines if ("PM n/a" in ln and "AH n/a" in ln))

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=int, default=1)
    ap.add_argument("--path", required=True)
    args = ap.parse_args()

    if not os.path.exists(args.path):
        print(f"ERROR: report file missing: {args.path}", file=sys.stderr, flush=True)
        return 1

    txt = open(args.path, "r", encoding="utf-8", errors="replace").read()

    # Hard fail: a runner kifejezetten jelzi, ha nincs report.
    if "NO_DATA" in txt:
        print("ERROR: NO_DATA a reportban (runner crash / hiányzó fájl / forráshiba).", file=sys.stderr, flush=True)
        return 1

    # Rugalmas ticker-sor keresés.
    lines = extract_ticker_lines(txt)
    if not lines:
        # Ne bukjon el, ha a reportban csak makró/hír blokk van, vagy a formátum változott.
        # Ilyenkor WARN, de exit 0.
        print("WARNING: nem találtam ticker sort a riportban (formátumváltozás vagy üres ticker-blokk).", file=sys.stderr, flush=True)
        return 0

    na_count = count_na(lines)

    if na_count == len(lines):
        print(f"WARNING: minden ticker n/a ({na_count}/{len(lines)}) — forráshiba / sessionen kívüli lekérdezés / Yahoo limit.", file=sys.stderr, flush=True)
        return 0

    print(f"OK: ticker sorok={len(lines)}, n/a={na_count}", file=sys.stderr, flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
