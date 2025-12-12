#!/usr/bin/env python3
"""Post-process #1 report markdown with macro / analyst / catalyst / high-conv blocks.

Usage:
    python postprocess_report.py --md reports/summary_report_1.md --bundle-dir reports

postprocess_report.py – v3.3.1-biblia1-dedupe-jobsumend

Fixek (#1):
- Kötelező blokkok mindig megjelennek (null-blokkokkal).
- Deduplikáció: ha a runner már beleírta a Makró / 5-6-7 blokkokat, ezeket kiszedi és egyszer rakja vissza.
- Job summary blokkot a fájl LEGVÉGÉRE mozgatja (Raw URL sorokkal együtt).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, List, Tuple

from analyst_block_builder import build_block_from_file as build_analyst_block
from highconv_block_builder import (
    build_catalysts_block_from_file,
    build_highconv_block_from_file,
)

# ---------- I/O segédfüggvények ----------

def _read_md(path: Path) -> str:
    return path.read_text(encoding="utf-8")

def _write_md(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")

def _load_json(path: Path) -> Any:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

# ---------- Blokk builder-ek ----------

def _build_macro_block(macro_json_path: Path) -> str:
    """BIBLIA #1: Makró blokk MINDIG megjelenik (akkor is, ha üres)."""
    data = _load_json(macro_json_path)

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = (
            data.get("headlines")
            or data.get("items")
            or data.get("news")
            or data.get("macro")
        )
        if not isinstance(items, list):
            items = []
    else:
        items = []

    lines: list[str] = ["### Makró / Politika / FED", ""]
    count = 0

    for raw in items:
        if isinstance(raw, str):
            txt = raw.strip()
            if not txt:
                continue
            lines.append(f"- {txt}")
        elif isinstance(raw, dict):
            h = (raw.get("headline") or raw.get("title") or "").strip()
            s = (raw.get("summary") or "").strip()
            src = (raw.get("source") or "").strip()
            ts = (raw.get("time_str") or raw.get("time") or "").strip()
            parts = [p for p in (h, s, ts, src) if p]
            if parts:
                lines.append(f"- {' – '.join(parts)}")
        else:
            continue

        count += 1
        if count >= 8:
            break

    if count == 0:
        lines.append("- Nincs piacmozgató makró/FED/politikai headline az AH/PM sávban.")

    lines.append("")
    return "\n".join(lines)

def _ensure_block(block: str, title: str, empty_line: str) -> str:
    b = (block or "").strip()
    if not b:
        return f"### {title}\n\n- {empty_line}\n"
    first = b.splitlines()[0].strip()
    if not first.startswith("###"):
        return f"### {title}\n\n{b}\n"
    return b + "\n"

# ---------- Markdown segédek ----------

def _find_first(lines: List[str], predicate) -> int:
    for i, line in enumerate(lines):
        if predicate(line):
            return i
    return -1

def _extract_job_summary(lines: List[str]) -> Tuple[List[str], List[str]]:
    """Kiemeli a Job summary blokkot (a sorától a fájl végéig) és visszaadja: (body_lines, job_lines)."""
    idx = _find_first(lines, lambda s: s.strip().startswith("Job summary generated at run-time"))
    if idx == -1:
        return lines, []
    body = lines[:idx]
    job = lines[idx:]
    # trim leading/trailing empties in job block
    while job and job[0].strip() == "":
        job = job[1:]
    while body and body[-1].strip() == "":
        body = body[:-1]
    return body, job

def _remove_section_by_heading(lines: List[str], heading_variants: List[str]) -> List[str]:
    """Eltávolítja az első előforduló '### {heading}' szekciót (a következő ###-ig). Többször ismétli, amíg van."""
    out = lines[:]
    # remove repeatedly
    while True:
        idx = -1
        for i, line in enumerate(out):
            if line.strip() in heading_variants:
                idx = i
                break
        if idx == -1:
            break
        # find end
        j = idx + 1
        while j < len(out):
            if out[j].strip().startswith("### ") and j != idx:
                break
            j += 1
        out = out[:idx] + out[j:]
        # clean double empty lines around removal
        while idx < len(out) and idx > 0 and out[idx-1].strip()=="" and out[idx].strip()=="":
            out.pop(idx)
    return out

def _insert_macro_after_coverage(lines: List[str], macro_block: str) -> List[str]:
    if not macro_block:
        return lines
    idx = _find_first(lines, lambda s: s.strip().startswith("Lefedettség:"))
    macro_lines = macro_block.rstrip().splitlines()
    if idx == -1:
        # top
        return macro_lines + [""] + lines
    return lines[:idx+1] + [""] + macro_lines + [""] + lines[idx+1:]

def _append_blocks(lines: List[str], blocks: List[str]) -> List[str]:
    out = lines[:]
    # ensure trailing single blank
    while out and out[-1].strip() == "":
        out.pop()
    out.append("")
    for b in blocks:
        b_lines = (b or "").rstrip().splitlines()
        if not b_lines:
            continue
        out.extend(b_lines)
        out.append("")
    # trim end to single newline handled later
    while out and out[-1].strip() == "":
        out.pop()
    return out

# ---------- CLI ----------

def main() -> None:
    parser = argparse.ArgumentParser(description="Post-process #1 report markdown (BIBLIA blocks + dedupe + job summary end).")
    parser.add_argument("--md", required=True, help="Path to summary_report_1.md")
    parser.add_argument("--bundle-dir", required=True, help="Directory with macro/analyst/catalyst/high-conv json files.")
    parser.add_argument("--report", default="1", help="(Backcompat) ignored; kept for old callers.")
    args = parser.parse_args()

    md_path = Path(args.md)
    bundle_dir = Path(args.bundle_dir)

    macro_json = bundle_dir / "macro_news_1.json"
    analyst_json = bundle_dir / "analyst_1.json"
    catalysts_json = bundle_dir / "catalysts_1.json"
    highconv_json = bundle_dir / "high_conv_1.json"

    raw = _read_md(md_path)
    lines = raw.splitlines()

    # 0) Job summary blokk kiemelése (mindig a végére tesszük vissza)
    body_lines, job_lines = _extract_job_summary(lines)

    # 1) Deduplikáció: ha a runner már beírta a blokkokat, kiszedjük
    body_lines = _remove_section_by_heading(body_lines, ["### Makró / Politika / FED", "### Politika / FED / Makró", "### Politika / FED / Makró"])
    body_lines = _remove_section_by_heading(body_lines, ["### Bejelentések & fel/lemínősítések", "### 🧩 Bejelentések & fel/lemínősítések"])
    body_lines = _remove_section_by_heading(body_lines, ["### Közeli katalizátorok", "### ⏳ Közelgő katalizátorok", "### Közelgő katalizátorok"])
    body_lines = _remove_section_by_heading(body_lines, ["### Listán kívüli, 3–12 hónapos high-conviction jelöltek", "### 🚀 Listán kívüli, 3–12 hónapos high-conviction jelöltek"])

    # 2) Makró blokk beszúrása a lefedettség után (mindig)
    macro_block = _build_macro_block(macro_json)
    body_lines = _insert_macro_after_coverage(body_lines, macro_block)

    # 3) Builder blokkok (mindig, null-blokkal)
    analyst_block_raw = build_analyst_block(analyst_json)
    catalysts_block_raw = build_catalysts_block_from_file(catalysts_json)
    highconv_block_raw = build_highconv_block_from_file(highconv_json)

    analyst_block = _ensure_block(
        analyst_block_raw,
        title="Bejelentések & fel/lemínősítések",
        empty_line="Nincs új, anyagilag lényeges vállalati közlés vagy elemzői lépés az AH/PM sávban.",
    )
    catalysts_block = _ensure_block(
        catalysts_block_raw,
        title="Közeli katalizátorok",
        empty_line="Nincs kiemelendő, közelgő katalizátor az AH/PM sávban.",
    )
    highconv_block = _ensure_block(
        highconv_block_raw,
        title="Listán kívüli, 3–12 hónapos high-conviction jelöltek",
        empty_line="Nincs új, ismételt erős jelzés a listán kívül az AH/PM sávban.",
    )

    body_lines = _append_blocks(body_lines, [analyst_block, catalysts_block, highconv_block])

    # 4) Job summary blokk vissza a LEGVÉGÉRE (ha van)
    final_lines = body_lines[:]
    if job_lines:
        # ensure one blank line before job summary
        if final_lines and final_lines[-1].strip() != "":
            final_lines.append("")
        final_lines.extend(job_lines)

    text = "\n".join(final_lines).rstrip() + "\n"
    _write_md(md_path, text)

if __name__ == "__main__":
    main()
