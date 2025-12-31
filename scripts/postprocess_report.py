# Version: v3.8.12
# Last updated: 2025-12-31T11:15:00Z
#!/usr/bin/env python3
"""postprocess_report.py – v3.4.2-format-reflow-robust

Bocsáss meg Uram, mert balfék voltam; add Uram, hogy ne legyen hibás ez a módosítás.

#1: kényszerített tördelés akkor is, ha a runner 1 sorba lapította a teljes jelentést.

- Kötelező blokkok mindig megjelennek (null-blokkokkal).
- Dedupe: Makró + 5/6/7 blokkok egyszer szerepelnek.
- Job summary blokk a fájl LEGVÉGÉRE kerül.
- FORCE REFLOW: minden '###' és '- ' elem új soron indul, még teljesen lapított inputnál is.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, List, Tuple

from analyst_block_builder import build_block_from_file as build_analyst_block
from highconv_block_builder import (
    build_catalysts_block_from_file,
    build_highconv_block_from_file,
)

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

def _force_reflow(raw: str) -> str:
    s = raw.replace("\r\n", "\n").replace("\r", "\n")

    # 1) biztosítsuk, hogy ezek a tokenek mindig új soron induljanak
    tokens = [
        "## After-hours & Premarket",
        "**Script verzió:**",
        "**Futás ideje:**",
        "**Időablakok",
        "**Árforrás:**",
        "Lefedettség:",
        "### ",
        "Job summary generated at run-time",
        "Raw URL for this run:",
    ]
    for t in tokens:
        s = s.replace(t, f"\n{t}")

    # 2) Headings: minden '### ' előtt 2 sortörés (szebb)
    s = re.sub(r"\n\s*(##+\s+)", r"\n\n\1", s)

    # 3) bullet: minden ' - ' vagy '\t- ' új sorra
    s = re.sub(r"\s+-\s+", r"\n- ", s)

    # 4) több üres sort tömörít
    s = re.sub(r"\n{3,}", "\n\n", s)

    return s.strip() + "\n"


def _build_macro_block(macro_json_path: Path) -> str:
    """
    Makró blokk builder.

    Támogatott JSON formátumok:
    - dict: { narrative: "..." }  (elsődleges, 3–6 soros narratíva)
    - dict: { headlines/items/news/macro: [...] }
    - list: [ ... ]
    """
    data = _load_json(macro_json_path)

    narrative = ""
    items = []

    if isinstance(data, dict):
        n = data.get("narrative") or data.get("text") or data.get("summary_text") or ""
        if isinstance(n, str):
            narrative = n.strip()

        cand = (
            data.get("headlines")
            or data.get("items")
            or data.get("news")
            or data.get("macro")
            or data.get("data")
            or data.get("events")
        )
        if isinstance(cand, list):
            items = cand
    elif isinstance(data, list):
        items = data

    lines: list[str] = ["### Makró / Politika / FED", ""]

    # 1) Ha van narratíva: 3–6 sor max (te kérted: piacmozgató narratíva)
    if narrative:
        for ln in [x.strip() for x in narrative.replace("\r", "").split("\n") if x.strip()]:
            # ne engedjük elszállni a hosszát
            if len(lines) >= 2 + 6:
                break
            # ha már bullet, hagyjuk; ha nem, bullet-ezzük
            if ln.startswith("-"):
                lines.append(ln)
            else:
                lines.append(f"- {ln}")
        lines.append("")
        return "\n".join(lines)

    # 2) Fallback: listás headline-ok (max 8)
    count = 0
    for raw in items:
        if isinstance(raw, str):
            txt = raw.strip()
            if not txt:
                continue
            lines.append(f"- {txt}")
        elif isinstance(raw, dict):
            h = (raw.get("headline") or raw.get("title") or "").strip()
            s = (raw.get("summary") or raw.get("text") or "").strip()
            src = (raw.get("source") or "").strip()
            ts = (raw.get("time_str") or raw.get("time") or raw.get("ts") or "").strip()
            parts = [p for p in (h, s, ts, src) if p]
            if parts:
                lines.append(f"- {' – '.join(parts)}")
        else:
            continue

        count += 1
        if count >= 8:
            break

    if count == 0:
        lines.append("- Nem érkezett a #1 jelentés kritériumait teljesítő makró/FED/politikai headline az AH/PM sávban.")

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

def _find_first(lines: List[str], predicate) -> int:
    for i, line in enumerate(lines):
        if predicate(line):
            return i
    return -1

def _extract_job_summary(lines: List[str]) -> Tuple[List[str], List[str]]:
    idx = _find_first(lines, lambda s: s.strip().startswith("Job summary generated at run-time"))
    if idx == -1:
        return lines, []
    body = lines[:idx]
    job = lines[idx:]
    while job and job[0].strip() == "":
        job = job[1:]
    while body and body[-1].strip() == "":
        body = body[:-1]
    return body, job

def _remove_section_by_heading(lines: List[str], heading_variants: List[str]) -> List[str]:
    out = lines[:]
    while True:
        idx = -1
        for i, line in enumerate(out):
            if line.strip() in heading_variants:
                idx = i
                break
        if idx == -1:
            break
        j = idx + 1
        while j < len(out):
            if out[j].strip().startswith("### ") and j != idx:
                break
            j += 1
        out = out[:idx] + out[j:]
        while idx < len(out) and idx > 0 and out[idx-1].strip()=="" and out[idx].strip()=="":
            out.pop(idx)
    return out


def _remove_macro_section_loose(lines: List[str]) -> List[str]:
    """Remove any existing macro block even if it is formatted as bullets (e.g. '🧠 Makró / FED / Politika').

    This prevents postprocess from inserting a second macro section when the runner already printed one.
    """
    out: List[str] = []
    i = 0

    # Matches:
    #   🧠 Makró / FED / Politika
    #   🕒 Makró / FED / Politika
    #   - 🧠 Makró / FED / Politika
    macro_start_re = re.compile(r"^\s*-?\s*(?:[🧠🕒]\s*)?Makró\s*/\s*.*FED.*Politika.*$", re.IGNORECASE)

    # Stop when we hit a new major section header
    def _is_next_section(line: str) -> bool:
        s = line.strip()
        return s.startswith("## ") or s.startswith("### ")

    while i < len(lines):
        if macro_start_re.match(lines[i]):
            # Skip until next section header (but keep blank separation)
            j = i + 1
            while j < len(lines) and not _is_next_section(lines[j]):
                j += 1
            # Also remove leading blank lines right after removed macro, to avoid double spacing
            while j < len(lines) and lines[j].strip() == "":
                j += 1
            i = j
            continue
        out.append(lines[i])
        i += 1

    return out

def _insert_macro_after_coverage(lines: List[str], macro_block: str) -> List[str]:
    """Insert macro block right after the 'Lefedettség:' line.

    Robusztus: ha a Lefedettség sor összelapított (nem külön sor), vagy hiányzik,
    akkor fallback:
      1) soron belüli beszúrás a 'Lefedettség:' első előfordulása után,
      2) ha nincs, akkor a fő report cím után,
      3) ha az sem, akkor a fájl elejére.
    """
    if not macro_block or not macro_block.strip():
        return lines

    macro_lines = macro_block.strip().splitlines()

    # 1) klasszikus: külön sorban szereplő Lefedettség:
    for idx, ln in enumerate(lines):
        if ln.strip().lower().startswith("lefedettség:"):
            return lines[:idx+1] + [""] + macro_lines + [""] + lines[idx+1:]

    # 2) összelapított: a Lefedettség: csak soron belül van
    for idx, ln in enumerate(lines):
        if "lefedettség:" in ln.lower():
            # beszúrjuk a sor UTÁN (nem a sor közepébe), hogy tördelés helyreálljon
            return lines[:idx+1] + [""] + macro_lines + [""] + lines[idx+1:]

    # 3) fallback: a főcím után (After-hours & Premarket)
    for idx, ln in enumerate(lines):
        if ln.strip().startswith("##"):
            return lines[:idx+1] + [""] + macro_lines + [""] + lines[idx+1:]

    # 4) végső fallback: elejére
    return macro_lines + [""] + lines

def _append_blocks(lines: List[str], blocks: List[str]) -> List[str]:
    out = lines[:]
    while out and out[-1].strip() == "":
        out.pop()
    out.append("")
    for b in blocks:
        b_lines = (b or "").rstrip().splitlines()
        if not b_lines:
            continue
        out.extend(b_lines)
        out.append("")
    while out and out[-1].strip() == "":
        out.pop()
    return out



def _safe_json_load(path: Path):
    try:
        if not path.exists():
            return None
        txt = path.read_text(encoding="utf-8").strip()
        if not txt:
            return None
        return json.loads(txt)
    except Exception:
        return None

def build_earnings_block_from_file(path: Path) -> str:
    data = _safe_json_load(path)
    items = None
    if isinstance(data, dict):
        items = data.get("items") or data.get("earnings") or data.get("data")
    elif isinstance(data, list):
        items = data
    if not items:
        return ""
    lines = ["### 📅 Közelgő jelentések (1–7 nap)"]
    for it in items[:30]:
        t = (it.get("ticker") or it.get("symbol") or "").upper()
        dt = it.get("datetime_local") or it.get("datetime") or it.get("date") or ""
        when = it.get("when") or it.get("time") or it.get("session") or ""
        title = it.get("title") or it.get("name") or ""
        if not t:
            continue
        parts = [f"**{t}**"]
        if title:
            parts.append(f"– {title}")
        if dt:
            parts.append(f"– {dt}")
        if when:
            parts.append(f"({when})")
        lines.append("- " + " ".join(parts))
    return "\n".join(lines) + "\n"

def build_yahoo_analyst_block_from_file(path: Path) -> str:
    data = _safe_json_load(path)
    items = None
    if isinstance(data, dict):
        items = data.get("items") or data.get("events") or data.get("data")
    elif isinstance(data, list):
        items = data
    if not items:
        return ""
    lines = ["### 🧠 Yahoo – elemzői események (max 30 nap)"]
    for it in items[:50]:
        t = (it.get("ticker") or it.get("symbol") or "").upper()
        firm = it.get("firm") or it.get("analyst") or it.get("source") or ""
        action = it.get("action") or it.get("rating") or it.get("event") or ""
        pt = it.get("pt") or it.get("price_target") or it.get("target") or ""
        dt = it.get("date_local") or it.get("date") or it.get("datetime") or ""
        if not t:
            continue
        bits = [f"**{t}**"]
        if action:
            bits.append(action)
        if firm:
            bits.append(f"({firm})")
        if pt:
            bits.append(f"PT: {pt}")
        if dt:
            bits.append(f"– {dt}")
        lines.append("- " + " ".join(bits))
    return "\n".join(lines) + "\n"




def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--md", required=True)
    p.add_argument("--bundle-dir", required=True)
    p.add_argument("--report", default="1")
    args = p.parse_args()

    md_path = Path(args.md)
    bundle_dir = Path(args.bundle_dir)

    macro_json = bundle_dir / f"macro_news_{args.report}.json"
    analyst_json = bundle_dir / f"analyst_{args.report}.json"
    catalysts_json = bundle_dir / f"catalysts_{args.report}.json"
    highconv_json = bundle_dir / f"high_conv_{args.report}.json"
    earnings_json = bundle_dir / f"earnings_{args.report}.json"
    yahoo_analyst_json = bundle_dir / f"yahoo_analyst_{args.report}.json"

    raw = _read_md(md_path)
    raw = _force_reflow(raw)
    lines = raw.splitlines()

    body_lines, job_lines = _extract_job_summary(lines)

    body_lines = _remove_section_by_heading(body_lines, ["### Makró / Politika / FED", "## Makró / Politika / FED", "### Makró / FED / Politika", "## Makró / FED / Politika", "### Politika / FED / Makró", "## Politika / FED / Makró", "### 🧠 Makró / FED / Politika", "## 🧠 Makró / FED / Politika", "### 🧠 Makró / Politika / FED", "## 🧠 Makró / Politika / FED", "### 🕒 Makró / FED / Politika", "## 🕒 Makró / FED / Politika", "### 🕒 Makró / Politika / FED", "## 🕒 Makró / Politika / FED"])
    body_lines = _remove_section_by_heading(body_lines, ["### Bejelentések & fel/lemínősítések", "### 🧩 Bejelentések & fel/lemínősítések"])
    body_lines = _remove_section_by_heading(body_lines, ["### Közeli katalizátorok", "### ⏳ Közelgő katalizátorok", "### Közelgő katalizátorok"])
    body_lines = _remove_section_by_heading(body_lines, ["### Listán kívüli, 3–12 hónapos high-conviction jelöltek", "### 🚀 Listán kívüli, 3–12 hónapos high-conviction jelöltek"])

    body_lines = _remove_macro_section_loose(body_lines)
    macro_block = _build_macro_block(macro_json)
    body_lines = _insert_macro_after_coverage(body_lines, macro_block)

    analyst_block = _ensure_block(
        build_analyst_block(analyst_json),
        "Bejelentések & fel/lemínősítések",
        "Nem érkezett a #1 jelentés kritériumait teljesítő vállalati közlés vagy elemzői lépés az AH/PM sávban.",
    )
    catalysts_block = _ensure_block(
        build_catalysts_block_from_file(catalysts_json),
        "Közeli katalizátorok",
        "Nem volt a #1 jelentés kritériumait teljesítő, közelgő katalizátor az AH/PM sávban.",
    )
    highconv_block = _ensure_block(
        build_highconv_block_from_file(highconv_json),
        "Listán kívüli, 3–12 hónapos high-conviction jelöltek",
        "Nem volt a #1 jelentés kritériumait teljesítő, listán kívüli ismételt erős jelzés az AH/PM sávban.",
    )

    body_lines = _append_blocks(body_lines, [analyst_block, catalysts_block, highconv_block])

    final_lines = body_lines[:]

    # --- Earnings block injection (optional) ---
    earnings_block = ""
    try:
        earnings_block = build_earnings_block_from_file(earnings_json)
    except Exception:
        earnings_block = ""

    if earnings_block:
        # beszúrjuk a riport végére, a Job summary ELÉ (hogy mindig látszódjon)
        if final_lines and final_lines[-1].strip() != "":
            final_lines.append("")
        final_lines.append(earnings_block.rstrip())
        final_lines.append("")

    if job_lines:
        if final_lines and final_lines[-1].strip() != "":
            final_lines.append("")
        final_lines.extend(job_lines)

    _write_md(md_path, "\n".join(final_lines).rstrip() + "\n")

if __name__ == "__main__":
    main()


# --- GUARANTEE MACRO PLACEHOLDER (v3.8.1) ---
# If macro JSON is empty, postprocess must still render a placeholder block.
