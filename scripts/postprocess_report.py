#!/usr/bin/env python3
"""postprocess_report.py – v3.3.5-biblia1-force-reflow

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
    s = re.sub(r"\n\s*(###\s+)", r"\n\n\1", s)

    # 3) bullet: minden ' - ' vagy '\t- ' új sorra
    s = re.sub(r"\s+-\s+", r"\n- ", s)

    # 4) több üres sort tömörít
    s = re.sub(r"\n{3,}", "\n\n", s)

    return s.strip() + "\n"

def _build_macro_block(macro_json_path: Path) -> str:
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

def _insert_macro_after_coverage(lines: List[str], macro_block: str) -> List[str]:
    idx = _find_first(lines, lambda s: s.strip().startswith("Lefedettség:"))
    macro_lines = macro_block.rstrip().splitlines()
    if idx == -1:
        return macro_lines + [""] + lines
    return lines[:idx+1] + [""] + macro_lines + [""] + lines[idx+1:]


def _insert_after_coverage(lines: List[str], block: str) -> List[str]:
    """Beszúr egy blokkot közvetlenül a 'Lefedettség:' sor után."""
    idx = _find_first(lines, lambda s: s.strip().startswith("Lefedettség:"))
    b_lines = (block or "").rstrip().splitlines()
    if not b_lines:
        return lines
    if idx == -1:
        return b_lines + [""] + lines
    return lines[:idx+1] + [""] + b_lines + [""] + lines[idx+1:]


def _as_list_payload(data: Any) -> List[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("items", "headlines", "news", "macro", "earnings", "events"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    return []


def _build_feed_health_block(bundle_dir: Path, report: str) -> str:
    """Röviden jelzi, hogy melyik feed adott használható adatot."""
    checks = [
        ("Makró", bundle_dir / f"macro_news_{report}.json"),
        ("Analyst", bundle_dir / f"analyst_{report}.json"),
        ("Catalysts", bundle_dir / f"catalysts_{report}.json"),
        ("High-conv", bundle_dir / f"high_conv_{report}.json"),
        ("Earnings", bundle_dir / f"earnings_{report}.json"),
        ("Yahoo analyst", bundle_dir / f"yahoo_analyst_{report}.json"),
    ]

    ok = []
    miss = []

    for name, p in checks:
        data = _load_json(p)
        items = _as_list_payload(data)
        if items:
            ok.append(name)
        else:
            # akkor is 'miss', ha fájl nincs vagy üres lista / parse error
            miss.append(name)

    lines = ["### Forrás-ellenőrzés", ""]
    lines.append(f"- OK: {', '.join(ok) if ok else '—'}")
    lines.append(f"- Nincs adat / üres: {', '.join(miss) if miss else '—'}")
    return "\n".join(lines)


def _build_earnings_block(earnings_json_path: Path) -> str:
    data = _load_json(earnings_json_path)
    items = _as_list_payload(data)

    lines = ["### Közelgő jelentések", ""]
    if not items:
        lines.append("- Nem találtam közelgő jelentést (feed üres / nem elérhető).")
        return "\n".join(lines)

    # Várható mezők: ticker/symbol, date, time/session
    def _get(d: Any, *keys: str) -> str:
        if not isinstance(d, dict):
            return ""
        for k in keys:
            v = d.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return ""

    for it in items[:20]:
        t = _get(it, "ticker", "symbol")
        dt = _get(it, "date", "reportDate", "earningsDate")
        when = _get(it, "time", "session", "when")
        extra = _get(it, "source", "provider")
        parts = [p for p in [t, dt, when] if p]
        tail = f" ({extra})" if extra else ""
        if parts:
            lines.append(f"- {' — '.join(parts)}{tail}")
    return "\n".join(lines)


def _build_yahoo_analyst_block(path: Path) -> str:
    data = _load_json(path)
    items = _as_list_payload(data)

    lines = ["### Yahoo – elemzői események", ""]
    if not items:
        lines.append("- Nem találtam friss Yahoo analyst eseményt (feed üres / nem elérhető).")
        return "\n".join(lines)

    def _get(d: Any, *keys: str) -> str:
        if not isinstance(d, dict):
            return ""
        for k in keys:
            v = d.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return ""

    for it in items[:30]:
        t = _get(it, "ticker", "symbol")
        action = _get(it, "action", "type", "event")
        firm = _get(it, "firm", "broker", "analyst")
        pt = _get(it, "pt", "priceTarget", "target")
        dt = _get(it, "date", "time", "timestamp")
        bit = " | ".join([x for x in [action, firm, pt, dt] if x])
        if t and bit:
            lines.append(f"- {t}: {bit}")
        elif t:
            lines.append(f"- {t}")
    return "\n".join(lines)

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

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--md", required=True)
    p.add_argument("--bundle-dir", required=True)
    p.add_argument("--report", default="1")
    args = p.parse_args()

    md_path = Path(args.md)
    bundle_dir = Path(args.bundle_dir)

    report = str(args.report or "1")

    macro_json = bundle_dir / f"macro_news_{report}.json"
    analyst_json = bundle_dir / f"analyst_{report}.json"
    catalysts_json = bundle_dir / f"catalysts_{report}.json"
    highconv_json = bundle_dir / f"high_conv_{report}.json"

    raw = _read_md(md_path)
    raw = _force_reflow(raw)
    lines = raw.splitlines()

    body_lines, job_lines = _extract_job_summary(lines)

    body_lines = _remove_section_by_heading(body_lines, ["### Forrás-ellenőrzés"])
    body_lines = _remove_section_by_heading(body_lines, ["### Makró / Politika / FED", "### Politika / FED / Makró"])
    body_lines = _remove_section_by_heading(body_lines, ["### Közelgő jelentések"])
    body_lines = _remove_section_by_heading(body_lines, ["### Yahoo – elemzői események"])
    body_lines = _remove_section_by_heading(body_lines, ["### Bejelentések & fel/lemínősítések", "### 🧩 Bejelentések & fel/lemínősítések"])
    body_lines = _remove_section_by_heading(body_lines, ["### Közeli katalizátorok", "### ⏳ Közelgő katalizátorok", "### Közelgő katalizátorok"])
    body_lines = _remove_section_by_heading(body_lines, ["### Listán kívüli, 3–12 hónapos high-conviction jelöltek", "### 🚀 Listán kívüli, 3–12 hónapos high-conviction jelöltek"])

    health_block = _build_feed_health_block(bundle_dir, report)
    body_lines = _insert_after_coverage(body_lines, health_block)

    macro_block = _build_macro_block(macro_json)
    body_lines = _insert_after_coverage(body_lines, macro_block)

    earnings_block = _build_earnings_block(bundle_dir / f"earnings_{report}.json")
    yahoo_block = _build_yahoo_analyst_block(bundle_dir / f"yahoo_analyst_{report}.json")

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

    body_lines = _append_blocks(body_lines, [earnings_block, yahoo_block, analyst_block, catalysts_block, highconv_block])

    final_lines = body_lines[:]
    if job_lines:
        if final_lines and final_lines[-1].strip() != "":
            final_lines.append("")
        final_lines.extend(job_lines)

    _write_md(md_path, "\n".join(final_lines).rstrip() + "\n")

if __name__ == "__main__":
    main()
