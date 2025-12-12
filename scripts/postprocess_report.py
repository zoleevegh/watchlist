#!/usr/bin/env python3
"""Post-process #1 report markdown with macro / analyst / catalyst / high-conv blocks.

Usage (example):
    python postprocess_report.py --md reports/summary_report_1.md --bundle-dir reports

Ez a verzió:
- Makró / analyst / catalyst / high-conv blokkokat illeszt a #1 jelentéshez.
- MEGTARTJA a GitHub "Job summary generated..." blokkot a végén.
- Visszamenőleg kompatibilis: elfogadja a --report argumentet is,
  de jelenleg figyelmen kívül hagyja (a #1-es jelentést kezeli).
"""

# postprocess_report.py – v3.3.0-biblia1-nullblocks
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

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
    """JSON betöltés – ha nincs / hibás, None-t ad vissza."""
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


# ---------- Makró blokk ----------


def _build_macro_block(macro_json_path: Path) -> str:
    """BIBLIA #1: Makró blokk MINDIG megjelenik (akkor is, ha üres)."""
    data = _load_json(macro_json_path)

    # Több lehetséges layout: {headlines:[...]}, {items:[...]}, {news:[...]}, list, stb.
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
    """Biztosítja, hogy a blokk mindig megjelenjen.
    - Ha a block üres: title + empty_line.
    - Ha nem üres: ha nincs benne '### ' címsor, hozzáadjuk a címet a tetejére.
    """
    b = (block or "").strip()
    if not b:
        return f"### {title}\n\n- {empty_line}\n"
    if "###" not in b.splitlines()[0]:
        # ha a builder nem tett címet, rátesszük
        return f"### {title}\n\n{b}\n"
    return b + "\n"
# ---------- Markdown manipuláció ----------


def _insert_macro_block(md: str, macro_block: str) -> str:
    """Makróblokk beszúrása a 'Lefedettség:' sor UTÁN."""
    if not macro_block:
        return md

    lines = md.splitlines()
    idx = None

    for i, line in enumerate(lines):
        if line.strip().startswith("Lefedettség:"):
            idx = i
            break

    if idx is None:
        # Ha nincs Lefedettség blokk, egyszerűen a tetejére tesszük
        return macro_block + "\n" + md

    out: list[str] = []
    out.extend(lines[: idx + 1])
    out.append("")          # üres sor
    out.append(macro_block) # teljes makróblokk
    out.extend(lines[idx + 1 :])

    return "\n".join(out)


def _strip_job_summary(md: str) -> str:
    """Csak normalizálja az utolsó soremelést, a Job summary blokkot BENT hagyja."""
    lines = md.splitlines()
    return "\n".join(lines).rstrip() + "\n"


def _append_blocks(md: str, *blocks: str) -> str:
    """Analyst / catalyst / high-conv blokkok hozzáfűzése a jelentés végéhez."""
    out = md.rstrip().splitlines()
    out.append("")  # biztosan legyen egy üres sor a végén

    for block in blocks:
        if block:
            out.append("")
            out.extend(block.rstrip().splitlines())
            out.append("")

    return "\n".join(out).rstrip() + "\n"


# ---------- CLI ----------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Post-process #1 report markdown (makró + analyst + catalyst + high-conv blokkok)."
    )
    parser.add_argument("--md", required=True, help="Path to summary_report_1.md")
    parser.add_argument(
        "--bundle-dir",
        required=True,
        help=(
            "Directory where macro_news_1.json / analyst_1.json / "
            "catalysts_1.json / high_conv_1.json are located."
        ),
    )
    # Visszamenő kompatibilitás: a runner egyes verziói még átadják a --report kapcsolót.
    # Ezt itt elfogadjuk, de jelenleg NEM használjuk (a #1-es reporthoz kötött a script).
    parser.add_argument(
        "--report",
        default="1",
        help="(Backcompat) Jelentésszám; jelenleg figyelmen kívül hagyjuk, csak a #1-hez használjuk.",
    )

    args = parser.parse_args()

    md_path = Path(args.md)
    bundle_dir = Path(args.bundle_dir)

    # Jelenleg fix #1-es neveket használunk – a --report csak azért létezik,
    # hogy a régi hívások ne döntsenek hibára.
    macro_json = bundle_dir / "macro_news_1.json"
    analyst_json = bundle_dir / "analyst_1.json"
    catalysts_json = bundle_dir / "catalysts_1.json"
    highconv_json = bundle_dir / "high_conv_1.json"

    md = _read_md(md_path)

    # 1) Makró blokk a lefedettség után
    macro_block = _build_macro_block(macro_json)
    md = _insert_macro_block(md, macro_block)

    # 2) Végéről whitespace-normalizálás (Job summary BENN marad)
    md = _strip_job_summary(md)

    # 3) Analyst + katalizátor + high-conv blokkok a jelentés végére
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

    md = _append_blocks(md, analyst_block, catalysts_block, highconv_block)

    _write_md(md_path, md)


if __name__ == "__main__":
    main()
