#!/usr/bin/env python3
# postprocess_report_1_v1_0_0.py
# Version: 1.0.0 - macro / analyst / catalyst / high-conv blokkok + job summary kiszedés

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, List, Sequence


SCRIPT_VERSION = "1.0.0-postprocess-report-1"


# --------- I/O segédfüggvények --------- #

def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def load_json(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return None


# --------- Makró / FED / piaci hangulat blokk --------- #

def _extract_list_like(data: Any, keys: Sequence[str]) -> List[Any]:
    """
    Általános segédfüggvény, ami a leggyakoribb kulcsneveken (items, news, events, stb.)
    próbál listát találni. Ha maga a data lista, azt adja vissza.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in keys:
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def build_macro_block(macro_data: Any, max_items: int = 5) -> str:
    """
    macro_news_1.json → 'Politika / FED / Makró' blokk.
    Elvárás: 3–5 soros Bloomberg-szerű kivonat.
    A JSON-ben a tipikus kulcsokat próbálja értelmezni, de ha csak sima string lista,
    azt is tudja kezelni.
    """
    if not macro_data:
        return ""

    items = _extract_list_like(macro_data, keys=("macro_news", "items", "news", "headlines"))
    if not items:
        return ""

    lines: List[str] = []
    for raw in items:
        if isinstance(raw, str):
            text = raw.strip()
        elif isinstance(raw, dict):
            headline = (raw.get("headline")
                        or raw.get("title")
                        or raw.get("summary")
                        or "").strip()
            snippet = (raw.get("snippet")
                       or raw.get("description")
                       or raw.get("note")
                       or "").strip()
            source = (raw.get("source") or "").strip()
            ts = (raw.get("time_str")
                  or raw.get("time")
                  or raw.get("timestamp")
                  or "").strip()

            parts: List[str] = []
            if headline:
                parts.append(headline)
            if snippet:
                parts.append(snippet)
            meta_parts: List[str] = []
            if source:
                meta_parts.append(source)
            if ts:
                meta_parts.append(ts)
            if meta_parts:
                parts.append(" | ".join(meta_parts))

            text = " — ".join(p for p in parts if p)
        else:
            continue

        if text:
            lines.append(f"- {text}")

        if len(lines) >= max_items:
            break

    if not lines:
        return ""

    block_lines = [
        "### Politika / FED / Makró",
        ""
    ]
    block_lines.extend(lines)
    block_lines.append("")  # záró üres sor

    return "\n".join(block_lines)


# --------- High-conv blokk (@ végén) --------- #

def build_highconv_block(highconv_data: Any) -> str:
    """
    high_conv_1.json → 'Listán kívüli, 3–12 hónapos high-conv jelöltek' blokk.
    Feltételezzük, hogy a JSON-ból már ki vannak szűrve a portfólió-/watchlist-nevek
    (biblia szerint).
    """
    if not highconv_data:
        return ""

    items = _extract_list_like(highconv_data, keys=("high_conv", "items", "candidates"))
    if not items:
        return ""

    lines: List[str] = []
    for raw in items:
        if isinstance(raw, str):
            text = raw.strip()
        elif isinstance(raw, dict):
            ticker = (raw.get("ticker") or raw.get("symbol") or "").strip()
            thesis = (raw.get("thesis")
                      or raw.get("reason")
                      or raw.get("summary")
                      or "").strip()
            catalyst = (raw.get("catalyst") or "").strip()

            parts: List[str] = []
            if ticker:
                parts.append(ticker)
            if thesis:
                parts.append(thesis)
            if catalyst:
                parts.append(catalyst)

            text = " – ".join(p for p in parts if p)
        else:
            continue

        if text:
            lines.append(f"- {text}")

    if not lines:
        return ""

    block_lines = [
        "### Listán kívüli, 3–12 hónapos high-conv jelöltek",
        ""
    ]
    block_lines.extend(lines)
    block_lines.append("")  # záró üres sor

    return "\n".join(block_lines)


# --------- Bejelentések & fel/lemínősítések --------- #

def build_analyst_block(analyst_data: Any) -> str:
    """
    analyst_1.json → 'Bejelentések & fel/lemínősítések' blokk.
    Csak a lényeges lépések (biblia szerint a JSON-ban már le van szűrve).
    """
    if not analyst_data:
        return ""

    items = _extract_list_like(analyst_data, keys=("events", "items", "analyst_actions"))
    if not items:
        return ""

    lines: List[str] = []
    for raw in items:
        if isinstance(raw, str):
            text = raw.strip()
        elif isinstance(raw, dict):
            ticker = (raw.get("ticker") or raw.get("symbol") or "").strip()
            action = (raw.get("action") or raw.get("type") or "").strip()
            firm = (raw.get("firm") or raw.get("broker") or "").strip()
            analyst = (raw.get("analyst") or "").strip()
            detail = (raw.get("detail")
                      or raw.get("summary")
                      or raw.get("note")
                      or "").strip()

            parts: List[str] = []
            if ticker:
                parts.append(ticker)
            if action:
                parts.append(action)
            if firm or analyst:
                who_parts = [p for p in (firm, analyst) if p]
                parts.append(", ".join(who_parts))
            if detail:
                parts.append(detail)

            text = " – ".join(p for p in parts if p)
        else:
            continue

        if text:
            lines.append(f"- {text}")

    if not lines:
        return ""

    block_lines = [
        "### Bejelentések & fel/lemínősítések",
        ""
    ]
    block_lines.extend(lines)
    block_lines.append("")

    return "\n".join(block_lines)


# --------- Közeli katalizátorok blokk --------- #

def build_catalysts_block(catalyst_data: Any) -> str:
    """
    catalysts_1.json → 'Közelgő katalizátorok' blokk.
    Earnings / event / product launch, ami pár napon belül van.
    """
    if not catalyst_data:
        return ""

    items = _extract_list_like(catalyst_data, keys=("events", "items", "catalysts"))
    if not items:
        return ""

    lines: List[str] = []
    for raw in items:
        if isinstance(raw, str):
            text = raw.strip()
        elif isinstance(raw, dict):
            ticker = (raw.get("ticker") or raw.get("symbol") or "").strip()
            event_type = (raw.get("event_type")
                          or raw.get("type")
                          or "").strip()
            date_str = (raw.get("date_str")
                        or raw.get("date")
                        or "").strip()
            detail = (raw.get("detail")
                      or raw.get("summary")
                      or raw.get("note")
                      or "").strip()

            parts: List[str] = []
            if ticker:
                parts.append(ticker)
            if event_type:
                parts.append(event_type)
            if date_str:
                parts.append(date_str)
            if detail:
                parts.append(detail)

            text = " – ".join(p for p in parts if p)
        else:
            continue

        if text:
            lines.append(f"- {text}")

    if not lines:
        return ""

    block_lines = [
        "### Közelgő katalizátorok",
        ""
    ]
    block_lines.extend(lines)
    block_lines.append("")

    return "\n".join(block_lines)


# --------- Markdown manipuláció --------- #

def insert_macro_block(markdown: str, macro_block: str) -> str:
    """
    A makró blokkot a fejléc után illeszti be, még a darabszámos tickerek előtt.
    Anchor: az első 'Lefedettség:' sort keresi meg.
    """
    if not macro_block:
        return markdown

    lines = markdown.splitlines()
    idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("Lefedettség:"):
            idx = i
            break

    if idx is None:
        # ha nincs Lefedettség sor, csak az elejére tesszük a címsorok után
        return markdown + "\n\n" + macro_block

    # keressük az első üres sort a Lefedettség sor után
    insert_pos = idx + 1
    while insert_pos < len(lines) and lines[insert_pos].strip() != "":
        insert_pos += 1

    # egy üres sort meghagyunk, utána jön a makró blokk
    new_lines: List[str] = []
    new_lines.extend(lines[:insert_pos + 1])
    new_lines.append(macro_block)
    new_lines.extend(lines[insert_pos + 1:])

    return "\n".join(new_lines)


def strip_job_summary_line(markdown: str) -> str:
    """
    Kivágja a 'Job summary generated at run-time...' sort (debug-információ),
    hogy a végső gistben már ne szerepeljen.
    """
    lines = [
        line for line in markdown.splitlines()
        if not line.strip().startswith("Job summary generated at run-time")
    ]
    return "\n".join(lines).rstrip()  # felesleges üres sorok a végéről le


def append_blocks(
    markdown: str,
    analyst_block: str,
    catalysts_block: str,
    highconv_block: str,
) -> str:
    """
    A jelentés végére illeszti a Bejelentések, Katalizátorok, High-conv blokkokat
    (ebben a sorrendben), ha nem üresek.
    """
    parts: List[str] = [markdown.rstrip()]
    for block in (analyst_block, catalysts_block, highconv_block):
        if block:
            parts.append("")
            parts.append(block.rstrip())

    return "\n\n".join(parts) + "\n"


# --------- CLI / main --------- #

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Post-process #1 summary report: makró / analyst / catalysts / "
            "high-conv blokkok + job summary sor eltávolítása."
        )
    )
    parser.add_argument(
        "--md",
        type=Path,
        default=Path("reports/summary_report_1.md"),
        help="A #1-es jelentés markdown fájlja (alapértelmezés: reports/summary_report_1.md).",
    )
    parser.add_argument(
        "--bundle-dir",
        type=Path,
        default=Path("bundle"),
        help="A JSON bundle könyvtár (macro_news_1.json, analyst_1.json, catalysts_1.json, high_conv_1.json).",
    )
    args = parser.parse_args()

    md_path: Path = args.md
    bundle_dir: Path = args.bundle_dir

    macro_json = load_json(bundle_dir / "macro_news_1.json")
    analyst_json = load_json(bundle_dir / "analyst_1.json")
    catalysts_json = load_json(bundle_dir / "catalysts_1.json")
    highconv_json = load_json(bundle_dir / "high_conv_1.json")

    markdown = read_text(md_path)

    # Makró blokk a fejléc után
    macro_block = build_macro_block(macro_json)
    markdown = insert_macro_block(markdown, macro_block)

    # Job summary sor kiszedése
    markdown = strip_job_summary_line(markdown)

    # Végére: Bejelentések, Katalizátorok, High-conv
    analyst_block = build_analyst_block(analyst_json)
    catalysts_block = build_catalysts_block(catalysts_json)
    highconv_block = build_highconv_block(highconv_json)

    markdown = append_blocks(markdown, analyst_block, catalysts_block, highconv_block)

    write_text(md_path, markdown)


if __name__ == "__main__":
    main()
