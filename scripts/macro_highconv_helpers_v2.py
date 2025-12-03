"""
macro_highconv_helpers_v2.py
---------------------------------
Utófeldolgozó script a #1 jelentéshez.

Feladatok:
- Beolvassa a macro_news_1.json állományt, és rövid "Politika / FED / Makró"
  blokkot készít belőle.
- Beolvassa a high_conv_1.json állományt (ha létezik), és elkészíti a
  "Listán kívüli, 3–12 hónapos high‑conviction jelöltek" blokkot.
- A két blokkot beilleszti a summary_report_1.md fájlba:
    * a makró blokk a "Lefedettség: ..." sor UTÁN kerül,
    * a high‑conv blokk a "Job summary generated" sor ELÉ (ha van ilyen),
      különben a fájl végére.

Használat (alapértelmezett fájlnevekkel):
    python macro_highconv_helpers_v2.py

Vagy paraméterezve:
    python macro_highconv_helpers_v2.py \ 
        --report summary_report_1.md \ 
        --macro macro_news_1.json \ 
        --highconv high_conv_1.json

Elvárt JSON formátumok
----------------------
macro_news_1.json:
{
  "generated_at": "...",
  "report_type": 1,
  "items": [
    "Bloomberg: ...",
    "Reuters: ...",
    ...
  ]
}

high_conv_1.json:
Lehet közvetlen lista vagy objektum "items" kulccsal, pl:

[
  {
    "ticker": "XYZ",
    "summary": "Több felminősítés, céláremelés, erős guide.",
    "detail": "Hosszabb leírás (opcionális).",
    "source": "MarketBeat / Yahoo Finance",
    "url": "https://..."
  },
  ...
]

vagy

{
  "generated_at": "...",
  "items": [ { ... }, ... ]
}

A script rugalmasan kezeli mindkettőt.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from textwrap import fill
from typing import Any, Iterable, List, Optional


# -----------------------------
# Segéd típusok
# -----------------------------

@dataclass
class MacroNewsItem:
    text: str


@dataclass
class HighConvItem:
    ticker: str
    summary: str
    detail: Optional[str] = None
    source: Optional[str] = None
    url: Optional[str] = None


# -----------------------------
# JSON beolvasás
# -----------------------------

def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_macro_news(path: Path) -> List[MacroNewsItem]:
    data = _load_json(path)
    if not data:
        return []

    items_raw: Iterable[str]
    if isinstance(data, dict):
        items_raw = data.get("items") or []
    elif isinstance(data, list):
        items_raw = data
    else:
        return []

    items: List[MacroNewsItem] = []
    for raw in items_raw:
        if not raw:
            continue
        items.append(MacroNewsItem(text=str(raw).strip()))
    return items


def load_highconv(path: Path) -> List[HighConvItem]:
    data = _load_json(path)
    if not data:
        return []

    items_raw: Iterable[Any]
    if isinstance(data, dict):
        items_raw = data.get("items") or []
    elif isinstance(data, list):
        items_raw = data
    else:
        return []

    items: List[HighConvItem] = []
    for raw in items_raw:
        if not raw:
            continue

        if isinstance(raw, dict):
            ticker = str(raw.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            summary = str(raw.get("summary") or raw.get("reason") or "").strip()
            detail = str(raw.get("detail") or "").strip() or None
            source = str(raw.get("source") or "").strip() or None
            url = str(raw.get("url") or "").strip() or None
        else:
            # Ha csak egy sima string, akkor ticker nélkül, csak szövegként kezeljük
            ticker = ""
            summary = str(raw).strip()
            detail = None
            source = None
            url = None

        if not summary:
            continue

        items.append(HighConvItem(
            ticker=ticker,
            summary=summary,
            detail=detail,
            source=source,
            url=url,
        ))

    return items


# -----------------------------
# Markdown generálás
# -----------------------------

def build_macro_block(items: List[MacroNewsItem]) -> str:
    """
    3–5 soros, tömör makró blokk.
    A 8 headline-ból csak az első 3–5 kerül bele, hogy ne legyen túl hosszú.
    """
    if not items:
        return ""

    # első 4 headline bőven elég
    use_items = items[:4]

    lines = []
    lines.append("### 🌍 Politika / FED / Makró – rövid kivonat")
    lines.append("")
    for it in use_items:
        wrapped = fill(it.text, width=110)
        lines.append(f"- {wrapped}")
    lines.append("")

    return "\n".join(lines)


def build_highconv_block(items: List[HighConvItem]) -> str:
    """
    High-conviction blokk a riport végére.
    Csak akkor tér vissza nem üres sztringgel, ha van legalább 1 jelölt.
    """
    # Üres vagy nincs listán kívüli jelölt → ne jelenjen meg blokk
    if not items:
        return ""

    lines: List[str] = []
    lines.append("### 🚀 Listán kívüli, 3–12 hónapos high‑conviction jelöltek")
    lines.append("")

    for it in items:
        if it.ticker:
            base = f"**{it.ticker}** — {it.summary}"
        else:
            base = it.summary

        extra_parts = []
        if it.detail:
            extra_parts.append(it.detail)
        if it.source:
            extra_parts.append(f"Forrás: {it.source}")
        if it.url:
            extra_parts.append(it.url)

        extra = " ".join(extra_parts).strip()
        if extra:
            full = f"{base} ({extra})"
        else:
            full = base

        wrapped = fill(full, width=110)
        lines.append(f"- {wrapped}")

    lines.append("")
    return "\n".join(lines)


# -----------------------------
# Riport módosítás
# -----------------------------

def inject_blocks_into_report(
    report_path: Path,
    macro_block: str,
    highconv_block: str,
) -> None:
    """
    Beilleszti a makró- és high-conv blokkokat a md riportba.
    A fájlt HELYBEN módosítja.
    """
    text = report_path.read_text(encoding="utf-8")
    lines = text.splitlines()

    new_lines: List[str] = []

    inserted_macro = False
    inserted_highconv = False

    # Makró blokk: a "Lefedettség:" sort keressük
    for idx, line in enumerate(lines):
        new_lines.append(line)

        if (not inserted_macro) and line.strip().startswith("Lefedettség:") and macro_block.strip():
            # egy üres sor + a makró blokk
            new_lines.append("")
            new_lines.append(macro_block.rstrip())
            new_lines.append("")
            inserted_macro = True

    if not inserted_macro and macro_block.strip():
        # Ha nem talált "Lefedettség:" sort, tegyük a fájl eleje után
        new_lines.insert(0, macro_block.rstrip())
        new_lines.insert(0, "")
        inserted_macro = True

    # High-conv blokk: a "Job summary generated" elé
    final_lines: List[str] = []
    for line in new_lines:
        if (not inserted_highconv) and "Job summary generated" in line and highconv_block.strip():
            final_lines.append("")
            final_lines.append(highconv_block.rstrip())
            final_lines.append("")
            inserted_highconv = True
        final_lines.append(line)

    if not inserted_highconv and highconv_block.strip():
        # Ha nincs "Job summary generated" sor, akkor a végére tesszük
        final_lines.append("")
        final_lines.append(highconv_block.rstrip())
        final_lines.append("")

    report_path.write_text("\n".join(final_lines) + "\n", encoding="utf-8")


def maybe_update_latest(report_path: Path, latest_path: Path) -> None:
    """
    Ha létezik latest_1.md, akkor azt is frissítjük a summary_report_1.md
    aktuális tartalmával.
    """
    if report_path.exists() and latest_path.exists():
        latest_path.write_text(report_path.read_text(encoding="utf-8"), encoding="utf-8")


# -----------------------------
# CLI
# -----------------------------

def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Makró + high‑conv blokk beillesztése a #1 jelentésbe.")
    parser.add_argument("--report", type=Path, default=Path("summary_report_1.md"))
    parser.add_argument("--macro", type=Path, default=Path("macro_news_1.json"))
    parser.add_argument("--highconv", type=Path, default=Path("high_conv_1.json"))
    parser.add_argument("--latest", type=Path, default=Path("latest_1.md"))
    args = parser.parse_args(list(argv) if argv is not None else None)

    if not args.report.exists():
        raise SystemExit(f"Riport fájl nem található: {args.report}")

    macro_items = load_macro_news(args.macro)
    highconv_items = load_highconv(args.highconv)

    macro_block = build_macro_block(macro_items)
    highconv_block = build_highconv_block(highconv_items)

    if not macro_block and not highconv_block:
        # Semmi új blokk → nincs teendő
        return

    inject_blocks_into_report(args.report, macro_block, highconv_block)
    # latest_1.md szinkronban tartása (ha létezik)
    maybe_update_latest(args.report, args.latest)


if __name__ == "__main__":
    main()
