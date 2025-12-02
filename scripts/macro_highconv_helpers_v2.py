"""
macro_highconv_helpers_v2.py

Helper függvények a #1-es jelentés (summary_report_1.md) utólagos módosításához:
- macro_news_1.json → "Politika / FED / Makró" blokk a riport ELEJÉRE (Lefedettség: után)
- high_conv_1.json  → "Listán kívüli, 3–12 hónapos high-conviction jelöltek" blokk a riport VÉGÉRE
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Union
import json


JsonList = List[Dict[str, Any]]
PathLike = Union[str, Path]


def _load_json_items(path: PathLike) -> JsonList:
    """
    macro_news_1.json / high_conv_1.json betöltése.
    Elfogad: listát vagy {'items': [...]} / {'events': [...]} / {'data': [...]} dictet.
    Ha a fájl nem létezik, üres listát ad vissza.
    """
    p = Path(path)
    if not p.is_file():
        return []

    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "events", "data"):
            items = data.get(key)
            if isinstance(items, list):
                return items

    return []


def _parse_iso_dt(value: str) -> str:
    """
    ISO datetime → 'YYYY-MM-DD HH:MM' (lokális konverzió nélkül, egyszerűen).
    Ha nem értelmezhető, üres stringet adunk vissza.
    """
    if not value:
        return ""
    try:
        # 'Z' → '+00:00'
        v = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(v)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def render_macro_block(macro_items: JsonList, max_lines: int = 5) -> str:
    """
    'Politika / FED / Makró – kiemelt hírek' blokk markdownban.

    Elvárt JSON (példa):
    [
      {
        "published": "2025-11-28T14:30:00Z",
        "headline": "Fed official signals no cuts before mid-2026",
        "source": "Reuters",
        "short_summary": "Hawkish beszéd, hozam-emelkedés",
        "impact_score": 0.9
      },
      ...
    ]

    A listát impact_score szerint rendezi (ha van), és legfeljebb max_lines sort vesz fel.
    Ha nincs érdemi adat, üres stringgel tér vissza.
    """
    if not macro_items:
        return ""

    def _score(it: Dict[str, Any]) -> float:
        return float(it.get("impact_score") or it.get("score") or 0.0)

    sorted_items = sorted(macro_items, key=_score, reverse=True)
    top_items = sorted_items[:max_lines]

    lines: List[str] = []
    lines.append("### Politika / FED / Makró – kiemelt hírek")
    lines.append("")

    for item in top_items:
        ts = _parse_iso_dt(str(item.get("published") or item.get("time") or ""))
        headline = (item.get("headline") or item.get("title") or "").strip()
        source = (item.get("source") or "").strip()
        short = (item.get("short_summary") or item.get("summary") or "").strip()

        parts: List[str] = []
        if ts:
            parts.append(f"[{ts}]")
        if headline:
            parts.append(headline)
        if source:
            parts.append(f"({source})")

        base = " ".join(parts).strip()
        if not base:
            continue

        if short:
            line = f"- {base} – {short}"
        else:
            line = f"- {base}"

        lines.append(line)

    if len(lines) <= 2:
        # Nem sikerült érdemi sort generálni
        return ""

    lines.append("")
    return "\n".join(lines)


def render_highconv_block(hc_items: JsonList) -> str:
    """
    'Listán kívüli, 3–12 hónapos high-conviction jelöltek' blokk markdownban.

    Elvárt JSON (javasolt forma):
    [
      {
        "ticker": "ABC",
        "company": "ABC Corp",
        "thesis": "3–12 hónapos re-rating katalizátor: AI infra capex ciklus.",
        "score": 0.86,
        "signals": [
          "3 friss felminősítés nagy házaktól",
          "EPS-konszenzus felfelé húzva",
          "közelgő terméklaunch 6 hónapon belül"
        ]
      },
      ...
    ]

    A listát score szerint rendezi (ha van), és csak akkor ad vissza blokkot,
    ha legalább 1 elem értelmesen felépíthető.
    """
    if not hc_items:
        return ""

    def _score(it: Dict[str, Any]) -> float:
        return float(it.get("score") or 0.0)

    sorted_items = sorted(hc_items, key=_score, reverse=True)

    lines: List[str] = []
    lines.append("### Listán kívüli, 3–12 hónapos high-conviction jelöltek")
    lines.append("")
    lines.append("_Csak a portfólión / watchlisten kívüli nevek szerepelnek._")
    lines.append("")

    added_any = False

    for item in sorted_items:
        ticker = (item.get("ticker") or "").strip()
        company = (item.get("company") or item.get("name") or "").strip()
        thesis = (item.get("thesis") or item.get("reason") or "").strip()
        score_val = item.get("score")

        header_parts: List[str] = []
        if ticker:
            header_parts.append(f"**{ticker}**")
        if company:
            header_parts.append(company)
        if score_val is not None:
            try:
                header_parts.append(f"(score: {float(score_val):.2f})")
            except Exception:
                pass

        header_line = " ".join(header_parts).strip()
        if not header_line and not thesis:
            # sem ticker, sem érdemi szöveg → ugorjuk
            continue

        added_any = True

        if header_line:
            lines.append(f"- {header_line}")
        if thesis:
            lines.append(f"  - {thesis}")

        signals = item.get("signals") or item.get("drivers") or []
        if isinstance(signals, list) and signals:
            for s in signals:
                s_str = str(s).strip()
                if s_str:
                    lines.append(f"    - {s_str}")

        lines.append("")

    if not added_any:
        return ""

    # Végére egyetlen newline
    return "\n".join(lines).rstrip() + "\n"


def inject_macro_and_highconv_blocks(
    report_md_path: PathLike = "summary_report_1.md",
    macro_json_path: PathLike = "macro_news_1.json",
    highconv_json_path: PathLike = "high_conv_1.json",
) -> None:
    """
    A már elkészült summary_report_1.md módosítása:

    - Beolvassa az eredeti tartalmat.
    - Ha van makró blokk (macro_news_1.json → render_macro_block),
      azt a 'Lefedettség:' sort követően szúrja be.
    - Ha van high-conv blokk (high_conv_1.json → render_highconv_block),
      azt a fájl végére fűzi.

    Ha bármelyik JSON nem létezik vagy üres, az adott blokk egyszerűen kimarad.
    """
    report_path = Path(report_md_path)
    if not report_path.is_file():
        raise FileNotFoundError(f"Nem találom a riportot: {report_path}")

    text = report_path.read_text(encoding="utf-8")

    macro_items = _load_json_items(macro_json_path)
    hc_items = _load_json_items(highconv_json_path)

    macro_block = render_macro_block(macro_items).strip()
    highconv_block = render_highconv_block(hc_items).strip()

    lines = text.splitlines()

    # Makró blokk beszúrása a 'Lefedettség:' sort követően
    insert_idx = None
    for i, line in enumerate(lines):
        if line.startswith("Lefedettség:"):
            insert_idx = i + 1
            break

    new_lines = lines
    if macro_block:
        if insert_idx is None:
            # Ha nincs Lefedettség sor, a fájl elejére kerül
            new_lines = [macro_block, ""] + lines
        else:
            new_lines = (
                lines[:insert_idx]
                + [""]
                + [macro_block]
                + [""]
                + lines[insert_idx:]
            )

    text_with_macro = "\n".join(new_lines)

    # High-conv blokk a végére
    if highconv_block:
        if not text_with_macro.endswith("\n"):
            text_with_macro += "\n"
        text_with_macro = text_with_macro.rstrip() + "\n\n" + highconv_block + "\n"

    report_path.write_text(text_with_macro, encoding="utf-8")


if __name__ == "__main__":
    # Egyszerű manuális teszt / példa:
    #
    # 1) Tegyél a könyvtárba egy summary_report_1.md-t
    # 2) Adj meg egy macro_news_1.json / high_conv_1.json fájlt (dummy is lehet)
    # 3) Futtasd:
    #       python macro_highconv_helpers_v2.py
    #
    # A script módosítja a summary_report_1.md-t a fenti logika szerint.
    inject_macro_and_highconv_blocks()
