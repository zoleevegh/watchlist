#!/usr/bin/env python3
"""Post-process #1 report markdown with macro / analyst / catalyst / high-conv blocks.

Usage (example):
    python postprocess_report.py --md reports/summary_report_1.md --bundle-dir bundle

This script is intentionally tolerant to JSON layout; it only relies on a few key names.
"""

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


# ---------- Macro blokk ----------


def _build_macro_block(macro_json_path: Path) -> str:
    data = _load_json(macro_json_path)
    if not data:
        return ""
    # Try several possible layouts: {headlines:[...]}, {items:[...]}, or list
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

    lines = ["### Politika / FED / Makró", ""]
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
        return ""

    lines.append("")
    return "\n".join(lines)


# ---------- Markdown manipuláció ----------


def _insert_macro_block(md: str, macro_block: str) -> str:
    if not macro_block:
        return md
    lines = md.splitlines()
    idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("Lefedettség:"):
            idx = i
            break
    if idx is None:
        # prepend if we can't find the coverage block
        return macro_block + "\n" + md
    # Insert macro block *after* the Lefedettség sor
    out = []
    out.extend(lines[: idx + 1])
    out.append("")
    out.append(macro_block)
    out.extend(lines[idx + 1 :])
    return "\n".join(out)


def _strip_job_summary(md: str) -> str:
    lines = md.splitlines()
    out = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("Job summary generated at run-time"):
            # skip
            continue
        out.append(line)
    return "\n".join(out).rstrip() + "\n"


def _append_blocks(md: str, *blocks: str) -> str:
    out = md.rstrip().splitlines()
    out.append("")  # biztosan egy üres sor a végén
    for block in blocks:
        if block:
            out.append("")
            out.extend(block.rstrip().splitlines())
            out.append("")
    return "\n".join(out).rstrip() + "\n"


# ---------- CLI ----------


def main() -> None:
    parser = argparse.ArgumentParser(description="Post-process #1 report markdown.")
    parser.add_argument("--md", required=True, help="Path to summary_report_1.md")
    parser.add_argument(
        "--bundle-dir",
        required=True,
        help="Directory where macro_news_1.json / analyst_1.json / catalysts_1.json / high_conv_1.json are located.",
    )
    args = parser.parse_args()

    md_path = Path(args.md)
    bundle_dir = Path(args.bundle_dir)

    macro_json = bundle_dir / "macro_news_1.json"
    analyst_json = bundle_dir / "analyst_1.json"
    catalysts_json = bundle_dir / "catalysts_1.json"
    highconv_json = bundle_dir / "high_conv_1.json"

    md = _read_md(md_path)

    # 1) Makró blokk a lefedettség után
    macro_block = _build_macro_block(macro_json)
    md = _insert_macro_block(md, macro_block)

    # 2) Végéről leszedjük a Job summary sort
    md = _strip_job_summary(md)

    # 3) Analyst + katalizátor + high-conv blokkok a jelentés végére
    analyst_block = build_analyst_block(analyst_json)
    catalysts_block = build_catalysts_block_from_file(catalysts_json)
    highconv_block = build_highconv_block_from_file(highconv_json)

    md = _append_blocks(md, analyst_block, catalysts_block, highconv_block)

    _write_md(md_path, md)


if __name__ == "__main__":
    main()
