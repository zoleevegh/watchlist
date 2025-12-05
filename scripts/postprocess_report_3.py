# postprocess_report_3.py
# Version: v1.1.0
"""Postprocess module for assembling the final summary_report_3.md.

v1.1.0:
- Integrates real calls to blocks_intraday_3 and blocks_events_3.
- Adds ordering, safety checks, and consistent markdown assembly.
- Still defensive: missing blocks do not crash the report.

Expected inputs:
- raw_summary_path: markdown with coverage + macro
- latest_json_path: intraday computed JSON
- analyst_json_path: Apps Script analyst feed
- catalysts_json_path: Apps Script catalyst feed
- highconv_json_path: high-conv JSON
"""

import json
from typing import Any, Optional

# --- Real imports (modules must exist in /scripts) ---
try:
    from blocks_intraday_3 import build_intraday_blocks
except Exception:
    def build_intraday_blocks(_):
        return ("### Darabszámos tickerek – Open→Most\n(Hiba: intraday modul nem elérhető)\n",
                "### Watchlist – releváns ticker-mozgások\n(Hiba: intraday modul nem elérhető)\n")

try:
    from blocks_events_3 import (
        build_analyst_block,
        build_catalyst_block,
        build_highconv_block,
    )
except Exception:
    def build_analyst_block(_): return "### Bejelentések és fel/lemínősítések\n(Hiba: events modul nem elérhető)\n"
    def build_catalyst_block(_): return "### Közelgő katalizátorok\n(Hiba: events modul nem elérhető)\n"
    def build_highconv_block(_): return "### High-conv jelöltek\n(Hiba: events modul nem elérhető)\n"


def _load_json(path: str) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _load_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def run_postprocess_3(
    raw_summary_path: str,
    latest_json_path: str,
    analyst_json_path: str,
    catalysts_json_path: str,
    highconv_json_path: str,
    output_path: str
):
    """Main assembly function for summary_report_3.md (v1.1.0)."""

    # 1) Raw header (coverage + macro)
    raw_header = _load_text(raw_summary_path)

    # 2) JSON inputs
    latest_json = _load_json(latest_json_path)
    analyst_json = _load_json(analyst_json_path)
    catalysts_json = _load_json(catalysts_json_path)
    highconv_json = _load_json(highconv_json_path)

    # 3) Build major blocks
    pos_block, wl_block = build_intraday_blocks(latest_json)
    analyst_block = build_analyst_block(analyst_json)
    catalyst_block = build_catalyst_block(catalysts_json)
    highconv_block = build_highconv_block(highconv_json)

    # 4) Assemble final markdown in canonical #3 order
    sections = [
        raw_header.strip(),
        pos_block.strip(),
        wl_block.strip(),
        analyst_block.strip(),
        catalyst_block.strip(),
        highconv_block.strip(),
        "",
    ]

    final_md = "\n\n".join(sec for sec in sections if sec)

    # 5) Write the final summary_report_3.md
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_md + "\n")

    return output_path

