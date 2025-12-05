# postprocess_report_3.py
# Version: v1.0.0
"""Postprocess module for assembling the final summary_report_3.md.

This is a skeleton based on the approved architecture.
It expects:
- A raw markdown header file (summary_report_3_raw.md)
- latest_3.json (intraday prices)
- analyst_3.json
- catalysts_3.json
- high_conv_1.json

The orchestrator stitches all blocks into the final formatted report #3.
Actual logic will be implemented in later versions.
"""

import json
from typing import Any, Optional

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

# --- PLACEHOLDERS to be wired with real implementations ---
def _build_intraday_blocks(latest_json: Any):
    return "### Darabszámos tickerek – Open→Most\n", "### Watchlist – releváns ticker-mozgások\n"

def _build_analyst_block(analyst_json: Any):
    return "### Bejelentések és fel/lemínősítések (nyitástól mostanáig)\n"

def _build_catalyst_block(catalysts_json: Any):
    return "### Közelgő katalizátorok (mai módosítások)\n"

def _build_highconv_block(highconv_json: Any):
    return "### Listán kívüli, 3–12 hónapos high-conv jelöltek\n"

# -----------------------------------------------------------

def run_postprocess_3(
    raw_summary_path: str,
    latest_json_path: str,
    analyst_json_path: str,
    catalysts_json_path: str,
    highconv_json_path: str,
    output_path: str
):
    """Main assembly function for summary_report_3.md (v1.0.0 skeleton)."""

    # Load raw header / macro part
    raw_header = _load_text(raw_summary_path)

    # Load JSONs
    latest_json = _load_json(latest_json_path)
    analyst_json = _load_json(analyst_json_path)
    catalysts_json = _load_json(catalysts_json_path)
    highconv_json = _load_json(highconv_json_path)

    # Build individual blocks
    pos_block, wl_block = _build_intraday_blocks(latest_json)
    analyst_block = _build_analyst_block(analyst_json)
    catalyst_block = _build_catalyst_block(catalysts_json)
    highconv_block = _build_highconv_block(highconv_json)

    # Assemble final
    final = (
        raw_header + "\n"
        + pos_block + "\n"
        + wl_block + "\n"
        + analyst_block + "\n"
        + catalyst_block + "\n"
        + highconv_block + "\n"
    )

    # Write final report
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final)

    return output_path
