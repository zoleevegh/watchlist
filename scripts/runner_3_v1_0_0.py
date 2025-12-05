# runner_3.py
# Version: v1.0.0
# Main runner for Report #3 (intraday)

import json
import sys
from pathlib import Path

from postprocess_report_3 import run_postprocess_3

BASE = Path(__file__).resolve().parent.parent
R3 = BASE / "reports" / "3"

RAW = R3 / "summary_report_3_raw.md"
LATEST = R3 / "latest_3.json"
ANALYST = R3 / "analyst_3.json"
CATALYSTS = R3 / "catalysts_3.json"
HIGHCONV = R3 / "high_conv_1.json"
OUTPUT = R3 / "summary_report_3.md"

def main():
    run_postprocess_3(
        raw_summary_path=str(RAW),
        latest_json_path=str(LATEST),
        analyst_json_path=str(ANALYST),
        catalysts_json_path=str(CATALYSTS),
        highconv_json_path=str(HIGHCONV),
        output_path=str(OUTPUT)
    )
    print("Report #3 generated:", OUTPUT)

if __name__ == "__main__":
    main()
