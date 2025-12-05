#!/usr/bin/env python3
# report_runner_3.py
# Version: v1.1.0
#
# Fő futtató a #3-as jelentéshez (Ma Open→Most, intraday).
# Feladat:
#   - summary_report_3_raw.md + latest_3.json + analyst_3.json + catalysts_3.json + high_conv_1.json
#     felhasználásával legenerálja a végleges reports/summary_report_3.md fájlt.
#
# A szükséges input fájloknak a `reports/` könyvtárban kell lenniük:
#   reports/summary_report_3_raw.md
#   reports/latest_3.json
#   reports/analyst_3.json
#   reports/catalysts_3.json
#   reports/high_conv_1.json

from pathlib import Path

from postprocess_report_3 import run_postprocess_3


def main() -> None:
    base = Path(__file__).resolve().parent.parent
    rdir = base / "reports"

    raw_md = rdir / "summary_report_3_raw.md"
    latest_json = rdir / "latest_3.json"
    analyst_json = rdir / "analyst_3.json"
    catalysts_json = rdir / "catalysts_3.json"
    highconv_json = rdir / "high_conv_1.json"
    output_md = rdir / "summary_report_3.md"

    run_postprocess_3(
        raw_summary_path=str(raw_md),
        latest_json_path=str(latest_json),
        analyst_json_path=str(analyst_json),
        catalysts_json_path=str(catalysts_json),
        highconv_json_path=str(highconv_json),
        output_path=str(output_md),
    )
    print(f\"[runner_3] Jelentés kész: {output_md}\")


if __name__ == \"__main__\":
    main()
