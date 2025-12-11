#!/usr/bin/env python3
# postprocess_report_2.py — v2.0.0-wrapper
#
# Cél:
#   - A #2-es jelentés (Tegnapi Open→Close) utófeldolgozása
#   - Valójában csak egy vékony wrapper a fő postprocess_report.py köré,
#     hogy legyen külön, név szerint elkülönített script a #2-es futáshoz is.
#
# Használat (repo gyökérből):
#   python -X utf8 scripts/postprocess_report_2.py
#
# Előfeltételek:
#   - scripts/postprocess_report.py létezik, és tudja kezelni a --report paramétert
#   - reports/summary_report_2.md már elkészült (report_runner.py futása után)
#
from __future__ import annotations

import sys
import subprocess
from pathlib import Path


def main() -> None:
    base_dir = Path(__file__).resolve().parent.parent  # repo/
    scripts_dir = base_dir / "scripts"
    reports_dir = base_dir / "reports"

    runner = scripts_dir / "postprocess_report.py"
    if not runner.exists():
        raise SystemExit(f\"Nem található a fő postprocess script: {runner}\")

    md_path = reports_dir / "summary_report_2.md"
    bundle_dir = reports_dir

    cmd = [
        sys.executable,
        str(runner),
        "--report",
        "2",
        "--md",
        str(md_path),
        "--bundle-dir",
        str(bundle_dir),
    ]

    print(f\"[postprocess_report_2] Hívás: {' '.join(cmd)}\")
    result = subprocess.run(cmd)
    raise SystemExit(result.returncode)


if __name__ == \"__main__\":
    main()
