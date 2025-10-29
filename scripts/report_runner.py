# scripts/report_runner.py
import argparse
import os
from datetime import datetime
import pandas as pd

def run_report(report: str, csv_url: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    os.makedirs("out", exist_ok=True)

    summary_lines = [f"# Report {report}", f"*Run:* {ts}"]
    df = None

    if csv_url:
        try:
            df = pd.read_csv(csv_url)
            summary_lines.append(f"*CSV source:* {csv_url}")
            summary_lines.append(f"*Rows:* {len(df)}")
            summary_lines.append(f"*Columns:* {', '.join(map(str, df.columns.tolist()))}")
            # minta-előkészítés
            preview_rows = min(20, len(df))
            df.head(preview_rows).to_csv("out/data_preview.csv", index=False)
        except Exception as e:
            summary_lines.append(f"**WARN:** CSV beolvasási hiba: {e}")
    else:
        summary_lines.append("**WARN:** Nincs CSV URL megadva (üres SECRET vagy input).")

    # report-specifikus placeholder logika
    if report == "1":
        summary_lines.append("\n## Jelentés #1 – placeholder\nÍrd ide az AH/premarket logikát…")
    elif report == "2":
        summary_lines.append("\n## Jelentés #2 – placeholder\nÍrd ide a nyitás→zárás logikát…")
    elif report == "3":
        summary_lines.append("\n## Jelentés #3 – placeholder\nÍrd ide a nyitás→most logikát…")
    else:
        summary_lines.append("\n**WARN:** Ismeretlen report azonosító.")

    # mentés
    with open("out/report_summary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))

    print("Kész: out/report_summary.md")
    if os.path.exists("out/data_preview.csv"):
        print("Kész: out/data_preview.csv")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--report", required=True, choices=["1","2","3"])
    p.add_argument("--csv-url", dest="csv_url", default="")
    args = p.parse_args()
    run_report(args.report, args.csv_url)

if __name__ == "__main__":
    main()
