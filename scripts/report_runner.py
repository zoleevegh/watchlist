# scripts/report_runner.py
# Teljes, futókész példa:
# - Beolvassa a publikus CSV-t (pl. Google Sheets -> Publish to CSV)
# - Készít összefoglalót (Markdown) + gépileg feldolgozható JSON-t
# - mindkettőt az "out/" mappába menti (a workflow innen commitolja a repo-ba)

import argparse
import os
import json
from datetime import datetime

import pandas as pd


def run_report(report: str, csv_url: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    os.makedirs("out", exist_ok=True)

    payload = {
        "report": report,
        "timestamp": ts,
        "csv_source": csv_url or "",
        "row_count": 0,
        "columns": [],
        "sample": [],
        "notes": [],
    }

    lines = [f"# Report {report}", f"*Run:* {ts}"]

    # CSV beolvasás
    if csv_url:
        try:
            df = pd.read_csv(csv_url)
            payload["row_count"] = int(len(df))
            payload["columns"] = [str(c) for c in df.columns.tolist()]
            payload["sample"] = df.head(min(10, len(df))).to_dict(orient="records")
            lines += [
                f"*CSV source:* {csv_url}",
                f"*Rows:* {len(df)}",
                f"*Columns:* {', '.join(payload['columns'])}",
            ]
        except Exception as e:
            msg = f"**WARN:** CSV read error: {e}"
            lines.append(msg)
            payload["notes"].append(msg)
    else:
        msg = "**WARN:** No CSV URL provided (empty SECRET or input)."
        lines.append(msg)
        payload["notes"].append(msg)

    # Jelentés-specifikus helyőrzők – ide jöhet a valós logika
    if report == "1":
        lines += ["", "## Jelentés #1 – placeholder", "Írd ide az AH/premarket logikát…"]
    elif report == "2":
        lines += ["", "## Jelentés #2 – placeholder", "Írd ide a nyitás→zárás logikát…"]
    elif report == "3":
        lines += ["", "## Jelentés #3 – placeholder", "Írd ide a nyitás→most logikát…"]
    else:
        warn = "**WARN:** Ismeretlen report azonosító."
        lines.append(warn)
        payload["notes"].append(warn)

    # Kimenetek mentése
    with open("out/report_summary.md", "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))

    with open("out/report.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print("OK: out/report_summary.md")
    print("OK: out/report.json")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--report", required=True, choices=["1", "2", "3"])
    p.add_argument("--csv-url", dest="csv_url", default="")
    args = p.parse_args()
    run_report(args.report, args.csv_url)


if __name__ == "__main__":
    main()

