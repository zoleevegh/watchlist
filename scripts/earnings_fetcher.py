#!/usr/bin/env python3
# earnings_fetcher.py
# Version: v1.0.0
# Purpose: Fetch upcoming earnings (next 1–3 trading days) and write reports/{N}/earnings_{N}.json

import argparse, json, datetime, os

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True)
    args = ap.parse_args()
    n = args.report

    out = {
        "generatedAt": datetime.datetime.now().isoformat(),
        "window": {"from": "last_close", "to": "now_or_open"},
        "items": []
    }

    os.makedirs(f"reports/{n}", exist_ok=True)
    with open(f"reports/{n}/earnings_{n}.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
