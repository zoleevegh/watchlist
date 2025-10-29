# scripts/report_runner.py
import argparse

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--report", required=True, choices=["1","2","3"])
    p.add_argument("--csv-url", dest="csv_url", required=False, default="")
    args = p.parse_args()

    print(f"Running report {args.report}")
    if args.csv_url:
        print(f"Using CSV: {args.csv_url}")
    # TODO: ide jön a tényleges lekérdezés + jelentésgenerálás

if __name__ == "__main__":
    main()
