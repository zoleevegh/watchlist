
#!/usr/bin/env python3
# analyst_marketbeat.py — v0.3.44-us-only-test
#
# PURPOSE:
# - Single-endpoint test against MarketBeat to rule out "too many endpoints" hypothesis.
# - ONLY hits: https://www.marketbeat.com/ratings/us/
# - ONE request (direct). No retries, no fallbacks.
# - If blocked -> report BLOCKED with reason.
#
# NOTE:
# This version is intentionally minimal and diagnostic.

import argparse
import requests
from pathlib import Path
from datetime import datetime
import sys

URL = "https://www.marketbeat.com/ratings/us/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"

def looks_blocked(html: str, status: int) -> bool:
    if status in (401, 403):
        return True
    markers = ["just a moment", "cdn-cgi", "cloudflare", "turnstile", "verify you are human"]
    h = html.lower()
    return any(m in h for m in markers)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    debug_dir = Path("reports/debug_marketbeat")
    debug_dir.mkdir(parents=True, exist_ok=True)

    try:
        r = requests.get(URL, headers={"User-Agent": UA}, timeout=20)
        status = r.status_code
        html = r.text
    except Exception as e:
        print(f"ERROR: request failed: {e}")
        sys.exit(2)

    if args.debug:
        (debug_dir / f"us_only_direct_{status}_{ts}.html").write_text(html, encoding="utf-8", errors="ignore")

    if looks_blocked(html, status):
        print("RESULT: BLOCKED")
        print(f"status={status}")
        print("reason=cloudflare_or_bot_protection")
        sys.exit(0)

    # If we ever get here, MarketBeat is reachable from this runner.
    print("RESULT: OK")
    print(f"status={status}")
    print("MarketBeat /ratings/us/ reachable from this environment.")
    sys.exit(0)

if __name__ == "__main__":
    main()
