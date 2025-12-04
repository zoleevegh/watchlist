#!/usr/bin/env python3
# macro_fetcher.py
# Version: 2.0.0 - Fetch macro text from Apps Script Webapp

import os
import requests
from pathlib import Path

def fetch_macro_text(report: int, out_path: str, base_url_env: str) -> str:
    url = os.environ.get(base_url_env, "").strip()
    if not url:
        return ""
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        text = r.text.strip()
    except Exception:
        return ""
    p = Path(out_path)
    p.write_text(text, encoding="utf-8")
    return text
