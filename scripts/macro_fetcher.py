import os
import json
from typing import Optional
from urllib import request, error


def fetch_macro_text(
    report: int = 1,
    out_path: str = "reports/macro_1.txt",
    base_url_env: str = "MACRO_FEED_URL_1",
) -> str:
    """
    Makró / FED / piaci hangulat szöveg betöltése az Apps Script webappból.

    - report: 1 / 2 / 3 (#1/#2/#3)
    - out_path: ide írjuk ki a nyers szöveget (log / debug)
    - base_url_env: env-ben lévő base URL (pl. MACRO_FEED_URL_1)

    Visszaad: a 'text' mező, amit közvetlenül át lehet adni
    a biblia_helper.format_macro_block() függvénynek.
    """
    env_name = base_url_env
    if report == 2:
        env_name = "MACRO_FEED_URL_2"
    elif report == 3:
        env_name = "MACRO_FEED_URL_3"

    base_url = os.environ.get(env_name)
    if not base_url:
        return ""

    # Ha a base_url-ben nincs report paraméter, itt hozzáfűzzük
    if "report=" not in base_url:
        if "?" in base_url:
            url = f"{base_url}&report={report}"
        else:
            url = f"{base_url}?report={report}"
    else:
        url = base_url

    try:
        req = request.Request(url, headers={"User-Agent": "watchlist-macro-fetcher/1.0"})
        with request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except error.HTTPError as e:
        print(f"[macro_fetcher] HTTP error: {e}")
        return ""
    except Exception as e:
        print(f"[macro_fetcher] Fetch error: {e}")
        return ""

    try:
        data = json.loads(raw)
    except Exception as e:
        print(f"[macro_fetcher] JSON parse error: {e}")
        return ""

    text = str(data.get("text", "")).strip()
    if not text:
        return ""

    # Írjuk ki logként
    try:
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        pass

    return text
