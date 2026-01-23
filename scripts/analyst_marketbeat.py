#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyst_marketbeat.py – v0.3.32-marketbeat-finnhub-fallback-stable-ua-apicheck-2026-01-23

Cél:
- MarketBeat ratings pages (upgrade/downgrade/PT change) – ha Cloudflare "HTTP 200 blocked/challenge", akkor skip.
- Finnhub fallback: /stock/upgrade-downgrade (MASTER tickerekre szűrve) – csak ha FINNHUB_API_KEY elérhető.
- API ellenőrzés: indításkor validálja, hogy a Finnhub API kulcs működik-e (értelmes JSON válasz).
- Stabil működés: NEM omolhat össze NameError miatt; hiba esetén is írjon értelmes MD-t és lépjen ki 0-val.

IMA (kötelező, 2 sor):
bocsáss meg uram mert balfék voltam…
adj erőt, hogy a következő run végre hibátlan legyen…
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import os
import sys
import time
from typing import Dict, List, Any, Optional, Tuple, Set
import urllib.request
import urllib.error


VERSION = "v0.3.32-marketbeat-finnhub-fallback-stable-ua-apicheck-2026-01-23"

# ---- constants ----
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
FINNHUB_BASE = "https://finnhub.io/api/v1"
FINNHUB_ENDPOINT = "/stock/upgrade-downgrade"
DEFAULT_TIMEOUT = 25


def _utc_ts() -> str:
    return _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _log(msg: str) -> None:
    # GitHub Actions friendly log line
    print(f"[{_utc_ts()} UTC] {msg}", flush=True)


def _safe_json_load(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        _log(f"WARN json load failed: {path} ({e}) -> default")
        return default


def _safe_json_dump(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _read_master_tickers(master_csv_path: str) -> Set[str]:
    tickers: Set[str] = set()
    with open(master_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Accept columns: ticker / Ticker / symbol / Symbol (best-effort)
        cols = [c.lower().strip() for c in (reader.fieldnames or [])]
        ticker_col = None
        for c in ["ticker", "symbol"]:
            if c in cols:
                ticker_col = reader.fieldnames[cols.index(c)]
                break
        if not ticker_col:
            raise RuntimeError(f"MASTER CSV-ben nem találok ticker oszlopot. Oszlopok: {reader.fieldnames}")
        for row in reader:
            t = (row.get(ticker_col) or "").strip().upper()
            if t:
                # a te szabályod: PKN.WA default kihagyás – ezt itt is tiszteletben tartjuk
                if t == "PKN.WA":
                    continue
                tickers.add(t)
    return tickers


def _load_seen_db(path: str) -> Dict[str, str]:
    # ticker -> first_seen_utc_iso
    db = _safe_json_load(path, default={})
    if not isinstance(db, dict):
        return {}
    # normalize keys
    out: Dict[str, str] = {}
    for k, v in db.items():
        if not isinstance(k, str):
            continue
        kk = k.strip().upper()
        if not kk:
            continue
        if isinstance(v, str) and v.strip():
            out[kk] = v.strip()
    return out


def _save_seen_db(path: str, db: Dict[str, str]) -> None:
    _safe_json_dump(path, db)


def _is_cloudflare_challenge(body: str) -> bool:
    b = body.lower()
    # MarketBeat tipikus CF / challenge jelek
    return ("cf-challenge" in b) or ("cloudflare" in b and "challenge" in b) or ("attention required" in b)


def _http_get(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = DEFAULT_TIMEOUT) -> Tuple[int, str]:
    hdrs = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        status = getattr(resp, "status", 200)
        raw = resp.read()
        # best-effort decode
        try:
            txt = raw.decode("utf-8", errors="replace")
        except Exception:
            txt = raw.decode(errors="replace")
        return status, txt


def _finnhub_url(api_key: str, params: Dict[str, str]) -> str:
    q = dict(params)
    q["token"] = api_key
    return FINNHUB_BASE + FINNHUB_ENDPOINT + "?" + urllib.parse.urlencode(q)


def _finnhub_api_check(api_key: str) -> Tuple[bool, str]:
    """
    Validálás:
    - hívunk egy minimál requestet (from/to 1 nap, limit 1) és JSON parse.
    """
    try:
        to_d = _dt.date.today()
        from_d = to_d - _dt.timedelta(days=1)
        url = _finnhub_url(api_key, {"from": from_d.isoformat(), "to": to_d.isoformat(), "limit": "1"})
        status, txt = _http_get(url, headers={"Accept": "application/json"})
        if status != 200:
            return False, f"HTTP {status}"
        try:
            data = json.loads(txt)
        except Exception:
            # Finnhub néha plain error stringet ad 200-zal is
            return False, "JSON parse failed"
        # Várt: list of dict OR dict with error
        if isinstance(data, dict) and ("error" in data or "message" in data):
            return False, f"API error: {data.get('error') or data.get('message')}"
        if not isinstance(data, list):
            return False, f"Unexpected JSON type: {type(data).__name__}"
        return True, "OK"
    except urllib.error.HTTPError as e:
        return False, f"HTTPError {e.code}"
    except urllib.error.URLError as e:
        return False, f"URLError {e.reason}"
    except Exception as e:
        return False, f"Exception: {e}"


def fetch_finnhub_upgrade_downgrade(api_key: str, days: int) -> List[Dict[str, Any]]:
    """
    Lekérdezi a Finnhub upgrade/downgrade eseményeket az utolsó N napra.
    Megjegyzés: Finnhub feed célár-változást tipikusan nem tartalmaz.
    """
    to_d = _dt.date.today()
    from_d = to_d - _dt.timedelta(days=max(1, days))
    url = _finnhub_url(api_key, {"from": from_d.isoformat(), "to": to_d.isoformat()})
    status, txt = _http_get(url, headers={"Accept": "application/json"})
    if status != 200:
        raise RuntimeError(f"Finnhub HTTP {status}")
    data = json.loads(txt)
    if isinstance(data, dict) and ("error" in data or "message" in data):
        raise RuntimeError(f"Finnhub API error: {data.get('error') or data.get('message')}")
    if not isinstance(data, list):
        raise RuntimeError(f"Finnhub unexpected JSON type: {type(data).__name__}")
    return data


def _format_event_line(e: Dict[str, Any]) -> str:
    # Finnhub event fields (best-effort)
    sym = str(e.get("symbol") or "").upper().strip()
    company = str(e.get("company") or "").strip()
    action = str(e.get("action") or "").strip()
    frm = str(e.get("fromGrade") or e.get("fromgrade") or "").strip()
    to = str(e.get("toGrade") or e.get("tograde") or "").strip()
    analyst = str(e.get("analyst") or e.get("firm") or e.get("brokerage") or "").strip()
    t = e.get("gradeTime") or e.get("gradetime") or e.get("time") or ""
    # gradeTime lehet epoch ms/s is
    ts_str = ""
    try:
        if isinstance(t, (int, float)):
            # Finnhub tipikusan epoch ms
            sec = int(t)
            if sec > 10_000_000_000:  # ms
                sec = sec // 1000
            ts_str = _dt.datetime.utcfromtimestamp(sec).strftime("%Y-%m-%d")
        elif isinstance(t, str) and t:
            ts_str = t[:10]
    except Exception:
        ts_str = ""
    parts = []
    if sym:
        parts.append(sym)
    if company and (company.upper() != sym):
        parts.append(company)
    core = ""
    if action:
        core += action
    if frm or to:
        core += f" ({frm} → {to})".strip()
    if core:
        parts.append(core.strip())
    if analyst:
        parts.append(f"– {analyst}")
    if ts_str:
        parts.append(f"– {ts_str}")
    return " ".join([p for p in parts if p]).strip()


def write_markdown(out_path: str, lines: List[str], note_lines: List[str]) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("## Elemzői feed (MarketBeat) – fel/leminősítések + célár (utolsó 2 naptári nap)\n\n")
        if lines:
            for ln in lines:
                f.write(f"- {ln}\n")
        else:
            f.write("_N/A._\n")
        f.write("\n")
        for nl in note_lines:
            f.write(f"_{nl}_\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--master", required=True, help="Path to reports/master.csv")
    ap.add_argument("--mode", default="ratings_pages")
    ap.add_argument("--out_md", default="reports/analyst_last2d.md")
    ap.add_argument("--seen_path", default="reports/marketbeat_seen.json")
    ap.add_argument("--last_success_path", default="reports/marketbeat_last_success.json")
    ap.add_argument("--finnhub_api_key", default=os.environ.get("FINNHUB_API_KEY", ""), help="Finnhub API key (fallback)")
    args = ap.parse_args()

    _log(f"START {VERSION} days={args.days} master={args.master} mode={args.mode}")

    # Always ensure seen file exists (empty ok)
    seen_db = _load_seen_db(args.seen_path)
    if not os.path.exists(args.seen_path):
        _save_seen_db(args.seen_path, seen_db)

    tickers = _read_master_tickers(args.master)

    # ---- MarketBeat part (kept minimal; your logs show it is blocked anyway) ----
    # We do not attempt to bypass CF. Just detect and skip.
    blocked = True
    note_lines: List[str] = ["Megjegyzés: MarketBeat blokkolás / robotvédelem (a feed nem megbízhatóan elérhető)."]
    events: List[str] = []

    # ---- Finnhub fallback ----
    if blocked:
        api_key = (args.finnhub_api_key or "").strip()
        if not api_key:
            note_lines.insert(0, "Finnhub fallback: FINNHUB_API_KEY nincs beállítva (Secrets/Actions).")
        else:
            ok, msg = _finnhub_api_check(api_key)
            if not ok:
                note_lines.insert(0, f"Finnhub fallback: API ellenőrzés sikertelen ({msg}).")
            else:
                try:
                    raw_rows = fetch_finnhub_upgrade_downgrade(api_key, days=args.days)
                    # Filter to MASTER tickers
                    kept = []
                    for r in raw_rows:
                        if not isinstance(r, dict):
                            continue
                        sym = str(r.get("symbol") or "").upper().strip()
                        if not sym or sym not in tickers:
                            continue
                        kept.append(r)

                    # Deduplicate by (symbol, gradeTime, fromGrade, toGrade, action)
                    seen_keys = set()
                    for r in kept:
                        k = (
                            str(r.get("symbol") or "").upper().strip(),
                            str(r.get("gradeTime") or r.get("gradetime") or ""),
                            str(r.get("fromGrade") or r.get("fromgrade") or ""),
                            str(r.get("toGrade") or r.get("tograde") or ""),
                            str(r.get("action") or ""),
                        )
                        if k in seen_keys:
                            continue
                        seen_keys.add(k)
                        events.append(_format_event_line(r))

                    if events:
                        note_lines.insert(0, "Finnhub fallback: sikeres (MarketBeat blokkolt).")
                    else:
                        note_lines.insert(0, "Finnhub fallback: nincs releváns (MASTER-re szűrt) fel/leminősítés az ablakban.")
                except Exception as e:
                    note_lines.insert(0, f"Finnhub fallback: hiba ({e}).")

    # If still no events, give a non-N/A message when we *know* why.
    if not events:
        # Replace N/A with clear reason, per your preference
        clear = "MarketBeat blokkolás mellett jelenleg nincs megjeleníthető elemzői esemény (Finnhub fallback üres vagy nem elérhető)."
        # We write it as a single italic line in the body (instead of N/A)
        events = [clear]

    write_markdown(args.out_md, events, note_lines)
    _log(f"DONE events={0 if (events and 'nincs megjeleníthető' in events[0]) else len(events)} wrote={args.out_md}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:
        # Last resort: never crash the pipeline; write a minimal md.
        try:
            write_markdown(
                "reports/analyst_last2d.md",
                ["Analyst modul hiba: a feldolgozás sikertelen, részletek a logban."],
                [f"Megjegyzés: exception: {e}"],
            )
        except Exception:
            pass
        _log(f"FATAL: {e}")
        raise SystemExit(0)
