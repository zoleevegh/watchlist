#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyst_marketbeat.py — MarketBeat analyst feed (FREE) — upgrades/downgrades/price target changes

Version: v0.3.41-marketbeat-rolling-window-hu-2026-02-04

IMÁDSÁG (2 sor):
Bocsáss meg Uram, hogy napokig a dátumot kerestem ott, ahol nincs.
Adj erőt, hogy cache-ből építsek gördülő ablakot, és ne szívassam a gazdámat. 🙏

Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

Miért így?
- A MarketBeat FREE /ratings/* listák „Today’s …” jellegűek, soronkénti timestamp nélkül.
- Emiatt N napos visszatekintést NEM lehet megbízhatóan „vissza-le” lekérdezéssel kérni.
- A helyes megoldás: minden futáskor begyűjtjük aznapi listát, és perzisztens cache-be tesszük.
- A riportban az utolsó N naptári nap cache-éből állítjuk össze a feed-et (akkor is, ha ma épp üres).

Kimenet:
- Markdown blokk (ticker csoportosítva)
- JSON payload

Fájlok:
- reports/marketbeat_seen.json  (cache — a workflow már ezt restore/save-eli)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import html
import json
import random
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, asdict
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Dict, List, Optional, Tuple

VERSION = "v0.3.41-marketbeat-rolling-window-hu-2026-02-04"
BASE = "https://www.marketbeat.com"
JINA = "https://r.jina.ai/http://www.marketbeat.com"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# MarketBeat FREE sources (today's lists)
RATINGS_SOURCES: List[Tuple[str, str]] = [
    ("upgrade", "/ratings/upgrades/"),
    ("downgrade", "/ratings/downgrades/"),
    ("pt_change", "/ratings/pricetargetchanges/"),
]

ACTION_HU = {
    "upgrade": "Felminősítés",
    "downgrade": "Lemínősítés",
    "pt_change": "Célár módosítás",
}

RATING_HU = {
    "Strong Buy": "Erős vétel",
    "Buy": "Vétel",
    "Outperform": "Felülteljesítés",
    "Overweight": "Felülsúlyozás",
    "Accumulate": "Gyűjtés",
    "Positive": "Pozitív",
    "Moderate Buy": "Mérsékelt vétel",
    "Neutral": "Semleges",
    "Hold": "Tartás",
    "Equal-Weight": "Semleges súly",
    "Market Perform": "Piaci teljesítés",
    "In-Line": "Piaccal megegyező",
    "Peer Perform": "Szektortársakkal azonos",
    "Underperform": "Alulteljesítés",
    "Underweight": "Alulsúlyozás",
    "Reduce": "Csökkentés",
    "Sell": "Eladás",
    "Strong Sell": "Erős eladás",
}


@dataclass
class AnalystEvent:
    ticker: str
    date: str  # ISO date YYYY-MM-DD (last seen date in cache)
    firm: str
    action: str  # HU label
    rating_from: Optional[str]
    rating_to: Optional[str]
    pt_from: Optional[float]
    pt_to: Optional[float]
    currency: str
    source: str


def _event_key(e: AnalystEvent) -> str:
    """Stable dedup key across runs."""

    def norm(x) -> str:
        if x is None:
            return ""
        if isinstance(x, float):
            return f"{x:.4f}"
        return str(x).strip()

    raw = "|".join(
        [
            e.ticker.upper().strip(),
            norm(e.firm),
            norm(e.action),
            norm(e.rating_from),
            norm(e.rating_to),
            norm(e.pt_from),
            norm(e.pt_to),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _log(msg: str) -> None:
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}Z] {msg}", flush=True)


def _mk_opener() -> urllib.request.OpenerDirector:
    cj = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    return opener


def _fetch_url(opener: urllib.request.OpenerDirector, url: str, timeout: int) -> Tuple[int, str, bytes]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "close",
        },
        method="GET",
    )
    try:
        with opener.open(req, timeout=timeout) as resp:
            code = getattr(resp, "status", 200)
            ctype = resp.headers.get("Content-Type", "")
            raw = resp.read()
            if resp.headers.get("Content-Encoding", "").lower() == "gzip":
                raw = gzip.decompress(raw)
            return int(code), ctype, raw
    except urllib.error.HTTPError as e:
        try:
            raw = e.read()
        except Exception:
            raw = b""
        return int(e.code), "", raw
    except Exception as e:
        return 0, "", (str(e).encode("utf-8", errors="ignore"))


_CHALLENGE_RE = re.compile(r"(cloudflare|cf-.*?challenge|captcha|verify you are human|attention required)", re.I)


def _looks_blocked(html_text: str) -> bool:
    if not html_text:
        return True
    if _CHALLENGE_RE.search(html_text):
        return True
    # known page signatures
    if "Stock Analyst Upgrades" in html_text:
        return False
    if "Stock Analyst Downgrades" in html_text:
        return False
    if "Price Target Changes" in html_text:
        return False
    # heuristic
    if len(html_text) < 2000:
        return True
    return False


def _fetch_with_jina_fallback(
    opener: urllib.request.OpenerDirector,
    url: str,
    timeout: int,
    debug_dir: Optional[Path],
    tag: str,
) -> Tuple[bool, int, str]:
    """Returns ok, http_code, html_text."""
    code, _, raw = _fetch_url(opener, url, timeout)
    txt = raw.decode("utf-8", errors="ignore")
    if debug_dir:
        debug_dir.mkdir(parents=True, exist_ok=True)
        (debug_dir / f"{tag}_direct_{code}.html").write_text(txt, encoding="utf-8")
    if code and 200 <= code < 400 and not _looks_blocked(txt):
        return True, code, txt

    jina_url = f"{JINA}{url.replace('https://www.marketbeat.com', '')}"
    time.sleep(0.6 + random.random() * 0.6)
    code2, _, raw2 = _fetch_url(opener, jina_url, timeout)
    txt2 = raw2.decode("utf-8", errors="ignore")
    if debug_dir:
        (debug_dir / f"{tag}_jina_{code2}.html").write_text(txt2, encoding="utf-8")
    if code2 and 200 <= code2 < 400 and not _looks_blocked(txt2):
        return True, code2, txt2

    return False, (code2 or code), (txt2 or txt)


def read_master_tickers(master_csv: Path) -> List[str]:
    cols = ["ticker", "Ticker", "symbol", "Symbol"]
    out: List[str] = []
    with master_csv.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            return out
        field = None
        for c in cols:
            if c in rdr.fieldnames:
                field = c
                break
        if not field:
            field = rdr.fieldnames[0]
        for row in rdr:
            t = (row.get(field) or "").strip().upper()
            if t and re.fullmatch(r"[A-Z.\-]{1,10}", t):
                out.append(t)
    return sorted(set(out))


_TICKER_FROM_URL = re.compile(r"/stocks/[a-z]+/([a-z0-9.\-]+)/", re.I)
_TICKER_DATA_CLEAN = re.compile(r"data-clean\s*=\s*\"(.*?)\"", re.I)


def _strip_tags(s: str) -> str:
    s = re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", "", s, flags=re.I)
    s = re.sub(r"<style\b[^<]*(?:(?!</style>)<[^<]*)*</style>", "", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_ticker(row_html: str) -> Optional[str]:
    m = _TICKER_DATA_CLEAN.search(row_html)
    if m:
        t = html.unescape(m.group(1)).strip().upper()
        if re.fullmatch(r"[A-Z.\-]{1,10}", t):
            return t

    m = _TICKER_FROM_URL.search(row_html)
    if m:
        t = m.group(1).strip().upper()
        if re.fullmatch(r"[A-Z0-9.\-]{1,10}", t):
            return t

    txt = _strip_tags(row_html)
    tok = (txt.split(" ", 1)[0] if txt else "").strip().upper()
    if re.fullmatch(r"[A-Z.\-]{1,10}", tok):
        return tok
    return None


_ARROW_RE = re.compile(r"(\$[0-9.,]+)\s*(?:→|->|➝|to)\s*(\$[0-9.,]+)", re.I)
_DOLLAR_RE = re.compile(r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)")


def _parse_price_targets(row_html: str) -> Tuple[Optional[float], Optional[float], str]:
    txt = _strip_tags(row_html)
    cur = "USD"
    m = _ARROW_RE.search(txt)
    if m:
        fa = _DOLLAR_RE.search(m.group(1))
        fb = _DOLLAR_RE.search(m.group(2))
        if fa and fb:
            return float(fa.group(1).replace(",", "")), float(fb.group(1).replace(",", "")), cur

    dollars = _DOLLAR_RE.findall(txt)
    if dollars:
        last = float(dollars[-1].replace(",", ""))
        return None, last, cur

    return None, None, cur


def _parse_rating_change(row_html: str) -> Tuple[Optional[str], Optional[str]]:
    txt = _strip_tags(row_html)
    m = re.search(r"([A-Za-z\- ]{3,30})\s*(?:→|->|➝)\s*([A-Za-z\- ]{3,30})", txt)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    for r in [
        "Strong Buy",
        "Moderate Buy",
        "Buy",
        "Hold",
        "Neutral",
        "Sell",
        "Strong Sell",
        "Outperform",
        "Underperform",
        "Overweight",
        "Underweight",
        "Market Perform",
        "Equal-Weight",
    ]:
        if r.lower() in txt.lower():
            return None, r

    return None, None


def _parse_firm(row_html: str) -> str:
    txt = _strip_tags(row_html)
    m = re.search(r"\bby\b\s+([A-Za-z0-9.&'\- ]{3,60})", txt)
    if m:
        firm = m.group(1).strip()
        firm = re.split(r"\b(Analyst|Current|Price|Target|Rating)\b", firm)[0].strip()
        firm = re.sub(r"\s{2,}", " ", firm)
        return firm[:60]
    return ""


def _extract_rows(html_text: str) -> List[str]:
    idx = html_text.lower().find(">company<")
    if idx < 0:
        idx = html_text.lower().find("company</")
    if idx < 0:
        return []
    sl = html_text[idx : idx + 220000]
    rows = re.findall(r"(<tr\b[^>]*>.*?</tr>)", sl, flags=re.I | re.S)
    out = []
    for r in rows:
        if "Company" in r and "Brokerage" in r:
            continue
        if "/stocks/" in r or "data-clean" in r:
            out.append(r)
    return out


def parse_events_from_page(
    page_html: str,
    source_url: str,
    action_key: str,
    master_set: set[str],
    run_date_iso: str,
) -> List[AnalystEvent]:
    rows = _extract_rows(page_html)
    events: List[AnalystEvent] = []

    for row in rows:
        t = _extract_ticker(row)
        if not t:
            continue
        t = t.upper()
        if t not in master_set:
            continue

        firm = _parse_firm(row)
        r_from, r_to = _parse_rating_change(row)
        pt_from, pt_to, cur = _parse_price_targets(row)
        action_hu = ACTION_HU.get(action_key, action_key)

        events.append(
            AnalystEvent(
                ticker=t,
                date=run_date_iso,
                firm=firm,
                action=action_hu,
                rating_from=r_from,
                rating_to=r_to,
                pt_from=pt_from,
                pt_to=pt_to,
                currency=cur,
                source=source_url,
            )
        )

    return events


def load_seen_cache(path: Path) -> Dict:
    if not path.exists():
        return {"version": VERSION, "events": {}}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": VERSION, "events": {}}

    if isinstance(obj, dict) and isinstance(obj.get("events"), dict):
        return obj

    # legacy — do not crash, just reset
    return {"version": VERSION, "events": {}}


def save_seen_cache(path: Path, cache: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cache["version"] = VERSION
    cache["saved_utc"] = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    path.write_text(json.dumps(cache, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def update_cache_with_today(cache: Dict, fetched: List[AnalystEvent], run_date_iso: str) -> None:
    events_map = cache.setdefault("events", {})

    for e in fetched:
        k = _event_key(e)
        entry = events_map.get(k)
        if not entry:
            events_map[k] = {
                "first_seen": run_date_iso,
                "last_seen": run_date_iso,
                "seen_dates": [run_date_iso],
                "event": asdict(e),
            }
        else:
            entry["last_seen"] = run_date_iso
            sd = entry.get("seen_dates") or []
            if run_date_iso not in sd:
                sd.append(run_date_iso)
            entry["seen_dates"] = sorted(set(sd))
            entry["event"] = asdict(e)

    # prune: keep last 45 days
    cutoff = (dt.date.fromisoformat(run_date_iso) - dt.timedelta(days=45)).isoformat()
    to_del = []
    for k, entry in events_map.items():
        ls = (entry.get("last_seen") or "")
        if ls and ls < cutoff:
            to_del.append(k)
    for k in to_del:
        events_map.pop(k, None)


def events_from_cache_window(cache: Dict, days: int, today_iso: str) -> List[AnalystEvent]:
    events_map = cache.get("events") or {}
    today = dt.date.fromisoformat(today_iso)
    cutoff = (today - dt.timedelta(days=max(1, days) - 1)).isoformat()  # inclusive

    out: List[AnalystEvent] = []
    for _, entry in events_map.items():
        ls = entry.get("last_seen") or ""
        if not ls or ls < cutoff:
            continue
        ev = entry.get("event") or {}
        try:
            ae = AnalystEvent(**ev)
        except Exception:
            continue
        ae.date = ls
        out.append(ae)

    out.sort(key=lambda e: (e.date, e.ticker), reverse=True)
    return out


def _fmt_money(x: Optional[float]) -> str:
    if x is None:
        return "-"
    return f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def write_outputs(out_md: Path, out_json: Optional[Path], events: List[AnalystEvent], days: int, status_map: Dict[str, str]) -> None:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    out_md.parent.mkdir(parents=True, exist_ok=True)

    title = f"## Elemzői feed (MarketBeat) – fel/lemínősítések + célár (utolsó {days} naptári nap)"
    lines: List[str] = [title, ""]

    st = []
    for k in ["upgrade", "downgrade", "pt_change"]:
        st.append(f"{k}:{status_map.get(k, '?')}")
    lines.append(f"_forrás státusz: {', '.join(st)}_")
    lines.append("")

    if not events:
        if any("BLOCKED" in v for v in status_map.values()):
            lines.append("_BLOCKED_ (MarketBeat challenge/bot or HTTP error) – cache used if available.")
        else:
            lines.append("_NO_EVENTS_")
        lines.append("")
        out_md.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    else:
        by_t: Dict[str, List[AnalystEvent]] = {}
        for e in events:
            by_t.setdefault(e.ticker, []).append(e)

        for t in sorted(by_t.keys()):
            lines.append(f"## {t}")
            for e in sorted(by_t[t], key=lambda x: x.date, reverse=True):
                if e.rating_from and e.rating_to:
                    rating_part = f"Ajánlás: {RATING_HU.get(e.rating_from, e.rating_from)} → {RATING_HU.get(e.rating_to, e.rating_to)}"
                elif e.rating_to:
                    rating_part = f"Ajánlás: {RATING_HU.get(e.rating_to, e.rating_to)}"
                else:
                    rating_part = "Ajánlás: -"

                if e.pt_from is not None and e.pt_to is not None:
                    pt_part = f"Célár: {e.currency} {_fmt_money(e.pt_from)} → {_fmt_money(e.pt_to)}"
                elif e.pt_to is not None:
                    pt_part = f"Célár: {e.currency} {_fmt_money(e.pt_to)}"
                else:
                    pt_part = "Célár: -"

                firm = e.firm or "-"
                lines.append(
                    f"- {e.date} – {firm} – {e.action} | {rating_part} | {pt_part} | Forrás: {e.source}"
                )
            lines.append("")

        out_md.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    if out_json:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": VERSION,
            "generated_utc": now,
            "days": days,
            "count": len(events),
            "source_status": status_map,
            "events": [asdict(e) for e in events],
        }
        out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def fetch_today_events(tickers: List[str], timeout: int, sleep_s: float, debug_dir: Optional[Path]) -> Tuple[List[AnalystEvent], Dict[str, str]]:
    opener = _mk_opener()
    master_set = set(tickers)
    run_date_iso = dt.datetime.utcnow().date().isoformat()

    all_events: List[AnalystEvent] = []
    status_map: Dict[str, str] = {}

    for action_key, path in RATINGS_SOURCES:
        url = f"{BASE}{path}"
        ok, code, page = _fetch_with_jina_fallback(opener, url, timeout, debug_dir, action_key)
        if not ok:
            status_map[action_key] = f"BLOCKED({code})"
            time.sleep(sleep_s)
            continue

        status_map[action_key] = f"OK({code})"
        all_events.extend(parse_events_from_page(page, url, action_key, master_set, run_date_iso))
        time.sleep(sleep_s)

    return all_events, status_map


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-json", default="")
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--sleep", type=float, default=0.9)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug-dir", default="reports/debug_marketbeat")
    args = ap.parse_args()

    master = Path(args.master)
    if not master.exists():
        _log(f"ERROR: MASTER CSV not found: {master}")
        return 2

    out_md = Path(args.out_md)
    out_json = Path(args.out_json) if args.out_json else None
    debug_dir = Path(args.debug_dir) if args.debug else None

    tickers = read_master_tickers(master)
    _log(f"START {VERSION} tickers={len(tickers)} days={args.days}")

    seen_path = out_md.parent / "marketbeat_seen.json"
    cache = load_seen_cache(seen_path)

    fetched, status_map = fetch_today_events(
        tickers=tickers,
        timeout=int(args.timeout),
        sleep_s=max(0.2, float(args.sleep)),
        debug_dir=debug_dir,
    )

    today_iso = dt.datetime.utcnow().date().isoformat()
    update_cache_with_today(cache, fetched, today_iso)
    save_seen_cache(seen_path, cache)

    # Rolling window comes from CACHE, not only today's fetch
    window_events = events_from_cache_window(cache, args.days, today_iso)
    write_outputs(out_md, out_json, window_events, args.days, status_map)

    _log(
        f"DONE fetched_today={len(fetched)} window_events={len(window_events)} cache_events={len((cache.get('events') or {}))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
