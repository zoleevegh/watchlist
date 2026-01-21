#!/usr/bin/env python3
# analyst_marketbeat.py — v0.3.7-marketbeat-free-2026-01-21
# MarketBeat FREE analyst feed collector (CI-friendly).
#
# Changelog (v0.3.6):
# - FIX: ratings pages have no explicit date column; previous logic dropped all rows.
# - Parse tickers from ratings table (data-clean / stock URL) and treat event date as "run day (UTC)".
# - Better empty-state messaging: "no fresh" only when source OK; otherwise "source blocked/unavailable" with HTTP codes.
#
# Verzió-szabály: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import io
import json
import random
import re
import html
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, asdict
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Dict, List, Optional, Tuple


VERSION = "v0.3.7-marketbeat-free-2026-01-21"
BASE = "https://www.marketbeat.com"

# MarketBeat "Today's" ratings lists (FREE).
RATINGS_SOURCES: List[Tuple[str, str]] = [
    ("upgrade", "/ratings/upgrades/"),
    ("downgrade", "/ratings/downgrades/"),
    ("pt_change", "/ratings/pricetargetchanges/"),
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

@dataclass
class AnalystEvent:
    ticker: str
    date: str  # ISO date YYYY-MM-DD (run day UTC)
    firm: str
    action: str
    rating_from: Optional[str]
    rating_to: Optional[str]
    pt_from: Optional[float]
    pt_to: Optional[float]
    currency: str
    source_url: str


def _log(msg: str) -> None:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now} UTC] {msg}", flush=True)


def _make_opener() -> urllib.request.OpenerDirector:
    cj = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    return opener


def _decode_body(resp: urllib.response.addinfourl, raw: bytes) -> str:
    enc = resp.headers.get("Content-Encoding", "").lower()
    if "gzip" in enc:
        try:
            raw = gzip.decompress(raw)
        except Exception:
            # sometimes servers lie; best-effort
            pass
    # marketbeat is utf-8
    return raw.decode("utf-8", errors="replace")


def _jina_url(url: str) -> str:
    # r.jina.ai can proxy-render HTML in CI environments.
    # Format: https://r.jina.ai/http(s)://example.com/path
    if url.startswith("https://"):
        return "https://r.jina.ai/https://" + url[len("https://") :]
    if url.startswith("http://"):
        return "https://r.jina.ai/http://" + url[len("http://") :]
    return "https://r.jina.ai/https://" + url


def _http_get(
    opener: urllib.request.OpenerDirector,
    url: str,
    timeout: int,
    debug_dir: Optional[Path],
    tag: str,
    *,
    allow_jina_fallback: bool = True,
    max_tries: int = 3,
) -> Tuple[int, str]:
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip",
        "Connection": "close",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    }

    last_status = 0
    last_text = ""
    for attempt in range(1, max_tries + 1):
        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with opener.open(req, timeout=timeout) as resp:
                raw = resp.read()
                status = getattr(resp, "status", 200)
                text = _decode_body(resp, raw)
                last_status, last_text = status, text
        except urllib.error.HTTPError as e:
            status = int(getattr(e, "code", 0) or 0)
            try:
                raw = e.read()
            except Exception:
                raw = b""
            text = raw.decode("utf-8", errors="replace") if raw else ""
            last_status, last_text = status, text
        except Exception as e:
            last_status, last_text = 0, f"EXC: {type(e).__name__}: {e}"

        if debug_dir:
            debug_dir.mkdir(parents=True, exist_ok=True)
            (debug_dir / f"{tag}_direct_{attempt}.status.txt").write_text(str(last_status), encoding="utf-8")
            (debug_dir / f"{tag}_direct_{attempt}.html").write_text(last_text, encoding="utf-8")

        # Success
        if 200 <= last_status < 300:
            return last_status, last_text

        # Retry on 403/429/5xx with backoff+jitter
        if last_status in (403, 429) or last_status >= 500 or last_status == 0:
            time.sleep(0.8 + random.random() * 0.9)
            continue

        # Other 4xx: don't spin
        break

    # Jina fallback if still blocked and enabled
    if allow_jina_fallback and last_status in (0, 403, 429):
        jurl = _jina_url(url)
        try:
            req = urllib.request.Request(jurl, headers={"User-Agent": UA, "Accept-Encoding": "gzip"}, method="GET")
            with opener.open(req, timeout=timeout) as resp:
                raw = resp.read()
                status = getattr(resp, "status", 200)
                text = _decode_body(resp, raw)
                if debug_dir:
                    (debug_dir / f"{tag}_jina.status.txt").write_text(str(status), encoding="utf-8")
                    (debug_dir / f"{tag}_jina.html").write_text(text, encoding="utf-8")
                return int(status), text
        except Exception as e:
            if debug_dir:
                (debug_dir / f"{tag}_jina.status.txt").write_text("0", encoding="utf-8")
                (debug_dir / f"{tag}_jina.html").write_text(f"EXC: {type(e).__name__}: {e}", encoding="utf-8")
            return 0, f"EXC: {type(e).__name__}: {e}"

    return last_status, last_text


def _warmup_session(opener: urllib.request.OpenerDirector, timeout: int, debug_dir: Optional[Path]) -> int:
    status, _ = _http_get(opener, BASE + "/", timeout, debug_dir, "warmup_home", allow_jina_fallback=False, max_tries=2)
    return status


# Back-compat alias (in case older main() calls warmup_session)
def warmup_session(opener: urllib.request.OpenerDirector, timeout: int, debug_dir: Optional[Path]) -> int:
    return _warmup_session(opener, timeout, debug_dir)


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _parse_money(s: str) -> Optional[float]:
    s = s.strip()
    m = re.search(r"(-?\d+(?:\.\d+)?)", s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _split_arrow(s: str) -> Tuple[Optional[str], Optional[str]]:
    s = _clean_text(s)
    if not s:
        return None, None
    # normalize separators
    for sep in ["➝", "→", "»", "->"]:
        if sep in s:
            parts = [p.strip() for p in s.split(sep)]
            if len(parts) >= 2:
                return (parts[0] or None), (parts[1] or None)
    return (s or None), None


def _action_from_kind(kind: str, action_cell: str) -> str:
    a = _clean_text(action_cell)
    if a:
        # e.g. "Upgraded by", "Target Raised by"
        return a
    if kind == "upgrade":
        return "Upgraded"
    if kind == "downgrade":
        return "Downgraded"
    if kind == "pt_change":
        return "Price Target Changed"
    return kind


def _extract_events_from_ratings_page(
    html_text: str,
    kind: str,
    master_set: set,
    asof_date: str,
    source_url: str,
) -> List[AnalystEvent]:
    # NOTE: We intentionally avoid external deps to keep GH Actions lean.
    # MarketBeat ratings pages contain a sortable table with rows referencing tickers via:
    # - data-clean="TICKER|Company"
    # - or stock link /stocks/EXCHANGE/TICKER/
    #
    # We parse rows with regex + tag stripping. This is robust enough for our use.
    if not html_text:
        return []

    # Locate rows (best-effort)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not rows:
        return []

    def _strip_tags(s: str) -> str:
        s = re.sub(r"<script[\s\S]*?</script>", " ", s, flags=re.IGNORECASE)
        s = re.sub(r"<style[\s\S]*?</style>", " ", s, flags=re.IGNORECASE)
        s = re.sub(r"<[^>]+>", " ", s)
        s = html.unescape(s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    events: List[AnalystEvent] = []

    for row_html in rows:
        # Grab td cells
        tds = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, flags=re.IGNORECASE | re.DOTALL)
        if len(tds) < 6:
            continue

        # Ticker: prefer data-clean attribute in the first cell
        ticker = ""
        m_dc = re.search(r'data-clean="(?P<t>[A-Z0-9\.\-]+)\|', tds[0], flags=re.IGNORECASE)
        if m_dc:
            ticker = m_dc.group("t").upper().strip()
        if not ticker:
            m2 = re.search(r"/stocks/[A-Z0-9]+/(?P<t>[A-Z0-9\.\-]+)/", row_html, flags=re.IGNORECASE)
            if m2:
                ticker = m2.group("t").upper().strip()

        if not ticker or ticker not in master_set:
            continue

        # Column order observed:
        # 0 Company, 1 Action, 2 Brokerage, 3 Analyst, 4 Current Price, 5 Price Target, 6 Rating, 7 Details (optional)
        company_cell = _strip_tags(tds[0])
        action_cell = _strip_tags(tds[1]) if len(tds) > 1 else ""
        brokerage_cell = _strip_tags(tds[2]) if len(tds) > 2 else ""
        firm = _clean_text(brokerage_cell) or "—"

        pt_cell = _strip_tags(tds[5]) if len(tds) >= 6 else ""
        rating_cell = _strip_tags(tds[6]) if len(tds) >= 7 else ""

        # Parse PT (may be single value or arrow)
        pt_from = pt_to = None
        if pt_cell:
            pf, pt = _split_arrow(pt_cell)
            if pt is None:
                pt_to = _parse_money(pf or "")
            else:
                pt_from = _parse_money(pf or "")
                pt_to = _parse_money(pt or "")

        # Parse Rating (single or arrow)
        rating_from = rating_to = None
        if rating_cell:
            rf, rt = _split_arrow(rating_cell)
            if rt is None:
                rating_to = rf
            else:
                rating_from, rating_to = rf, rt

        # Source URL: prefer stock page if present
        stock_url = ""
        m_href = re.search(r'href="(?P<h>[^"]*/stocks/[^"]+)"', row_html, flags=re.IGNORECASE)
        if m_href:
            href = m_href.group("h")
            if href.startswith("http"):
                stock_url = href
            else:
                stock_url = BASE + href
        src_url = stock_url or source_url

        events.append(
            AnalystEvent(
                ticker=ticker,
                date=asof_date,
                firm=firm,
                action=_action_from_kind(kind, action_cell),
                rating_from=rating_from,
                rating_to=rating_to,
                pt_from=pt_from,
                pt_to=pt_to,
                currency="USD",
                source_url=src_url,
            )
        )

    return events



def fetch_events_from_ratings_pages(
    master_tickers: List[str],
    days: int,
    timeout: int,
    sleep_s: float,
    debug_dir: Optional[Path],
) -> Tuple[List[AnalystEvent], Dict[str, int]]:
    # Note: MarketBeat "Today's" ratings pages do NOT expose per-row timestamps in the table.
    # We treat the event date as "run day UTC" and rely on the source being a real-time list.
    opener = _make_opener()

    statuses: Dict[str, int] = {}
    warm = _warmup_session(opener, timeout, debug_dir)
    statuses["warmup_home"] = warm
    _log(f"WARMUP home: HTTP {warm}")

    master_set = {t.upper() for t in master_tickers}
    asof_date = dt.datetime.utcnow().date().isoformat()

    out: List[AnalystEvent] = []
    seen: set = set()

    for kind, path in RATINGS_SOURCES:
        url = BASE + path
        status, page_html = _http_get(opener, url, timeout, debug_dir, f"ratings_{kind}", allow_jina_fallback=True, max_tries=3)
        statuses[f"ratings_{kind}"] = status
        if status >= 400 or status == 0:
            _log(f"RATINGS {kind}: HTTP {status} (skip)")
            time.sleep(sleep_s)
            continue

        events = _extract_events_from_ratings_page(page_html, kind, master_set, asof_date, url)
        for e in events:
            k = (e.ticker, e.firm, e.action, e.rating_from, e.rating_to, e.pt_from, e.pt_to, e.source_url)
            if k in seen:
                continue
            seen.add(k)
            out.append(e)

        time.sleep(sleep_s)

    # days param is kept for label/compat; asof_date is always today UTC.
    # If someone calls with days<1, return nothing.
    if days < 1:
        out = []

    return out, statuses


def read_master_tickers(master_csv: Path) -> List[str]:
    # Read CSV and return unique tickers (best-effort on column name).
    raw = master_csv.read_text(encoding="utf-8", errors="replace").splitlines()
    reader = csv.DictReader(raw)
    if not reader.fieldnames:
        return []
    # find likely ticker column
    cols = [c.strip() for c in reader.fieldnames if c]
    key = None
    for cand in ["ticker", "symbol", "TICKER", "Symbol", "Ticker"]:
        if cand in cols:
            key = cand
            break
    if key is None:
        # fallback: first column
        key = cols[0]
    tickers: List[str] = []
    for row in reader:
        t = (row.get(key) or "").strip().upper()
        # allow "PKN.WA" etc but you later filter out elsewhere if needed
        if t and re.fullmatch(r"[A-Z0-9\.\-]+", t):
            tickers.append(t)
    # uniq preserve order
    seen=set()
    out=[]
    for t in tickers:
        if t not in seen:
            seen.add(t); out.append(t)
    return out


def write_outputs(
    out_md: Path,
    out_json: Optional[Path],
    events: List[AnalystEvent],
    days: int,
    source_status: Dict[str, int],
) -> None:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = []
    lines.append(f"# Analyst feed (upgrade/downgrade + PT) — last {days} calendar days")
    lines.append("")
    lines.append(f"Verzió: {VERSION}")
    lines.append(f"Generálva (UTC): {now}")
    lines.append("")

    any_fail = any((k.startswith("ratings_") and (v >= 400 or v == 0)) for k, v in source_status.items())
    if not events:
        if any_fail:
            # show clear source error, not "no events"
            parts=[]
            for k,v in sorted(source_status.items()):
                if k.startswith("ratings_"):
                    parts.append(f"{k.replace('ratings_','')}={v}")
            msg = ", ".join(parts) if parts else "unknown"
            lines.append(f"_MarketBeat forrás nem elérhető / blokkolva, ezért nem tudtam friss analyst eseményeket lekérni._ (HTTP: {msg})")
        else:
            lines.append(f"_Nincs friss (≤{days} naptári nap) fel/leminősítés vagy célár-frissítés a forrásban._")
    else:
        by: Dict[str, List[AnalystEvent]] = {}
        for e in events:
            by.setdefault(e.ticker, []).append(e)
        for t in sorted(by.keys()):
            lines.append(f"## {t}")
            for e in sorted(by[t], key=lambda x: x.date, reverse=True):
                parts = [f"- {e.date} — {e.firm} — {e.action}"]
                if e.rating_from or e.rating_to:
                    parts.append(f"Rating: {e.rating_from or '—'} → {e.rating_to or '—'}")
                if e.pt_from is not None or e.pt_to is not None:
                    if e.pt_from is not None and e.pt_to is not None:
                        parts.append(f"PT: {e.currency} {e.pt_from:.2f} → {e.pt_to:.2f}")
                    elif e.pt_to is not None:
                        parts.append(f"PT: {e.currency} {e.pt_to:.2f}")
                parts.append(f"Forrás: {e.source_url}")
                lines.append("  ".join(parts))
            lines.append("")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    if out_json:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": VERSION,
            "generated_utc": now,
            "days": days,
            "count": len(events),
            "source_status": source_status,
            "events": [asdict(e) for e in events],
        }
        out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-json", default="")
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug-dir", default="reports/debug_marketbeat")
    args = ap.parse_args()

    master = Path(args.master)
    if not master.exists():
        _log(f"ERROR: MASTER CSV not found: {master}")
        return 2

    debug_dir = Path(args.debug_dir) if args.debug else None
    out_md = Path(args.out_md)
    out_json = Path(args.out_json) if args.out_json else None

    _log(f"START {VERSION} days={args.days} master={master} mode=ratings_pages")

    tickers = read_master_tickers(master)

    events, status_map = fetch_events_from_ratings_pages(
        tickers,
        days=args.days,
        timeout=args.timeout,
        sleep_s=max(0.2, float(args.sleep)),
        debug_dir=debug_dir,
    )

    write_outputs(out_md, out_json, events, args.days, status_map)

    _log(f"DONE events={len(events)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
