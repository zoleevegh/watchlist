#!/usr/bin/env python3
"""
Részvények Projekt – analyst_catalyst_builder
Verzió: v1.0.0 (2025-12-16)

Cél:
- Apps Script helyett (mert 401/403 bot-blokkolás) Pythonból állítsuk elő az analyst/catalyst feedet.
- Kimenet kompatibilis maradjon a meglévő pipeline-nal:
    reports/analyst_{report}.json
    reports/catalysts_{report}.json

Források / stratégia (praktikus, anti-block):
- MarketBeat oldalak lekérése 3 lépcsőben:
    1) direct (requests)
    2) r.jina.ai proxy (HTML text mirror)  -> gyakran átmegy ott, ahol a direct 403
    3) r.jina.ai/http(s) váltogatás
- MarketWatch / 247WallSt: jelenleg tipikusan 401/403 GitHub Actions alatt -> csak best-effort (proxyval).
- Ha minden blokkolva: üres lista + részletes coverage/meta log a stdout-ra (a riportban így nem “némán” hal el).

Megjegyzés:
- Ez a builder “nyers” eseménylistát gyárt. A meglévő analyst_block_builder / postprocess csinálja a végső riport logikát.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


# ------------------------- utilities -------------------------

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0 Safari/537.36"
)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def debug(msg: str) -> None:
    print(msg, file=sys.stderr)

def safe_write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def _req_get(url: str, timeout: int = 20) -> Tuple[int, str]:
    r = requests.get(
        url,
        headers={"User-Agent": UA, "Accept": "text/html,*/*", "Accept-Language": "en-US,en;q=0.9"},
        timeout=timeout,
    )
    return r.status_code, r.text

def fetch_with_fallback(url: str) -> Tuple[bool, int, str, str]:
    """
    Returns (ok, status, text, via)
    via: direct | jina | jina_http | jina_https
    """
    # 1) direct
    try:
        st, txt = _req_get(url)
        if 200 <= st < 300 and txt and len(txt) > 200:
            return True, st, txt, "direct"
    except Exception as e:
        st, txt = 0, f"{e!r}"

    # 2) jina.ai proxies (text mirror)
    def jina(u: str) -> str:
        # r.jina.ai/http(s)://...
        return "https://r.jina.ai/" + u

    # ensure scheme in url
    u = url.strip()
    if not u.startswith("http"):
        u = "https://" + u.lstrip("/")

    for via, jurl in [
        ("jina", jina(u)),
        ("jina_http", jina(u.replace("https://", "http://"))),
        ("jina_https", jina(u.replace("http://", "https://"))),
    ]:
        try:
            st2, txt2 = _req_get(jurl, timeout=25)
            if 200 <= st2 < 300 and txt2 and len(txt2) > 200:
                return True, st2, txt2, via
        except Exception:
            pass

    # last return: what we have
    return False, st if isinstance(st, int) else 0, (txt or ""), "failed"


# ------------------------- data model -------------------------

@dataclass
class SourceStat:
    ok: bool
    http_status: int
    via: str
    count: int
    error: str = ""

@dataclass
class Event:
    # normalized keys (lowercase) to match existing parser’s expectations
    datetime: str
    ticker: str
    action: str
    firm: str = ""
    price_target: str = ""
    notes: str = ""
    url: str = ""
    source: str = ""

    def to_dict(self) -> Dict:
        return {
            "datetime": self.datetime,
            "ticker": self.ticker,
            "action": self.action,
            "firm": self.firm,
            "price_target": self.price_target,
            "notes": self.notes,
            "url": self.url,
            "source": self.source,
        }


# ------------------------- parsers -------------------------

_MB_BASE = "https://www.marketbeat.com"

def parse_marketbeat_ratings(html: str, source: str, page_url: str) -> List[Event]:
    """
    MarketBeat ratings/price-target/initiations pages:
    - rows have data-ticker and data-date in <tr ...>
    We parse conservatively with regex (works both on normal HTML and on jina mirror).
    """
    out: List[Event] = []

    # capture per-row blocks
    rows = re.findall(r'(<tr[^>]+class="rating-[^"]*"[\s\S]*?</tr>)', html, flags=re.I)
    if not rows:
        # sometimes jina mirror strips tags; try alternative: data-ticker occurrences
        # We'll create pseudo rows around data-ticker chunks
        chunks = re.split(r'(data-ticker="[^"]+")', html)
        # too messy -> bail
        return out

    for r in rows:
        ticker = _m1(r, r'data-ticker="([^"]+)"').upper()
        date_raw = _m1(r, r'data-date="([^"]+)"')
        firm = _m1(r, r'class="firm"[^>]*>\s*([^<]+)\s*<').strip()
        rating = _m1(r, r'class="rating"[^>]*>\s*([^<]+)\s*<').strip()
        notes_cell = _m1(r, r'class="notes"[^>]*>([\s\S]*?)</td>').strip()
        notes = clean_text(notes_cell)
        href = _m1(r, r'href="([^"]+)"')

        if not ticker or len(ticker) > 7:
            continue

        action = rating or "Rating"
        price_target = ""
        if source.lower().find("pt") >= 0 or "price target" in action.lower():
            # try to extract "$X to $Y" from notes
            pt_from = _m1(notes, r'from\s+\$([0-9.]+)', flags=re.I)
            pt_to = _m1(notes, r'to\s+\$([0-9.]+)', flags=re.I)
            if pt_from and pt_to:
                price_target = f"{pt_from}->{pt_to}"
                action = "Price Target"

        dt = to_iso_guess(date_raw) or now_iso()
        url = ""
        if href:
            url = href if href.startswith("http") else _MB_BASE + href

        out.append(
            Event(
                datetime=dt,
                ticker=ticker,
                action=action,
                firm=firm,
                price_target=price_target,
                notes=notes,
                url=url or page_url,
                source=source,
            )
        )

    return out

def clean_text(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _m1(text: str, pat: str, flags=0) -> str:
    m = re.search(pat, text, flags)
    return m.group(1) if m else ""

def to_iso_guess(s: str) -> str:
    # MarketBeat often uses YYYY-MM-DD
    if not s:
        return ""
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%b %d, %Y"):
        try:
            d = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return d.isoformat().replace("+00:00", "Z")
        except Exception:
            pass
    return ""


# ------------------------- builders -------------------------

def build_marketbeat_analyst(pages: int = 2) -> Tuple[List[Event], SourceStat]:
    url_tpl = "https://www.marketbeat.com/ratings/usa/?page={p}"
    all_events: List[Event] = []
    last_stat = SourceStat(ok=False, http_status=0, via="failed", count=0, error="")
    for p in range(1, pages + 1):
        url = url_tpl.format(p=p)
        ok, st, html, via = fetch_with_fallback(url)
        if not ok:
            last_stat = SourceStat(False, st, via, 0, f"FETCH_FAIL {url}")
            continue
        ev = parse_marketbeat_ratings(html, "MarketBeat Analyst", url)
        all_events.extend(ev)
        last_stat = SourceStat(True, st, via, len(ev), "")
    return all_events, last_stat

def build_marketbeat_price_targets(pages: int = 2) -> Tuple[List[Event], SourceStat]:
    url_tpl = "https://www.marketbeat.com/ratings/price-target/?page={p}"
    all_events: List[Event] = []
    last_stat = SourceStat(ok=False, http_status=0, via="failed", count=0, error="")
    for p in range(1, pages + 1):
        url = url_tpl.format(p=p)
        ok, st, html, via = fetch_with_fallback(url)
        if not ok:
            last_stat = SourceStat(False, st, via, 0, f"FETCH_FAIL {url}")
            continue
        ev = parse_marketbeat_ratings(html, "MarketBeat PT", url)
        all_events.extend(ev)
        last_stat = SourceStat(True, st, via, len(ev), "")
    return all_events, last_stat

def build_marketbeat_initiations(pages: int = 2) -> Tuple[List[Event], SourceStat]:
    url_tpl = "https://www.marketbeat.com/ratings/initiations/?page={p}"
    all_events: List[Event] = []
    last_stat = SourceStat(ok=False, http_status=0, via="failed", count=0, error="")
    for p in range(1, pages + 1):
        url = url_tpl.format(p=p)
        ok, st, html, via = fetch_with_fallback(url)
        if not ok:
            last_stat = SourceStat(False, st, via, 0, f"FETCH_FAIL {url}")
            continue
        ev = parse_marketbeat_ratings(html, "MarketBeat Initiations", url)
        all_events.extend(ev)
        last_stat = SourceStat(True, st, via, len(ev), "")
    return all_events, last_stat


def filter_time_window(events: List[Event], report: str) -> List[Event]:
    """
    Minimal compatibility: keep existing report windows for analyst/catalyst.
    (A report #1 macro window is handled elsewhere.)
    """
    # We only filter if event datetime is parseable; otherwise keep.
    def parse_iso(x: str) -> Optional[datetime]:
        try:
            if x.endswith("Z"):
                return datetime.fromisoformat(x.replace("Z", "+00:00"))
            return datetime.fromisoformat(x)
        except Exception:
            return None

    now = datetime.now(timezone.utc)

    if report == "1":
        # prev day 22:00 CET -> today 15:30 CET is business logic in your “biblia”,
        # but we use UTC-only here; leave filtering minimal (avoid false negatives).
        # Keep last 48h.
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)  # today 00:00 UTC
        start = start.replace(day=start.day - 1)  # yesterday 00:00 UTC
        end = now
    elif report == "2":
        start = now.replace(day=now.day - 2)  # rough
        end = now.replace(day=now.day - 1)
    elif report == "3":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
    else:
        return [e for e in events]

    out = []
    for e in events:
        d = parse_iso(e.datetime)
        if not d:
            out.append(e)
            continue
        if start <= d <= end:
            out.append(e)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default=os.environ.get("REPORT", "1"))
    ap.add_argument("--pages", type=int, default=int(os.environ.get("ANALYST_PAGES", "2")))
    ap.add_argument("--analyst-out", default=None)
    ap.add_argument("--catalysts-out", default=None)
    args = ap.parse_args()

    report = str(args.report).strip()
    analyst_out = Path(args.analyst_out or f"reports/analyst_{report}.json")
    catalysts_out = Path(args.catalysts_out or f"reports/catalysts_{report}.json")

    meta = {
        "generatedAt": now_iso(),
        "report": report,
        "sources": {},
    }

    # Analyst = ratings + PT
    analyst_events: List[Event] = []
    ev1, st1 = build_marketbeat_analyst(pages=args.pages)
    meta["sources"]["MarketBeat Analyst"] = asdict(st1)
    analyst_events.extend(ev1)

    ev2, st2 = build_marketbeat_price_targets(pages=args.pages)
    meta["sources"]["MarketBeat PT"] = asdict(st2)
    analyst_events.extend(ev2)

    # Catalyst = initiations
    catalyst_events: List[Event] = []
    ev3, st3 = build_marketbeat_initiations(pages=args.pages)
    meta["sources"]["MarketBeat Initiations"] = asdict(st3)
    catalyst_events.extend(ev3)

    # Filter windows (lightweight)
    analyst_events = filter_time_window(analyst_events, report)
    catalyst_events = filter_time_window(catalyst_events, report)

    # Dedup by (ticker, action, firm, date bucket)
    def dedup(items: List[Event]) -> List[Event]:
        seen = set()
        out: List[Event] = []
        for e in items:
            key = (e.ticker, e.action.lower(), e.firm.lower(), e.datetime[:10])
            if key in seen:
                continue
            seen.add(key)
            out.append(e)
        return out

    analyst_events = dedup(analyst_events)
    catalyst_events = dedup(catalyst_events)

    # Write outputs (keep old structure: plain list) + sidecar meta to stderr
    safe_write_json(analyst_out, [e.to_dict() for e in analyst_events])
    safe_write_json(catalysts_out, [e.to_dict() for e in catalyst_events])

    debug("[analyst_catalyst_builder] coverage/meta: " + json.dumps(meta, ensure_ascii=False))
    debug(f"[analyst_catalyst_builder] wrote: {analyst_out} ({len(analyst_events)})")
    debug(f"[analyst_catalyst_builder] wrote: {catalysts_out} ({len(catalyst_events)})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
