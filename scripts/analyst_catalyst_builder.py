#!/usr/bin/env python3
"""
analyst_catalyst_builder.py
Verzió: v1.0.2-jina-md-date-urlfix

Fixek v1.0.1-hez képest:
- Date parse: a MarketBeat r.jina.ai "Markdown Content" chunkból kinyeri a dátumot (pl. "December 16, 2025")
  és ISO formára teszi: YYYY-MM-DD.
- URL fix: nem duplázza a domain-t ("https://www.marketbeat.comhttps://...").
- Stabilabb split: több entry-t nem mos össze; chunk-okon belül "Details" linket is normalizálja.
- HEALTH JSON megmarad.

Források:
- MarketBeat: Ratings / PriceTargets / Initiations (r.jina.ai proxy, plain-text)
- MarketWatch: upgrades/downgrades (best-effort, ha átmegy)

Megjegyzés:
- Best-effort parser. Ha 0 találat, a health megmutatja miért.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

__version__ = "1.0.2-jina-md-date-urlfix"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

S = requests.Session()
S.headers.update(
    {
        "User-Agent": UA,
        "Accept": "text/plain,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
)


def jina(url: str) -> str:
    url = url.strip()
    if url.startswith("http://"):
        return "https://r.jina.ai/http://" + url[len("http://") :]
    if url.startswith("https://"):
        return "https://r.jina.ai/https://" + url[len("https://") :]
    return "https://r.jina.ai/https://" + url


def fetch_text(url: str, timeout: int = 25) -> Tuple[Optional[str], Dict[str, Any]]:
    meta: Dict[str, Any] = {"url": url, "ok": False, "httpStatus": None, "error": "", "bytes": 0, "ms": 0}
    t0 = datetime.now().timestamp()
    try:
        r = S.get(url, timeout=timeout)
        meta["httpStatus"] = r.status_code
        meta["bytes"] = len(r.content or b"")
        meta["ms"] = int((datetime.now().timestamp() - t0) * 1000)
        if r.status_code >= 400:
            meta["error"] = f"HTTP_{r.status_code}"
            return None, meta
        meta["ok"] = True
        return r.text or "", meta
    except Exception as e:
        meta["ms"] = int((datetime.now().timestamp() - t0) * 1000)
        meta["error"] = f"{type(e).__name__}: {e}"
        return None, meta


def _strip(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


# --- MarketBeat r.jina.ai "Markdown Content" parser ---

ACTION_WORDS = [
    "Upgraded by",
    "Downgraded by",
    "Initiated by",
    "Reiterated by",
    "Maintained by",
    "Target Raised by",
    "Target Lowered by",
    "Target Set by",
    "Target Cut by",
    "Target Increased by",
    "Target Decreased by",
]
ACTION_RE = re.compile(r"(?i)\b(" + "|".join(re.escape(a) for a in ACTION_WORDS) + r")\b")

FIRM_RE = re.compile(r"Image\s+\d+:\s+(.+?)\s+logo", re.IGNORECASE)

TICKER_COMPANY_RE = re.compile(
    r"\)\s*([A-Z]{1,6}(?:\.[A-Z]{1,3})?)\s+([A-Za-z0-9][^\]]{1,80}?)\]\(https://www\.marketbeat\.com/stocks/",
    re.IGNORECASE,
)

RATING_ARROW_RE = re.compile(r"([A-Za-z][A-Za-z \-/]{1,40})\s+➝\s+([A-Za-z][A-Za-z \-/]{1,40})")
PT_ARROW_RE = re.compile(r"(\$[0-9][0-9,]*\.?[0-9]{0,2})\s+➝\s+(\$[0-9][0-9,]*\.?[0-9]{0,2})")
USD_RE = re.compile(r"\$[0-9][0-9,]*\.?[0-9]{0,2}")

DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})\b",
    re.IGNORECASE,
)

DETAILS_RE = re.compile(r"https://www\.marketbeat\.com/all-access/ratings-screener/details/\d+/")


def to_iso_date(m: re.Match) -> str:
    month_name = m.group(1).lower()
    day = int(m.group(2))
    year = int(m.group(3))
    months = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    month = months.get(month_name, 1)
    return f"{year:04d}-{month:02d}-{day:02d}"


def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    # kill accidental double prefix
    u = u.replace("https://www.marketbeat.comhttps://www.marketbeat.com", "https://www.marketbeat.com")
    if u.startswith("(") and u.endswith(")"):
        u = u[1:-1]
    return u


def split_entries(md: str) -> List[str]:
    """
    Split on each stock entry. In jina markdown, each entry reliably contains:
    - "stock logo](" thumbnail
    - ticker/company link to /stocks/
    """
    if not md:
        return []
    # Normalize to reduce weird line breaks
    md = md.replace("\r\n", "\n")
    # Split on "stock logo](" occurrences (entry marker)
    parts = re.split(r"(?i)\bstock logo\]\(", md)
    if len(parts) <= 1:
        return [md]
    entries: List[str] = []
    for i, p in enumerate(parts):
        if i == 0:
            continue
        entries.append("stock logo](" + p)
    return entries


def parse_marketbeat_md(md: str, source_tag: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for chunk in split_entries(md):
        chunk = chunk[:6000]

        m_tc = TICKER_COMPANY_RE.search(chunk)
        if not m_tc:
            continue
        ticker = (m_tc.group(1) or "").upper()
        company = _strip(m_tc.group(2) or "")

        m_act = ACTION_RE.search(chunk)
        action = _strip(m_act.group(1)) if m_act else ""

        m_firm = FIRM_RE.search(chunk)
        firm = _strip(m_firm.group(1)) if m_firm else ""

        # Date (best-effort)
        iso_date = ""
        m_date = DATE_RE.search(chunk)
        if m_date:
            iso_date = to_iso_date(m_date)

        fr = ""
        tr = ""
        m_rat = RATING_ARROW_RE.search(chunk)
        if m_rat:
            fr = _strip(m_rat.group(1))
            tr = _strip(m_rat.group(2))

        pt = ""
        m_pt = PT_ARROW_RE.search(chunk)
        if m_pt:
            pt = _strip(f"{m_pt.group(1)} -> {m_pt.group(2)}")
        else:
            if "Target" in action:
                usd = USD_RE.findall(chunk)
                if usd:
                    pt = usd[-1]

        url = ""
        m_det = DETAILS_RE.search(chunk)
        if m_det:
            url = normalize_url(m_det.group(0))

        if not (action or firm or fr or tr or pt):
            continue

        events.append(
            {
                "ticker": ticker,
                "company": company,
                "date": iso_date,  # YYYY-MM-DD (üres, ha nem volt benne)
                "firm": firm,
                "action": action.replace(" by", "").strip(),
                "from_rating": fr,
                "to_rating": tr,
                "price_target": pt,
                "notes": source_tag,
                "url": url,
                "source": "MarketBeat",
            }
        )
    return events


def parse_marketwatch_md(md: str) -> List[Dict[str, Any]]:
    if not md:
        return []
    text = _strip(md)
    out: List[Dict[str, Any]] = []
    for m in re.finditer(
        r"\b([A-Z]{1,5})\b.{0,60}\b(upgraded|downgraded|initiated|maintained)\b.{0,120}\bby\b\s+([A-Z][A-Za-z&\.\- ]{2,40})",
        text,
        flags=re.I,
    ):
        out.append(
            {
                "ticker": m.group(1).upper(),
                "company": "",
                "date": "",
                "firm": _strip(m.group(3)),
                "action": m.group(2).capitalize(),
                "from_rating": "",
                "to_rating": "",
                "price_target": "",
                "notes": "MarketWatch U/D (best-effort)",
                "url": "https://www.marketwatch.com/tools/upgrades-downgrades",
                "source": "MarketWatch",
            }
        )
        if len(out) >= 50:
            break
    return out


def dump(path: str, obj: Any) -> None:
    from pathlib import Path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="1")
    ap.add_argument("--analyst-out", default="reports/analyst_1.json")
    ap.add_argument("--catalysts-out", default="reports/catalysts_1.json")
    ap.add_argument("--health-out", default="reports/health_analyst_1.json")
    ap.add_argument("--pages", type=int, default=2)
    args = ap.parse_args()

    report = str(args.report).strip() or "1"
    pages = max(1, int(args.pages))

    sources_health: Dict[str, Any] = {}
    analyst_events: List[Dict[str, Any]] = []
    catalysts: List[Dict[str, Any]] = []

    def pull_mb(name: str, base_url: str, tag: str):
        nonlocal analyst_events
        total = 0
        errs: List[str] = []
        meta_all: List[Dict[str, Any]] = []
        for p in range(1, pages + 1):
            url = jina(base_url + str(p))
            md, meta = fetch_text(url)
            meta_all.append(meta)
            if not md:
                errs.append(meta.get("error", ""))
                continue
            try:
                evs = parse_marketbeat_md(md, tag)
                total += len(evs)
                analyst_events.extend(evs)
            except Exception as e:
                errs.append(f"{type(e).__name__}: {e}")
        sources_health[name] = {
            "ok": total > 0,
            "count": total,
            "errors": [e for e in errs if e],
            "fetch": meta_all[:5],
        }

    pull_mb("MarketBeat Ratings", "https://www.marketbeat.com/ratings/usa/?page=", "ratings")
    pull_mb("MarketBeat PriceTargets", "https://www.marketbeat.com/ratings/price-target/?page=", "price-target")
    pull_mb("MarketBeat Initiations", "https://www.marketbeat.com/ratings/initiations/?page=", "initiations")

    mw_url = jina("https://www.marketwatch.com/tools/upgrades-downgrades")
    mw_md, mw_meta = fetch_text(mw_url)
    mw_count = 0
    mw_err = ""
    if mw_md:
        try:
            mw_items = parse_marketwatch_md(mw_md)
            mw_count = len(mw_items)
            analyst_events.extend(mw_items)
        except Exception as e:
            mw_err = f"{type(e).__name__}: {e}"
    else:
        mw_err = mw_meta.get("error", "")
    sources_health["MarketWatch Up/Dn"] = {
        "ok": mw_count > 0,
        "count": mw_count,
        "errors": [mw_err] if mw_err else [],
        "fetch": [mw_meta],
    }

    # De-dupe
    def key(ev: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
        return (
            (ev.get("ticker") or "").upper(),
            _strip(ev.get("firm") or ""),
            _strip(ev.get("action") or ""),
            _strip(ev.get("from_rating") or ""),
            _strip(ev.get("to_rating") or ""),
        )

    uniq: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
    for ev in analyst_events:
        k = key(ev)
        if k not in uniq:
            uniq[k] = ev
    analyst_events = list(uniq.values())

    dump(args.analyst_out, analyst_events)
    dump(args.catalysts_out, catalysts)

    health = {
        "ok": True,
        "type": "health",
        "report": report,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "analystCount": len(analyst_events),
        "catalystCount": len(catalysts),
        "sources": sources_health,
        "version": __version__,
    }
    dump(args.health_out, health)

    print(f"[analyst_catalyst_builder] v={__version__} report={report} analyst={len(analyst_events)} catalysts={len(catalysts)}")
    for k, v in sources_health.items():
        print(f"  - {k}: ok={v.get('ok')} count={v.get('count')} err={';'.join(v.get('errors') or [])}")


if __name__ == "__main__":
    main()
