#!/usr/bin/env python3
"""analyst_catalyst_builder.py
Verzió: v1.0.0

Cél:
- Analyst (fel/leminősítések, PT-változások, coverage/initiations) + "katalizátor" jellegű események
  legyártása JSON-be a #1/#2/#3 riportokhoz.
- Apps Script helyett készült, mert több forrás (MarketBeat/MarketWatch/247WallSt) gyakran 401/403-at ad
  Google Apps Script környezetből.

Megközelítés:
- "r.jina.ai" proxy használata HTML letöltésre (sok helyen átviszi a botvédelmet).
- Források: MarketBeat (3 oldal), + opcionális MarketWatch upgrades/downgrades (best effort).
- MINDIG ír egy health JSON-t is: melyik source adott adatot, melyik nem, milyen hibával.

Megjegyzés:
- A parserek "best effort" jellegűek: ha a HTML változik, inkább legyen 0 találat + health-ben hiba,
  mint rossz adat.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

__version__ = "1.0.0"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
        r = SESSION.get(url, timeout=timeout)
        meta["httpStatus"] = r.status_code
        meta["bytes"] = len(r.content or b"")
        meta["ms"] = int((datetime.now().timestamp() - t0) * 1000)
        if r.status_code >= 400:
            meta["error"] = f"HTTP_{r.status_code}"
            return None, meta
        txt = r.text or ""
        meta["ok"] = True
        return txt, meta
    except Exception as e:
        meta["ms"] = int((datetime.now().timestamp() - t0) * 1000)
        meta["error"] = f"{type(e).__name__}: {e}"
        return None, meta


def _strip(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _unescape_basic(html: str) -> str:
    html = (
        html.replace("&amp;", "&")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
    )
    return html


def _parse_table_rows(html: str) -> List[str]:
    return re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", html)


def _extract_href(row_html: str) -> str:
    m = re.search(r'(?is)href="([^"]+)"', row_html)
    if not m:
        return ""
    href = m.group(1).strip()
    if href.startswith("/"):
        return "https://www.marketbeat.com" + href
    return href


def _extract_tds(row_html: str) -> List[str]:
    tds = re.findall(r"(?is)<td[^>]*>(.*?)</td>", row_html)
    out: List[str] = []
    for td in tds:
        td = re.sub(r"(?is)<.*?>", " ", td)
        td = _unescape_basic(td)
        out.append(_strip(td))
    return out


def parse_marketbeat_like(html: str, tag_note: str = "") -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for row in _parse_table_rows(html):
        tds = _extract_tds(row)
        if len(tds) < 5:
            continue

        ticker = ""
        for cell in tds[:6]:
            if re.fullmatch(r"[A-Z]{1,6}(\.[A-Z]{1,3})?", cell or ""):
                ticker = cell
                break

        date = tds[0] if tds else ""
        firm = tds[3] if len(tds) > 3 else ""
        action = tds[4] if len(tds) > 4 else ""
        fr = tds[5] if len(tds) > 5 else ""
        tr = tds[6] if len(tds) > 6 else ""
        pt = tds[7] if len(tds) > 7 else ""
        notes = tds[8] if len(tds) > 8 else ""

        url = _extract_href(row)

        if not (ticker or firm or action):
            continue

        if tag_note:
            notes = _strip(f"{notes} ({tag_note})")

        events.append(
            {
                "ticker": (ticker or "").upper(),
                "date": date,
                "firm": firm,
                "action": action,
                "from_rating": fr,
                "to_rating": tr,
                "price_target": pt,
                "notes": notes,
                "url": url,
                "source": "MarketBeat",
            }
        )
    return events


def parse_marketwatch_updown(html: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    text = re.sub(r"(?is)<script.*?</script>", " ", html)
    text = re.sub(r"(?is)<style.*?</style>", " ", text)
    text = re.sub(r"(?is)<.*?>", " ", text)
    text = _strip(_unescape_basic(text))
    for m in re.finditer(
        r"([A-Z]{1,5}).{0,40}(upgraded|downgraded|initiated|maintained).{0,80}by\s+([A-Z][A-Za-z&\.\- ]{2,40})",
        text,
        flags=re.I,
    ):
        ticker = m.group(1).upper()
        action = m.group(2).capitalize()
        firm = _strip(m.group(3))
        items.append(
            {
                "ticker": ticker,
                "date": "",
                "firm": firm,
                "action": action,
                "from_rating": "",
                "to_rating": "",
                "price_target": "",
                "notes": "MarketWatch U/D (best-effort parse)",
                "url": "https://www.marketwatch.com/tools/upgrades-downgrades",
                "source": "MarketWatch",
            }
        )
        if len(items) >= 50:
            break
    return items


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="1", help="1|2|3 (csak fájlnevekhez/health-hez)")
    ap.add_argument("--analyst-out", default="reports/analyst_1.json")
    ap.add_argument("--catalysts-out", default="reports/catalysts_1.json")
    ap.add_argument("--health-out", default="reports/health_analyst_1.json")
    ap.add_argument("--pages", type=int, default=2, help="MarketBeat lapozás (page=1..N)")
    args = ap.parse_args()

    report = str(args.report).strip() or "1"
    pages = max(1, int(args.pages))

    sources_health: Dict[str, Any] = {}
    analyst_events: List[Dict[str, Any]] = []
    catalysts: List[Dict[str, Any]] = []

    def pull_mb(name: str, base_url: str, tag_note: str):
        nonlocal analyst_events
        total = 0
        errs: List[str] = []
        meta_all: List[Dict[str, Any]] = []
        for p in range(1, pages + 1):
            url = jina(base_url + str(p))
            html, meta = fetch_text(url)
            meta_all.append(meta)
            if not html:
                if meta.get("error"):
                    errs.append(meta["error"])
                continue
            try:
                evs = parse_marketbeat_like(html, tag_note=tag_note)
                total += len(evs)
                analyst_events.extend(evs)
            except Exception as e:
                errs.append(f"{type(e).__name__}: {e}")
        sources_health[name] = {"ok": total > 0, "count": total, "errors": errs, "fetch": meta_all[:5]}

    pull_mb("MarketBeat Ratings", "https://www.marketbeat.com/ratings/usa/?page=", "ratings")
    pull_mb("MarketBeat PriceTargets", "https://www.marketbeat.com/ratings/price-target/?page=", "price-target")
    pull_mb("MarketBeat Initiations", "https://www.marketbeat.com/ratings/initiations/?page=", "initiation/coverage")

    mw_url = jina("https://www.marketwatch.com/tools/upgrades-downgrades")
    mw_html, mw_meta = fetch_text(mw_url)
    mw_count = 0
    mw_errs: List[str] = []
    if mw_html:
        try:
            mw_items = parse_marketwatch_updown(mw_html)
            mw_count = len(mw_items)
            analyst_events.extend(mw_items)
        except Exception as e:
            mw_errs.append(f"{type(e).__name__}: {e}")
    else:
        if mw_meta.get("error"):
            mw_errs.append(mw_meta["error"])

    sources_health["MarketWatch Up/Dn"] = {"ok": mw_count > 0, "count": mw_count, "errors": mw_errs, "fetch": [mw_meta]}

    def k(ev: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
        return (
            (ev.get("ticker") or "").upper(),
            _strip(ev.get("firm") or ""),
            _strip(ev.get("action") or ""),
            _strip(ev.get("date") or ""),
            _strip(ev.get("price_target") or ""),
        )

    uniq: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
    for ev in analyst_events:
        kk = k(ev)
        if kk not in uniq:
            uniq[kk] = ev
    analyst_events = list(uniq.values())

    def dump(path: str, obj: Any) -> None:
        from pathlib import Path
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

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
    }
    dump(args.health_out, health)

    print(f"[analyst_catalyst_builder] report={report} analyst={len(analyst_events)} catalysts={len(catalysts)}")
    for name, v in sources_health.items():
        print(f"  - {name}: ok={v.get('ok')} count={v.get('count')}")


if __name__ == "__main__":
    main()
