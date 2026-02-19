#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyst_marketbeat.py — v0.2.2-briefing-stdlib-2026-02-19

- Forrás: https://hosting.briefing.com/fidelity/Calendars/UpgradesDowngrades.htm
- Nincs külső függőség (nincs requests / bs4).
- Egyetlen HTTP kérés / futás.
- MASTER tickerekre szűr.
- Magyar terminológia + “Ajánlás változatlan (...)” sor, ha nincs rating-változás.
- Ablak: --days (alap: 4).

VERZIÓSZABÁLY: bármely fájl módosításakor a verziószámot folytatólagosan kell növelni, kihagyás nélkül.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Tuple
from urllib.request import Request, urlopen
import html


BRIEFING_URL = "https://hosting.briefing.com/fidelity/Calendars/UpgradesDowngrades.htm"

RATING_MAP = {
    "strong buy": "Erős vétel",
    "buy": "Vétel",
    "overweight": "Felülsúlyozás",
    "outperform": "Piac feletti teljesítés",
    "market outperform": "Piac feletti teljesítés",
    "accumulate": "Felhalmozás",
    "positive": "Pozitív",
    "neutral": "Semleges",
    "hold": "Tartás",
    "equal weight": "Piaccal megegyező súly",
    "market perform": "Piaccal megegyező teljesítés",
    "sector perform": "Szektornak megfelelő teljesítés",
    "underperform": "Piac alatti teljesítés",
    "underweight": "Alulsúlyozás",
    "sell": "Eladás",
    "strong sell": "Erős eladás",
    "reduce": "Csökkentés",
}


def _norm_ticker(t: str) -> str:
    t = (t or "").strip().upper()
    t = re.sub(r"\s+", "", t)
    return t


def _hu_grade(g: str) -> str:
    g0 = (g or "").strip()
    if not g0:
        return "n/a"
    k = g0.lower().strip()
    return RATING_MAP.get(k, g0)


def _strip_tags(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<\s*br\s*/?>", " ", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def _fetch_html(url: str) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; PriceEngine/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    with urlopen(req, timeout=30) as resp:
        raw = resp.read()
    for enc in ("utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except Exception:
            pass
    return raw.decode("utf-8", errors="replace")


def _parse_updated_date(html_text: str, fallback_today: date) -> date:
    # Updated: 19-Feb-26 07:50 ET
    m = re.search(r"Updated:\s*(\d{1,2}-[A-Za-z]{3}-\d{2})", html_text)
    if not m:
        return fallback_today
    try:
        return datetime.strptime(m.group(1), "%d-%b-%y").date()
    except Exception:
        return fallback_today


@dataclass
class AnalystEvent:
    event_date: str
    ticker: str
    firm: str
    action: str
    prev_rating: str
    new_rating: str
    pt_text: str


def _iter_tables(html_text: str) -> Iterable[Tuple[str, str]]:
    sections = [
        "Upgrades",
        "Downgrades",
        "Coverage Initiated",
        "Coverage Reiterated/Price Tgt Changed*",
        "Coverage Reiterated/Price Target Changed",
        "Coverage Reiterated",
        "Price Tgt Changed",
        "Price Target Changed",
    ]
    lower = html_text.lower()
    for title in sections:
        idx = lower.find(title.lower())
        if idx == -1:
            continue
        m = re.search(r"<table[^>]*>.*?</table>", html_text[idx:], flags=re.I | re.S)
        if m:
            yield title, m.group(0)


def _parse_table_rows(table_html: str) -> List[List[str]]:
    rows: List[List[str]] = []
    for tr in re.findall(r"<tr[^>]*>.*?</tr>", table_html, flags=re.I | re.S):
        tds = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.I | re.S)
        cells = [_strip_tags(td) for td in tds]
        if not cells:
            continue
        if len(cells) >= 2 and cells[0].lower() == "company" and cells[1].lower() == "ticker":
            continue
        rows.append(cells)
    return rows


def parse_briefing_events(html_text: str, today_ref: date) -> List[AnalystEvent]:
    events: List[AnalystEvent] = []

    for title, table_html in _iter_tables(html_text):
        rows = _parse_table_rows(table_html)
        for cells in rows:
            ticker = cells[1] if len(cells) > 1 else ""
            firm = cells[2] if len(cells) > 2 else ""
            change = cells[3] if len(cells) > 3 else ""
            pt = cells[4] if len(cells) > 4 else ""

            t = _norm_ticker(ticker)
            if not t:
                continue

            prev_r, new_r = "n/a", "n/a"
            if change:
                parts = re.split(r"\s*[»>→]+\s*", change)
                if len(parts) >= 2:
                    prev_r, new_r = parts[0].strip(), parts[1].strip()
                else:
                    new_r = change.strip()

            t0 = title.lower()
            if "upgrade" in t0:
                action = "felminősítés"
            elif "downgrade" in t0:
                action = "leminősítés"
            elif "initiated" in t0:
                action = "új ajánlás"
            else:
                action = "megerősítés"

            pt_text = ""
            if pt:
                pt_text = pt.replace("»", "→").replace(">", "→")
                pt_text = re.sub(r"\s+", " ", pt_text).strip()

            events.append(
                AnalystEvent(
                    event_date=today_ref.isoformat(),  # a Briefing oldalon soronként nincs külön dátum
                    ticker=t,
                    firm=firm or "n/a",
                    action=action,
                    prev_rating=_hu_grade(prev_r),
                    new_rating=_hu_grade(new_r),
                    pt_text=pt_text,
                )
            )

    return events


def read_master_tickers(master_csv: str) -> List[str]:
    with open(master_csv, "r", encoding="utf-8", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        reader = csv.reader(f, dialect)
        rows = list(reader)

    if not rows:
        return []

    header = [c.strip().lower() for c in rows[0]]
    idxs = [i for i, c in enumerate(header) if c in ("ticker", "symbol", "tickers")]

    tickers: List[str] = []
    for r in rows[1:]:
        if not r:
            continue
        if idxs:
            for i in idxs:
                if i < len(r):
                    t = _norm_ticker(r[i])
                    if t:
                        tickers.append(t)
        else:
            t = _norm_ticker(r[0])
            if t:
                tickers.append(t)

    seen = set()
    out: List[str] = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _format_md(ordered: Dict[str, List[AnalystEvent]], days: int, status_line: str) -> str:
    lines: List[str] = []
    lines.append(f"## Elemzői feed (MarketBeat) – fel/lemínősítések + célár (utolsó {days} naptári nap)")
    lines.append("")
    lines.append(status_line)
    lines.append("")

    any_event = False
    for t in ordered:
        evs = ordered[t]
        if not evs:
            continue
        any_event = True
        lines.append(f"## {t}")
        for e in evs:
            if e.prev_rating == e.new_rating and e.new_rating != "n/a":
                line = f"- {e.event_date} — {e.firm} — {e.action} | Ajánlás változatlan ({e.new_rating})"
            else:
                line = f"- {e.event_date} — {e.firm} — {e.action} | Ajánlás: {e.prev_rating} → {e.new_rating}"
            if e.pt_text:
                line += f" | Célár: {e.pt_text}"
            lines.append(line)
        lines.append("")

    if not any_event:
        lines.append("_Nincs releváns elemzői esemény a megadott ablakban._")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", required=True)
    ap.add_argument("--days", type=int, default=4)
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--out-json", default="")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    tickers = read_master_tickers(args.master)
    ticker_set = set(tickers)

    html_text = _fetch_html(BRIEFING_URL)
    today_ref = _parse_updated_date(html_text, fallback_today=date.today())
    start = today_ref - timedelta(days=max(args.days - 1, 0))
    end = today_ref

    events_all = parse_briefing_events(html_text, today_ref=today_ref)

    ordered: Dict[str, List[AnalystEvent]] = {t: [] for t in tickers}
    ok = 0
    for e in events_all:
        if e.ticker in ticker_set:
            ordered[e.ticker].append(e)
            ok += 1

    ordered2 = {t: ordered[t] for t in tickers if ordered[t]}

    status_line = (
        f"_forrás státusz: ok={ok}, nincs_adat={len(ticker_set) - len(ordered2)}, fail=0 | "
        f"ablak: {start.isoformat()} → {end.isoformat()} | updated: {today_ref.isoformat()}_"
    )

    md = _format_md(ordered2, args.days, status_line)
    with open(args.out_md, "w", encoding="utf-8") as f:
        f.write(md)

    if args.out_json:
        payload = {
            "version": "v0.2.2-briefing-stdlib-2026-02-19",
            "source": "briefing",
            "updated_date": today_ref.isoformat(),
            "window": {"start": start.isoformat(), "end": end.isoformat(), "days": args.days},
            "tickers_total": len(ticker_set),
            "events_ok": ok,
            "events": [asdict(e) for t in ordered2 for e in ordered2[t]],
        }
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
