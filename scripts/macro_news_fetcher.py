import json
import sys
import os
import datetime as dt
import email.utils
import xml.etree.ElementTree as ET
from urllib.request import urlopen, Request
from typing import List, Tuple

YAHOO_FINANCE_RSS = "https://www.yahoo.com/news/rss/finance"
CNBC_MARKET_INSIDER_RSS = "https://www.cnbc.com/id/20409666/device/rss/rss.html"
BLOOMBERG_MARKETS_RSS = "https://feeds.bloomberg.com/markets/news.rss"


def get_window(report_type: int, now_cet: dt.datetime) -> Tuple[dt.datetime, dt.datetime]:
    """Időablak számítása (CEST) a #1/#2/#3 reporthoz használt logika alapján.

    - #1 és #2: előző kereskedési nap 15:30 → now_cet
    - #3: előző piaczárás 22:00 → now_cet

    Hétfőn visszalépünk 3 napot (péntekre), egyébként 1 napot. US ünnepeket nem vesszük külön figyelembe.
    """
    if now_cet.weekday() == 0:  # hétfő
        delta_days = 3
    else:
        delta_days = 1

    if report_type in (1, 2):
        start = (now_cet - dt.timedelta(days=delta_days)).replace(
            hour=15, minute=30, second=0, microsecond=0
        )
    else:
        start = (now_cet - dt.timedelta(days=delta_days)).replace(
            hour=22, minute=0, second=0, microsecond=0
        )

    return start, now_cet


def fetch_rss(url: str, source_label: str) -> List[Tuple[dt.datetime, str]]:
    """Egyszerű RSS fetch + cím/időpár lista.

    A pubDate mezőt próbáljuk RFC2822 szerint parse-olni. Ha nincs vagy hibás,
    az adott itemet kihagyjuk.
    """
    items: List[Tuple[dt.datetime, str]] = []

    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=15) as resp:
            data = resp.read()
    except Exception:
        return items

    try:
        root = ET.fromstring(data)
    except Exception:
        return items

    # Az RSS struktúra tipikusan: <rss><channel><item>...</item></channel></rss>
    for item in root.findall(".//item"):
        title_el = item.find("title")
        date_el = item.find("pubDate")
        if title_el is None or date_el is None:
            continue

        title = (title_el.text or "").strip()
        raw_date = (date_el.text or "").strip()
        if not title or not raw_date:
            continue

        try:
            dt_obj = email.utils.parsedate_to_datetime(raw_date)
        except Exception:
            continue

        if dt_obj.tzinfo is None:
            # Feltételezzük, hogy UTC/GMT, ha nincs timezone.
            dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)

        # Konvertáljuk CEST/CET-re, hogy az időablakkal könnyebb legyen dolgozni.
        cet = dt_obj.astimezone(dt.timezone(dt.timedelta(hours=1)))
        items.append((cet, f"{source_label}: {title}"))

    return items


def collect_macro_news(report_type: int) -> List[str]:
    now_cet = dt.datetime.now(dt.timezone(dt.timedelta(hours=1)))
    start, end = get_window(report_type, now_cet)

    all_items: List[Tuple[dt.datetime, str]] = []
    all_items.extend(fetch_rss(YAHOO_FINANCE_RSS, "Yahoo Finance"))
    all_items.extend(fetch_rss(CNBC_MARKET_INSIDER_RSS, "CNBC"))
    all_items.extend(fetch_rss(BLOOMBERG_MARKETS_RSS, "Bloomberg"))

    # Szűrés időablakra
    in_window = [
        (ts, text) for ts, text in all_items if start <= ts <= end
    ]

    # Rendezzük idő szerint, legfrissebb elöl
    in_window.sort(key=lambda x: x[0], reverse=True)

    # Deduplikáció cím alapján
    seen = set()
    unique_texts: List[str] = []
    for ts, text in in_window:
        if text in seen:
            continue
        seen.add(text)
        unique_texts.append(text)

    # Maximum 8 headline – a helper úgyis legfeljebb 4-et használ
    return unique_texts[:8]


def main(argv: List[str]) -> None:
    report_type = 1
    if len(argv) > 1:
        try:
            report_type = int(argv[1])
        except ValueError:
            pass

    report_type = 1 if report_type not in (1, 2, 3) else report_type

    items = collect_macro_news(report_type)
    now_cet = dt.datetime.now(dt.timezone(dt.timedelta(hours=1)))

    data = {
        "generated_at": now_cet.isoformat(),
        "report_type": report_type,
        "items": items,
    }

    os.makedirs("reports", exist_ok=True)
    out_path = os.path.join("reports", f"macro_news_{report_type}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main(sys.argv)
