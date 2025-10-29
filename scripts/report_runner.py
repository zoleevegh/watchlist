# -*- coding: utf-8 -*-
# scripts/report_runner.py
# Riportok #1 / #2 / #3 a megadott szabálykönyv szerint.
# – Időzóna: Europe/Budapest
# – Árfolyam: yfinance (1p gyertyák, prepost=True)
# – Hírek: Yahoo Finance RSS (feedparser) + SEC EDGAR 8-K/6-K
# – „Okok”: max 4 mondat; ha nincs releváns hír, üresen hagyjuk
# – Darabszámos tickerek mindig; watchlist csak ha |±3.00%|
# – Kimenet: out/report_summary.md + out/report.json

from __future__ import annotations
import argparse
import os
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf
import feedparser  # RSS

# --- Időzónák ---
try:
    from zoneinfo import ZoneInfo
    EU_BUD = ZoneInfo("Europe/Budapest")
except Exception:
    EU_BUD = timezone(timedelta(hours=1))

# --- Időablakok (CET/CEST) ---
AFTER_HOURS_CEST = (22, 0, 2, 0)     # #1: 22:00 → 02:00
PREMARKET_CEST   = (10, 0, 15, 30)   # #1: 10:00 → 15:30
OPEN_CEST  = (15, 30)                # US nyitás
CLOSE_CEST = (22, 0)                 # US zárás

# --- Paraméterek ---
THRESHOLD_OTHER = 3.0    # watchlist: csak ha |±3.00%|
NEWS_MAX_ITEMS = 3
NEWS_MAX_SENTENCES = 4
REQUEST_TIMEOUT = 12

NEWS_SOURCE_PRIORITY = [
    "reuters", "apnews", "associated press", "bloomberg",
    "businesswire", "prnewswire", "globenewswire",
    "sec.gov", "investor relations", "ir."
]

ANALYST_PATTERNS = [
    r"\b(upgrade[sd]?|downgrade[sd]?|initiates? coverage)\b",
    r"\b(price target|pt)\b",
    r"\b(overweight|equal[- ]weight|underweight|buy|hold|sell|outperform|market perform|neutral)\b",
    r"\braise[sd]? target\b|\blower[sd]? target\b",
]

# -------------------- Segédek --------------------

def now_bud() -> datetime:
    return datetime.now(EU_BUD)

def to_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc)

def fmt_pct(p: Optional[float]) -> str:
    return "n/a" if p is None else f"{p:+.2f}%"

def safe_upper(x: Any) -> str:
    return str(x).strip().upper() if pd.notna(x) else ""

def load_csv(csv_url: str) -> pd.DataFrame:
    df = pd.read_csv(csv_url)
    df.rename(columns={c: c.strip() for c in df.columns}, inplace=True)
    if "Ticker" not in df.columns:
        raise ValueError("Hiányzik a 'Ticker' oszlop a CSV-ből.")
    qty_col = None
    for cand in ["Darabszám", "Qty", "Quantity", "db", "Darab", "Shares"]:
        if cand in df.columns:
            qty_col = cand
            break
    df["_Ticker"] = df["Ticker"].apply(safe_upper)
    df["_Qty"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0).astype(int) if qty_col else 0
    # PKN.WA kihagyása
    df = df[(df["_Ticker"] != "") & (df["_Ticker"] != "PKN.WA")]
    df = df.drop_duplicates(subset=["_Ticker"]).reset_index(drop=True)
    return df

def get_prev_close(ticker: str) -> Optional[float]:
    try:
        hist = yf.download(ticker, period="7d", interval="1d", auto_adjust=False, progress=False, prepost=False)
        if hist is None or hist.empty:
            return None
        return float(hist["Close"].iloc[-1])
    except Exception:
        return None

def get_last_in_window(ticker: str, start_utc: datetime, end_utc: datetime) -> Optional[float]:
    try:
        hist = yf.download(
            ticker, interval="1m",
            start=start_utc - timedelta(minutes=2),
            end=end_utc + timedelta(minutes=2),
            prepost=True, progress=False,
        )
        if hist is None or hist.empty:
            return None
        return float(hist["Close"].iloc[-1])
    except Exception:
        return None

def get_open_and_last_change(ticker: str, start_utc: datetime, end_utc: datetime) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    try:
        hist = yf.download(
            ticker, interval="1m",
            start=start_utc - timedelta(minutes=2),
            end=end_utc + timedelta(minutes=2),
            prepost=True, progress=False,
        )
        if hist is None or hist.empty:
            return None, None, None
        o = float(hist["Open"].iloc[0])
        l = float(hist["Close"].iloc[-1])
        if o == 0:
            return o, l, None
        return o, l, (l - o) / o * 100.0
    except Exception:
        return None, None, None

def compute_windows_for_report(report_id: str, now_local: datetime) -> Dict[str, Tuple[datetime, datetime]]:
    today = now_local.date()
    if report_id == "1":
        ah_start = datetime.combine(today - timedelta(days=1), datetime.min.time(), EU_BUD).replace(
            hour=AFTER_HOURS_CEST[0], minute=AFTER_HOURS_CEST[1]
        )
        ah_end = datetime.combine(today, datetime.min.time(), EU_BUD).replace(
            hour=AFTER_HOURS_CEST[2], minute=AFTER_HOURS_CEST[3]
        )
        pm_start = datetime.combine(today, datetime.min.time(), EU_BUD).replace(
            hour=PREMARKET_CEST[0], minute=PREMARKET_CEST[1]
        )
        pm_end = datetime.combine(today, datetime.min.time(), EU_BUD).replace(
            hour=PREMARKET_CEST[2], minute=PREMARKET_CEST[3]
        )
        return {"AH": (ah_start, ah_end), "PM": (pm_start, pm_end)}
    elif report_id == "2":
        prev = today - timedelta(days=1)
        o = datetime.combine(prev, datetime.min.time(), EU_BUD).replace(hour=OPEN_CEST[0], minute=OPEN_CEST[1])
        c = datetime.combine(prev, datetime.min.time(), EU_BUD).replace(hour=CLOSE_CEST[0], minute=CLOSE_CEST[1])
        return {"OC": (o, c)}
    elif report_id == "3":
        o = datetime.combine(today, datetime.min.time(), EU_BUD).replace(hour=OPEN_CEST[0], minute=OPEN_CEST[1])
        n = now_local if now_local > o else o
        return {"ON": (o, n)}
    else:
        raise ValueError("Ismeretlen riport azonosító (1/2/3).")

# -------------------- Hírek --------------------

def _prefer_sources(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def score(e: Dict[str, Any]) -> int:
        src = (e.get("source", "") + " " + e.get("link", "")).lower()
        for i, key in enumerate(NEWS_SOURCE_PRIORITY):
            if key in src:
                return i
        return 999
    return sorted(entries, key=score)

def fetch_yahoo_ticker_news(ticker: str, win_start_utc: datetime, win_end_utc: datetime) -> List[Dict[str, Any]]:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    out: List[Dict[str, Any]] = []
    try:
        feed = feedparser.parse(url)
        for e in feed.entries:
            pub = None
            if "published_parsed" in e and e.published_parsed:
                pub = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
            elif "updated_parsed" in e and e.updated_parsed:
                pub = datetime(*e.updated_parsed[:6], tzinfo=timezone.utc)
            if not pub or not (win_start_utc <= pub <= win_end_utc):
                continue
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            source = e.get("source", {})
            if isinstance(source, dict):
                source = source.get("title", "")
            out.append({"title": title, "link": link, "published": pub.isoformat(), "source": source or ""})
    except Exception:
        pass
    return _prefer_sources(out)

def fetch_edgar_company_items(ticker: str, win_start_utc: datetime, win_end_utc: datetime) -> List[Dict[str, Any]]:
    try:
        map_url = "https://www.sec.gov/files/company_tickers.json"
        hdrs = {"User-Agent": "Mozilla/5.0 (GitHub Actions script; admin@example.com)"}
        m = requests.get(map_url, headers=hdrs, timeout=REQUEST_TIMEOUT).json()
        cik = None
        for _, rec in m.items():
            if rec.get("ticker", "").upper() == ticker.upper():
                cik = str(rec.get("cik_str")).zfill(10)
                break
        if not cik:
            return []
        feed_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=8-K|6-K&owner=exclude&count=40&output=atom"
        r = requests.get(feed_url, headers=hdrs, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return []
        from xml.etree import ElementTree as ET
        root = ET.fromstring(r.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        out: List[Dict[str, Any]] = []
        for e in root.findall("atom:entry", ns):
            title = (e.findtext("atom:title", default="", namespaces=ns) or "").strip()
            link_el = e.find("atom:link", ns)
            link = link_el.attrib.get("href") if link_el is not None else ""
            updated_txt = (e.findtext("atom:updated", default="", namespaces=ns) or "").strip()
            try:
                pub = datetime.fromisoformat(updated_txt.replace("Z", "+00:00"))
            except Exception:
                continue
            if win_start_utc <= pub <= win_end_utc:
                out.append({"title": title, "link": link, "published": pub.isoformat(), "source": "SEC EDGAR"})
        return out
    except Exception:
        return []

def summarize_news(entries: List[Dict[str, Any]], max_items=NEWS_MAX_ITEMS, max_sentences=NEWS_MAX_SENTENCES) -> str:
    """Visszatér: legfeljebb 4 mondat. Ha nincs releváns hír, üres stringgel tér vissza."""
    if not entries:
        return ""
    entries = entries[:max_items]
    text = ". ".join([e.get("title", "").strip() for e in entries if e.get("title")]).strip()
    if not text:
        return ""
    if not text.endswith("."):
        text += "."
    sents = [s.strip() for s in text.split(".") if s.strip()]
    sents = sents[:max_sentences]
    return (". ".join(sents) + ".") if sents else ""

def extract_analyst_hits(entries: List[Dict[str, Any]]) -> List[str]:
    out = []
    pat = re.compile("|".join(ANALYST_PATTERNS), flags=re.IGNORECASE)
    for e in entries:
        title = e.get("title", "")
        if pat.search(title):
            out.append(title)
    # egyszerű dedup
    seen, uniq = set(), []
    for t in out:
        n = re.sub(r"[^a-z0-9]+", "", t.lower())
        if n in seen:
            continue
        seen.add(n)
        uniq.append(t)
    return uniq

def fetch_macro_headlines(win_start_utc: datetime, win_end_utc: datetime) -> List[str]:
    feeds = [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.reuters.com/reuters/marketsNews",
        "https://apnews.com/hub/apf-business?utm_source=rss",
    ]
    hits: List[str] = []
    for url in feeds:
        try:
            f = feedparser.parse(url)
            for e in f.entries:
                pub = None
                if "published_parsed" in e and e.published_parsed:
                    pub = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                elif "updated_parsed" in e and e.updated_parsed:
                    pub = datetime(*e.updated_parsed[:6], tzinfo=timezone.utc)
                if not pub or not (win_start_utc <= pub <= win_end_utc):
                    continue
                title = (e.get("title") or "").strip()
                if title:
                    hits.append(title)
        except Exception:
            continue
    # dedup
    seen, uniq = set(), []
    for t in hits:
        n = re.sub(r"[^a-z0-9]+", "", t.lower())
        if n in seen:
            continue
        seen.add(n)
        uniq.append(t)
    return uniq[:5]

# -------------------- Riportépítés --------------------

def build_report(report_id: str, df: pd.DataFrame, csv_url: str) -> Dict[str, Any]:
    t0 = now_bud()
    windows_local = compute_windows_for_report(report_id, t0)

    df["_IsPos"] = df["_Qty"].astype(int) > 0
    pos = df[df["_IsPos"]]["_Ticker"].tolist()
    oth = df[~df["_IsPos"]]["_Ticker"].tolist()

    reported: List[Dict[str, Any]] = []
    missing: List[str] = []
    annc_block: List[str] = []

    def process_1(tk: str, is_pos: bool):
        prev = get_prev_close(tk)
        ah_s, ah_e = windows_local["AH"]
        pm_s, pm_e = windows_local["PM"]
        ah = get_last_in_window(tk, to_utc(ah_s), to_utc(ah_e))
        pm = get_last_in_window(tk, to_utc(pm_s), to_utc(pm_e))
        ah_pct = ((ah - prev) / prev * 100.0) if (ah is not None and prev) else None
        pm_pct = ((pm - prev) / prev * 100.0) if (pm is not None and prev) else None

        should = is_pos or (
            (ah_pct is not None and abs(ah_pct) >= THRESHOLD_OTHER) or
            (pm_pct is not None and abs(pm_pct) >= THRESHOLD_OTHER)
        )
        if not should:
            return

        ah_news = fetch_yahoo_ticker_news(tk, to_utc(ah_s), to_utc(ah_e))
        pm_news = fetch_yahoo_ticker_news(tk, to_utc(pm_s), to_utc(pm_e))
        ah_news = (fetch_edgar_company_items(tk, to_utc(ah_s), to_utc(ah_e)) or []) + ah_news
        pm_news = (fetch_edgar_company_items(tk, to_utc(pm_s), to_utc(pm_e)) or []) + pm_news

        ah_reason = summarize_news(ah_news) if ah_pct is not None else ""
        pm_reason = summarize_news(pm_news) if pm_pct is not None else ""

        for line in extract_analyst_hits(ah_news + pm_news):
            annc_block.append(f"{tk} — {line}")

        max_move = max([abs(x) for x in [ah_pct or 0.0, pm_pct or 0.0]])
        effect = (
            "Várható erősebb elmozdulás a nyitás után." if max_move >= 3.0
            else "Mérsékelt hatás várható a nyitás után." if max_move >= 1.0
            else "Érdemi nyitáskori hatás nem valószínű."
        )

        reported.append({
            "ticker": tk,
            "is_position": is_pos,
            "after_hours": {"pct": ah_pct, "reason": ah_reason},
            "premarket":   {"pct": pm_pct, "reason": pm_reason},
            "effect_open": effect,
        })

    def process_2(tk: str, is_pos: bool):
        oc_s, oc_e = windows_local["OC"]
        o, l, oc_pct = get_open_and_last_change(tk, to_utc(oc_s), to_utc(oc_e))
        should = is_pos or (oc_pct is not None and abs(oc_pct) >= THRESHOLD_OTHER)
        if not should:
            return
        news = fetch_yahoo_ticker_news(tk, to_utc(oc_s), to_utc(oc_e))
        news = (fetch_edgar_company_items(tk, to_utc(oc_s), to_utc(oc_e)) or []) + news
        reason = summarize_news(news) if oc_pct is not None else ""
        for line in extract_analyst_hits(news):
            annc_block.append(f"{tk} — {line}")
        reported.append({"ticker": tk, "is_position": is_pos, "open_close": {"pct": oc_pct, "reason": reason}})

    def process_3(tk: str, is_pos: bool):
        on_s, on_e = windows_local["ON"]
        o, l, on_pct = get_open_and_last_change(tk, to_utc(on_s), to_utc(on_e))
        should = is_pos or (on_pct is not None and abs(on_pct) >= THRESHOLD_OTHER)
        if not should:
            return
        news = fetch_yahoo_ticker_news(tk, to_utc(on_s), to_utc(on_e))
        news = (fetch_edgar_company_items(tk, to_utc(on_s), to_utc(on_e)) or []) + news
        reason = summarize_news(news) if on_pct is not None else ""
        for line in extract_analyst_hits(news):
            annc_block.append(f"{tk} — {line}")
        reported.append({"ticker": tk, "is_position": is_pos, "open_now": {"pct": on_pct, "reason": reason}})

    if report_id == "1":
        runner = process_1
    elif report_id == "2":
        runner = process_2
    else:
        runner = process_3

    for tk in pos:
        try:
            runner(tk, True)
        except Exception:
            missing.append(tk)

    for tk in oth:
        try:
            runner(tk, False)
        except Exception:
            missing.append(tk)

    coverage = "TELJES" if not missing else f"HIÁNYOS – nem elérhető ticker(ek): {', '.join(sorted(set(missing)))}"

    # Makró/FED/politika blokk
    if report_id == "1":
        mac_s = to_utc(windows_local["AH"][0]); mac_e = to_utc(windows_local["PM"][1])
    elif report_id == "2":
        mac_s = to_utc(windows_local["OC"][0]); mac_e = to_utc(windows_local["OC"][1])
    else:
        mac_s = to_utc(windows_local["ON"][0]); mac_e = to_utc(windows_local["ON"][1])

    macro_hits = fetch_macro_headlines(mac_s, mac_e)
    trump_line = next((h for h in macro_hits if re.search(r"\btrump\b", h, flags=re.I)), None)

    # --- MD output ---
    rep_title_map = {
        "1": "Jelentés #1 – After-hours & Premarket",
        "2": "Jelentés #2 – Open→Close (előző nap)",
        "3": "Jelentés #3 – Open→Most (ma)",
    }
    lines: List[str] = []
    lines.append(f"# {rep_title_map[report_id]}")
    lines.append(f"*Futás:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append(f"*Forrás CSV:* {csv_url or 'n/a'}")
    lines.append(f"**Lefedettség:** {coverage}")
    lines.append("")
    lines.append("## Politika / FED / Makró")
    if trump_line:
        lines.append(f"- **Trump-napihír:** {trump_line}")
    if macro_hits:
        for h in macro_hits[:5]:
            if h != trump_line:
                lines.append(f"- {h}")
    else:
        lines.append("- Nincs kiemelhető makró/FED/politikai hír a vizsgált ablakban.")
    lines.append("")

    rep_pos = [r for r in reported if r["is_position"]]; rep_pos.sort(key=lambda x: x["ticker"])
    rep_oth = [r for r in reported if not r["is_position"]]; rep_oth.sort(key=lambda x: x["ticker"])

    def section_1(items: List[Dict[str, Any]], title: str):
        if not items: return
        lines.append(f"## {title}")
        for r in items:
            tk = r["ticker"]; ah = r.get("after_hours", {}); pm = r.get("premarket", {})
            lines.append(f"**{tk}**")
            ah_pct, ah_reason = ah.get("pct"), (ah.get("reason") or "").strip()
            pm_pct, pm_reason = pm.get("pct"), (pm.get("reason") or "").strip()
            if ah_pct is not None:
                lines.append(f"- After-hours: {fmt_pct(ah_pct)}" + (f" – {ah_reason}" if ah_reason else ""))
            else:
                lines.append("- After-hours: n/a – nincs adat a vizsgált sávban.")
            if pm_pct is not None:
                lines.append(f"- Premarket: {fmt_pct(pm_pct)}" + (f" – {pm_reason}" if pm_reason else ""))
            else:
                lines.append("- Premarket: n/a – nincs adat a vizsgált sávban.")
            lines.append(f"- Várható hatás nyitáskor: {r.get('effect_open','')}")
            lines.append("")
        lines.append("")

    def section_simple(items: List[Dict[str, Any]], title: str, key: str):
        if not items: return
        lines.append(f"## {title}")
        for r in items:
            tk = r["ticker"]; bloc = r.get(key, {})
            pct = bloc.get("pct"); reason = (bloc.get("reason") or "").strip()
            if reason:
                lines.append(f"**{tk}** — {fmt_pct(pct)} – {reason}")
            else:
                lines.append(f"**{tk}** — {fmt_pct(pct)}")
        lines.append("")

    if report_id == "1":
        section_1(rep_pos, "Darabszámos tickerek")
        section_1(rep_oth, "Watchlist – (küszöb: ±3,00%)")
    elif report_id == "2":
        section_simple(rep_pos, "Darabszámos tickerek (Open→Close)", "open_close")
        section_simple(rep_oth, "Watchlist (Open→Close, küszöb: ±3,00%)", "open_close")
    else:
        section_simple(rep_pos, "Darabszámos tickerek (Open→Most)", "open_now")
        section_simple(rep_oth, "Watchlist (Open→Most, küszöb: ±3,00%)", "open_now")

    lines.append("## Bejelentések & fel/lemínősítések")
    if annc_block:
        # dedup
        seen, ded = set(), []
        for t in annc_block:
            n = re.sub(r"[^a-z0-9]+", "", t.lower())
            if n in seen: continue
            seen.add(n); ded.append(t)
        for t in ded:
            lines.append(f"- {t}")
    else:
        lines.append("- Nincs releváns bejelentés vagy elemzői lépés a vizsgált ablakban.")
    lines.append("")
    lines.append("## Közelgő katalizátorok (pár nap)")
    lines.append("- n/a (manuálisan egészíthető ki)")
    lines.append("")

    result = {
        "report": report_id,
        "timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "coverage": coverage,
        "tickers_total": int(len(df)),
        "tickers_reported": int(len(reported)),
        "items": reported,
        "macro": macro_hits[:5],
        "announcements": annc_block,
        "missing": sorted(list(set(missing))),
    }

    os.makedirs("out", exist_ok=True)
    with open("out/report_summary.md", "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))
    with open("out/report.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    return result

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, choices=["1", "2", "3"])
    ap.add_argument("--csv-url", dest="csv_url", default="")
    args = ap.parse_args()

    if not args.csv_url:
        print("**WARN**: Nincs CSV URL megadva. Futtatható, de kevés elem lesz.")

    try:
        df = load_csv(args.csv_url) if args.csv_url else pd.DataFrame(columns=["Ticker", "_Ticker", "_Qty"])
    except Exception as e:
        os.makedirs("out", exist_ok=True)
        with open("out/report_summary.md", "w", encoding="utf-8") as f:
            f.write(f"# Riport {args.report}\n**Hiba a CSV beolvasásakor:** {e}")
        with open("out/report.json", "w", encoding="utf-8") as f:
            json.dump({"error": str(e)}, f, ensure_ascii=False, indent=2)
        print(f"CSV hiba: {e}")
        return

    build_report(args.report, df, args.csv_url)

if __name__ == "__main__":
    main()
