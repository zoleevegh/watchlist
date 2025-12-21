#!/usr/bin/env python3
"""
SEC EDGAR fetcher — v1.0.0

Cél
- Stabil, "mindig legyen adat" jellegű katalizátor-forrás: SEC EDGAR filingek (8-K, 6-K, 10-Q, 10-K, S-1, DEF 14A, stb.)
- Kimenet: a pipeline által felhasználható, egységesített catalyst esemény JSON.

Fő elvek (BIBLIA-kompatibilis)
- SEC = tényforrás, nem híraggregátor.
- Szigorú rate-limit + kötelező User-Agent (SEC policy).
- Cache-eli a ticker→CIK mappinget.

Kimeneti séma (rugalmas)
- A crawler_analyst_catalyst.py normalizálója bármilyen extra mezőt elvisel.
- Kötelező mezők: ticker, event_type, headline, summary, source, ts
- Extra: url, form, accession, filing_date

Használat
  python scripts/sec_edgar_fetcher_v1.0.0.py \
      --report 1 \
      --master-csv reports/master.csv \
      --out reports/1/catalysts_1.json

Ajánlott a workflow-ban:
- futtasd postprocess előtt
- report=1/2/3 szerint külön fájlba írjon

Megjegyzés
- A SEC "submissions" JSON nem ad megbízható perces timestampet, tipikusan csak filingDate.
  Emiatt a v1 "lookback" időablakkal dolgozik (órában).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests  # type: ignore
except ImportError:
    requests = None


SEC_TICKER_JSON_PRIMARY = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik10}.json"
SEC_ARCHIVES_DOC = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no_nodash}/{primary_doc}"

DEFAULT_FORMS = [
    # "mindig hasznos" / anyagilag lényeges formok
    "8-K", "6-K", "10-Q", "10-K", "20-F", "40-F",
    "S-1", "S-3", "F-1", "F-3",
    "424B1", "424B2", "424B3", "424B4", "424B5",
    "DEF 14A", "DEFA14A",
    "SC 13D", "SC 13G", "13F-HR",
]

# SEC rate limit: nem túl agresszív. 0.2–0.3s requestenként általában oké.
DEFAULT_SLEEP_SEC = 0.25


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def require_requests() -> None:
    if requests is None:
        raise RuntimeError("Hiányzó függőség: requests. Add hozzá a requirements.txt-hez: requests")


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_yyyymmdd(s: str) -> Optional[dt.date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def safe_slug(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return s[:150]


@dataclass
class Filing:
    ticker: str
    cik10: str
    filing_date: str
    form: str
    accession: str
    primary_doc: str

    def to_event(self) -> Dict[str, Any]:
        # filingDate jellemzően YYYY-MM-DD
        # ts-nek: date + "T00:00:00Z" (jobb híján), de később lehet finomítani.
        ts = f"{self.filing_date}T00:00:00Z" if self.filing_date else ""
        url = ""
        try:
            cik_int = str(int(self.cik10))
            acc_no_nodash = self.accession.replace("-", "")
            if self.primary_doc:
                url = SEC_ARCHIVES_DOC.format(cik_int=cik_int, acc_no_nodash=acc_no_nodash, primary_doc=self.primary_doc)
        except Exception:
            url = ""

        headline = f"SEC filing: {self.form}"
        summary = f"{self.form} benyújtva az SEC-hez ({self.filing_date})."
        return {
            "ticker": self.ticker,
            "event_type": "sec_filing",
            "headline": headline,
            "summary": summary,
            "source": "SEC EDGAR",
            "ts": ts,
            "url": url,
            "form": self.form,
            "accession": self.accession,
            "filing_date": self.filing_date,
            "primary_doc": self.primary_doc,
        }


class SecClient:
    def __init__(self, user_agent: str, cache_dir: Path, sleep_sec: float = DEFAULT_SLEEP_SEC, timeout: int = 25) -> None:
        require_requests()
        self.user_agent = user_agent
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.sleep_sec = sleep_sec
        self.timeout = timeout
        self._session = requests.Session()  # type: ignore
        self._session.headers.update({
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json,text/plain,*/*",
        })

    def _get(self, url: str) -> Any:
        resp = self._session.get(url, timeout=self.timeout)  # type: ignore
        if resp.status_code == 403:
            raise RuntimeError("SEC 403: valószínűleg hiányzó/gyenge User-Agent vagy túl gyors lekérés.")
        resp.raise_for_status()
        time.sleep(self.sleep_sec)
        ctype = resp.headers.get("content-type", "")
        if "json" in ctype or url.endswith(".json"):
            return resp.json()
        return resp.text

    def load_ticker_map(self, force_refresh: bool = False) -> Dict[str, str]:
        """
        Visszaad: { "AAPL": "0000320193", ... } (CIK 10 számjegyre zéróval kitöltve)
        """
        cache_path = self.cache_dir / "sec_company_tickers.json"
        if cache_path.exists() and not force_refresh:
            try:
                return json.loads(cache_path.read_text(encoding="utf-8"))
            except Exception:
                pass

        data = self._get(SEC_TICKER_JSON_PRIMARY)
        # company_tickers.json: dict indexelt elemekkel
        out: Dict[str, str] = {}
        if isinstance(data, dict):
            for _, row in data.items():
                try:
                    ticker = str(row.get("ticker", "")).upper().strip()
                    cik = str(row.get("cik_str", "")).strip()
                    if ticker and cik:
                        out[ticker] = str(int(cik)).zfill(10)
                except Exception:
                    continue

        cache_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    def fetch_submissions(self, cik10: str) -> Dict[str, Any]:
        url = SEC_SUBMISSIONS.format(cik10=cik10)
        data = self._get(url)
        if not isinstance(data, dict):
            raise RuntimeError("SEC submissions válasz nem dict.")
        return data


def read_master_tickers(master_csv: Path) -> List[str]:
    """
    MASTER CSV minimál támogatás:
    - első oszlopban lehet 'ticker' vagy 'symbol'
    - ha nincs header, akkor az első oszlopot tickernek vesszük
    """
    import csv
    if not master_csv.exists():
        raise FileNotFoundError(f"MASTER CSV nem található: {master_csv}")
    rows = master_csv.read_text(encoding="utf-8").splitlines()
    if not rows:
        return []
    reader = csv.reader(rows)
    header = rows[0].lower()
    has_header = ("ticker" in header) or ("symbol" in header)
    tickers: List[str] = []
    if has_header:
        dict_reader = csv.DictReader(rows)
        for r in dict_reader:
            t = (r.get("ticker") or r.get("symbol") or "").strip().upper()
            if t:
                tickers.append(t)
    else:
        # első oszlop
        for r in reader:
            if not r:
                continue
            t = (r[0] or "").strip().upper()
            if t and t != "TICKER":
                tickers.append(t)
    # dedup while preserving order
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def report_default_lookback_hours(report: int) -> int:
    # pragmatikus defaultok
    if report == 1:
        return 48
    if report == 2:
        return 96
    return 24


def within_lookback(filing_date: str, lookback_hours: int, ref_utc: dt.datetime) -> bool:
    d = parse_yyyymmdd(filing_date)
    if not d:
        return False
    # filingDate "nap" szinten van. Kezeljük úgy, hogy a nap 00:00 UTC.
    filing_dt = dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)
    delta = ref_utc - filing_dt
    return delta.total_seconds() >= 0 and delta.total_seconds() <= lookback_hours * 3600


def extract_recent_filings(submissions: Dict[str, Any], ticker: str, cik10: str, forms_allow: set[str], lookback_hours: int, ref_utc: dt.datetime) -> List[Filing]:
    """
    submissions['filings']['recent']:
      accessionNumber[], filingDate[], form[], primaryDocument[]
    """
    filings = submissions.get("filings", {}).get("recent", {})
    acc = filings.get("accessionNumber", []) or []
    dates = filings.get("filingDate", []) or []
    forms = filings.get("form", []) or []
    docs = filings.get("primaryDocument", []) or []

    out: List[Filing] = []
    n = min(len(acc), len(dates), len(forms))
    for i in range(n):
        form = str(forms[i] or "").strip()
        if forms_allow and form not in forms_allow:
            continue
        filing_date = str(dates[i] or "").strip()
        if not within_lookback(filing_date, lookback_hours, ref_utc):
            continue
        accession = str(acc[i] or "").strip()
        primary_doc = str(docs[i] or "").strip() if i < len(docs) else ""
        out.append(Filing(
            ticker=ticker,
            cik10=cik10,
            filing_date=filing_date,
            form=form,
            accession=accession,
            primary_doc=primary_doc
        ))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="SEC EDGAR filings → catalysts JSON (BIBLIA v1)")
    ap.add_argument("--report", type=int, required=True, choices=[1,2,3], help="Report # (1/2/3)")
    ap.add_argument("--master-csv", required=True, help="MASTER CSV (tickerek)")
    ap.add_argument("--out", required=True, help="Kimeneti JSON path (pl. reports/1/catalysts_1.json)")
    ap.add_argument("--forms", default=",".join(DEFAULT_FORMS), help="Vesszővel elválasztott SEC form whitelist")
    ap.add_argument("--lookback-hours", type=int, default=0, help="Lookback órában (0=report default)")
    ap.add_argument("--cache-dir", default=".cache/sec", help="Cache könyvtár")
    ap.add_argument("--sleep-sec", type=float, default=DEFAULT_SLEEP_SEC, help="SEC request sleep (sec)")
    ap.add_argument("--timeout", type=int, default=25, help="HTTP timeout (sec)")
    ap.add_argument("--refresh-ticker-map", action="store_true", help="Ticker→CIK cache frissítése")
    ap.add_argument("--user-agent", default=os.getenv("SEC_USER_AGENT",""), help="Kötelező! Pl: 'ZoliStocksBot/1.0 (email@domain.com)'")
    ap.add_argument("--max-tickers", type=int, default=0, help="0=korlátlan (debughoz hasznos)")
    args = ap.parse_args()

    ua = (args.user_agent or "").strip()
    if not ua or len(ua) < 10:
        raise SystemExit("HIBA: Adj meg erős SEC User-Agentet --user-agent vagy SEC_USER_AGENT env alatt. Pl: 'ZoliStocksBot/1.0 (contact: you@domain.com)'")

    report = args.report
    lookback_hours = args.lookback_hours or report_default_lookback_hours(report)

    master_csv = Path(args.master_csv)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tickers = read_master_tickers(master_csv)
    if args.max_tickers and args.max_tickers > 0:
        tickers = tickers[:args.max_tickers]

    ref = now_utc()
    cache_dir = Path(args.cache_dir)
    client = SecClient(user_agent=ua, cache_dir=cache_dir, sleep_sec=args.sleep_sec, timeout=args.timeout)

    ticker_map = client.load_ticker_map(force_refresh=bool(args.refresh_ticker_map))

    events: List[Dict[str, Any]] = []
    missing_cik: List[str] = []
    errors: List[Tuple[str,str]] = []

    forms_allow = set([f.strip() for f in (args.forms or "").split(",") if f.strip()])

    for t in tickers:
        cik10 = ticker_map.get(t)
        if not cik10:
            missing_cik.append(t)
            continue
        try:
            submissions = client.fetch_submissions(cik10)
            filings = extract_recent_filings(
                submissions=submissions,
                ticker=t,
                cik10=cik10,
                forms_allow=forms_allow,
                lookback_hours=lookback_hours,
                ref_utc=ref,
            )
            for f in filings:
                events.append(f.to_event())
        except Exception as e:
            errors.append((t, repr(e)))
            continue

    # Dedupe: ticker+form+filing_date+accession
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for ev in events:
        key = (ev.get("ticker",""), ev.get("form",""), ev.get("filing_date",""), ev.get("accession",""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(ev)

    out_obj = {
        "meta": {
            "source": "SEC EDGAR",
            "generated_utc": ref.isoformat(),
            "report": report,
            "lookback_hours": lookback_hours,
            "forms_allow": sorted(list(forms_allow)),
            "tickers_total": len(tickers),
            "events_total": len(deduped),
            "missing_cik": missing_cik[:200],
            "errors": [{"ticker": t, "error": err} for t, err in errors[:200]],
        },
        "events": deduped,
    }

    out_path.write_text(json.dumps(out_obj, ensure_ascii=False, indent=2), encoding="utf-8")

    eprint(f"[sec_edgar_fetcher] Mentve: {out_path} (events: {len(deduped)})")
    if missing_cik:
        eprint(f"[sec_edgar_fetcher] FIGYELEM: {len(missing_cik)} tickerhez nincs CIK mapping (pl. {missing_cik[:8]})")
    if errors:
        eprint(f"[sec_edgar_fetcher] FIGYELEM: {len(errors)} ticker hiba (pl. {errors[:3]})")


if __name__ == "__main__":
    main()
