# scripts/diagnose_quotes.py
import os, sys, json, time, datetime, requests

def yahoo_quote(ticker: str):
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    r = requests.get(url, params={"symbols": ticker}, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    j = r.json()["quoteResponse"]["result"][0]
    now = datetime.datetime.utcfromtimestamp(j.get("regularMarketTime", 0)).strftime("%Y-%m-%d %H:%M:%S UTC")
    reg_price = j.get("regularMarketPrice")
    reg_open  = j.get("regularMarketOpen")
    open_to_now = None
    if reg_price and reg_open:
        open_to_now = (reg_price - reg_open) / reg_open * 100.0
    return {
        "symbol": ticker,
        "source": "Yahoo v7",
        "marketState": j.get("marketState"),
        "regularMarketOpen": reg_open,
        "regularMarketPrice": reg_price,
        "regularMarketChangePercent": j.get("regularMarketChangePercent"),  # prev close -> now
        "preMarketPrice": j.get("preMarketPrice"),
        "preMarketChangePercent": j.get("preMarketChangePercent"),
        "postMarketPrice": j.get("postMarketPrice"),
        "postMarketChangePercent": j.get("postMarketChangePercent"),
        "regularMarketTime": now,
        "openToNowPercent": open_to_now
    }

if __name__ == "__main__":
    tickers = sys.argv[1:] or ["META","MSTR"]
    for t in tickers:
        d = yahoo_quote(t)
        print(json.dumps(d, ensure_ascii=False, indent=2))
