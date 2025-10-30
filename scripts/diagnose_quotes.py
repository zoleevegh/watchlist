- name: Diagnosztika (yahoo v7)
  if: ${{ github.event.inputs.diag == 'true' }}
  shell: bash
  continue-on-error: true
  run: |
    python - << 'PY'
import requests, json, datetime
def q(sym):
    u = "https://query1.finance.yahoo.com/v7/finance/quote"
    j = requests.get(u, params={"symbols": sym}, headers={"User-Agent":"Mozilla/5.0"}).json()["quoteResponse"]["result"][0]
    ts = j.get("regularMarketTime", 0)
    t  = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")
    open_ = j.get("regularMarketOpen")
    price = j.get("regularMarketPrice")
    oc = None
    if price is not None and open_:
        oc = (price - open_) / open_ * 100.0
    out = {
        "symbol": sym,
        "marketState": j.get("marketState"),
        "regularMarketOpen": open_,
        "regularMarketPrice": price,
        "regularMarketChangePercent": j.get("regularMarketChangePercent"),
        "openToNowPercent": oc,
        "regularMarketTime": t
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
for s in ["META","CRWV","MSTR"]:
    q(s)
PY
