"""
每日由 GitHub Actions 執行：
  • 依所有用過的股票代碼抓『前復權股價』與『累積拆股倍率』
  • 同步 TWD=X 匯率
"""
import os, json
from datetime import datetime, date, timedelta
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app

#────────────────────────────────────────
# Firebase 初始化
#────────────────────────────────────────
svc_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
cred     = credentials.Certificate(svc_info)
firebase_admin.initialize_app(cred, { "projectId": svc_info["project_id"] })
db = firestore.client()

#────────────────────────────────────────
def all_symbols() -> set:
    syms = set()
    for doc in db.collection_group("transactions").stream():
        sym = doc.to_dict().get("symbol", "").upper()
        if sym: syms.add(sym)
    return syms

#────────────────────────────────────────
def fetch_symbol(sym: str, since: date) -> dict:
    if sym == "TWD=X":
        ser = yf.Ticker(sym).history(start=since, interval="1d")["Close"]
        return { "rates": { d.strftime("%Y-%m-%d"): float(v) for d, v in ser.items() } }

    tkr   = yf.Ticker(sym)
    price = tkr.history(start=since, interval="1d", auto_adjust=True)["Close"]
    splits= tkr.splits

    pricesAdj = { d.strftime("%Y-%m-%d"): float(v) for d, v in price.items() }

    cum, cumMap = 1, {}
    for d, r in splits.sort_index().items():
        cum *= r
        cumMap[d.strftime("%Y-%m-%d")] = cum

    return { "pricesAdj": pricesAdj, "cumSplitRatio": cumMap }

#────────────────────────────────────────
def update(sym: str):
    col = "exchange_rates" if sym == "TWD=X" else "price_history"
    doc = db.collection(col).document(sym)
    start = date(2000,1,1)
    snap  = doc.get()
    if snap.exists and (lu := snap.to_dict().get("lastUpdated")):
        start = datetime.fromisoformat(lu).date() + timedelta(days=1)
    if start > date.today(): return

    data = fetch_symbol(sym, start)
    if not data: return
    data["lastUpdated"] = datetime.utcnow().isoformat()
    doc.set(data, merge=True)
    print(f"{sym} updated from {start}")

#────────────────────────────────────────
if __name__ == "__main__":
    print("▸ Daily price update start")
    for s in all_symbols() | {"TWD=X"}:
        update(s)
    print("✅ Done")
