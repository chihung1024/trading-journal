"""
每天由 GitHub Actions 觸發：
1. 對所有曾出現過的股票代碼抓『前復權價格』並寫入
   price_history/{SYM}.{pricesAdj, cumSplitRatio}
2. 同步 TWD=X 匯率到 exchange_rates/{doc}.rates
"""
import os, json
from datetime import datetime, timedelta, date
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app

# ── Firebase 初始化 ────────────────────────────────────────────────
sa_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
cred = credentials.Certificate(sa_info)
firebase_admin.initialize_app(cred, { 'projectId': sa_info.get("project_id") })
db = firestore.client()

# ── 取得所有 symbol ────────────────────────────────────────────────
def all_user_symbols():
    syms = set()
    for doc in db.collection_group("transactions").stream():
        s = (doc.to_dict().get("symbol") or "").upper()
        if s: syms.add(s)
    return syms

# ── 抓取並寫入 ─────────────────────────────────────────────────────
def fetch_symbol(symbol, start_date):
    tkr = yf.Ticker(symbol)

    if symbol == "TWD=X":                       # 匯率
        ser = tkr.history(start=start_date, interval="1d")['Close']
        return { "rates": { d.strftime("%Y-%m-%d"): float(v) for d, v in ser.items() } }

    # 股票：前復權價格 & 拆股倍率
    ser_price = tkr.history(start=start_date, interval="1d",
                            auto_adjust=True)['Close']
    splits    = tkr.splits

    pricesAdj = { d.strftime("%Y-%m-%d"): float(v) for d, v in ser_price.items() }

    cum, cumMap = 1, {}
    if not splits.empty:
        for d, r in splits.sort_index().items():
            cum *= r
            cumMap[d.strftime("%Y-%m-%d")] = cum

    return { "pricesAdj": pricesAdj, "cumSplitRatio": cumMap }

def update_symbol(symbol):
    col = "exchange_rates" if symbol == "TWD=X" else "price_history"
    doc = db.collection(col).document(symbol)
    start = date(2000, 1, 1)

    if (snap := doc.get()).exists:
        lu = snap.to_dict().get("lastUpdated")
        if lu:
            start = datetime.fromisoformat(lu).date() + timedelta(days=1)
    if start > date.today():
        return

    data = fetch_symbol(symbol, start)
    if not data:
        return
    data["lastUpdated"] = datetime.utcnow().isoformat()
    doc.set(data, merge=True)
    print(f"{symbol} 更新完成（起始 {start}）")

# ── 主程式 ─────────────────────────────────────────────────────────
def main():
    syms = all_user_symbols() | {"TWD=X"}
    for s in syms:
        update_symbol(s)
    print("✅  每日市場資料更新完成")

if __name__ == "__main__":
    main()
