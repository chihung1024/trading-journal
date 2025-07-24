import os, json
from datetime import datetime, timedelta, date
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore

# ========= 你的 firebaseConfig（與前端相同即可） =========
FIREBASE_CONFIG = {
    "apiKey": "AIzaSyAlymQXtutAG8UY48-TehVU70jD9RmCiBE",
    "authDomain": "trading-journal-4922c.firebaseapp.com",
    "projectId": "trading-journal-4922c",
    "storageBucket": "trading-journal-4922c.appspot.com",
    "messagingSenderId": "50863752247",
    "appId": "1:50863752247:web:766eaa892d2c8d28bb6bb2"
}
PROJECT_ID = FIREBASE_CONFIG["projectId"]
# ========================================================

# ---------- 初始化 Firebase Admin ----------
service_json = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
cred = credentials.Certificate(service_json)
try:
    firebase_admin.initialize_app(cred)
except ValueError:
    pass
db = firestore.client()

# ---------- 工具 ----------
def str_date(ts: date) -> str:
    return ts.strftime('%Y-%m-%d')

def find_price(history: dict, target: date):
    """history: {'YYYY-MM-DD': price}"""
    for i in range(7):
        d_str = str_date(target - timedelta(days=i))
        if d_str in history:
            return history[d_str]
    return None

# ---------- 1. 讀取所有使用者交易 ----------
def get_all_user_transactions():
    all_tx = {}
    users_ref = db.collection("artifacts").document(PROJECT_ID).collection("users")
    for user_doc in users_ref.stream():
        uid = user_doc.id
        txs  = []
        for t_doc in user_doc.reference.collection("transactions").stream():
            d = t_doc.to_dict()
            d["id"] = t_doc.id
            if isinstance(d.get("date"), datetime):
                d["date"] = str_date(d["date"].date())
            txs.append(d)
        all_tx[uid] = sorted(txs, key=lambda x: x["date"])
    return all_tx

# ---------- 2. 抓取並寫入市場資料 ----------
def fetch_and_save(symbols: list[str]) -> dict:
    market = {}

    for sym in symbols:
        try:
            print(f"抓取 {sym} ...")
            raw = yf.Ticker(sym).history(period="max", interval="1d")["Close"]
            if not raw.empty:
                market[sym] = {idx.strftime('%Y-%m-%d'): v for idx, v in raw.items()}
                print("  OK")
        except Exception as e:
            print(f"  失敗: {e}")

    # USD/TWD
    try:
        raw = yf.Ticker("TWD=X").history(period="max", interval="1d")["Close"]
        market["USD_TWD"] = {idx.strftime('%Y-%m-%d'): v for idx, v in raw.items()}
        print("匯率 OK")
    except Exception as e:
        print(f"匯率失敗: {e}")

    # 寫入 Firestore
    price_col = db.collection("public_data").document(PROJECT_ID).collection("price_history")
    rate_col  = db.collection("public_data").document(PROJECT_ID).collection("exchange_rates")

    for sym, prices in market.items():
        if sym == "USD_TWD":
            rate_col.document("USD_TWD").set({
                "rates": prices,
                "lastUpdated": datetime.utcnow().isoformat()
            })
        else:
            price_col.document(sym).set({
                "prices": prices,
                "lastUpdated": datetime.utcnow().isoformat()
            })
    return market

# ---------- 3. 依日期計算資產曲線 ----------
def calc_and_save_history(uid: str, txs: list[dict], market: dict):
    if not txs:
        return

    print(f"計算 {uid} ...")
    first = datetime.strptime(txs[0]["date"], '%Y-%m-%d').date()
    today = datetime.utcnow().date()
    usd_hist = market.get("USD_TWD", {})
    hist_out = {}

    d = first
    while d <= today:
        mv = 0       # market value in TWD
        holdings = {}

        # 持股張數
        for t in (t for t in txs if t["date"] <= str_date(d)):
            sym = t["symbol"].upper()
            holdings.setdefault(sym, {"qty":0, "cur": t.get("currency","TWD")})
            qty = float(t["quantity"])
            holdings[sym]["qty"] += qty if t["type"]=="buy" else -qty

        # 計算市值
        rate = find_price(usd_hist, d) or 1
        for sym, h in holdings.items():
            if h["qty"] <= 1e-9: continue
            price = find_price(market.get(sym, {}), d)
            if price is None:    continue
            mv += h["qty"] * price * (rate if h["cur"]=="USD" else 1)

        if mv:
            hist_out[str_date(d)] = mv
        d += timedelta(days=1)

    # 寫入
    doc_ref = (db.collection("artifacts")
                 .document(PROJECT_ID)
                 .collection("users")
                 .document(uid)
                 .collection("user_data")
                 .document("portfolio_history"))
    doc_ref.set({"history": hist_out})
    print(f"  -> 已寫入 {len(hist_out)} 日資料")

# ---------- 主流程 ----------
if __name__ == "__main__":
    all_tx = get_all_user_transactions()
    all_syms = {t["symbol"].upper() for txs in all_tx.values() for t in txs}

    if not all_syms:
        print("無新交易，結束。")
        exit(0)

    market = fetch_and_save(sorted(all_syms))
    for uid, txs in all_tx.items():
        calc_and_save_history(uid, txs, market)

    print("✅ 所有資料更新完成")
