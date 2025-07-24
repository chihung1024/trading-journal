# main.py
import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore
import json
from datetime import datetime, timedelta

# ==============================================================================
# 1. 請將您前端使用的 firebaseConfig 物件內容，完整貼到此處
# ==============================================================================
FIREBASE_CONFIG = {
    "apiKey": "AIzaSyAlymQXtutAG8UY48-TehVU70jD9RmCiBE",
    "authDomain": "trading-journal-4922c.firebaseapp.com",
    "projectId": "trading-journal-4922c",
    "storageBucket": "trading-journal-4922c.appspot.com",
    "messagingSenderId": "50863752247",
    "appId": "1:50863752247:web:766eaa892d2c8d28bb6bb2"
}
# ==============================================================================

# 從 GitHub Secrets 讀取服務帳戶金鑰
service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
cred = credentials.Certificate(service_account_info)

# 初始化 Firebase Admin SDK
try:
    firebase_admin.initialize_app(cred)
except ValueError:
    print("Firebase App already initialized.")

db = firestore.client()
app_id = FIREBASE_CONFIG.get("appId", "default-app-id")

def get_all_symbols():
    """從所有使用者的交易紀錄中讀取不重複的股票代碼"""
    symbols = set()
    users_ref = db.collection(f"artifacts/{app_id}/users")
    for user_doc in users_ref.stream():
        transactions_ref = user_doc.reference.collection("transactions")
        for trans_doc in transactions_ref.stream():
            symbol = trans_doc.to_dict().get("symbol")
            if symbol:
                symbols.add(symbol.upper())
    print(f"找到的股票代碼: {list(symbols)}")
    return list(symbols)

def fetch_and_save_stock_data(symbol):
    """使用 yfinance 抓取歷史股價並存入 Firestore"""
    try:
        print(f"正在抓取 {symbol} 的股價...")
        stock = yf.Ticker(symbol)
        # 抓取最大可用範圍的歷史資料
        hist = stock.history(period="max", interval="1d")
        
        if hist.empty:
            print(f"找不到 {symbol} 的歷史資料。")
            return

        price_history = {}
        for index, row in hist.iterrows():
            date_str = index.strftime('%Y-%m-%d')
            # 使用 'Close' 價格，因為 yfinance 的 adjClose 在近期可能為空
            price_history[date_str] = row['Close']

        doc_ref = db.collection(f"artifacts/{app_id}/public/price_history").document(symbol)
        doc_ref.set({
            "prices": price_history,
            "lastUpdated": datetime.now().isoformat()
        })
        print(f"成功儲存 {symbol} 的股價資料。")
    except Exception as e:
        print(f"抓取或儲存 {symbol} 股價時發生錯誤: {e}")

def fetch_and_save_exchange_rates():
    """抓取美元兌台幣的歷史匯率"""
    try:
        print("正在抓取 USD/TWD 匯率...")
        # yfinance 中 USD/TWD 的代碼是 TWD=X
        twd_rate = yf.Ticker("TWD=X")
        hist = twd_rate.history(period="max", interval="1d")

        if hist.empty:
            print("找不到 USD/TWD 的匯率資料。")
            return
            
        rate_history = {}
        for index, row in hist.iterrows():
            date_str = index.strftime('%Y-%m-%d')
            rate_history[date_str] = row['Close']

        doc_ref = db.collection(f"artifacts/{app_id}/public/exchange_rates").document("USD_TWD")
        doc_ref.set({
            "rates": rate_history,
            "lastUpdated": datetime.now().isoformat()
        })
        print("成功儲存 USD/TWD 匯率資料。")
    except Exception as e:
        print(f"抓取或儲存匯率時發生錯誤: {e}")


if __name__ == "__main__":
    # 1. 更新匯率
    fetch_and_save_exchange_rates()
    
    # 2. 更新所有股票價格
    all_symbols = get_all_symbols()
    if all_symbols:
        for symbol in all_symbols:
            fetch_and_save_stock_data(symbol)
    else:
        print("在資料庫中找不到任何交易紀錄，無需更新股價。")
    
    print("所有資料更新完成！")
