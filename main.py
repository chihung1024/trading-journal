import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore
import json
from datetime import datetime, timedelta

# ==============================================================================
# 1. 請確認您前端使用的 firebaseConfig 物件內容，貼在此處
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

# 初始化 Firebase
service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
cred = credentials.Certificate(service_account_info)
try:
    firebase_admin.initialize_app(cred)
except ValueError:
    pass # App already initialized

db = firestore.client()
PROJECT_ID = FIREBASE_CONFIG.get("projectId")
if not PROJECT_ID:
    raise ValueError("projectId not found in firebaseConfig")

def get_all_user_transactions():
    """獲取所有使用者的交易紀錄"""
    all_transactions = {}
    users_ref = db.collection(f"artifacts/{PROJECT_ID}/users")
    for user_doc in users_ref.stream():
        uid = user_doc.id
        transactions = []
        transactions_ref = user_doc.reference.collection("transactions")
        for trans_doc in transactions_ref.stream():
            data = trans_doc.to_dict()
            data['id'] = trans_doc.id
            # Firebase 的 timestamp 需要轉換
            if 'date' in data and hasattr(data['date'], 'strftime'):
                 data['date'] = data['date'].strftime('%Y-%m-%d')
            transactions.append(data)
        all_transactions[uid] = sorted(transactions, key=lambda x: x['date'])
    return all_transactions

def fetch_and_save_data(symbols):
    """一次性抓取所有需要的市場資料"""
    price_data = {}
    for symbol in symbols:
        try:
            print(f"正在抓取 {symbol} 的股價...")
            stock = yf.Ticker(symbol)
            hist = stock.history(period="max", interval="1d")
            if not hist.empty:
                price_data[symbol] = hist['Close'].to_dict()
                print(f"成功抓取 {symbol}。")
        except Exception as e:
            print(f"抓取 {symbol} 股價時發生錯誤: {e}")

    try:
        print("正在抓取 USD/TWD 匯率...")
        twd_rate = yf.Ticker("TWD=X")
        hist = twd_rate.history(period="max", interval="1d")
        if not hist.empty:
            price_data["USD_TWD"] = hist['Close'].to_dict()
            print("成功抓取 USD/TWD 匯率。")
    except Exception as e:
        print(f"抓取匯率時發生錯誤: {e}")

    # 將所有股價資料存入 Firestore
    for symbol, prices in price_data.items():
        if symbol != "USD_TWD":
            price_history_dict = {k.strftime('%Y-%m-%d'): v for k, v in prices.items()}
            doc_ref = db.collection(f"public_data/{PROJECT_ID}/price_history").document(symbol)
            doc_ref.set({
                "prices": price_history_dict,
                "lastUpdated": datetime.now().isoformat()
            })
            print(f"成功儲存 {symbol} 的股價資料。")

    # 儲存匯率資料
    if "USD_TWD" in price_data:
        rate_history_dict = {k.strftime('%Y-%m-%d'): v for k, v in price_data["USD_TWD"].items()}
        doc_ref = db.collection(f"public_data/{PROJECT_ID}/exchange_rates").document("USD_TWD")
        doc_ref.set({
            "rates": rate_history_dict,
            "lastUpdated": datetime.now().isoformat()
        })
        print("成功儲存 USD/TWD 匯率資料。")

    return price_data

def find_price_for_date(history, target_date):
    """在歷史資料中尋找特定日期的價格（若無則往前找）"""
    for i in range(7): # 最多回溯7天
        d = target_date - timedelta(days=i)
        if d in history:
            return history[d]
    return None

def calculate_and_save_portfolio_history(uid, transactions, market_data):
    """計算指定使用者的每日資產歷史並存入 Firestore"""
    if not transactions:
        return

    print(f"正在為使用者 {uid} 計算資產歷史...")
    portfolio_history = {}
    first_date = datetime.strptime(transactions[0]['date'], '%Y-%m-%d').date()
    today = datetime.now().date()
    
    rate_history_raw = market_data.get("USD_TWD", {})
    rate_history = {k.date(): v for k, v in rate_history_raw.items()}

    d = first_date
    while d <= today:
        date_str = d.strftime('%Y-%m-%d')
        daily_market_value = 0
        daily_holdings = {}

        relevant_transactions = [t for t in transactions if t['date'] <= date_str]
        
        for t in relevant_transactions:
            symbol = t['symbol'].upper()
            if symbol not in daily_holdings:
                daily_holdings[symbol] = {'quantity': 0, 'currency': t.get('currency', 'TWD')}
            
            quantity = float(t['quantity'])
            if t['type'] == 'buy':
                daily_holdings[symbol]['quantity'] += quantity
            elif t['type'] == 'sell':
                daily_holdings[symbol]['quantity'] -= quantity

        rate_on_date = find_price_for_date(rate_history, d) or 1

        for symbol, h_data in daily_holdings.items():
            if h_data['quantity'] > 1e-9:
                price_history_raw = market_data.get(symbol, {})
                price_history = {k.date(): v for k, v in price_history_raw.items()}
                price_on_date = find_price_for_date(price_history, d)

                if price_on_date is not None:
                    rate = rate_on_date if h_data['currency'] == 'USD' else 1
                    daily_market_value += h_data['quantity'] * price_on_date * rate
        
        if daily_market_value > 0:
            portfolio_history[date_str] = daily_market_value
        
        d += timedelta(days=1)

    # **關鍵修改：使用新的、正確的資料庫路徑**
    doc_ref = db.collection(f"artifacts/{PROJECT_ID}/users/{uid}/user_data").document("portfolio_history")
    doc_ref.set({"history": portfolio_history})
    print(f"成功儲存使用者 {uid} 的資產歷史。")


if __name__ == "__main__":
    all_user_transactions = get_all_user_transactions()
    
    all_symbols = set()
    for uid, transactions in all_user_transactions.items():
        for t in transactions:
            all_symbols.add(t['symbol'].upper())

    if not all_symbols:
        print("找不到任何交易紀錄，無需更新。")
    else:
        market_data = fetch_and_save_data(list(all_symbols))
        for uid, transactions in all_user_transactions.items():
            calculate_and_save_portfolio_history(uid, transactions, market_data)

    print("所有資料更新完成！")
