import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app
import json
from datetime import datetime
import pandas as pd

def initialize_firebase():
    try:
        get_app()
    except ValueError:
        try:
            service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
            cred = credentials.Certificate(service_account_info)
            firebase_admin.initialize_app(cred)
        except Exception as e:
            print(f"Firebase initialization failed: {e}")
            exit(1)
    return firestore.client()

def get_all_symbols_from_transactions(db):
    all_symbols = set()
    try:
        transactions_group = db.collection_group("transactions")
        for trans_doc in transactions_group.stream():
            symbol = trans_doc.to_dict().get('symbol')
            if symbol:
                all_symbols.add(symbol.upper())
    except Exception as e:
        print(f"Warning: Could not read transactions. Error: {e}")
    print(f"Found {len(all_symbols)} unique symbols from transactions: {list(all_symbols)}")
    return list(all_symbols)

# [修正] 將遺漏的函式加回來
def get_all_benchmarks_from_preferences(db):
    """從所有使用者的偏好設定中讀取所有自定義的 benchmark symbol。"""
    all_benchmarks = set()
    try:
        # 尋找所有 user_data 子集合中名為 preferences 的文件
        prefs_docs = db.collection_group("user_data").where("__name__", "==", "preferences").stream()
        for pref_doc in prefs_docs:
            if pref_doc.exists:
                symbol = pref_doc.to_dict().get('benchmarkSymbol')
                if symbol:
                    all_benchmarks.add(symbol.upper())
    except Exception as e:
        # Firebase 的 collection_group 查詢需要索引，如果沒有索引可能會出錯，這裡溫和地處理
        print(f"Warning: Could not read preferences, possibly due to a missing Firestore index. Error: {e}")
    
    # 將預設的 SPY 加入，確保最少有 SPY 的數據
    all_benchmarks.add('SPY')
    
    print(f"Found {len(all_benchmarks)} unique benchmark symbols: {list(all_benchmarks)}")
    return list(all_benchmarks)

def fetch_and_update_market_data(db, symbols):
    all_symbols_to_update = list(set(symbols + ["TWD=X"]))
    
    for symbol in all_symbols_to_update:
        print(f"--- Processing: {symbol} ---")
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(start="2000-01-01", interval="1d", auto_adjust=False, back_adjust=False)
            
            if hist.empty:
                print(f"Warning: No history found for {symbol}.")
                continue

            prices = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items() if not pd.isna(val)}
            dividends = {idx.strftime('%Y-%m-%d'): val for idx, val in stock.dividends.items() if val > 0}

            is_forex = symbol.endswith("=X")
            collection_name = "exchange_rates" if is_forex else "price_history"
            doc_ref = db.collection(collection_name).document(symbol)

            payload = {
                "prices": prices,
                "dividends": dividends,
                "lastUpdated": datetime.now().isoformat(),
                "dataSource": "yfinance-user-defined-split-model-v1"
            }
            if is_forex:
                payload["rates"] = payload.pop("prices")
                del payload["dividends"]

            payload["splits"] = {}

            doc_ref.set(payload)
            print(f"Successfully wrote price/dividend data for {symbol} to Firestore.")

        except Exception as e:
            print(f"ERROR: Failed to process data for {symbol}. Reason: {e}")

def get_all_user_ids(db):
    user_ids = set()
    try:
        users = db.collection("users").stream()
        for user in users:
            user_ids.add(user.id)
    except Exception as e:
        print(f"Warning: Could not read users. Error: {e}")
    print(f"Found {len(user_ids)} unique users: {list(user_ids)}")
    return list(user_ids)

# 這是一個範例函式，您的專案中可能沒有或有不同的實現
# 它的作用是觸發後端重新計算，通常是透過更新一個特定的欄位
def trigger_recalculation_for_users(db, user_ids):
    print(f"Triggering recalculation for {len(user_ids)} users...")
    for user_id in user_ids:
        try:
            doc_ref = db.collection("users").document(user_id).collection("user_data").document("current_holdings")
            doc_ref.set({"force_recalc_timestamp": datetime.now().isoformat()}, merge=True)
            print(f"Trigger sent for user: {user_id}")
        except Exception as e:
            print(f"Failed to send trigger for user {user_id}. Error: {e}")


if __name__ == "__main__":
    db_client = initialize_firebase()
    print("Starting market data update script (User-Defined Split Model)...")
    
    # 1. 取得所有交易紀錄中的股票代碼
    symbols_from_transactions = get_all_symbols_from_transactions(db_client)
    
    # 2. 取得所有使用者設定的 benchmark 代碼 (包含預設的 SPY)
    symbols_from_benchmarks = get_all_benchmarks_from_preferences(db_client)

    # 3. 將兩者合併，確保所有需要的資料都被更新
    all_symbols_to_update = list(set(symbols_from_transactions + symbols_from_benchmarks))
    
    if all_symbols_to_update:
        print(f"Total symbols to update: {len(all_symbols_to_update)} -> {all_symbols_to_update}")
        fetch_and_update_market_data(db_client, all_symbols_to_update)
        # 價格更新後，後端的 onWrite trigger 會自動處理重新計算，此處不再需要手動觸發
        # user_ids = get_all_user_ids(db_client)
        # if user_ids:
        #     trigger_recalculation_for_users(db_client, user_ids)
    else:
        print("No transactions or benchmarks found to update.")
        
    print("Market data update script finished.")
