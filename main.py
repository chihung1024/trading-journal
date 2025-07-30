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
            # 優先從環境變數讀取，適用於 GitHub Actions
            service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
            cred = credentials.Certificate(service_account_info)
            firebase_admin.initialize_app(cred)
        except Exception:
            # 若失敗，則嘗試本地檔案，適用於本地端開發
            print("Could not initialize from environment variable. Trying local file...")
            cred = credentials.Certificate("serviceAccountKey.json") # 請確保本地有此檔案
            firebase_admin.initialize_app(cred)
            
    return firestore.client()

def get_all_symbols_to_update(db):
    all_symbols = set()
    
    # 1. 從交易紀錄中獲取股票代碼
    try:
        transactions_group = db.collection_group("transactions")
        for trans_doc in transactions_group.stream():
            symbol = trans_doc.to_dict().get('symbol')
            if symbol:
                all_symbols.add(symbol.upper())
    except Exception as e:
        print(f"Warning: Could not read transactions. Error: {e}")
        
    # 2. 從使用者資料中獲取 Benchmark 代碼
    try:
        users_collection = db.collection("users")
        for user_doc in users_collection.stream():
            user_data_ref = user_doc.reference.collection("user_data").document("current_holdings")
            user_data = user_data_ref.get()
            if user_data.exists:
                benchmark = user_data.to_dict().get('benchmarkSymbol')
                if benchmark:
                    all_symbols.add(benchmark.upper())
    except Exception as e:
        print(f"Warning: Could not read user benchmark symbols. Error: {e}")

    print(f"Found {len(all_symbols)} unique symbols to update: {list(all_symbols)}")
    return list(all_symbols)

def fetch_and_update_market_data(db, symbols):
    # 將 TWD=X 匯率固定加入更新列表
    all_symbols_to_update = list(set(symbols + ["TWD=X"]))
    
    for symbol in all_symbols_to_update:
        if not symbol: continue
        print(f"--- Processing: {symbol} ---")
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="max", interval="1d", auto_adjust=False, back_adjust=False)
            
            if hist.empty:
                print(f"Warning: No history found for {symbol}.")
                continue

            prices = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items() if not pd.isna(val)}
            
            # 獲取股利資訊
            dividends_df = stock.dividends
            dividends = {idx.strftime('%Y-%m-%d'): val for idx, val in dividends_df.items()} if not dividends_df.empty else {}

            is_forex = "=" in symbol
            collection_name = "exchange_rates" if is_forex else "price_history"
            doc_ref = db.collection(collection_name).document(symbol)

            payload = {
                "prices": prices,
                "dividends": dividends,
                "lastUpdated": datetime.now().isoformat(),
                "dataSource": "yfinance-scheduled-update-v2"
            }
            if is_forex:
                payload["rates"] = payload.pop("prices")
                del payload["dividends"]

            # 我們不再從 yfinance 儲存拆股數據，因為我們使用手動輸入的拆股事件
            payload["splits"] = {}

            doc_ref.set(payload)
            print(f"Successfully wrote price/dividend data for {symbol} to Firestore.")

        except Exception as e:
            print(f"ERROR: Failed to process data for {symbol}. Reason: {e}")

if __name__ == "__main__":
    db_client = initialize_firebase()
    print("Starting market data update script...")
    symbols_to_update = get_all_symbols_to_update(db_client)
    if symbols_to_update:
        fetch_and_update_market_data(db_client, symbols_to_update)
    else:
        print("No symbols found to update.")
    print("Market data update script finished.")
