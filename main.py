import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app
import json
from datetime import datetime, timedelta
import pandas as pd
import time

def initialize_firebase():
    """Initializes the Firebase app, trying environment variables first, then a local file."""
    try:
        get_app()
    except ValueError:
        try:
            # Priority for GitHub Actions: read from environment variable
            service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
            cred = credentials.Certificate(service_account_info)
            firebase_admin.initialize_app(cred)
            print("Firebase initialized successfully from environment variable.")
        except Exception as e1:
            # Fallback for local development: try a local file
            print(f"Could not initialize from environment variable ({e1}). Trying local file 'serviceAccountKey.json'...")
            try:
                cred = credentials.Certificate("serviceAccountKey.json")
                firebase_admin.initialize_app(cred)
                print("Firebase initialized successfully from local file.")
            except Exception as e2:
                print(f"FATAL: Firebase initialization failed from both sources. Error (local file): {e2}")
                exit(1)
            
    return firestore.client()

def get_all_symbols_to_update(db):
    """Gathers all unique symbols from user transactions and benchmark settings."""
    all_symbols = set()
    
    try:
        transactions_group = db.collection_group("transactions")
        for trans_doc in transactions_group.stream():
            symbol = trans_doc.to_dict().get('symbol')
            if symbol:
                all_symbols.add(symbol.upper())
        print(f"Found {len(all_symbols)} symbols from transactions.")
    except Exception as e:
        print(f"Warning: Could not read transactions. Error: {e}")
        
    try:
        holdings_group = db.collection_group("current_holdings")
        for holding_doc in holdings_group.stream():
            benchmark = holding_doc.to_dict().get('benchmarkSymbol')
            if benchmark:
                all_symbols.add(benchmark.upper())
        print(f"Found symbols from benchmarks, total unique symbols now: {len(all_symbols)}.")
    except Exception as e:
        print(f"Warning: Could not read user benchmark symbols. Error: {e}")

    final_list = list(filter(None, all_symbols))
    print(f"Found {len(final_list)} unique symbols to update: {final_list}")
    return final_list

def fetch_and_update_market_data(db, symbols):
    all_symbols_to_update = list(set(symbols + ["TWD=X", "SPY"])) # 匯率和通用 benchmark 固定更新
    
    for symbol in all_symbols_to_update:
        if not symbol: continue
        
        print(f"--- Processing: {symbol} ---")
        
        start_date = None
        try:
            # 讀取 metadata 決定起始日期
            metadata_ref = db.collection("stock_metadata").document(symbol).get()
            if metadata_ref.exists:
                earliest_tx_timestamp = metadata_ref.to_dict().get("earliestTxDate")
                if earliest_tx_timestamp:
                    # 轉換為 datetime 物件並往前推 30 天作為緩衝
                    earliest_dt = earliest_tx_timestamp.replace(tzinfo=None)
                    start_dt = earliest_dt - timedelta(days=30)
                    start_date = start_dt.strftime('%Y-%m-%d')
                    print(f"Found metadata for {symbol}. Fetching data from {start_date}.")
        except Exception as e:
            print(f"Could not read metadata for {symbol}, falling back to max period. Error: {e}")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                stock = yf.Ticker(symbol)
                # 如果有 start_date 則使用，否則抓取全部歷史 (max) 作為備用
                hist = stock.history(start=start_date, interval="1d", auto_adjust=False, back_adjust=False)
                
                if hist.empty:
                    print(f"Warning: No history found for {symbol}.")
                    break

                print(f"Successfully fetched data for {symbol} on attempt {attempt + 1}.")
                
                prices = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items() if pd.notna(val)}
                dividends_df = hist['Dividends']
                dividends = {idx.strftime('%Y-%m-%d'): val for idx, val in dividends_df.items() if val > 0}

                is_forex = "=" in symbol
                collection_name = "exchange_rates" if is_forex else "price_history"
                doc_ref = db.collection(collection_name).document(symbol)

                payload = {
                    "prices": prices,
                    "dividends": dividends,
                    "lastUpdated": datetime.now().isoformat(),
                    "dataSource": "yfinance-daily-refresh-v8"
                }
                if is_forex:
                    payload["rates"] = payload.pop("prices")
                    del payload["dividends"]
                
                payload["splits"] = {}

                # 使用 set merge，以合併方式寫入，避免覆蓋掉使用者手動輸入的 splits
                doc_ref.set(payload, merge=True)
                print(f"Successfully wrote price/dividend data for {symbol} to Firestore.")
                break

            except Exception as e:
                print(f"ERROR on attempt {attempt + 1} for {symbol}: {e}")
                if attempt < max_retries - 1:
                    print("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    print(f"FATAL: Failed to process data for {symbol} after {max_retries} attempts.")

if __name__ == "__main__":
    db_client = initialize_firebase()
    print("Starting market data update script...")
    symbols_to_update = get_all_symbols_to_update(db_client)
    if symbols_to_update:
        fetch_and_update_market_data(db_client, symbols_to_update)
    else:
        print("No symbols found in user portfolios to update.")
    print("Market data update script finished.")
