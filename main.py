import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app
import json
from datetime import datetime, timedelta
import pandas as pd

def initialize_firebase():
    try:
        get_app()
    except ValueError:
        try:
            service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
            cred = credentials.Certificate(service_account_info)
            firebase_admin.initialize_app(cred)
            print("Firebase initialized successfully from environment variable.")
        except Exception as e1:
            print(f"Could not initialize from env var ({e1}). Trying local 'serviceAccountKey.json'...")
            try:
                cred = credentials.Certificate("serviceAccountKey.json")
                firebase_admin.initialize_app(cred)
                print("Firebase initialized successfully from local file.")
            except Exception as e2:
                print(f"FATAL: Firebase initialization failed. Error: {e2}")
                exit(1)
    return firestore.client()

def get_all_active_symbols(db):
    active_symbols = set()
    try:
        holdings_group = db.collection_group("current_holdings")
        for holding_doc in holdings_group.stream():
            data = holding_doc.to_dict()
            if "holdings" in data and isinstance(data["holdings"], dict):
                for symbol in data["holdings"].keys():
                    if symbol:
                        active_symbols.add(symbol.upper())
            if "benchmarkSymbol" in data and data["benchmarkSymbol"]:
                active_symbols.add(data["benchmarkSymbol"].upper())
    except Exception as e:
        print(f"Warning: Could not read active symbols from holdings. Error: {e}")

    final_list = list(filter(None, active_symbols))
    print(f"Found {len(final_list)} active symbols to update: {final_list}")
    return final_list

def fetch_and_merge_recent_data(db, symbols):
    all_symbols_to_update = list(set(symbols + ["TWD=X"]))
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=10)

    for symbol in all_symbols_to_update:
        if not symbol: continue
        print(f"--- Processing recent data for: {symbol} ---")
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval="1d", auto_adjust=False, back_adjust=False)
            
            if hist.empty:
                print(f"Warning: No recent history found for {symbol}.")
                continue

            prices = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items() if pd.notna(val)}
            dividends = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Dividends'].items() if val > 0}

            is_forex = "=" in symbol
            collection_name = "exchange_rates" if is_forex else "price_history"
            doc_ref = db.collection(collection_name).document(symbol)

            update_data = {}
            if is_forex:
                for date, rate in prices.items():
                    update_data[f'rates.{date}'] = rate
            else:
                for date, price in prices.items():
                    update_data[f'prices.{date}'] = price
                for date, dividend in dividends.items():
                    update_data[f'dividends.{date}'] = dividend
            
            update_data['lastUpdated'] = datetime.now().isoformat()
            update_data['dataSource'] = 'scheduled-merge-v4'

            if update_data:
                # 使用 set + merge 來確保文件存在，並更新巢狀欄位
                doc_ref.set(update_data, merge=True)
                print(f"Successfully merged recent data for {symbol} into Firestore.")

        except Exception as e:
            print(f"ERROR: Failed to process recent data for {symbol}. Reason: {e}")

if __name__ == "__main__":
    db_client = initialize_firebase()
    print("Starting scheduled data update script...")
    active_symbols = get_all_active_symbols(db_client)
    if active_symbols:
        fetch_and_merge_recent_data(db_client, active_symbols)
    else:
        print("No active symbols found to update.")
    print("Scheduled data update script finished.")
