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
    print(f"Found {len(all_symbols)} unique symbols: {list(all_symbols)}")
    return list(all_symbols)

# Simplified data fetcher. It no longer performs reverse adjustments.
# It fetches and stores the split-adjusted prices directly from yfinance.
def fetch_and_update_market_data(db, symbols):
    all_symbols_to_update = list(set(symbols + ["TWD=X"]))
    
    for symbol in all_symbols_to_update:
        print(f"--- Processing: {symbol} ---")
        try:
            stock = yf.Ticker(symbol)
            # auto_adjust=False gives split-adjusted prices and separate split/dividend data.
            hist = stock.history(start="2000-01-01", interval="1d", auto_adjust=False, back_adjust=False)
            
            if hist.empty:
                print(f"Warning: No history found for {symbol}.")
                continue

            prices = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items() if not pd.isna(val)}
            splits = {idx.strftime('%Y-%m-%d'): val for idx, val in stock.splits.items()}
            dividends = {idx.strftime('%Y-%m-%d'): val for idx, val in stock.dividends.items()}

            is_forex = symbol == "TWD=X"
            collection_name = "exchange_rates" if is_forex else "price_history"
            doc_ref = db.collection(collection_name).document(symbol)

            payload = {
                "prices": prices,
                "splits": splits,
                "dividends": dividends,
                "lastUpdated": datetime.now().isoformat(),
                "dataSource": "yfinance-split-adjusted-v1"
            }
            if is_forex:
                payload["rates"] = payload.pop("prices")

            doc_ref.set(payload)
            print(f"Successfully wrote split-adjusted price data for {symbol} to Firestore.")

        except Exception as e:
            print(f"ERROR: Failed to process data for {symbol}. Reason: {e}")

if __name__ == "__main__":
    db_client = initialize_firebase()
    print("Starting market data update script (Simplified Model)...")
    symbols = get_all_symbols_from_transactions(db_client)
    if symbols:
        fetch_and_update_market_data(db_client, symbols)
    else:
        print("No transactions found.")
    print("Market data update script finished.")
