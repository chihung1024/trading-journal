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

# Simplified data fetcher. It no longer fetches or stores split data.
# It only fetches prices (which are split-adjusted) and dividends.
def fetch_and_update_market_data(db, symbols):
    all_symbols_to_update = list(set(symbols + ["TWD=X"]))
    
    for symbol in all_symbols_to_update:
        print(f"--- Processing: {symbol} ---")
        try:
            stock = yf.Ticker(symbol)
            # auto_adjust=False gives split-adjusted prices and separate dividend data.
            hist = stock.history(start="2000-01-01", interval="1d", auto_adjust=False, back_adjust=False)
            
            if hist.empty:
                print(f"Warning: No history found for {symbol}.")
                continue

            prices = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items() if not pd.isna(val)}
            dividends = {idx.strftime('%Y-%m-%d'): val for idx, val in stock.dividends.items()}

            is_forex = symbol == "TWD=X"
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
                del payload["dividends"] # No dividends for forex

            # We no longer store split data from yfinance
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


if __name__ == "__main__":
    db_client = initialize_firebase()
    print("Starting market data update script (User-Defined Split Model)...")
    symbols = get_all_symbols_from_transactions(db_client)
    if symbols:
        fetch_and_update_market_data(db_client, symbols)
        user_ids = get_all_user_ids(db_client)
        if user_ids:
            trigger_recalculation_for_users(db_client, user_ids)
    else:
        print("No transactions found.")
    print("Market data update script finished.")
