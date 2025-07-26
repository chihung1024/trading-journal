import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app
import json
from datetime import datetime
import pandas as pd

def initialize_firebase():
    """Initializes Firebase connection if not already initialized."""
    try:
        get_app()
        print("Firebase app already initialized.")
    except ValueError:
        try:
            service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
            cred = credentials.Certificate(service_account_info)
            project_id = service_account_info.get("project_id")
            firebase_admin.initialize_app(cred, {'projectId': project_id})
            print("Firebase initialized successfully.")
        except (ValueError, KeyError) as e:
            print(f"CRITICAL: Firebase initialization failed: {e}. Ensure FIREBASE_SERVICE_ACCOUNT is set.")
            exit(1)
    return firestore.client()

def get_all_symbols_from_transactions(db):
    """
    Scans the 'transactions' collection group to find all unique stock symbols
    across all users.
    """
    all_symbols = set()
    transactions_group = db.collection_group("transactions")
    for trans_doc in transactions_group.stream():
        try:
            symbol = trans_doc.to_dict().get('symbol')
            if symbol:
                all_symbols.add(symbol.upper())
        except Exception as e:
            print(f"Warning: Could not process transaction doc {trans_doc.id}. Error: {e}")
    print(f"Found {len(all_symbols)} unique symbols across all users: {list(all_symbols)}")
    return list(all_symbols)

def fetch_and_update_market_data(db, symbols):
    """
    Fetches and updates historical prices, splits, and dividends for a list of symbols.
    Also handles exchange rate updates for TWD=X.
    """
    all_symbols_to_update = list(set(symbols + ["TWD=X"]))
    
    for symbol in all_symbols_to_update:
        is_forex = symbol == "TWD=X"
        collection_name = "exchange_rates" if is_forex else "price_history"
        doc_ref = db.collection(collection_name).document(symbol)
        
        start_date = "2000-01-01"
        
        print(f"--- Processing: {symbol} ---")
        print(f"Fetching full history for {symbol} from {start_date}...")

        try:
            stock = yf.Ticker(symbol)
            
            hist = stock.history(start=start_date, interval="1d", auto_adjust=False, back_adjust=False)
            
            splits = stock.splits
            dividends = stock.dividends if not is_forex else None

            payload = {}

            if not hist.empty:
                prices_data = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items() if not pd.isna(val)}
                payload["prices"] = prices_data
                print(f"Found {len(prices_data)} price points.")
            
            if is_forex:
                payload = {
                    "rates": payload.get("prices", {}),
                    "lastUpdated": datetime.now().isoformat(),
                    "dataSource": "yfinance",
                    "description": "Historical daily exchange rates for USD to TWD."
                }
            else:
                if splits is not None and not splits.empty:
                    splits_data = {idx.strftime('%Y-%m-%d'): val for idx, val in splits.items()}
                    payload["splits"] = splits_data
                    print(f"Found {len(splits_data)} split events.")
                else:
                    payload["splits"] = {}

                if dividends is not None and not dividends.empty:
                    dividends_data = {idx.strftime('%Y-%m-%d'): val for idx, val in dividends.items()}
                    payload["dividends"] = dividends_data
                    print(f"Found {len(dividends_data)} dividend events.")
                else:
                    payload["dividends"] = {}
                
                payload["lastUpdated"] = datetime.now().isoformat()
                payload["dataSource"] = "yfinance"

            if payload.get("prices") or payload.get("rates"):
                doc_ref.set(payload)
                print(f"Successfully wrote complete market data for {symbol} to Firestore.")
            else:
                print(f"Warning: No data found for {symbol}. Skipping database write.")

        except Exception as e:
            print(f"ERROR: Failed to fetch or save data for {symbol}. Reason: {e}")

if __name__ == "__main__":
    db_client = initialize_firebase()
    
    print("Starting daily market data update script...")
    
    symbols_to_update = get_all_symbols_from_transactions(db_client)

    if symbols_to_update:
        fetch_and_update_market_data(db_client, symbols_to_update)
    else:
        print("No user transactions found. No market data to update.")

    print("Market data update script finished.")
