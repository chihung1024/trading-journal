import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app
import json
from datetime import datetime
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
    
    # 1. Get symbols from all user transactions
    try:
        transactions_group = db.collection_group("transactions")
        for trans_doc in transactions_group.stream():
            symbol = trans_doc.to_dict().get('symbol')
            if symbol:
                all_symbols.add(symbol.upper())
        print(f"Found {len(all_symbols)} symbols from transactions.")
    except Exception as e:
        print(f"Warning: Could not read transactions. Error: {e}")
        
    # 2. Get benchmark symbols from all user holdings
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
    """
    Fetches market data for a list of symbols with a retry mechanism
    and overwrites the data in Firestore for maximum robustness.
    """
    # Always include the TWD exchange rate in the update list
    all_symbols_to_update = list(set(symbols + ["TWD=X"]))
    
    for symbol in all_symbols_to_update:
        if not symbol: continue
        
        print(f"--- Processing: {symbol} ---")
        
        # Retry mechanism for fetching data
        max_retries = 3
        for attempt in range(max_retries):
            try:
                stock = yf.Ticker(symbol)
                # Fetch full history to ensure data integrity and self-heal any gaps
                hist = stock.history(period="max", interval="1d", auto_adjust=False, back_adjust=False)
                
                if hist.empty:
                    print(f"Warning: No history found for {symbol}. It might be a delisted or invalid ticker.")
                    break # No point in retrying if history is empty

                # If successful, break the retry loop
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
                    "dataSource": "yfinance-daily-full-refresh-v4" # Updated source name
                }
                if is_forex:
                    payload["rates"] = payload.pop("prices")
                    del payload["dividends"]

                # Splits are handled by user manual input, so we provide an empty map here.
                payload["splits"] = {}

                # Overwrite the document to ensure data is always complete and consistent
                doc_ref.set(payload)
                print(f"Successfully wrote full price/dividend data for {symbol} to Firestore.")
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
