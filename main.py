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

            # --- Reverse-adjust prices to get original historical prices ---
            prices_df = hist[['Close']].copy()
            splits_df = stock.splits

            if not prices_df.index.tz:
                prices_df.index = prices_df.index.tz_localize('UTC')
            if not splits_df.index.tz:
                splits_df.index = splits_df.index.tz_localize('UTC')

            prices_df.index = prices_df.index.tz_convert(None)
            splits_df.index = splits_df.index.tz_convert(None)

            prices_df.sort_index(ascending=False, inplace=True)
            splits_df.sort_index(ascending=False, inplace=True)

            cumulative_ratio = 1.0
            split_iloc = 0
            
            for i in range(len(prices_df)):
                current_date = prices_df.index[i]
                while split_iloc < len(splits_df) and current_date < splits_df.index[split_iloc]:
                    cumulative_ratio *= splits_df.iloc[split_iloc]
                    split_iloc += 1
                
                prices_df.iloc[i, 0] = prices_df.iloc[i, 0] * cumulative_ratio

            prices_df.sort_index(ascending=True, inplace=True)
            original_prices = {idx.strftime('%Y-%m-%d'): val for idx, val in prices_df['Close'].items() if not pd.isna(val)}

            # --- Prepare final payload ---
            dividends = stock.dividends
            is_forex = symbol == "TWD=X"
            collection_name = "exchange_rates" if is_forex else "price_history"
            doc_ref = db.collection(collection_name).document(symbol)

            payload = {
                "prices": original_prices,
                "splits": {idx.strftime('%Y-%m-%d'): val for idx, val in splits_df.items()},
                "dividends": {idx.strftime('%Y-%m-%d'): val for idx, val in dividends.items()} if dividends is not None else {},
                "lastUpdated": datetime.now().isoformat(),
                "dataSource": "yfinance-original-price-model-v4"
            }
            if is_forex:
                payload["rates"] = payload.pop("prices")

            doc_ref.set(payload)
            print(f"Successfully wrote original price data for {symbol} to Firestore.")

        except Exception as e:
            print(f"ERROR: Failed to process data for {symbol}. Reason: {e}")

if __name__ == "__main__":
    db_client = initialize_firebase()
    print("Starting market data update script...")
    symbols = get_all_symbols_from_transactions(db_client)
    if symbols:
        fetch_and_update_market_data(db_client, symbols)
    else:
        print("No transactions found.")
    print("Market data update script finished.")
