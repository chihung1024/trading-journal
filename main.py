import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app
import json
from datetime import datetime, timedelta, date

# --- Firebase Initialization ---
try:
    # In a deployed environment, GOOGLE_APPLICATION_CREDENTIALS will be set
    # with the service account JSON.
    if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
        cred = credentials.ApplicationDefault()
    else:
        # For local development, use a service account file
        service_account_key_path = os.environ.get("SERVICE_ACCOUNT_KEY_PATH")
        if not service_account_key_path:
            raise ValueError("SERVICE_ACCOUNT_KEY_PATH environment variable not set.")
        cred = credentials.Certificate(service_account_key_path)

    # Get project ID from environment or service account
    project_id = os.environ.get("GCP_PROJECT")
    if not project_id:
        if isinstance(cred, credentials.Certificate):
             with open(cred.service_account_file) as f:
                project_id = json.load(f).get('project_id')
        elif hasattr(cred, '_project_id'):
            project_id = cred._project_id

    firebase_admin.initialize_app(cred, {
        'projectId': project_id,
    })
except Exception as e:
    print(f"Error initializing Firebase: {e}")
    # Exit if initialization fails
    exit(1)

db = firestore.client()

def get_all_user_transactions():
    """Fetches all user transactions from Firestore."""
    all_transactions = {}
    transactions_group = db.collection_group("transactions")
    for trans_doc in transactions_group.stream():
        path_parts = trans_doc.reference.path.split('/')
        if len(path_parts) >= 3 and path_parts[0] == 'users':
            uid = path_parts[1]
            if uid not in all_transactions:
                all_transactions[uid] = []
            
            data = trans_doc.to_dict()
            data['id'] = trans_doc.id
            # Convert timestamp to date string if necessary
            if 'date' in data and hasattr(data['date'], 'strftime'):
                 data['date'] = data['date'].strftime('%Y-%m-%d')
            elif 'date' in data and isinstance(data['date'], str):
                 try:
                    data['date'] = datetime.fromisoformat(data['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
                 except ValueError:
                    pass # Keep original string if format is wrong
            all_transactions[uid].append(data)
    return all_transactions

def fetch_and_update_market_data(symbols):
    """Fetches and updates price, split, and dividend data for given symbols."""
    all_symbols = list(symbols) + ["TWD=X"]
    
    for symbol in all_symbols:
        is_forex = symbol.endswith("=X")
        collection_name = "exchange_rates" if is_forex else "price_history"
        doc_ref = db.collection(collection_name).document(symbol)
        
        start_date_str = "2000-01-01"
        try:
            doc = doc_ref.get()
            if doc.exists:
                last_updated_str = doc.to_dict().get("lastUpdated")
                if last_updated_str:
                    # Add one day to the last updated date to avoid duplicates
                    last_updated_date = datetime.fromisoformat(last_updated_str.replace("Z", "+00:00")).date()
                    start_date = last_updated_date + timedelta(days=1)
                    start_date_str = start_date.strftime('%Y-%m-%d')

        except Exception as e:
            print(f"Error reading last update date for {symbol}: {e}")

        if start_date_str > datetime.now().strftime('%Y-%m-%d'):
            print(f"Data for {symbol} is up to date. Skipping.")
            continue

        try:
            print(f"Fetching data for {symbol} from {start_date_str}...")
            stock = yf.Ticker(symbol)
            
            history_df = stock.history(start=start_date_str, interval="1d", auto_adjust=False, back_adjust=False)
            
            update_payload = {}

            if not history_df.empty:
                if is_forex:
                    update_payload["rates"] = {idx.strftime('%Y-%m-%d'): val for idx, val in history_df['Close'].items() if val}
                else:
                    update_payload["prices"] = {idx.strftime('%Y-%m-%d'): val for idx, val in history_df['Close'].items() if val}
            
            # For stocks, also fetch full history for splits and dividends to ensure completeness
            if not is_forex:
                all_events_df = stock.history(period="max", interval="1d", actions=True, auto_adjust=False, back_adjust=False)
                
                if not all_events_df['Stock Splits'].empty:
                    splits = all_events_df[all_events_df['Stock Splits'] > 0]['Stock Splits']
                    if not splits.empty:
                        update_payload["splits"] = {idx.strftime('%Y-%m-%d'): val for idx, val in splits.items()}

                if not all_events_df['Dividends'].empty:
                    dividends = all_events_df[all_events_df['Dividends'] > 0]['Dividends']
                    if not dividends.empty:
                        update_payload["dividends"] = {idx.strftime('%Y-%m-%d'): val for idx, val in dividends.items()}

            if update_payload:
                update_payload["lastUpdated"] = datetime.now().isoformat()
                doc_ref.set(update_payload, merge=True)
                print(f"Successfully updated data for {symbol}.")
            else:
                print(f"No new data found for {symbol} since {start_date_str}.")

        except Exception as e:
            print(f"Error fetching or saving data for {symbol}: {e}")

if __name__ == "__main__":
    print("Starting daily market data update script...")
    transactions_by_user = get_all_user_transactions()
    all_symbols = set()
    for uid, transactions in transactions_by_user.items():
        for t in transactions:
            if t.get('symbol'):
                all_symbols.add(t['symbol'].upper())
    
    if all_symbols:
        fetch_and_update_market_data(all_symbols)
    else:
        print("No transactions found, skipping market data update.")
        
    print("Market data update finished.")
