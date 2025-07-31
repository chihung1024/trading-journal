import os
import firebase_admin
from firebase_admin import credentials, firestore, get_app
import json

def initialize_firebase():
    """Initializes the Firebase app, trying environment variables first, then a local file."""
    try:
        get_app()
    except ValueError:
        try:
            service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
            cred = credentials.Certificate(service_account_info)
            firebase_admin.initialize_app(cred)
            print("Firebase initialized successfully from environment variable.")
        except Exception as e1:
            print(f"Could not initialize from environment variable ({e1}). Trying local file 'serviceAccountKey.json'...")
            try:
                cred = credentials.Certificate("serviceAccountKey.json")
                firebase_admin.initialize_app(cred)
                print("Firebase initialized successfully from local file.")
            except Exception as e2:
                print(f"FATAL: Firebase initialization failed from both sources. Error (local file): {e2}")
                exit(1)
            
    return firestore.client()

def bootstrap_metadata(db):
    print("Starting to bootstrap stock metadata...")
    all_transactions = db.collection_group("transactions").stream()
    
    earliest_dates = {}
    
    print("Step 1: Reading all transactions to find the earliest date for each symbol...")
    count = 0
    for trans in all_transactions:
        data = trans.to_dict()
        symbol = data.get("symbol")
        date = data.get("date")
        if symbol and date:
            symbol = symbol.upper()
            if hasattr(date, 'to_datetime'):
                date = date.to_datetime()

            if symbol not in earliest_dates or date < earliest_dates[symbol]:
                earliest_dates[symbol] = date
        count += 1
    print(f"Processed {count} transactions. Found {len(earliest_dates)} unique symbols.")
    
    print("Step 2: Writing metadata to Firestore...")
    batch = db.batch()
    for symbol, date in earliest_dates.items():
        metadata_ref = db.collection("stock_metadata").document(symbol)
        payload = {
            "symbol": symbol,
            "earliestTxDate": date,
            "lastUpdated": firestore.SERVER_TIMESTAMP
        }
        batch.set(metadata_ref, payload, merge=True)
        print(f"  - Staging update for {symbol} with earliest date {date.strftime('%Y-%m-%d')}")
        
    batch.commit()
    print("Bootstrap complete. All metadata has been written.")

if __name__ == "__main__":
    db_client = initialize_firebase()
    bootstrap_metadata(db_client)
