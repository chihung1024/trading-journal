import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore
import json
from datetime import datetime

def initialize_firebase():
    """Initializes Firebase connection."""
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

        firebase_admin.initialize_app(cred, {'projectId': project_id})
        print("Firebase initialized successfully.")
        return firestore.client()
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        return None

def populate_initial_exchange_rates(db):
    """Fetches TWD/USD exchange rates from 2000 to now and stores them in Firestore."""
    symbol = "TWD=X"
    start_date = "2000-01-01"
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    print(f"Fetching {symbol} exchange rate data from {start_date} to {today_str}...")
    
    try:
        hist = yf.Ticker(symbol).history(start=start_date, interval="1d")
        
        if hist.empty:
            print(f"Error: No history found for {symbol}.")
            return

        rates_data = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items() if val}
        
        doc_ref = db.collection("exchange_rates").document(symbol)
        
        payload = {
            "rates": rates_data,
            "lastUpdated": datetime.now().isoformat(),
            "dataSource": "yfinance",
            "description": "Historical daily exchange rates for USD to TWD."
        }
        
        doc_ref.set(payload)
        print(f"Successfully stored {len(rates_data)} exchange rate entries in Firestore.")

    except Exception as e:
        print(f"An error occurred while processing exchange rates: {e}")

if __name__ == "__main__":
    db_client = initialize_firebase()
    if db_client:
        populate_initial_exchange_rates(db_client)
