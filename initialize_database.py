import os
import json
import sys
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, firestore
from transaction_manager import process_transaction

# --- Firebase Initialization with Error Handling ---
firebase_credentials = os.environ.get("FIREBASE_ADMIN_CREDENTIALS")
if not firebase_credentials:
    print("Error: FIREBASE_ADMIN_CREDENTIALS environment variable not set.")
    sys.exit(1)

try:
    cred_json = json.loads(firebase_credentials)
except json.JSONDecodeError:
    print("Error: FIREBASE_ADMIN_CREDENTIALS is not a valid JSON string.")
    sys.exit(1)

cred = credentials.Certificate(cred_json)
firebase_admin.initialize_app(cred)
db = firestore.client()
# -----------------------------------------------------

def initialize_database():
    # ... (rest of the function is the same)
    for col in ['stocks', 'transactions', 'rates']:
        docs = db.collection(col).stream()
        for doc in docs:
            doc.reference.delete()

    transactions = [
        {
            'stock_id': 'AAPL',
            'date': (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d'),
            'type': 'buy',
            'shares_original': 10,
            'price_original': 150.0,
            'currency_original': 'USD'
        },
        {
            'stock_id': 'TSLA',
            'date': (datetime.now() - timedelta(days=800)).strftime('%Y-%m-%d'),
            'type': 'buy',
            'shares_original': 5,
            'price_original': 900.0,
            'currency_original': 'USD'
        },
        {
            'stock_id': 'GOOGL',
            'date': (datetime.now() - timedelta(days=900)).strftime('%Y-%m-%d'),
            'type': 'buy',
            'shares_original': 2,
            'price_original': 2200.0,
            'currency_original': 'USD'
        }
    ]

    for transaction in transactions:
        processed_transaction = process_transaction(transaction)
        db.collection('transactions').add(processed_transaction)
        print(f"Added transaction for {processed_transaction['stock_id']}")

if __name__ == '__main__':
    initialize_database()
