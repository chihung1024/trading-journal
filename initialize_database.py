import os
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, firestore
from transaction_manager import process_transaction

# Initialize Firebase Admin SDK
cred = credentials.Certificate(os.environ.get("FIREBASE_ADMIN_CREDENTIALS"))
firebase_admin.initialize_app(cred)

db = firestore.client()

def initialize_database():
    # Clear existing data
    for col in ['stocks', 'transactions', 'rates']:
        docs = db.collection(col).stream()
        for doc in docs:
            doc.reference.delete()

    # Sample transactions
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
            'shares_original': 5, # This was before a 3-for-1 split in Aug 2022
            'price_original': 900.0, # Price before split
            'currency_original': 'USD'
        },
        {
            'stock_id': 'GOOGL',
            'date': (datetime.now() - timedelta(days=900)).strftime('%Y-%m-%d'),
            'type': 'buy',
            'shares_original': 2, # This was before a 20-for-1 split in Jul 2022
            'price_original': 2200.0, # Price before split
            'currency_original': 'USD'
        }
    ]

    for transaction in transactions:
        processed_transaction = process_transaction(transaction)
        db.collection('transactions').add(processed_transaction)
        print(f"Added transaction for {processed_transaction['stock_id']}")

if __name__ == '__main__':
    initialize_database()
