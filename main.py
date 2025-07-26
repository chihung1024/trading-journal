import os
import json
import sys
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
from forex_python.converter import CurrencyRates
from transaction_manager import get_split_multiplier

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
c = CurrencyRates()
# -----------------------------------------------------

def update_data():
    """Update stock prices, exchange rates, and adjust transactions for splits."""
    # ... (rest of the function is the same)
    rates = c.get_rates('USD')
    rates['USD'] = 1.0
    db.collection('rates').document('latest').set(rates)

    transactions = db.collection('transactions').stream()
    stock_ids = set(t.to_dict()['stock_id'] for t in transactions)

    for stock_id in stock_ids:
        stock = yf.Ticker(stock_id)
        price = stock.history(period="1d")["Close"][0]
        db.collection("stocks").document(stock_id).set({
            "price": price,
            "currency": "USD",
            "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

        transactions_to_update = db.collection('transactions').where('stock_id', '==', stock_id).stream()
        for trans_doc in transactions_to_update:
            trans_data = trans_doc.to_dict()
            split_multiplier = get_split_multiplier(stock_id, trans_data['date'])
            new_adjusted_shares = trans_data['shares_original'] * split_multiplier
            if new_adjusted_shares != trans_data.get('shares_adjusted'):
                trans_doc.reference.update({'shares_adjusted': new_adjusted_shares})

if __name__ == '__main__':
    update_data()
