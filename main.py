import os
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
from forex_python.converter import CurrencyRates
from transaction_manager import get_split_multiplier

cred = credentials.Certificate(os.environ.get("FIREBASE_ADMIN_CREDENTIALS"))
firebase_admin.initialize_app(cred)

db = firestore.client()
c = CurrencyRates()

def update_data(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # 1. Update stock prices and check for new splits
    transactions = db.collection('transactions').stream()
    unique_stock_ids = set(t.to_dict()['stock_id'] for t in transactions)

    for stock_id in unique_stock_ids:
        stock = yf.Ticker(stock_id)
        
        # Update price
        price = stock.history(period="1d")["Close"][0]
        db.collection("stocks").document(stock_id).set({
            "price": price,
            "currency": "USD",
            "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

        # Check for new splits and re-process transactions if necessary
        # A more robust solution would be to store the last checked split date
        # and only check for splits after that date.
        # For simplicity, we re-calculate for all transactions of this stock.
        transactions_to_update = db.collection('transactions').where('stock_id', '==', stock_id).stream()
        for trans_doc in transactions_to_update:
            trans_data = trans_doc.to_dict()
            
            split_multiplier = get_split_multiplier(stock_id, trans_data['date'])
            new_adjusted_shares = trans_data['shares_original'] * split_multiplier
            
            if new_adjusted_shares != trans_data.get('shares_adjusted'):
                trans_doc.reference.update({'shares_adjusted': new_adjusted_shares})

    # 2. Update exchange rates
    rates = c.get_rates('USD') # Base currency USD
    rates['USD'] = 1.0
    db.collection('rates').document('latest').set(rates)
