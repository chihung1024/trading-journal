import yfinance as yf
from datetime import datetime, timedelta

def get_split_multiplier(stock_id, transaction_date):
    """
    Calculates the cumulative split multiplier for a stock from a given date onwards.
    """
    stock = yf.Ticker(stock_id)
    
    # yfinance returns splits as a pandas Series with dates as index and multiplier as value
    # e.g., 2024-06-10 00:00:00-04:00    10.0
    splits = stock.splits
    
    # Ensure the transaction_date is timezone-aware to match the splits index
    # We'll make it naive and then localize to the stock's exchange timezone, or just compare dates
    transaction_date_dt = datetime.strptime(transaction_date, '%Y-%m-%d').date()

    multiplier = 1.0
    for date, split_ratio in splits.items():
        if date.date() > transaction_date_dt:
            multiplier *= split_ratio
            
    return multiplier

def process_transaction(transaction):
    """
    Processes a single transaction record, calculating the adjusted shares.
    This function should be called when a new transaction is added.
    """
    stock_id = transaction['stock_id']
    transaction_date = transaction['date'] # Expects 'YYYY-MM-DD'
    
    split_multiplier = get_split_multiplier(stock_id, transaction_date)
    
    original_shares = transaction['shares_original']
    
    transaction['shares_adjusted'] = original_shares * split_multiplier
    
    # Calculate cost basis in local currency (the currency of the transaction)
    transaction['cost_basis_local'] = transaction['price_original'] * transaction['shares_original']
    
    return transaction
