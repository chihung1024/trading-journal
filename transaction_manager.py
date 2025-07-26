import yfinance as yf
from datetime import datetime

def get_split_multiplier(stock_id: str, start_date: str) -> float:
    """Calculates the cumulative split multiplier for a stock from a given date onwards."""
    stock = yf.Ticker(stock_id)
    splits = stock.splits
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    multiplier = 1.0
    for date, split_ratio in splits.items():
        if date.date() > start_date_dt:
            multiplier *= split_ratio
    return multiplier

def process_transaction(transaction: dict) -> dict:
    """Processes a single transaction record, calculating the adjusted shares."""
    stock_id = transaction['stock_id']
    transaction_date = transaction['date']
    split_multiplier = get_split_multiplier(stock_id, transaction_date)
    transaction['shares_adjusted'] = transaction['shares_original'] * split_multiplier
    transaction['cost_basis_local'] = transaction['price_original'] * transaction['shares_original']
    return transaction
