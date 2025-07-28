import os, json
from datetime import datetime
import pandas as pd
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app


# ────────────────────────────────
# 0. Firebase 初始化
# ────────────────────────────────
def initialize_firebase():
    try:
        get_app()
    except ValueError:
        cred = credentials.Certificate(
            json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"]))
        firebase_admin.initialize_app(cred)
    return firestore.client()


# ────────────────────────────────
# 1. 輔助函式
# ────────────────────────────────
def get_all_symbols_from_transactions(db):
    symbols = { d.to_dict().get('symbol', '').upper()
                for d in db.collection_group('transactions').stream()
                if d.to_dict().get('symbol') }
    print(f'[INFO] symbols found in transactions: {symbols}')
    return list(symbols)

def get_all_user_ids(db):
    user_ids = [u.id for u in db.collection('users').stream()]
    return user_ids

def trigger_recalc(db, user_ids):
    if not user_ids: return
    batch = db.batch()
    for uid in user_ids:
        ref = (db.collection('users').document(uid)
                 .collection('user_data').document('current_holdings'))
        batch.set(ref,
                  {'force_recalc_timestamp': firestore.SERVER_TIMESTAMP},
                  merge=True)
    batch.commit()
    print(f'[INFO] recalculation triggered for {len(user_ids)} users')

def write_market_data(db, symbol, prices, dividends):
    is_fx = symbol == 'TWD=X'
    collection = 'exchange_rates' if is_fx else 'price_history'

    payload = {
        'lastUpdated': datetime.utcnow().isoformat(),
        'dataSource' : 'yfinance',
        'splits'     : {}
    }
    if is_fx:
        payload['rates'] = prices        # 匯率沒有 dividends
    else:
        payload['prices']    = prices
        payload['dividends'] = dividends

    db.collection(collection).document(symbol).set(payload, merge=True)
    print(f'[INFO] {symbol} written to {collection} (merge=True)')


# ────────────────────────────────
# 2. 主流程
# ────────────────────────────────
def fetch_and_update_market_data(db, symbols):
    for symbol in set(symbols + ['TWD=X']):
        print(f'--- Updating {symbol} ---')
        try:
            t = yf.Ticker(symbol)
            hist = t.history(start='2000-01-01', interval='1d',
                             auto_adjust=False, back_adjust=False)
            if hist.empty:
                print(f'[WARN] no data for {symbol}')
                continue

            prices = {d.strftime('%Y-%m-%d'): v
                      for d, v in hist['Close'].items()
                      if not pd.isna(v)}
            dividends = {d.strftime('%Y-%m-%d'): v
                         for d, v in t.dividends.items()}

            write_market_data(db, symbol, prices, dividends)

        except Exception as e:
            print(f'[ERROR] {symbol}: {e}')

    # 完成所有股票後，一次性觸發所有使用者重新計算
    trigger_recalc(db, get_all_user_ids(db))


# ────────────────────────────────
# 3. CLI 進入點（GitHub Action）
# ────────────────────────────────
if __name__ == '__main__':
    db = initialize_firebase()
    symbols = get_all_symbols_from_transactions(db)
    if symbols:
        fetch_and_update_market_data(db, symbols)
    else:
        print('[INFO] no symbols found, nothing to update')
