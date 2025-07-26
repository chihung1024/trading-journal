import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore
import json
from datetime import datetime

def initialize_firebase():
    """初始化 Firebase 連線"""
    try:
        service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
        cred = credentials.Certificate(service_account_info)
        project_id = service_account_info.get("project_id")
        firebase_admin.initialize_app(cred, {'projectId': project_id})
        print("Firebase 初始化成功。")
        return firestore.client()
    except (ValueError, KeyError) as e:
        print(f"Firebase 初始化失敗: {e}。請確保 FIREBASE_SERVICE_ACCOUNT 環境變數已正確設定。")
        return None

def populate_initial_exchange_rates(db):
    """抓取從 2000 年至今的 TWD/USD 匯率並存入 Firestore"""
    symbol = "TWD=X"
    start_date = "2000-01-01"import os
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
    for col in ['stocks', 'transactions']:
        docs = db.collection(col).stream()
        for doc in docs:
            doc.reference.delete()

    # Sample transactions
    transactions = [
        {
            'stock_id': 'AAPL',
            'date': (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
            'type': 'buy',
            'shares_original': 10,
            'price_original': 150.0,
            'currency_original': 'USD'
        },
        {
            'stock_id': 'TSLA',
            'date': (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d'),
            'type': 'buy',
            'shares_original': 5,
            'price_original': 250.0,
            'currency_original': 'USD'
        },
        {
            'stock_id': 'GOOGL',
            'date': (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'),
            'type': 'buy',
            'shares_original': 20,
            'price_original': 120.0,
            'currency_original': 'USD'
        }
    ]

    for transaction in transactions:
        processed_transaction = process_transaction(transaction)
        db.collection('transactions').add(processed_transaction)

if __name__ == '__main__':
    initialize_database()
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    print(f"正在抓取 {symbol} 從 {start_date} 到 {today_str} 的歷史匯率資料...")
    
    try:
        # 抓取資料
        hist = yf.Ticker(symbol).history(start=start_date, interval="1d")
        
        if hist.empty:
            print(f"錯誤：找不到 {symbol} 的任何歷史資料。")
            return

        # 準備要寫入的資料
        rates_data = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items()}
        
        # 準備 Firestore 的文件參考
        doc_ref = db.collection("exchange_rates").document(symbol)
        
        # 準備要寫入的完整 payload
        payload = {
            "rates": rates_data,
            "lastUpdated": datetime.now().isoformat(),
            "dataSource": "yfinance",
            "description": "Historical daily exchange rates for USD to TWD."
        }
        
        print(f"正在將 {len(rates_data)} 筆匯率資料寫入 Firestore...")
        
        # 執行寫入操作
        doc_ref.set(payload)
        
        print("成功！匯率基礎資料庫已建立。")

    except Exception as e:
        print(f"在處理匯率資料時發生嚴重錯誤: {e}")

if __name__ == "__main__":
    db_client = initialize_firebase()
    if db_client:
        populate_initial_exchange_rates(db_client)
