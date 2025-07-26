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
    start_date = "2000-01-01"
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
