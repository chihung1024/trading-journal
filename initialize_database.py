"""
僅負責『第一次』匯入 TWD=X 匯率（前復權對匯率無意義，程式保持不變）
"""
import os, json
from datetime import datetime
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore

def init_firebase():
    sa_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
    firebase_admin.initialize_app(
        credentials.Certificate(sa_info),
        { 'projectId': sa_info.get("project_id") }
    )
    return firestore.client()

def populate_exchange_rates(db):
    symbol = "TWD=X"
    hist = yf.Ticker(symbol).history(start="2000-01-01", interval="1d")['Close']
    rates = { d.strftime("%Y-%m-%d"): float(v) for d, v in hist.items() }
    db.collection("exchange_rates").document(symbol).set({
        "rates": rates,
        "lastUpdated": datetime.utcnow().isoformat(),
        "dataSource": "yfinance-init"
    })
    print(f"已寫入 {len(rates)} 筆匯率資料")

if __name__ == "__main__":
    db = init_firebase()
    populate_exchange_rates(db)
