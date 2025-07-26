"""
僅首次佈署時執行：
  • 匯入 2000-01-01 ~ 今日 的 USD→TWD 匯率
"""
import os, json
from datetime import datetime
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore

#──────── Firebase
svc_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
firebase_admin.initialize_app(
    credentials.Certificate(svc_info),
    { "projectId": svc_info["project_id"] }
)
db = firestore.client()

#──────── 匯率匯入
sym = "TWD=X"
hist = yf.Ticker(sym).history(start="2000-01-01", interval="1d")["Close"]
rates = { d.strftime("%Y-%m-%d"): float(v) for d, v in hist.items() }

db.collection("exchange_rates").document(sym).set({
    "rates"      : rates,
    "lastUpdated": datetime.utcnow().isoformat(),
    "dataSource" : "yfinance-init"
})
print(f"✅ {sym} {len(rates)} records imported")
