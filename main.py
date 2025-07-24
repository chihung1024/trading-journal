import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app
import json
from datetime import datetime, timedelta, date

# 初始化 Firebase
try:
    service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])import os
import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore, get_app
import json
from datetime import datetime, timedelta, date

# 初始化 Firebase
try:
    service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
    cred = credentials.Certificate(service_account_info)
    project_id = service_account_info.get("project_id")
    firebase_admin.initialize_app(cred, {
        'projectId': project_id
    })
except (ValueError, KeyError) as e:
    print(f"Firebase 初始化失敗: {e}。請確保 FIREBASE_SERVICE_ACCOUNT 環境變數已正確設定。")
    # 本地測試備用方案
    # cred = credentials.Certificate("path/to/serviceAccountKey.json")
    # firebase_admin.initialize_app(cred)

db = firestore.client()
try:
    PROJECT_ID = get_app().project_id
except ValueError:
    PROJECT_ID = service_account_info.get("project_id")

if not PROJECT_ID:
    raise ValueError("無法確定 Firebase Project ID，請檢查服務帳號憑證。")

def get_all_user_ids():
    """獲取所有存在過資料的使用者ID"""
    user_ids = set()
    users_ref = db.collection("users")
    for user_doc in users_ref.stream():
        user_ids.add(user_doc.id)
    return list(user_ids)

def get_all_user_transactions():
    """使用 collection_group 查詢獲取所有使用者的交易紀錄並按使用者分組"""
    all_transactions = {}
    transactions_group = db.collection_group("transactions")
    for trans_doc in transactions_group.stream():
        path_parts = trans_doc.reference.path.split('/')
        if len(path_parts) >= 3 and path_parts[0] == 'users':
            uid = path_parts[1]
            if uid not in all_transactions:
                all_transactions[uid] = []
            
            data = trans_doc.to_dict()
            data['id'] = trans_doc.id
            if 'date' in data and hasattr(data['date'], 'strftime'):
                 data['date'] = data['date'].strftime('%Y-%m-%d')
            elif 'date' in data and isinstance(data['date'], str):
                 try:
                    data['date'] = datetime.fromisoformat(data['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
                 except ValueError:
                    pass
            all_transactions[uid].append(data)

    for uid in all_transactions:
        all_transactions[uid] = sorted(all_transactions[uid], key=lambda x: x['date'])
        
    return all_transactions

def fetch_and_update_market_data(symbols):
    """增量更新市場資料"""
    all_symbols = list(symbols) + ["TWD=X"]
    
    for symbol in all_symbols:
        is_forex = symbol == "TWD=X"
        collection_name = "exchange_rates" if is_forex else "price_history"
        doc_ref = db.collection(collection_name).document(symbol)
        
        start_date = None
        try:
            doc = doc_ref.get()
            if doc.exists:
                last_updated_str = doc.to_dict().get("lastUpdated")
                start_date = datetime.fromisoformat(last_updated_str).date() + timedelta(days=1)
        except Exception as e:
            print(f"讀取 {symbol} 的最後更新日期失敗: {e}")

        if start_date is None:
            start_date = date(2000, 1, 1)

        if start_date > date.today():
            print(f"{symbol} 的資料已經是最新，無需更新。")
            continue

        try:
            print(f"正在抓取 {symbol} 從 {start_date.strftime('%Y-%m-%d')} 開始的資料...")
            stock = yf.Ticker(symbol)
            
            # 抓取股價和分割歷史
            hist = stock.history(start=start_date, interval="1d", auto_adjust=False, back_adjust=False)
            splits = stock.splits
            
            update_payload = {}

            if not hist.empty:
                new_prices = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items()}
                update_payload["prices"] = new_prices
                print(f"成功抓取 {symbol} 的 {len(new_prices)} 筆新股價。")

            if not splits.empty:
                new_splits = {idx.strftime('%Y-%m-%d'): val for idx, val in splits.items()}
                update_payload["splits"] = new_splits
                print(f"成功抓取 {symbol} 的 {len(new_splits)} 筆分割歷史。")

            if update_payload:
                update_payload["lastUpdated"] = datetime.now().isoformat()
                doc_ref.set(update_payload, merge=True)
                print(f"成功更新 {symbol} 的資料。")
            else:
                print(f"找不到 {symbol} 在 {start_date.strftime('%Y-%m-%d')}之後的新資料。")

        except Exception as e:
            print(f"抓取或儲存 {symbol} 資料時發生錯誤: {e}")



if __name__ == "__main__":
    print("開始執行每日市場資料更新腳本...")
    
    # 獲取所有有交易紀錄的使用者，以確定需要哪些市場資料
    transactions_by_user = get_all_user_transactions()
    all_symbols = set()
    for uid, transactions in transactions_by_user.items():
        for t in transactions:
            all_symbols.add(t['symbol'].upper())

    # 根據找到的股票代碼，更新市場資料
    if all_symbols:
        fetch_and_update_market_data(all_symbols)
    else:
        print("找不到任何交易紀錄，無需更新市場資料。")

    print("市場資料更新完成！")
    cred = credentials.Certificate(service_account_info)
    project_id = service_account_info.get("project_id")
    firebase_admin.initialize_app(cred, {
        'projectId': project_id
    })
except (ValueError, KeyError) as e:
    print(f"Firebase 初始化失敗: {e}。請確保 FIREBASE_SERVICE_ACCOUNT 環境變數已正確設定。")
    # 本地測試備用方案
    # cred = credentials.Certificate("path/to/serviceAccountKey.json")
    # firebase_admin.initialize_app(cred)

db = firestore.client()
try:
    PROJECT_ID = get_app().project_id
except ValueError:
    PROJECT_ID = service_account_info.get("project_id")

if not PROJECT_ID:
    raise ValueError("無法確定 Firebase Project ID，請檢查服務帳號憑證。")

def get_all_user_ids():
    """獲取所有存在過資料的使用者ID"""
    user_ids = set()
    users_ref = db.collection("users")
    for user_doc in users_ref.stream():
        user_ids.add(user_doc.id)
    return list(user_ids)

def get_all_user_transactions():
    """使用 collection_group 查詢獲取所有使用者的交易紀錄並按使用者分組"""
    all_transactions = {}
    transactions_group = db.collection_group("transactions")
    for trans_doc in transactions_group.stream():
        path_parts = trans_doc.reference.path.split('/')
        if len(path_parts) >= 3 and path_parts[0] == 'users':
            uid = path_parts[1]
            if uid not in all_transactions:
                all_transactions[uid] = []
            
            data = trans_doc.to_dict()
            data['id'] = trans_doc.id
            if 'date' in data and hasattr(data['date'], 'strftime'):
                 data['date'] = data['date'].strftime('%Y-%m-%d')
            elif 'date' in data and isinstance(data['date'], str):
                 try:
                    data['date'] = datetime.fromisoformat(data['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
                 except ValueError:
                    pass
            all_transactions[uid].append(data)

    for uid in all_transactions:
        all_transactions[uid] = sorted(all_transactions[uid], key=lambda x: x['date'])
        
    return all_transactions

def fetch_and_update_market_data(symbols):
    """增量更新市場資料"""
    all_symbols = list(symbols) + ["TWD=X"]
    
    for symbol in all_symbols:
        is_forex = symbol == "TWD=X"
        collection_name = "exchange_rates" if is_forex else "price_history"
        doc_ref = db.collection(collection_name).document(symbol)
        
        start_date = None
        try:
            doc = doc_ref.get()
            if doc.exists:
                last_updated_str = doc.to_dict().get("lastUpdated")
                start_date = datetime.fromisoformat(last_updated_str).date() + timedelta(days=1)
        except Exception as e:
            print(f"讀取 {symbol} 的最後更新日期失敗: {e}")

        if start_date is None:
            start_date = date(2000, 1, 1)

        if start_date > date.today():
            print(f"{symbol} 的資料已經是最新，無需更新。")
            continue

        try:
            print(f"正在抓取 {symbol} 從 {start_date.strftime('%Y-%m-%d')} 開始的資料...")
            stock = yf.Ticker(symbol)
            hist = stock.history(start=start_date, interval="1d")
            
            if not hist.empty:
                new_prices = {idx.strftime('%Y-%m-%d'): val for idx, val in hist['Close'].items()}
                field_name = "rates" if is_forex else "prices"
                update_payload = { f"{field_name}.{k}": v for k, v in new_prices.items() }
                update_payload["lastUpdated"] = datetime.now().isoformat()
                doc_ref.set(update_payload, merge=True)
                print(f"成功更新 {symbol} 的資料 ({len(new_prices)} 筆)。")
            else:
                print(f"找不到 {symbol} 在 {start_date.strftime('%Y-%m-%d')}之後的新資料。")

        except Exception as e:
            print(f"抓取或儲存 {symbol} 資料時發生錯誤: {e}")



if __name__ == "__main__":
    print("開始執行每日市場資料更新腳本...")
    
    # 獲取所有有交易紀錄的使用者，以確定需要哪些市場資料
    transactions_by_user = get_all_user_transactions()
    all_symbols = set()
    for uid, transactions in transactions_by_user.items():
        for t in transactions:
            all_symbols.add(t['symbol'].upper())

    # 根據找到的股票代碼，更新市場資料
    if all_symbols:
        fetch_and_update_market_data(all_symbols)
    else:
        print("找不到任何交易紀錄，無需更新市場資料。")

    print("市場資料更新完成！")
