import os
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

def get_market_data_from_db(symbols):
    """從 Firestore 讀取所有需要的市場資料以供計算使用"""
    print("正在從 Firestore 讀取所有市場資料...")
    market_data = {}
    all_symbols = list(symbols) + ["TWD=X"]
    
    for symbol in all_symbols:
        is_forex = symbol == "TWD=X"
        collection_name = "exchange_rates" if is_forex else "price_history"
        field_name = "rates" if is_forex else "prices"
        
        doc_ref = db.collection(collection_name).document(symbol)
        doc = doc_ref.get()
        if doc.exists:
            market_data[symbol] = doc.to_dict().get(field_name, {})

    print("市場資料讀取完成。")
    return market_data

def find_price_for_date(history, target_date_str):
    """在歷史資料中尋找特定日期的價格（若無則往前找最多7天）"""
    if not history: return None
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
    for i in range(7):
        d_str = (target_date - timedelta(days=i)).strftime('%Y-%m-%d')
        if d_str in history:
            return history[d_str]
    return None

def calculate_and_save_portfolio_history(uid, transactions, market_data):
    """增量計算投資組合歷史"""
    history_doc_ref = db.collection("users", uid, "user_data").document("portfolio_history")

    # 如果沒有交易，清空歷史紀錄並返回
    if not transactions:
        print(f"使用者 {uid} 已無交易紀錄，正在清空其資產歷史...")
        history_doc_ref.set({"history": {}})
        return

    print(f"正在為使用者 {uid} 計算資產歷史...")
    try:
        history_doc = history_doc_ref.get()
        portfolio_history = history_doc.to_dict().get("history", {}) if history_doc.exists else {}
    except Exception as e:
        print(f"讀取使用者 {uid} 的現有資產歷史失敗: {e}")
        portfolio_history = {}

    if portfolio_history:
        last_saved_date_str = max(portfolio_history.keys())
        start_date = datetime.strptime(last_saved_date_str, '%Y-%m-%d').date()
    else:
        start_date = datetime.strptime(transactions[0]['date'], '%Y-%m-%d').date()

    today = datetime.now().date()
    if start_date > today:
        print(f"使用者 {uid} 的資產歷史已經是最新。")
        return

    rate_history = market_data.get("TWD=X", {})
    new_history_entries = {}
    
    d = start_date
    while d <= today:
        date_str = d.strftime('%Y-%m-%d')
        daily_holdings = {}
        relevant_transactions = [t for t in transactions if t['date'] <= date_str]
        
        for t in relevant_transactions:
            symbol = t['symbol'].upper()
            if symbol not in daily_holdings:
                daily_holdings[symbol] = {'quantity': 0, 'currency': t.get('currency', 'TWD')}
            
            quantity = float(t.get('quantity', 0))
            if t['type'] == 'buy':
                daily_holdings[symbol]['quantity'] += quantity
            elif t['type'] == 'sell':
                daily_holdings[symbol]['quantity'] -= quantity

        daily_market_value = 0
        rate_on_date = find_price_for_date(rate_history, date_str) or 1

        for symbol, h_data in daily_holdings.items():
            if h_data['quantity'] > 1e-9:
                price_history = market_data.get(symbol, {})
                price_on_date = find_price_for_date(price_history, date_str)

                if price_on_date is not None:
                    rate = rate_on_date if h_data['currency'] == 'USD' else 1
                    daily_market_value += h_data['quantity'] * price_on_date * rate
        
        if daily_market_value > 0:
            new_history_entries[date_str] = daily_market_value
        
        d += timedelta(days=1)

    if new_history_entries:
        history_doc_ref.set({"history": new_history_entries}, merge=True)
        print(f"成功更新使用者 {uid} 的資產歷史 ({len(new_history_entries)} 筆)。")
    else:
        print(f"使用者 {uid} 的資產歷史無需更新。")

if __name__ == "__main__":
    print("開始執行每日資料更新腳本...")
    
    # 獲取所有有交易紀錄的使用者
    transactions_by_user = get_all_user_transactions()
    all_symbols = set()
    for uid, transactions in transactions_by_user.items():
        for t in transactions:
            all_symbols.add(t['symbol'].upper())

    # 更新市場資料
    if all_symbols:
        fetch_and_update_market_data(all_symbols)
        market_data = get_market_data_from_db(all_symbols)
    else:
        market_data = {}
        print("找不到任何交易紀錄，無需更新市場資料。")

    # 獲取所有存在過的使用者，以處理已清空交易的使用者
    all_user_ids = get_all_user_ids()

    # 為所有使用者（包括已無交易者）更新資產歷史
    for uid in all_user_ids:
        user_transactions = transactions_by_user.get(uid, [])
        calculate_and_save_portfolio_history(uid, user_transactions, market_data)

    print("所有資料更新完成！")
