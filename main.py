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
    # 將 projectId 從服務帳號傳入，確保一致性
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
# 動態獲取 Project ID，不再依賴硬編碼的設定
try:
    PROJECT_ID = get_app().project_id
except ValueError:
    print("無法從 Firebase App 獲取 Project ID。")
    # 如果 get_app() 失敗，可以從服務帳號中再次嘗試獲取
    PROJECT_ID = service_account_info.get("project_id")

if not PROJECT_ID:
    raise ValueError("無法確定 Firebase Project ID，請檢查服務帳號憑證。")

def get_all_user_transactions():
    """使用 collection_group 查詢獲取所有使用者的交易紀錄並按使用者分組"""
    all_transactions = {}
    transactions_group = db.collection_group("transactions")
    for trans_doc in transactions_group.stream():
        path_parts = trans_doc.reference.path.split('/')
        if len(path_parts) >= 5 and path_parts[0] == 'artifacts' and path_parts[2] == 'users':
            uid = path_parts[3]
            if uid not in all_transactions:
                all_transactions[uid] = []
            
            data = trans_doc.to_dict()
            data['id'] = trans_doc.id
            # 確保日期是字串格式
            if 'date' in data and hasattr(data['date'], 'strftime'):
                 data['date'] = data['date'].strftime('%Y-%m-%d')
            elif 'date' in data and isinstance(data['date'], str):
                 try:
                    data['date'] = datetime.fromisoformat(data['date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
                 except ValueError:
                    pass # 假設格式已經是 YYYY-MM-DD
            all_transactions[uid].append(data)

    for uid in all_transactions:
        all_transactions[uid] = sorted(all_transactions[uid], key=lambda x: x['date'])
        
    return all_transactions

def fetch_and_update_market_data(symbols):
    """
    優化：增量更新市場資料。
    只抓取上次更新日期之後的新資料，並使用 update 合併，而不是 set 覆寫。
    """
    all_symbols = list(symbols) + ["TWD=X"] # 加入匯率
    
    for symbol in all_symbols:
        is_forex = symbol == "TWD=X"
        collection_name = "exchange_rates" if is_forex else "price_history"
        doc_ref = db.collection("public_data", PROJECT_ID, collection_name).document(symbol)
        
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
                update_payload = { f"prices.{k}": v for k, v in new_prices.items() }
                update_payload["lastUpdated"] = datetime.now().isoformat()
                
                doc_ref.update(update_payload)
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
        
        doc_ref = db.collection("public_data", PROJECT_ID, collection_name).document(symbol)
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

def calculate_and_save_current_holdings(uid, transactions, market_data):
    """計算指定使用者的當前持股狀態並存入 Firestore"""
    if not transactions:
        return

    print(f"正在為使用者 {uid} 計算當前持股...")
    holdings = {}
    rate_history = market_data.get("TWD=X", {})
    latest_rate = find_price_for_date(rate_history, date.today().strftime('%Y-%m-%d')) or 1

    for t in transactions:
        symbol = t['symbol'].upper()
        if symbol not in holdings:
            holdings[symbol] = {
                'symbol': symbol, 
                'quantity': 0, 
                'totalCostTWD': 0, 
                'avgCostTWD': 0, 
                'currency': t.get('currency', 'TWD'),
                'realizedPLTWD': 0
            }
        
        h = holdings[symbol]
        quantity = float(t.get('quantity', 0))
        price = float(t.get('price', 0))
        rate = find_price_for_date(rate_history, t['date']) or 1

        if t['type'] == 'buy':
            h['totalCostTWD'] += quantity * price * (rate if h['currency'] == 'USD' else 1)
            h['quantity'] += quantity
        elif t['type'] == 'sell':
            avg_cost = h['totalCostTWD'] / h['quantity'] if h['quantity'] > 0 else 0
            cost_of_sold_shares = avg_cost * quantity
            h['realizedPLTWD'] += (quantity * price * (rate if h['currency'] == 'USD' else 1)) - cost_of_sold_shares
            h['totalCostTWD'] -= cost_of_sold_shares
            h['quantity'] -= quantity
        elif t['type'] == 'dividend':
            h['realizedPLTWD'] += quantity * price * (rate if h['currency'] == 'USD' else 1)

    # 移除數量為零的持股並計算最終數據
    final_holdings = {}
    for symbol, h in holdings.items():
        if h['quantity'] > 1e-9:
            price_history = market_data.get(symbol, {})
            current_price = find_price_for_date(price_history, date.today().strftime('%Y-%m-%d'))
            h['avgCostTWD'] = h['totalCostTWD'] / h['quantity']
            h['currentPrice'] = current_price or 0
            h['marketValueTWD'] = h['quantity'] * h['currentPrice'] * (latest_rate if h['currency'] == 'USD' else 1)
            h['unrealizedPLTWD'] = h['marketValueTWD'] - h['totalCostTWD']
            h['returnRate'] = (h['unrealizedPLTWD'] / h['totalCostTWD'] * 100) if h['totalCostTWD'] > 0 else 0
            final_holdings[symbol] = h

    doc_ref = db.collection("artifacts", PROJECT_ID, "users", uid, "user_data").document("current_holdings")
    doc_ref.set({"holdings": final_holdings, "lastUpdated": datetime.now().isoformat()})
    print(f"成功儲存使用者 {uid} 的當前持股資料。")

def calculate_and_save_portfolio_history(uid, transactions, market_data):
    """
    優化：增量計算投資組合歷史。
    只計算自上次記錄以來的新日期的資產價值。
    """
    if not transactions:
        return

    print(f"正在為使用者 {uid} 計算資產歷史...")
    
    history_doc_ref = db.collection("artifacts", PROJECT_ID, "users", uid, "user_data").document("portfolio_history")
    
    try:
        history_doc = history_doc_ref.get()
        portfolio_history = history_doc.to_dict().get("history", {}) if history_doc.exists else {}
    except Exception as e:
        print(f"讀取使用者 {uid} 的現有資產歷史失敗: {e}")
        portfolio_history = {}

    # 決定計算的起始日期
    if portfolio_history:
        last_saved_date_str = max(portfolio_history.keys())
        start_date = datetime.strptime(last_saved_date_str, '%Y-%m-%d').date()
    else:
        # 如果沒有歷史，從第一筆交易開始
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
        
        # 重新計算當天的持股 (因為過去的交易可能被編輯)
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

        # 計算當日市值
        daily_market_value = 0
        rate_on_date = find_price_for_date(rate_history, date_str) or 1

        for symbol, h_data in daily_holdings.items():
            if h_data['quantity'] > 1e-9: # 避免浮點數精度問題
                price_history = market_data.get(symbol, {})
                price_on_date = find_price_for_date(price_history, date_str)

                if price_on_date is not None:
                    rate = rate_on_date if h_data['currency'] == 'USD' else 1
                    daily_market_value += h_data['quantity'] * price_on_date * rate
        
        # 只有在市值大於0時才記錄
        if daily_market_value > 0:
            new_history_entries[date_str] = daily_market_value
        
        d += timedelta(days=1)

    if new_history_entries:
        # 使用 update 進行增量更新
        history_doc_ref.set({"history": new_history_entries}, merge=True)
        print(f"成功更新使用者 {uid} 的資產歷史 ({len(new_history_entries)} 筆)。")
    else:
        print(f"使用者 {uid} 的資產歷史無需更新。")


if __name__ == "__main__":
    print("開始執行資料更新腳本...")
    all_user_transactions = get_all_user_transactions()
    
    all_symbols = set()
    for uid, transactions in all_user_transactions.items():
        for t in transactions:
            all_symbols.add(t['symbol'].upper())

    if not all_symbols:
        print("找不到任何交易紀錄，無需更新。")
    else:
        # 1. 增量更新所有需要的市場資料
        fetch_and_update_market_data(all_symbols)
        
        # 2. 從 DB 一次性讀取所有更新後的市場資料
        market_data = get_market_data_from_db(all_symbols)
        
        # 3. 為每個使用者計算
        for uid, transactions in all_user_transactions.items():
            if transactions:
                # 計算並儲存當前持股
                calculate_and_save_current_holdings(uid, transactions, market_data)
                # 增量計算並儲存資產歷史
                calculate_and_save_portfolio_history(uid, transactions, market_data)

    print("所有資料更新完成！")
