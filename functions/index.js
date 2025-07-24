const functions = require("firebase-functions");
const admin = require("firebase-admin");

admin.initializeApp();

const db = admin.firestore();

// 當交易紀錄有任何變動時，觸發此函式
exports.recalculateHoldings = functions.firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const { userId } = context.params;

        console.log(`Recalculating holdings for user: ${userId}`);

        const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
        const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);

        // 1. 獲取該使用者的所有交易紀錄
        const transactionsRef = db.collection(`users/${userId}/transactions`);
        const snapshot = await transactionsRef.get();
        const transactions = snapshot.docs.map(doc => doc.data());

        // **新增：如果沒有交易紀錄，則清空所有資料**
        if (transactions.length === 0) {
            console.log(`No transactions found for user ${userId}. Clearing all data.`);
            // 使用 Promise.all 來同時執行刪除/清空操作
            await Promise.all([
                holdingsDocRef.set({ holdings: {}, realizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
                historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
            ]);
            return null; // 結束函式
        }

        // 2. 獲取市場資料 (股價和匯率)
        const marketData = await getMarketDataFromDb(transactions);

        // 3. 計算當前持股
        const { holdings, realizedPL } = calculateCurrentHoldings(transactions, marketData);

        // 4. 將計算結果存回 Firestore
        return holdingsDocRef.set({
            holdings: holdings,
            realizedPL: realizedPL,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp()
        });
    });

async function getMarketDataFromDb(transactions) {
    const marketData = {};
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const allSymbols = [...symbols, "TWD=X"];

    for (const symbol of allSymbols) {
        const isForex = symbol === "TWD=X";
        const collectionName = isForex ? "exchange_rates" : "price_history";
        const fieldName = isForex ? "rates" : "prices";
        
        const docRef = db.collection(collectionName).doc(symbol);
        const doc = await docRef.get();
        if (doc.exists) {
            marketData[symbol] = doc.data()[fieldName] || {};
        }
    }
    return marketData;
}

function calculateCurrentHoldings(transactions, marketData) {
    const holdings = {};
    let totalRealizedPL = 0;
    const rateHistory = marketData["TWD=X"] || {};
    const latestRate = findPriceForDate(rateHistory, new Date()) || 1;

    // 將交易按日期排序，確保計算順序正確
    transactions.sort((a, b) => (a.date.toDate ? a.date.toDate() : new Date(a.date)) - (b.date.toDate ? b.date.toDate() : new Date(b.date)));

    for (const t of transactions) {
        const symbol = t.symbol.toUpperCase();
        if (!holdings[symbol]) {
            holdings[symbol] = {
                symbol: symbol,
                quantity: 0,
                totalCostTWD: 0,
                avgCostTWD: 0,
                currency: t.currency || 'TWD',
                realizedPLTWD: 0
            };
        }

        const h = holdings[symbol];
        const quantity = t.quantity || 0;
        const price = t.price || 0;
        const transactionDate = t.date.toDate ? t.date.toDate() : new Date(t.date);
        const rate = findPriceForDate(rateHistory, transactionDate) || 1;
        const costRate = t.currency === 'USD' ? rate : 1;

        if (t.type === 'buy') {
            h.totalCostTWD += quantity * price * costRate;
            h.quantity += quantity;
        } else if (t.type === 'sell') {
            const avgCost = h.quantity > 0 ? h.totalCostTWD / h.quantity : 0;
            const costOfSoldShares = avgCost * quantity;
            h.realizedPLTWD += (quantity * price * costRate) - costOfSoldShares;
            h.totalCostTWD -= costOfSoldShares;
            h.quantity -= quantity;
        } else if (t.type === 'dividend') {
            h.realizedPLTWD += quantity * price * costRate;
        }
    }

    const finalHoldings = {};
    for (const symbol in holdings) {
        const h = holdings[symbol];
        if (h.quantity > 1e-9) {
            const priceHistory = marketData[symbol] || {};
            const currentPrice = findPriceForDate(priceHistory, new Date());
            h.avgCostTWD = h.quantity > 0 ? h.totalCostTWD / h.quantity : 0;
            h.currentPrice = currentPrice || 0;
            const marketRate = h.currency === 'USD' ? latestRate : 1;
            h.marketValueTWD = h.quantity * h.currentPrice * marketRate;
            h.unrealizedPLTWD = h.marketValueTWD - h.totalCostTWD;
            h.returnRate = h.totalCostTWD > 0 ? (h.unrealizedPLTWD / h.totalCostTWD) * 100 : 0;
            finalHoldings[symbol] = h;
        } else {
            totalRealizedPL += h.realizedPLTWD;
        }
    }

    return { holdings: finalHoldings, realizedPL: totalRealizedPL };
}

function findPriceForDate(history, targetDate) {
    if (!history) return null;
    for (let i = 0; i < 7; i++) {
        const d = new Date(targetDate);
        d.setDate(d.getDate() - i);
        const d_str = d.toISOString().split('T')[0];
        if (history[d_str]) {
            return history[d_str];
        }
    }
    return null;
}
