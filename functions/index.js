const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

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

        // 2. 獲取市場資料 (股價和匯率) - 已升級為智慧模式
        const marketData = await getMarketDataFromDb(transactions);

        // 3. 計算當前持股 和 資產歷史
        const { holdings, realizedPL } = calculateCurrentHoldings(transactions, marketData);
        const portfolioHistory = calculatePortfolioHistory(transactions, marketData);

        // 4. 將計算結果存回 Firestore
        await Promise.all([
            holdingsDocRef.set({
                holdings: holdings,
                realizedPL: realizedPL,
                lastUpdated: admin.firestore.FieldValue.serverTimestamp()
            }),
            historyDocRef.set({
                history: portfolioHistory,
                lastUpdated: admin.firestore.FieldValue.serverTimestamp()
            })
        ]);

        return null;
    });

async function getMarketDataFromDb(transactions) {
    const marketData = {};
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const allSymbols = [...symbols, "TWD=X"];

    for (const symbol of allSymbols) {
        const isForex = symbol === "TWD=X";
        const collectionName = isForex ? "exchange_rates" : "price_history";
        
        const docRef = db.collection(collectionName).doc(symbol);
        const doc = await docRef.get();

        if (doc.exists) {
            const fieldName = isForex ? "rates" : "prices";
            marketData[symbol] = doc.data()[fieldName] || {};
        } else {
            // 【智慧升級】如果資料不存在，立即觸發即時抓取
            console.log(`Market data for ${symbol} not found in DB. Fetching from API...`);
            const newData = await fetchAndSaveMarketData(symbol);
            if (newData) {
                marketData[symbol] = newData;
            }
        }
    }
    return marketData;
}

async function fetchAndSaveMarketData(symbol) {
    try {
        const isForex = symbol === "TWD=X";
        const collectionName = isForex ? "exchange_rates" : "price_history";
        const fieldName = isForex ? "rates" : "prices";

        const queryOptions = { period1: '2000-01-01' }; // 從 2000 年開始抓
        const result = await yahooFinance.historical(symbol, queryOptions);

        if (!result || result.length === 0) {
            console.warn(`Could not fetch historical data for ${symbol} from Yahoo Finance.`);
            return null;
        }

        const newMarketData = {};
        for (const item of result) {
            const dateStr = item.date.toISOString().split('T')[0];
            newMarketData[dateStr] = item.close;
        }

        const docRef = db.collection(collectionName).doc(symbol);
        const payload = {
            [fieldName]: newMarketData,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            dataSource: 'yahoo-finance2-live',
        };

        await docRef.set(payload);
        console.log(`Successfully fetched and saved ${Object.keys(newMarketData).length} data points for ${symbol}.`);
        return newMarketData;

    } catch (error) {
        console.error(`Error fetching or saving market data for ${symbol}:`, error);
        return null;
    }
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
    if (!history || Object.keys(history).length === 0) return null;

    const sortedDates = Object.keys(history).sort();
    const targetDateStr = targetDate.toISOString().split('T')[0];

    for (let i = 0; i < 7; i++) {
        const d = new Date(targetDate);
        d.setDate(d.getDate() - i);
        const d_str = d.toISOString().split('T')[0];
        if (history[d_str]) {
            return history[d_str];
        }
    }

    let closestDate = null;
    for (const dateStr of sortedDates) {
        if (dateStr <= targetDateStr) {
            closestDate = dateStr;
        } else {
            break;
        }
    }
    if (closestDate) {
        return history[closestDate];
    }

    return null;
}

function getDatesBetween(startDate, endDate) {
    const dates = [];
    let currentDate = new Date(startDate);
    currentDate.setUTCHours(0, 0, 0, 0);
    const finalDate = new Date(endDate);
    finalDate.setUTCHours(0, 0, 0, 0);

    while (currentDate <= finalDate) {
        dates.push(new Date(currentDate));
        currentDate.setDate(currentDate.getDate() + 1);
    }
    return dates;
}

function calculatePortfolioHistory(transactions, marketData) {
    if (transactions.length === 0) {
        return {};
    }

    const portfolioHistory = {};
    const sortedTransactions = transactions.sort((a, b) => (a.date.toDate ? a.date.toDate() : new Date(a.date)) - (b.date.toDate ? b.date.toDate() : new Date(b.date)));
    const firstDate = sortedTransactions[0].date.toDate ? sortedTransactions[0].date.toDate() : new Date(sortedTransactions[0].date);
    const today = new Date();

    const allDates = getDatesBetween(firstDate, today);
    const rateHistory = marketData["TWD=X"] || {};

    for (const date of allDates) {
        const dateStr = date.toISOString().split('T')[0];
        const dailyHoldings = {};
        const relevantTransactions = sortedTransactions.filter(t => (t.date.toDate ? t.date.toDate() : new Date(t.date)) <= date);

        for (const t of relevantTransactions) {
            const symbol = t.symbol.toUpperCase();
            if (!dailyHoldings[symbol]) {
                dailyHoldings[symbol] = { quantity: 0, currency: t.currency || 'TWD' };
            }
            const quantity = t.quantity || 0;
            if (t.type === 'buy') {
                dailyHoldings[symbol].quantity += quantity;
            } else if (t.type === 'sell') {
                dailyHoldings[symbol].quantity -= quantity;
            }
        }

        let dailyMarketValue = 0;
        const rateOnDate = findPriceForDate(rateHistory, date) || 1;

        for (const symbol in dailyHoldings) {
            const h_data = dailyHoldings[symbol];
            if (h_data.quantity > 1e-9) {
                const priceHistory = marketData[symbol] || {};
                const priceOnDate = findPriceForDate(priceHistory, date);

                if (priceOnDate !== null) {
                    const rate = h_data.currency === 'USD' ? rateOnDate : 1;
                    dailyMarketValue += h_data.quantity * priceOnDate * rate;
                }
            }
        }

        if (dailyMarketValue > 0) {
            portfolioHistory[dateStr] = dailyMarketValue;
        }
    }

    return portfolioHistory;
}
