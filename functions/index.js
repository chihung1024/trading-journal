const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();

const db = admin.firestore();

// 當交易紀錄有任何變動時，觸發此函式
exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 540 }).firestore
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
            // 【修正】直接獲取整個文檔，確保 prices 和 splits 都被讀取
            marketData[symbol] = doc.data();
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

        if (isForex) {
            // 處理匯率
            const queryOptions = { period1: '2000-01-01' };
            const result = await yahooFinance.historical(symbol, queryOptions);
            if (!result || result.length === 0) return null;

            const newMarketData = {};
            for (const item of result) {
                newMarketData[item.date.toISOString().split('T')[0]] = item.close;
            }
            await db.collection(collectionName).doc(symbol).set({ rates: newMarketData, lastUpdated: admin.firestore.FieldValue.serverTimestamp() });
            console.log(`Successfully fetched and saved ${Object.keys(newMarketData).length} data points for ${symbol}.`);
            return { prices: newMarketData, splits: {} }; // 回傳統一格式
        } else {
            // 處理股票 (股價 + 分割)
            const queryOptions = { period1: '2000-01-01', events: 'split' };
            const [priceHistory, splitHistory] = await Promise.all([
                yahooFinance.historical(symbol, { period1: '2000-01-01' }),
                yahooFinance.historical(symbol, queryOptions).then(r => r.splits)
            ]);

            const prices = {};
            if (priceHistory) {
                for (const item of priceHistory) {
                    prices[item.date.toISOString().split('T')[0]] = item.close;
                }
            }

            const splits = {};
            if (splitHistory) {
                for (const item of splitHistory) {
                    splits[item.date.toISOString().split('T')[0]] = item.numerator / item.denominator;
                }
            }

            const payload = {
                prices: prices,
                splits: splits,
                lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
                dataSource: 'yahoo-finance2-live',
            };

            await db.collection(collectionName).doc(symbol).set(payload);
            console.log(`Successfully fetched and saved data for ${symbol}: ${Object.keys(prices).length} prices, ${Object.keys(splits).length} splits.`);
            return { prices, splits };
        }

    } catch (error) {
        console.error(`Error fetching or saving market data for ${symbol}:`, error);
        return null;
    }
}

function calculateCurrentHoldings(transactions, marketData) {
    const holdings = {};
    let totalRealizedPL = 0;
    const rateHistory = marketData["TWD=X"]?.prices || {};
    const latestRate = findPriceForDate(rateHistory, new Date()) || 1;

    const allSymbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    for (const symbol of allSymbols) {
        const symbolTransactions = transactions.filter(t => t.symbol.toUpperCase() === symbol);
        const priceHistory = marketData[symbol]?.prices || {};
        const splitHistory = marketData[symbol]?.splits || {};

        // 1. 創建一個包含所有事件（交易和分割）的合併列表
        const events = [];
        for (const t of symbolTransactions) {
            events.push({ ...t, type: t.type, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: 'transaction' });
        }
        for (const dateStr in splitHistory) {
            events.push({ date: new Date(dateStr), splitRatio: splitHistory[dateStr], eventType: 'split' });
        }

        // 2. 按日期對所有事件進行排序
        events.sort((a, b) => a.date - b.date);

        // 3. 迭代事件，模擬持股變化
        let quantity = 0;
        let totalCostTWD = 0;
        let totalCostOriginal = 0; // 新增：用於計算原幣總成本
        let realizedPLTWD = 0;

        for (const event of events) {
            if (event.eventType === 'transaction') {
                const t = event;
                const transactionDate = t.date.toDate ? t.date.toDate() : new Date(t.date);
                const t_quantity = t.quantity || 0;
                const t_price = t.price || 0;
                const rate = findPriceForDate(rateHistory, transactionDate) || 1;
                const costRate = t.currency === 'USD' ? rate : 1;

                if (t.type === 'buy') {
                    totalCostOriginal += t_quantity * t_price; // 計算原幣成本
                    totalCostTWD += t_quantity * t_price * costRate; // 計算台幣成本
                    quantity += t_quantity;
                } else if (t.type === 'sell') {
                    const avgCostOriginal = quantity > 0 ? totalCostOriginal / quantity : 0;
                    const avgCostTWD = quantity > 0 ? totalCostTWD / quantity : 0;
                    
                    totalCostOriginal -= avgCostOriginal * t_quantity;
                    totalCostTWD -= avgCostTWD * t_quantity;
                    
                    realizedPLTWD += (t_quantity * t_price * costRate) - (avgCostTWD * t_quantity);
                    quantity -= t_quantity;

                } else if (t.type === 'dividend') {
                    realizedPLTWD += t_quantity * t_price * costRate;
                }
            } else if (event.eventType === 'split') {
                console.log(`Applying split of ${event.splitRatio} for ${symbol} on ${event.date}`);
                quantity *= event.splitRatio;
            }
        }

        // 4. 計算最終持股數據
        if (quantity > 1e-9) {
            const currentPrice = findPriceForDate(priceHistory, new Date());
            const currency = symbolTransactions[0].currency || 'TWD';
            const marketRate = currency === 'USD' ? latestRate : 1;
            const marketValueTWD = quantity * currentPrice * marketRate;
            
            holdings[symbol] = {
                symbol: symbol,
                quantity: quantity,
                totalCostTWD: totalCostTWD, // 台幣總成本
                avgCost: quantity > 0 ? totalCostOriginal / quantity : 0, // 原幣平均成本
                currency: currency,
                realizedPLTWD: realizedPLTWD,
                currentPrice: currentPrice || 0, // 現價 (原幣)
                marketValueTWD: marketValueTWD, // 市值 (TWD)
                unrealizedPLTWD: marketValueTWD - totalCostTWD, // 未實現損益 (TWD)
                returnRate: totalCostTWD > 0 ? ((marketValueTWD - totalCostTWD) / totalCostTWD) * 100 : 0,
            };
        } else {
            totalRealizedPL += realizedPLTWD;
        }
    }

    return { holdings: holdings, realizedPL: totalRealizedPL };
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
