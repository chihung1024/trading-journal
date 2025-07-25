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
        const portfolioHistory = calculatePortfolioHistory(transactions, marketData); // 現在會正確計算

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
            marketData[symbol] = doc.data();
        } else {
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
            const queryOptions = { period1: '2000-01-01' };
            const result = await yahooFinance.historical(symbol, queryOptions);
            if (!result || result.length === 0) return null;

            const newMarketData = {};
            for (const item of result) {
                newMarketData[item.date.toISOString().split('T')[0]] = item.close;
            }
            await db.collection(collectionName).doc(symbol).set({ rates: newMarketData, lastUpdated: admin.firestore.FieldValue.serverTimestamp() });
            console.log(`Successfully fetched and saved ${Object.keys(newMarketData).length} data points for ${symbol}.`);
            return { rates: newMarketData }; // 修正：回傳的 key 應為 rates
        } else {
            const [priceHistory, eventsHistory] = await Promise.all([
                yahooFinance.historical(symbol, { period1: '2000-01-01' }),
                yahooFinance.historical(symbol, { period1: '2000-01-01', events: 'split' })
            ]);

            const prices = {};
            if (priceHistory) {
                for (const item of priceHistory) {
                    prices[item.date.toISOString().split('T')[0]] = item.close;
                }
            }

            const splits = {};
            if (eventsHistory && eventsHistory.splits) {
                for (const item of eventsHistory.splits) {
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
    const rateHistory = marketData["TWD=X"]?.rates || {};
    
    const allSymbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    for (const symbol of allSymbols) {
        const symbolTransactions = transactions.filter(t => t.symbol.toUpperCase() === symbol);
        const priceHistory = marketData[symbol]?.prices || {};
        const splitHistory = marketData[symbol]?.splits || {};
        const currency = symbolTransactions[0]?.currency || 'TWD';

        const events = [];
        symbolTransactions.forEach(t => events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: 'transaction' }));
        Object.keys(splitHistory).forEach(dateStr => events.push({ date: new Date(dateStr), splitRatio: splitHistory[dateStr], eventType: 'split' }));
        
        // 修正後的排序邏輯
        events.sort((a, b) => {
            const dateA = a.date.getTime();
            const dateB = b.date.getTime();
            if (dateA !== dateB) {
                return dateA - dateB;
            }
            // 日期相同時，'split' 事件優先
            if (a.eventType === 'split' && b.eventType !== 'split') {
                return -1;
            }
            if (a.eventType !== 'split' && b.eventType === 'split') {
                return 1;
            }
            return 0;
        });

        let currentShares = 0;
        let totalCostOriginal = 0;
        let totalCostTWD = 0;
        let symbolRealizedPLTWD = 0;

        for (const event of events) {
            if (event.eventType === 'transaction') {
                const t = event;
                const t_shares = parseFloat(t.quantity) || 0;
                const t_price = parseFloat(t.price) || 0;
                const t_date = t.date;
                
                let rateOnTransactionDate = 1;
                if (currency === 'USD') {
                    rateOnTransactionDate = findPriceForDate(rateHistory, t_date) || 1;
                }

                const valueOriginal = t_shares * t_price;
                const valueTWD = valueOriginal * rateOnTransactionDate;

                if (t.type === 'buy') {
                    currentShares += t_shares;
                    totalCostOriginal += valueOriginal;
                    totalCostTWD += valueTWD;
                } else if (t.type === 'sell') {
                    if (currentShares > 0) {
                        const proportionOfSharesSold = t_shares / currentShares;
                        const costOfSoldSharesTWD = totalCostTWD * proportionOfSharesSold;
                        symbolRealizedPLTWD += valueTWD - costOfSoldSharesTWD;
                        totalCostOriginal *= (1 - proportionOfSharesSold);
                        totalCostTWD *= (1 - proportionOfSharesSold);
                        currentShares -= t_shares;
                    }
                } else if (t.type === 'dividend') {
                    symbolRealizedPLTWD += valueTWD;
                }
            } else if (event.eventType === 'split') {
                currentShares *= event.splitRatio;
            }
        }

        if (currentShares > 1e-9) {
            const latestPrice = findPriceForDate(priceHistory, new Date());
            if (latestPrice === null) {
                console.error(`FATAL: 找不到 ${symbol} 的最新價格！將跳過此股票。`);
                continue;
            }

            let latestRate = 1;
            if (currency === 'USD') {
                latestRate = findPriceForDate(rateHistory, new Date()) || 1;
            }
            
            const marketValueTWD = currentShares * latestPrice * latestRate;
            const unrealizedPLTWD = marketValueTWD - totalCostTWD;
            const returnRate = totalCostTWD > 0 ? (unrealizedPLTWD / totalCostTWD) * 100 : 0;

            holdings[symbol] = {
                symbol: symbol,
                quantity: currentShares,
                avgCost: currentShares > 0 ? totalCostOriginal / currentShares : 0,
                totalCostTWD: totalCostTWD,
                currency: currency,
                currentPrice: latestPrice,
                marketValueTWD: marketValueTWD,
                unrealizedPLTWD: unrealizedPLTWD,
                realizedPLTWD: symbolRealizedPLTWD,
                returnRate: returnRate,
            };
        } else {
            totalRealizedPL += symbolRealizedPLTWD;
        }
    }

    return { holdings: holdings, realizedPL: totalRealizedPL };
}

function findPriceForDate(history, targetDate) {
    if (!history || Object.keys(history).length === 0) return null;
    const adjustedDate = new Date(targetDate.getTime());
    adjustedDate.setUTCHours(adjustedDate.getUTCHours() + 12);
    
    for (let i = 0; i < 7; i++) {
        const d = new Date(adjustedDate.getTime());
        d.setUTCDate(d.getUTCDate() - i);
        const d_str = d.toISOString().split('T')[0];
        if (history[d_str]) {
            return history[d_str];
        }
    }

    const targetDateStr = adjustedDate.toISOString().split('T')[0];
    const sortedDates = Object.keys(history).sort();
    let closestDate = null;
    for (const dateStr of sortedDates) {
        if (dateStr <= targetDateStr) {
            closestDate = dateStr;
        } else {
            break;
        }
    }
    return closestDate ? history[closestDate] : null;
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

// ==================================================================
// ===                修正後的 calculatePortfolioHistory             ===
// ==================================================================
function calculatePortfolioHistory(transactions, marketData) {
    if (transactions.length === 0) {
        return {};
    }

    const portfolioHistory = {};
    const sortedTransactions = transactions.sort((a, b) => (a.date.toDate ? a.date.toDate() : new Date(a.date)) - (b.date.toDate ? b.date.toDate() : new Date(b.date)));
    const firstDate = sortedTransactions[0].date.toDate ? sortedTransactions[0].date.toDate() : new Date(sortedTransactions[0].date);
    const today = new Date();

    const allDates = getDatesBetween(firstDate, today);
    const rateHistory = marketData["TWD=X"]?.rates || {};
    const allSymbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    // 為每個股票建立事件列表
    const eventsBySymbol = {};
    for (const symbol of allSymbols) {
        const symbolTransactions = transactions.filter(t => t.symbol.toUpperCase() === symbol);
        const splitHistory = marketData[symbol]?.splits || {};
        
        const events = [];
        symbolTransactions.forEach(t => events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: 'transaction' }));
        Object.keys(splitHistory).forEach(dateStr => events.push({ date: new Date(dateStr), splitRatio: splitHistory[dateStr], eventType: 'split' }));
        
        // 修正後的排序邏輯
        events.sort((a, b) => {
            const dateA = a.date.getTime();
            const dateB = b.date.getTime();
            if (dateA !== dateB) {
                return dateA - dateB;
            }
            if (a.eventType === 'split' && b.eventType !== 'split') {
                return -1;
            }
            if (a.eventType !== 'split' && b.eventType === 'split') {
                return 1;
            }
            return 0;
        });
        eventsBySymbol[symbol] = events;
    }

    for (const date of allDates) {
        let dailyMarketValue = 0;
        const rateOnDate = findPriceForDate(rateHistory, date) || 1;

        for (const symbol of allSymbols) {
            const priceHistory = marketData[symbol]?.prices || {};
            const priceOnDate = findPriceForDate(priceHistory, date);

            if (priceOnDate === null) continue; // 如果當天沒價格，無法計算市值

            const events = eventsBySymbol[symbol];
            const relevantEvents = events.filter(e => e.date <= date);
            
            let sharesOnDate = 0;
            for (const event of relevantEvents) {
                if (event.eventType === 'transaction') {
                    const quantity = event.quantity || 0;
                    if (event.type === 'buy') {
                        sharesOnDate += quantity;
                    } else if (event.type === 'sell') {
                        sharesOnDate -= quantity;
                    }
                } else if (event.eventType === 'split') {
                    sharesOnDate *= event.splitRatio;
                }
            }

            if (sharesOnDate > 1e-9) {
                const currency = transactions.find(t => t.symbol.toUpperCase() === symbol)?.currency || 'TWD';
                const rate = currency === 'USD' ? rateOnDate : 1;
                dailyMarketValue += sharesOnDate * priceOnDate * rate;
            }
        }

        if (dailyMarketValue > 0) {
            const dateStr = date.toISOString().split('T')[0];
            portfolioHistory[dateStr] = dailyMarketValue;
        }
    }

    return portfolioHistory;
}
