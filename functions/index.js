const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();

const db = admin.firestore();

// 當交易紀錄有任何變動時，觸發此函式
exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 540, memory: '1GB' }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const { userId } = context.params;
        console.log(`Recalculating holdings for user: ${userId}`);

        const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
        const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);
        const transactionsColRef = db.collection(`users/${userId}/transactions`);

        const snapshot = await transactionsColRef.get();
        
        if (snapshot.empty) {
            console.log(`No transactions left for user ${userId}. Clearing all data.`);
            await Promise.all([
                holdingsDocRef.delete(),
                historyDocRef.delete()
            ]);
            return null;
        }

        const transactions = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

        // 1. 獲取市場資料 (股價、匯率、分割)
        const marketData = await getMarketDataFromDb(transactions);

        // 2. 核心計算：基於前復權邏輯調整交易，並計算持股與損益
        const { holdings, realizedPL, updatedTransactions } = calculateHoldingsAndPL(transactions, marketData);
        
        // 3. 計算資產歷史
        const portfolioHistory = calculatePortfolioHistory(updatedTransactions, marketData);

        // 4. 使用批次寫入，將所有更新一次性提交
        const batch = db.batch();

        // 4.1 更新每筆交易紀錄，寫回計算的中間值
        updatedTransactions.forEach(tx => {
            const docRef = transactionsColRef.doc(tx.id);
            batch.update(docRef, {
                exchangeRate: tx.exchangeRate,
                totalValueTWD: tx.totalValueTWD,
                splitFactor: tx.splitFactor,
                adjustedQuantity: tx.adjustedQuantity
            });
        });

        // 4.2 更新計算好的持股與總已實現損益
        batch.set(holdingsDocRef, {
            holdings: holdings,
            realizedPL: realizedPL,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp()
        });

        // 4.3 更新計算好的資產歷史
        batch.set(historyDocRef, {
            history: portfolioHistory,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp()
        });

        await batch.commit();
        console.log(`Successfully recalculated and updated data for user: ${userId}`);
        return null;
    });

async function getMarketDataFromDb(transactions) {
    const marketData = {};
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const allSymbols = [...new Set([...symbols, "TWD=X"])]; // 確保匯率也被包含

    for (const symbol of allSymbols) {
        const isForex = symbol.endsWith("=X");
        const collectionName = isForex ? "exchange_rates" : "price_history";
        
        const docRef = db.collection(collectionName).doc(symbol);
        const doc = await docRef.get();

        if (doc.exists) {
            marketData[symbol] = doc.data();
        } else {
            // 如果資料庫中沒有，立即從 API 抓取
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
        const isForex = symbol.endsWith("=X");
        const collectionName = isForex ? "exchange_rates" : "price_history";

        if (isForex) {
            const queryOptions = { period1: '2000-01-01' };
            const result = await yahooFinance.historical(symbol, queryOptions);
            if (!result || result.length === 0) return null;

            const rates = {};
            for (const item of result) {
                rates[item.date.toISOString().split('T')[0]] = item.close;
            }
            const payload = { rates: rates, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
            await db.collection(collectionName).doc(symbol).set(payload);
            // 返回與股票相同的結構，以簡化後續處理
            return { prices: rates, splits: {} }; 
        } else {
            // 處理股票 (股價 + 分割)
            const [priceHistory, splitHistory] = await Promise.all([
                yahooFinance.historical(symbol, { period1: '2000-01-01' }),
                yahooFinance.historical(symbol, { period1: '2000-01-01', events: 'split' }).then(r => r.splits || [])
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
                    // 確保日期是 YYYY-MM-DD 格式
                    const dateStr = item.date.toISOString().split('T')[0];
                    splits[dateStr] = item.numerator / item.denominator;
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

function calculateHoldingsAndPL(transactions, marketData) {
    const holdings = {};
    let totalRealizedPL = 0;
    const rateHistory = marketData["TWD=X"]?.rates || {};
    
    // 1. 預處理所有交易，計算調整因子和TWD價值
    const updatedTransactions = transactions.map(t => {
        const t_date = t.date.toDate ? t.date.toDate() : new Date(t.date);
        const t_date_str = t_date.toISOString().split('T')[0];
        const currency = t.currency || 'TWD';
        const symbol = t.symbol.toUpperCase();
        const splitHistory = marketData[symbol]?.splits || {};

        // 計算拆股調整因子
        let splitFactor = 1;
        Object.keys(splitHistory).forEach(splitDateStr => {
            if (splitDateStr > t_date_str) {
                splitFactor *= splitHistory[splitDateStr];
            }
        });

        const adjustedQuantity = (t.quantity || 0) * splitFactor;

        // 計算匯率和台幣總值
        let exchangeRate = 1;
        if (currency === 'USD') {
            exchangeRate = findPriceForDate(rateHistory, t_date) || 1;
        }
        const totalValueTWD = (t.quantity || 0) * (t.price || 0) * exchangeRate;

        return {
            ...t,
            date: t_date, // 確保是 Date 物件
            splitFactor,
            adjustedQuantity,
            exchangeRate,
            totalValueTWD
        };
    });

    const allSymbols = [...new Set(updatedTransactions.map(t => t.symbol.toUpperCase()))];

    for (const symbol of allSymbols) {
        const symbolTransactions = updatedTransactions
            .filter(t => t.symbol.toUpperCase() === symbol)
            .sort((a, b) => a.date - b.date);

        if (symbolTransactions.length === 0) continue;

        const priceHistory = marketData[symbol]?.prices || {};
        const currency = symbolTransactions[0].currency;

        let currentAdjustedShares = 0;
        let totalCostTWD = 0;
        let symbolRealizedPLTWD = 0;

        for (const t of symbolTransactions) {
            if (t.type === 'buy') {
                currentAdjustedShares += t.adjustedQuantity;
                totalCostTWD += t.totalValueTWD;
            } else if (t.type === 'sell') {
                if (currentAdjustedShares > 1e-9) {
                    const proportionOfSharesSold = t.adjustedQuantity / currentAdjustedShares;
                    const costOfSoldSharesTWD = totalCostTWD * proportionOfSharesSold;
                    
                    symbolRealizedPLTWD += t.totalValueTWD - costOfSoldSharesTWD;
                    
                    totalCostTWD *= (1 - proportionOfSharesSold);
                    currentAdjustedShares -= t.adjustedQuantity;
                }
            } else if (t.type === 'dividend') {
                symbolRealizedPLTWD += t.totalValueTWD;
            }
        }

        // 計算最終持股數據
        if (currentAdjustedShares > 1e-9) {
            const latestPrice = findPriceForDate(priceHistory, new Date());
            if (latestPrice === null) {
                console.error(`FATAL: 找不到 ${symbol} 的最新價格！將跳過此股票。`);
                continue;
            }

            let latestRate = 1;
            if (currency === 'USD') {
                latestRate = findPriceForDate(rateHistory, new Date()) || 1;
            }
            
            const marketValueTWD = currentAdjustedShares * latestPrice * latestRate;
            const unrealizedPLTWD = marketValueTWD - totalCostTWD;
            const returnRate = totalCostTWD > 0 ? (unrealizedPLTWD / totalCostTWD) * 100 : 0;

            holdings[symbol] = {
                symbol: symbol,
                quantity: currentAdjustedShares, // 儲存調整後的股數
                avgCostTWD: currentAdjustedShares > 0 ? totalCostTWD / currentAdjustedShares : 0,
                totalCostTWD: totalCostTWD,
                currency: currency,
                currentPrice: latestPrice, // 這是調整後的價格
                marketValueTWD: marketValueTWD,
                unrealizedPLTWD: unrealizedPLTWD,
                realizedPLTWD: symbolRealizedPLTWD,
                returnRate: returnRate,
            };
        } else {
            // 如果股票已全部賣出，其所有損益都已實現
            totalRealizedPL += symbolRealizedPLTWD;
        }
    }

    return { holdings, realizedPL: totalRealizedPL, updatedTransactions };
}

function findPriceForDate(history, targetDate) {
    if (!history || Object.keys(history).length === 0) return null;

    const adjustedDate = new Date(targetDate.getTime());
    // 補償時區，確保日期字串正確
    adjustedDate.setUTCHours(adjustedDate.getUTCHours() + 12);
    
    // 從目標日期開始，往前回溯最多7天尋找價格
    for (let i = 0; i < 7; i++) {
        const d = new Date(adjustedDate.getTime());
        d.setUTCDate(d.getUTCDate() - i);
        const d_str = d.toISOString().split('T')[0];
        if (history[d_str] !== undefined && history[d_str] !== null) {
            return history[d_str];
        }
    }

    // 如果7天內找不到，則找小於等於目標日期的最後一個有效價格
    const sortedDates = Object.keys(history).sort((a, b) => b.localeCompare(a)); // 從最近的日期開始
    const targetDateStr = adjustedDate.toISOString().split('T')[0];
    for (const dateStr of sortedDates) {
        if (dateStr <= targetDateStr) {
            if (history[dateStr] !== undefined && history[dateStr] !== null) {
                return history[dateStr];
            }
        }
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

function calculatePortfolioHistory(updatedTransactions, marketData) {
    if (updatedTransactions.length === 0) {
        return {};
    }

    const portfolioHistory = {};
    const sortedTransactions = updatedTransactions.sort((a, b) => a.date - b.date);
    const firstDate = sortedTransactions[0].date;
    const today = new Date();

    const allDates = getDatesBetween(firstDate, today);
    const rateHistory = marketData["TWD=X"]?.rates || {};

    for (const date of allDates) {
        const dateStr = date.toISOString().split('T')[0];
        let dailyMarketValue = 0;

        const allSymbols = [...new Set(sortedTransactions.map(t => t.symbol.toUpperCase()))];

        for (const symbol of allSymbols) {
            const priceHistory = marketData[symbol]?.prices || {};
            const currency = sortedTransactions.find(t => t.symbol.toUpperCase() === symbol).currency;
            
            // 找出截至當天的所有交易
            const relevantTransactions = sortedTransactions.filter(t => 
                t.symbol.toUpperCase() === symbol && t.date <= date
            );

            let sharesOnDate = 0;
            for (const t of relevantTransactions) {
                if (t.type === 'buy') {
                    sharesOnDate += t.adjustedQuantity;
                } else if (t.type === 'sell') {
                    sharesOnDate -= t.adjustedQuantity;
                }
            }

            if (sharesOnDate > 1e-9) {
                const priceOnDate = findPriceForDate(priceHistory, date);
                if (priceOnDate !== null) {
                    const rateOnDate = currency === 'USD' ? (findPriceForDate(rateHistory, date) || 1) : 1;
                    dailyMarketValue += sharesOnDate * priceOnDate * rateOnDate;
                }
            }
        }

        if (dailyMarketValue > 0) {
            portfolioHistory[dateStr] = dailyMarketValue;
        }
    }

    return portfolioHistory;
}
