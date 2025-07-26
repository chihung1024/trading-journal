const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();

const db = admin.firestore();

// 當交易紀錄或手動拆股紀錄有任何變動時，觸發此函式
exports.recalculateHoldingsOnTransactionWrite = functions.runWith({ timeoutSeconds: 540, memory: '1GB' }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite((change, context) => {
        return recalculateHoldings(context.params.userId);
    });

exports.recalculateHoldingsOnSplitWrite = functions.runWith({ timeoutSeconds: 540, memory: '1GB' }).firestore
    .document("users/{userId}/manual_splits/{splitId}")
    .onWrite((change, context) => {
        return recalculateHoldings(context.params.userId);
    });

async function recalculateHoldings(userId) {
    console.log(`Recalculating holdings for user: ${userId}`);

    const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
    const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);
    const transactionsColRef = db.collection(`users/${userId}/transactions`);
    const manualSplitsColRef = db.collection(`users/${userId}/manual_splits`);

    const [txSnapshot, manualSplitsSnapshot] = await Promise.all([
        transactionsColRef.get(),
        manualSplitsColRef.get()
    ]);

    if (txSnapshot.empty) {
        console.log(`No transactions left for user ${userId}. Clearing all data.`);
        await Promise.all([holdingsDocRef.delete(), historyDocRef.delete()]);
        return null;
    }

    const transactions = txSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    const manualSplits = manualSplitsSnapshot.docs.map(doc => doc.data());

    const marketData = await getMarketDataFromDb(transactions);
    
    // 將手動拆股紀錄與市場資料合併
    const combinedMarketData = combineMarketData(marketData, manualSplits);

    const { holdings, realizedPL, updatedTransactions } = calculateHoldingsAndPL(transactions, combinedMarketData);
    const portfolioHistory = calculatePortfolioHistory(updatedTransactions, combinedMarketData);

    const batch = db.batch();

    updatedTransactions.forEach(tx => {
        const docRef = transactionsColRef.doc(tx.id);
        batch.update(docRef, {
            exchangeRate: tx.exchangeRate,
            totalValueTWD: tx.totalValueTWD,
            splitFactor: tx.splitFactor,
            adjustedQuantity: tx.adjustedQuantity
        });
    });

    batch.set(holdingsDocRef, {
        holdings: holdings,
        realizedPL: realizedPL,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
    });

    batch.set(historyDocRef, {
        history: portfolioHistory,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
    });

    await batch.commit();
    console.log(`Successfully recalculated and updated data for user: ${userId}`);
    return null;
}

function combineMarketData(marketData, manualSplits) {
    const combinedData = JSON.parse(JSON.stringify(marketData)); // Deep copy

    manualSplits.forEach(split => {
        const symbol = split.symbol.toUpperCase();
        if (!combinedData[symbol]) {
            combinedData[symbol] = { prices: {}, splits: {} };
        }
        if (!combinedData[symbol].splits) {
            combinedData[symbol].splits = {};
        }
        // 手動資料覆蓋 API 資料
        const dateStr = split.date.toDate ? split.date.toDate().toISOString().split('T')[0] : split.date;
        combinedData[symbol].splits[dateStr] = parseFloat(split.ratio);
        console.log(`Applied manual split for ${symbol} on ${dateStr} with ratio ${split.ratio}`);
    });

    return combinedData;
}

async function getMarketDataFromDb(transactions) {
    const marketData = {};
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const allSymbols = [...new Set([...symbols, "TWD=X"])];

    for (const symbol of allSymbols) {
        const isForex = symbol.endsWith("=X");
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
        const isForex = symbol.endsWith("=X");
        const collectionName = isForex ? "exchange_rates" : "price_history";

        if (isForex) {
            const result = await yahooFinance.historical(symbol, { period1: '2000-01-01' });
            const rates = {};
            if (result) {
                for (const item of result) {
                    rates[item.date.toISOString().split('T')[0]] = item.close;
                }
            }
            const payload = { rates, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
            await db.collection(collectionName).doc(symbol).set(payload);
            return { prices: rates, splits: {} };
        } else {
            const results = await yahooFinance.historical(symbol, { period1: '2000-01-01', events: 'split' });
            const prices = {};
            const splits = {};
            if (results) {
                for (const item of results) {
                    prices[item.date.toISOString().split('T')[0]] = item.close;
                }
                if (results.splits) {
                    for (const split of results.splits) {
                        splits[split.date.toISOString().split('T')[0]] = split.numerator / split.denominator;
                    }
                }
            }
            const payload = { prices, splits, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
            await db.collection(collectionName).doc(symbol).set(payload);
            return { prices, splits };
        }
    } catch (error) {
        console.error(`Error fetching market data for ${symbol}:`, error);
        return null;
    }
}

function calculateHoldingsAndPL(transactions, marketData) {
    const holdings = {};
    let totalRealizedPL = 0;
    const rateHistory = marketData["TWD=X"]?.rates || {};
    
    const updatedTransactions = transactions.map(t => {
        const t_date = t.date.toDate ? t.date.toDate() : new Date(t.date);
        const t_date_str = t_date.toISOString().split('T')[0];
        const currency = t.currency || 'TWD';
        const symbol = t.symbol.toUpperCase();
        const splitHistory = marketData[symbol]?.splits || {};

        let splitFactor = 1;
        Object.keys(splitHistory).forEach(splitDateStr => {
            if (splitDateStr > t_date_str) {
                splitFactor *= splitHistory[splitDateStr];
            }
        });

        const adjustedQuantity = (t.quantity || 0) * splitFactor;
        let exchangeRate = 1;
        if (currency === 'USD') {
            exchangeRate = findPriceForDate(rateHistory, t_date) || 1;
        }
        const totalValueTWD = (t.quantity || 0) * (t.price || 0) * exchangeRate;

        return { ...t, date: t_date, splitFactor, adjustedQuantity, exchangeRate, totalValueTWD };
    });

    const allSymbols = [...new Set(updatedTransactions.map(t => t.symbol.toUpperCase()))];

    for (const symbol of allSymbols) {
        const symbolTransactions = updatedTransactions.filter(t => t.symbol.toUpperCase() === symbol).sort((a, b) => a.date - b.date);
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
                    const proportion = t.adjustedQuantity / currentAdjustedShares;
                    const costOfSold = totalCostTWD * proportion;
                    symbolRealizedPLTWD += t.totalValueTWD - costOfSold;
                    totalCostTWD -= costOfSold;
                    currentAdjustedShares -= t.adjustedQuantity;
                }
            } else if (t.type === 'dividend') {
                symbolRealizedPLTWD += t.totalValueTWD;
            }
        }

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
                symbol, quantity: currentAdjustedShares, avgCostTWD: totalCostTWD / currentAdjustedShares,
                totalCostTWD, currency, currentPrice: latestPrice, marketValueTWD, unrealizedPLTWD,
                realizedPLTWD: symbolRealizedPLTWD, returnRate
            };
        } else {
            totalRealizedPL += symbolRealizedPLTWD;
        }
    }

    return { holdings, realizedPL, updatedTransactions };
}

function findPriceForDate(history, targetDate) {
    if (!history || Object.keys(history).length === 0) return null;

    const d = new Date(targetDate);
    // Go back up to 7 days to find a valid price
    for (let i = 0; i < 7; i++) {
        const dateStr = d.toISOString().split('T')[0];
        if (history[dateStr] !== undefined && history[dateStr] !== null) {
            return history[dateStr];
        }
        d.setDate(d.getDate() - 1);
    }

    // If still not found, find the latest available price before the target date
    const sortedDates = Object.keys(history).sort((a, b) => b.localeCompare(a)); // Sort descending
    const targetDateStr = targetDate.toISOString().split('T')[0];
    for (const dateStr of sortedDates) {
        if (dateStr <= targetDateStr) {
            return history[dateStr];
        }
    }

    return null; // Return null if no price is found
}

function getDatesBetween(startDate, endDate) {
    const dates = [];
    let currentDate = new Date(startDate.toISOString().split('T')[0]);
    while (currentDate <= endDate) {
        dates.push(new Date(currentDate));
        currentDate.setDate(currentDate.getDate() + 1);
    }
    return dates;
}

function calculatePortfolioHistory(updatedTransactions, marketData) {
    if (updatedTransactions.length === 0) return {};

    const portfolioHistory = {};
    const sortedTransactions = updatedTransactions.sort((a, b) => a.date - b.date);
    const firstDate = sortedTransactions[0].date;
    const today = new Date();
    const allDates = getDatesBetween(firstDate, today);
    const rateHistory = marketData["TWD=X"]?.rates || {};

    for (const date of allDates) {
        let dailyMarketValue = 0;
        const allSymbols = [...new Set(sortedTransactions.map(t => t.symbol.toUpperCase()))];

        for (const symbol of allSymbols) {
            const priceHistory = marketData[symbol]?.prices || {};
            const currency = sortedTransactions.find(t => t.symbol.toUpperCase() === symbol).currency;
            
            const relevantTx = sortedTransactions.filter(t => t.symbol.toUpperCase() === symbol && t.date <= date);
            let sharesOnDate = 0;
            relevantTx.forEach(t => {
                if (t.type === 'buy') sharesOnDate += t.adjustedQuantity;
                else if (t.type === 'sell') sharesOnDate -= t.adjustedQuantity;
            });

            if (sharesOnDate > 1e-9) {
                const priceOnDate = findPriceForDate(priceHistory, date);
                if (priceOnDate !== null) {
                    const rateOnDate = currency === 'USD' ? (findPriceForDate(rateHistory, date) || 1) : 1;
                    dailyMarketValue += sharesOnDate * priceOnDate * rateOnDate;
                }
            }
        }
        if (dailyMarketValue > 0) {
            portfolioHistory[date.toISOString().split('T')[0]] = dailyMarketValue;
        }
    }
    return portfolioHistory;
}
