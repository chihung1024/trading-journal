const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// =================================================================================
// === Core Calculation Logic (Refactored to be reusable) ========================
// =================================================================================
async function performRecalculation(userId) {
    const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
    const logs = [];
    const log = (message) => {
        const timestamp = new Date().toISOString();
        logs.push(`${timestamp}: ${message}`);
        console.log(`[${userId}] ${timestamp}: ${message}`);
    };

    try {
        log("--- Recalculation triggered (v31 - Unified Trigger) ---");

        const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
        const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);

        const [transactionsSnapshot, userSplitsSnapshot] = await Promise.all([
            db.collection(`users/${userId}/transactions`).get(),
            db.collection(`users/${userId}/splits`).get()
        ]);

        const transactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        const userSplits = userSplitsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

        if (transactions.length === 0) {
            log("No transactions found. Clearing data.");
            await Promise.all([
                holdingsDocRef.set({ holdings: {}, totalRealizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
                historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
            ]);
            return;
        }

        const marketData = await getMarketDataFromDb(transactions, log);
        if (!marketData || Object.keys(marketData).length === 0) {
            throw new Error("Market data is empty after fetch.");
        }

        log("Starting final, corrected calculation...");
        const result = calculatePortfolio(transactions, userSplits, marketData, log);
        if (!result) throw new Error("Calculation function returned undefined.");

        const { holdings, totalRealizedPL, portfolioHistory } = result;
        log(`Calculation complete. Holdings: ${Object.keys(holdings).length}, Realized P/L: ${totalRealizedPL}, History points: ${Object.keys(portfolioHistory).length}`);

        log("Saving results...");
        await Promise.all([
            holdingsDocRef.set({ holdings, totalRealizedPL, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
            historyDocRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
        ]);
        log("--- Recalculation finished successfully! ---");

    } catch (error) {
        console.error(`[${userId}] CRITICAL ERROR:`, error);
        log(`CRITICAL ERROR: ${error.message}. Stack: ${error.stack}`);
    } finally {
        await logRef.set({ entries: logs });
    }
}

// =================================================================================
// === Firestore Triggers ========================================================
// =================================================================================

// Trigger for Transaction changes
exports.recalculateOnTransaction = functions.runWith({ timeoutSeconds: 300, memory: '1GB' }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        await performRecalculation(context.params.userId);
    });

// NEW: Trigger for Split changes
exports.recalculateOnSplit = functions.runWith({ timeoutSeconds: 300, memory: '1GB' }).firestore
    .document("users/{userId}/splits/{splitId}")
    .onWrite(async (change, context) => {
        await performRecalculation(context.params.userId);
    });


// =================================================================================
// === Data Fetching and Processing Functions (Unchanged) ========================
// =================================================================================

async function getMarketDataFromDb(transactions, log) {
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const allSymbols = [...new Set([...symbols, "TWD=X"])];
    log(`Required symbols: [${allSymbols.join(', ')}]`);
    const marketData = {};

    for (const symbol of allSymbols) {
        const docRef = db.collection(symbol === "TWD=X" ? "exchange_rates" : "price_history").doc(symbol);
        const doc = await docRef.get();

        if (doc.exists) {
            log(`Found ${symbol} in Firestore.`);
            marketData[symbol] = doc.data();
        } else {
            log(`Data for ${symbol} not found. Performing emergency fetch...`);
            const fetchedData = await fetchAndSaveMarketData(symbol, log);
            if (fetchedData) {
                marketData[symbol] = fetchedData;
                await docRef.set(fetchedData);
                log(`Successfully fetched and saved data for ${symbol}.`);
            }
        }
    }
    return marketData;
}

async function fetchAndSaveMarketData(symbol, log) {
    try {
        log(`[Fetch] Fetching full history for ${symbol} from Yahoo Finance...`);
        const queryOptions = { period1: '2000-01-01' };
        const hist = await yahooFinance.historical(symbol, queryOptions);

        if (!hist || hist.length === 0) {
            log(`[Fetch] Warning: No data returned for ${symbol}.`);
            return null;
        }

        log(`[Fetch] Received ${hist.length} data points for ${symbol}.`);

        const prices = {};
        hist.forEach(item => {
            prices[item.date.toISOString().split('T')[0]] = item.close;
        });

        const payload = {
            prices: prices,
            splits: {}, // We no longer store splits from yfinance
            dividends: (hist.dividends || []).reduce((acc, d) => ({ ...acc, [d.date.toISOString().split('T')[0]]: d.amount }), {}),
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            dataSource: 'emergency-fetch-user-split-model-v2'
        };

        if (symbol === 'TWD=X') {
            payload.rates = payload.prices;
            delete payload.dividends;
        }

        return payload;
    } catch (e) {
        log(`[Fetch Error] for ${symbol}: ${e.message}`);
        return null;
    }
}

function calculatePortfolio(transactions, userSplits, marketData, log) {
    const events = [];
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    for (const t of transactions) {
        events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: 'transaction' });
    }
    for (const split of userSplits) {
        events.push({ ...split, date: split.date.toDate ? split.date.toDate() : new Date(split.date), eventType: 'split' });
    }
    for (const symbol of symbols) {
        const stockData = marketData[symbol];
        if (!stockData) continue;
        Object.entries(stockData.dividends || {}).forEach(([date, amount]) => {
            events.push({ date: new Date(date), symbol, amount, eventType: 'dividend' });
        });
    }
    events.sort((a, b) => new Date(a.date) - new Date(b.date));

    const portfolio = {};
    let totalRealizedPL = 0;

    for (const event of events) {
        const symbol = event.symbol.toUpperCase();
        if (!portfolio[symbol]) {
            portfolio[symbol] = { lots: [], currency: 'USD' };
        }

        const rateHistory = marketData["TWD=X"]?.rates || {};
        const rateOnDate = findNearestDataPoint(rateHistory, event.date);

        switch (event.eventType) {
            case 'transaction':
                const t = event;
                portfolio[symbol].currency = t.currency;
                const costPerShareOriginal = (t.totalCost || t.price);
                const costPerShareTWD = costPerShareOriginal * (t.currency === 'USD' ? rateOnDate : 1);

                if (t.type === 'buy') {
                    portfolio[symbol].lots.push({ quantity: t.quantity, pricePerShareTWD: costPerShareTWD, pricePerShareOriginal: costPerShareOriginal, date: event.date });
                } else if (t.type === 'sell') {
                    let sharesToSell = t.quantity;
                    const saleValueTWD = (t.totalCost || t.quantity * t.price) * (t.currency === 'USD' ? rateOnDate : 1);
                    let costOfSoldSharesTWD = 0;
                    while (sharesToSell > 0 && portfolio[symbol].lots.length > 0) {
                        const firstLot = portfolio[symbol].lots[0];
                        if (firstLot.quantity <= sharesToSell) {
                            costOfSoldSharesTWD += firstLot.quantity * firstLot.pricePerShareTWD;
                            sharesToSell -= firstLot.quantity;
                            portfolio[symbol].lots.shift();
                        } else {
                            costOfSoldSharesTWD += sharesToSell * firstLot.pricePerShareTWD;
                            firstLot.quantity -= sharesToSell;
                            sharesToSell = 0;
                        }
                    }
                    totalRealizedPL += saleValueTWD - costOfSoldSharesTWD;
                }
                break;

            case 'split':
                portfolio[symbol].lots.forEach(lot => { 
                    lot.quantity *= event.ratio;
                    lot.pricePerShareOriginal /= event.ratio;
                    lot.pricePerShareTWD /= event.ratio;
                });
                break;

            case 'dividend':
                const totalShares = portfolio[symbol].lots.reduce((sum, lot) => sum + lot.quantity, 0);
                const dividendTWD = event.amount * totalShares * (portfolio[symbol].currency === 'USD' ? rateOnDate : 1);
                totalRealizedPL += dividendTWD;
                break;
        }
    }

    const finalHoldings = calculateFinalHoldings(portfolio, marketData);
    const portfolioHistory = calculatePortfolioHistory(events, marketData);

    return { holdings: finalHoldings, totalRealizedPL, portfolioHistory };
}

function calculatePortfolioHistory(events, marketData) {
    const portfolioHistory = {};
    if (events.length === 0) return {};
    
    const firstDate = new Date(events[0].date);
    const today = new Date();
    let currentDate = new Date(firstDate);
    currentDate.setUTCHours(0, 0, 0, 0);

    while (currentDate <= today) {
        const dateStr = currentDate.toISOString().split('T')[0];
        const dailyPortfolioState = getPortfolioStateOnDate(events, currentDate);
        portfolioHistory[dateStr] = calculateDailyMarketValue(dailyPortfolioState, marketData, currentDate);
        currentDate.setDate(currentDate.getDate() + 1);
    }
    return portfolioHistory;
}

function getPortfolioStateOnDate(allEvents, targetDate) {
    const portfolioState = {};
    const relevantEvents = allEvents.filter(e => new Date(e.date) <= targetDate);

    for (const event of relevantEvents) {
        const symbol = event.symbol.toUpperCase();
        if (!portfolioState[symbol]) {
            portfolioState[symbol] = { lots: [], currency: 'USD' };
        }

        switch (event.eventType) {
            case 'transaction':
                const t = event;
                portfolioState[symbol].currency = t.currency;
                if (t.type === 'buy') {
                    portfolioState[symbol].lots.push({ quantity: t.quantity, date: event.date });
                } else if (t.type === 'sell') {
                    let sharesToSell = t.quantity;
                    while (sharesToSell > 0 && portfolioState[symbol].lots.length > 0) {
                        const firstLot = portfolioState[symbol].lots[0];
                        if (firstLot.quantity <= sharesToSell) {
                            sharesToSell -= firstLot.quantity;
                            portfolioState[symbol].lots.shift();
                        } else {
                            firstLot.quantity -= sharesToSell;
                            sharesToSell = 0;
                        }
                    }
                }
                break;
            case 'split':
                portfolioState[symbol].lots.forEach(lot => { lot.quantity *= event.ratio; });
                break;
        }
    }
    return portfolioState;
}

function calculateDailyMarketValue(portfolio, marketData, date) {
    let totalValue = 0;
    const rateOnDate = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, date);

    for (const symbol in portfolio) {
        const holding = portfolio[symbol];
        const totalQuantity = holding.lots.reduce((sum, lot) => sum + lot.quantity, 0);

        if (totalQuantity > 0) {
            const priceHistory = marketData[symbol]?.prices || {};
            const priceOnDate = findNearestDataPoint(priceHistory, date);
            const rate = holding.currency === 'USD' ? rateOnDate : 1;
            totalValue += totalQuantity * priceOnDate * rate;
        }
    }
    return totalValue;
}

function calculateFinalHoldings(portfolio, marketData) {
    const finalHoldings = {};
    const today = new Date();
    const latestRate = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, today);

    for (const symbol in portfolio) {
        const holding = portfolio[symbol];
        const totalQuantity = holding.lots.reduce((sum, lot) => sum + lot.quantity, 0);

        if (totalQuantity > 1e-9) {
            const totalCostTWD = holding.lots.reduce((sum, lot) => sum + (lot.quantity * lot.pricePerShareTWD), 0);
            const totalCostOriginal = holding.lots.reduce((sum, lot) => sum + (lot.quantity * lot.pricePerShareOriginal), 0);

            const priceHistory = marketData[symbol]?.prices || {};
            const latestPriceOriginal = findNearestDataPoint(priceHistory, today);
            const rate = holding.currency === 'USD' ? latestRate : 1;
            
            const marketValueTWD = totalQuantity * latestPriceOriginal * rate;
            const unrealizedPLTWD = marketValueTWD - totalCostTWD;

            finalHoldings[symbol] = {
                symbol: symbol,
                quantity: totalQuantity,
                avgCostOriginal: totalCostOriginal > 0 ? totalCostOriginal / totalQuantity : 0,
                totalCostTWD: totalCostTWD,
                currency: holding.currency,
                currentPriceOriginal: latestPriceOriginal,
                marketValueTWD: marketValueTWD,
                unrealizedPLTWD: unrealizedPLTWD,
                returnRate: totalCostTWD > 0 ? (unrealizedPLTWD / totalCostTWD) * 100 : 0,
            };
        }
    }
    return finalHoldings;
}

function findNearestDataPoint(history, targetDate) {
    if (!history || Object.keys(history).length === 0) return 1;

    const d = new Date(targetDate);
    d.setUTCHours(12, 0, 0, 0);

    for (let i = 0; i < 7; i++) {
        const searchDate = new Date(d);
        searchDate.setDate(searchDate.getDate() - i);
        const dateStr = searchDate.toISOString().split('T')[0];
        if (history[dateStr] !== undefined) {
            return history[dateStr];
        }
    }

    const sortedDates = Object.keys(history).sort();
    const targetDateStr = d.toISOString().split('T')[0];
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

    console.warn(`Could not find any historical data point for date: ${targetDateStr}`);
    return 1;
}
