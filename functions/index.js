const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// Final, complete, and audited version of the Cloud Function.
exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 300, memory: '1GB' }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const { userId } = context.params;
        const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
        const logs = [];

        const log = (message) => {
            const timestamp = new Date().toISOString();
            logs.push(`${timestamp}: ${message}`);
            console.log(`${timestamp}: ${message}`);
        };

        try {
            log("--- Recalculation triggered (v19 - Final Audit) ---");

            const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
            const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);

            const transactionsSnapshot = await db.collection(`users/${userId}/transactions`).get();
            const transactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

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

            log("Starting simplified FIFO calculation...");
            const result = calculatePortfolio(transactions, marketData, log);
            if (!result) throw new Error("Calculation function returned undefined.");

            const { holdings, totalRealizedPL, portfolioHistory } = result;
            log(`Calculation complete. Holdings: ${Object.keys(holdings).length}, Realized P/L: ${totalRealizedPL}`);

            log("Saving results...");
            await Promise.all([
                holdingsDocRef.set({ holdings, totalRealizedPL, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
                historyDocRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
            ]);
            log("--- Recalculation finished successfully! ---");

        } catch (error) {
            console.error("CRITICAL ERROR:", error);
            log(`CRITICAL ERROR: ${error.message}. Stack: ${error.stack}`);
        } finally {
            await logRef.set({ entries: logs });
        }
    });

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
            const fetchedData = await fetchAndReverseAdjustMarketData(symbol, log);
            if (fetchedData) {
                marketData[symbol] = fetchedData;
                await docRef.set(fetchedData);
                log(`Successfully fetched and saved data for ${symbol}.`);
            }
        }
    }
    return marketData;
}

async function fetchAndReverseAdjustMarketData(symbol, log) {
    try {
        log(`[Fetch] Fetching full history for ${symbol} from Yahoo Finance...`);
        const queryOptions = { period1: '2000-01-01' };
        const hist = await yahooFinance.historical(symbol, queryOptions);

        if (!hist || hist.length === 0) {
            log(`[Fetch] Warning: No data returned for ${symbol}.`);
            return null;
        }

        log(`[Fetch] Received ${hist.length} data points for ${symbol}. Reverse-adjusting prices...`);

        const prices = {};
        const splits = {};
        let cumulativeRatio = 1.0;

        const splitEvents = (hist.splits || []).sort((a, b) => new Date(b.date) - new Date(a.date));
        let splitIndex = 0;

        for (let i = hist.length - 1; i >= 0; i--) {
            const item = hist[i];
            const itemDate = new Date(item.date);

            while (splitIndex < splitEvents.length && itemDate < splitEvents[splitIndex].date) {
                cumulativeRatio *= splitEvents[splitIndex].numerator / splitEvents[splitIndex].denominator;
                splitIndex++;
            }
            prices[item.date.toISOString().split('T')[0]] = item.close * cumulativeRatio;
        }
        
        (hist.splits || []).forEach(s => {
            splits[s.date.toISOString().split('T')[0]] = s.numerator / s.denominator;
        });

        const payload = {
            prices: prices,
            splits: splits,
            dividends: (hist.dividends || []).reduce((acc, d) => ({ ...acc, [d.date.toISOString().split('T')[0]]: d.amount }), {}),
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            dataSource: 'emergency-fetch-original-price-v5'
        };

        if (symbol === 'TWD=X') {
            payload.rates = payload.prices;
            delete payload.prices;
        }

        return payload;
    } catch (e) {
        log(`[Fetch Error] for ${symbol}: ${e.message}`);
        return null;
    }
}

function calculatePortfolio(transactions, marketData, log) {
    const events = [];
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    for (const t of transactions) {
        events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: 'transaction' });
    }
    for (const symbol of symbols) {
        const stockData = marketData[symbol];
        if (!stockData) continue;
        Object.entries(stockData.splits || {}).forEach(([date, ratio]) => {
            events.push({ date: new Date(date), symbol, ratio, eventType: 'split' });
        });
        Object.entries(stockData.dividends || {}).forEach(([date, amount]) => {
            events.push({ date: new Date(date), symbol, amount, eventType: 'dividend' });
        });
    }
    events.sort((a, b) => new Date(a.date) - new Date(b.date));

    const portfolio = {};
    let totalRealizedPL = 0;
    const portfolioHistory = {};
    let lastProcessedDate = null;

    for (const event of events) {
        const eventDate = event.date;
        const symbol = event.symbol.toUpperCase();

        if (lastProcessedDate && lastProcessedDate < eventDate) {
            let currentDate = new Date(lastProcessedDate);
            currentDate.setDate(currentDate.getDate() + 1);
            while (currentDate < eventDate) {
                portfolioHistory[currentDate.toISOString().split('T')[0]] = calculateDailyMarketValue(portfolio, marketData, currentDate);
                currentDate.setDate(currentDate.getDate() + 1);
            }
        }

        if (!portfolio[symbol]) {
            portfolio[symbol] = { lots: [], currency: 'USD' };
        }

        const rateHistory = marketData["TWD=X"]?.rates || {};
        const rateOnDate = findNearestDataPoint(rateHistory, eventDate);

        switch (event.eventType) {
            case 'transaction':
                const t = event;
                portfolio[symbol].currency = t.currency;
                const costPerShareTWD = t.price * (t.currency === 'USD' ? rateOnDate : 1);

                if (t.type === 'buy') {
                    portfolio[symbol].lots.push({ quantity: t.quantity, pricePerShareTWD: costPerShareTWD, date: eventDate });
                } else if (t.type === 'sell') {
                    let sharesToSell = t.quantity;
                    const saleValueTWD = t.quantity * t.price * (t.currency === 'USD' ? rateOnDate : 1);
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
                portfolio[symbol].lots.forEach(lot => { lot.quantity *= event.ratio; });
                break;

            case 'dividend':
                const totalShares = portfolio[symbol].lots.reduce((sum, lot) => sum + lot.quantity, 0);
                const dividendTWD = event.amount * totalShares * (portfolio[symbol].currency === 'USD' ? rateOnDate : 1);
                totalRealizedPL += dividendTWD;
                break;
        }
        
        portfolioHistory[eventDate.toISOString().split('T')[0]] = calculateDailyMarketValue(portfolio, marketData, eventDate);
        lastProcessedDate = eventDate;
    }

    if (lastProcessedDate) {
        let currentDate = new Date(lastProcessedDate);
        currentDate.setDate(currentDate.getDate() + 1);
        const today = new Date();
        while (currentDate <= today) {
            portfolioHistory[currentDate.toISOString().split('T')[0]] = calculateDailyMarketValue(portfolio, marketData, currentDate);
            currentDate.setDate(currentDate.getDate() + 1);
        }
    }

    const finalHoldings = {};
    const today = new Date();
    const latestRate = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, today);

    for (const symbol of Object.keys(portfolio)) {
        const holding = portfolio[symbol];
        const totalOriginalQuantity = holding.lots.reduce((sum, lot) => sum + lot.quantity, 0);

        if (totalOriginalQuantity > 1e-9) {
            const totalCostTWD = holding.lots.reduce((sum, lot) => sum + (lot.quantity * lot.pricePerShareTWD), 0);
            const priceHistory = marketData[symbol]?.prices || {};
            const latestPrice = findNearestDataPoint(priceHistory, today);
            const rate = holding.currency === 'USD' ? latestRate : 1;
            
            const marketValueTWD = totalOriginalQuantity * latestPrice * rate;
            const unrealizedPLTWD = marketValueTWD - totalCostTWD;

            finalHoldings[symbol] = {
                symbol: symbol,
                quantity: totalOriginalQuantity,
                totalCostTWD: totalCostTWD,
                avgCost: totalCostTWD > 0 ? totalCostTWD / totalOriginalQuantity : 0,
                currency: holding.currency,
                currentPrice: latestPrice,
                marketValueTWD: marketValueTWD,
                unrealizedPLTWD: unrealizedPLTWD,
                returnRate: totalCostTWD > 0 ? (unrealizedPLTWD / totalCostTWD) * 100 : 0,
            };
        }
    }

    return { holdings: finalHoldings, totalRealizedPL, portfolioHistory };
}

function calculateDailyMarketValue(portfolio, marketData, date) {
    let totalValue = 0;
    const rateOnDate = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, date);

    for (const symbol in portfolio) {
        const holding = portfolio[symbol];
        const totalOriginalQuantity = holding.lots.reduce((sum, lot) => sum + lot.quantity, 0);

        if (totalOriginalQuantity > 0) {
            const priceHistory = marketData[symbol]?.prices || {};
            const priceOnDate = findNearestDataPoint(priceHistory, date);
            const rate = holding.currency === 'USD' ? rateOnDate : 1;
            totalValue += totalOriginalQuantity * priceOnDate * rate;
        }
    }
    return totalValue;
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
