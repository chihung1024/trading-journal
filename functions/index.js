const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// Final, definitive version with the "Adjust-on-the-fly" model, correctly implemented.
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
            log("--- Recalculation triggered (v25 - Single Source of Truth) ---");

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

            log("Starting final calculation with Adjust-on-the-fly model...");
            const result = calculatePortfolio(transactions, marketData, log);
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
            console.error("CRITICAL ERROR:", error);
            log(`CRITICAL ERROR: ${error.message}. Stack: ${error.stack}`);
        } finally {
            await logRef.set({ entries: logs });
        }
    });

// Simplified data fetcher. It no longer performs reverse adjustments.
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

        const splits = {};
        (hist.splits || []).forEach(s => {
            splits[s.date.toISOString().split('T')[0]] = s.numerator / s.denominator;
        });

        const payload = {
            prices: prices,
            splits: splits,
            dividends: (hist.dividends || []).reduce((acc, d) => ({ ...acc, [d.date.toISOString().split('T')[0]]: d.amount }), {}),
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            dataSource: 'emergency-fetch-split-adjusted-v2'
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

// Final, definitive calculation engine.
function calculatePortfolio(transactions, marketData, log) {
    const events = [];
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    for (const t of transactions) {
        events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: 'transaction' });
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
        const splitsHistory = marketData[symbol]?.splits || {};

        switch (event.eventType) {
            case 'transaction':
                const t = event;
                portfolio[symbol].currency = t.currency;

                const futureSplitRatio = getFutureSplitRatio(splitsHistory, eventDate);
                const adjustedQuantity = t.quantity * futureSplitRatio;
                const adjustedPrice = t.price / futureSplitRatio;
                
                const costPerAdjustedShareTWD = adjustedPrice * (t.currency === 'USD' ? rateOnDate : 1);

                if (t.type === 'buy') {
                    portfolio[symbol].lots.push({ 
                        quantity: adjustedQuantity, 
                        pricePerShareTWD: costPerAdjustedShareTWD, 
                        originalPrice: t.price, // Keep original for reference
                        date: eventDate 
                    });
                } else if (t.type === 'sell') {
                    let sharesToSell = adjustedQuantity;
                    const saleValueTWD = adjustedQuantity * adjustedPrice * (t.currency === 'USD' ? rateOnDate : 1);
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

            case 'dividend':
                const totalAdjustedShares = portfolio[symbol].lots.reduce((sum, lot) => sum + lot.quantity, 0);
                const dividendTWD = event.amount * totalAdjustedShares * (portfolio[symbol].currency === 'USD' ? rateOnDate : 1);
                totalRealizedPL += dividendTWD;
                break;
        }
        
        portfolioHistory[eventDate.toISOString().split('T')[0]] = calculateDailyMarketValue(portfolio, marketData, eventDate);
        lastProcessedDate = eventDate;
    }

    // ... (History generation from last event to today)

    const finalHoldings = {};
    const today = new Date();
    const latestRate = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, today);

    for (const symbol of Object.keys(portfolio)) {
        const holding = portfolio[symbol];
        const totalAdjustedQuantity = holding.lots.reduce((sum, lot) => sum + lot.quantity, 0);
        const splitsHistory = marketData[symbol]?.splits || {};
        const todaySplitRatio = getFutureSplitRatio(splitsHistory, today); // This will be 1
        const totalOriginalQuantity = totalAdjustedQuantity / todaySplitRatio; // Should be same as totalAdjustedQuantity

        if (totalAdjustedQuantity > 1e-9) {
            const totalCostTWD = holding.lots.reduce((sum, lot) => sum + (lot.quantity * lot.pricePerShareTWD), 0);
            const priceHistory = marketData[symbol]?.prices || {};
            const latestPrice = findNearestDataPoint(priceHistory, today);
            const rate = holding.currency === 'USD' ? latestRate : 1;
            
            const marketValueTWD = totalAdjustedQuantity * latestPrice * rate;
            const unrealizedPLTWD = marketValueTWD - totalCostTWD;

            finalHoldings[symbol] = {
                symbol: symbol,
                quantity: totalAdjustedQuantity, // Display the split-adjusted quantity
                avgCostOriginal: totalCostTWD > 0 ? totalCostTWD / totalAdjustedQuantity : 0, // This is now avg cost per *adjusted* share
                totalCostTWD: totalCostTWD,
                currency: holding.currency,
                currentPriceOriginal: latestPrice, // This is the split-adjusted price
                marketValueTWD: marketValueTWD,
                unrealizedPLTWD: unrealizedPLTWD,
                returnRate: totalCostTWD > 0 ? (unrealizedPLTWD / totalCostTWD) * 100 : 0,
            };
        }
    }

    return { holdings: finalHoldings, totalRealizedPL, portfolioHistory };
}

function getFutureSplitRatio(splits, fromDate) {
    if (!splits) return 1;
    let ratio = 1;
    Object.entries(splits).forEach(([dateStr, splitRatio]) => {
        if (new Date(dateStr) > fromDate) {
            ratio *= splitRatio;
        }
    });
    return ratio;
}

function calculateDailyMarketValue(portfolio, marketData, date) {
    let totalValue = 0;
    const rateOnDate = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, date);

    for (const symbol in portfolio) {
        const holding = portfolio[symbol];
        const totalAdjustedQuantity = holding.lots.reduce((sum, lot) => sum + lot.quantity, 0);

        if (totalAdjustedQuantity > 0) {
            const priceHistory = marketData[symbol]?.prices || {};
            const priceOnDate = findNearestDataPoint(priceHistory, date);
            const rate = holding.currency === 'USD' ? rateOnDate : 1;
            totalValue += totalAdjustedQuantity * priceOnDate * rate;
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
