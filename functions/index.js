const functions = require("firebase-functions");
const admin = require("firebase-admin");
const { onCall } = require("firebase-functions/v2/https");

admin.initializeApp();
const db = admin.firestore();

// This is the single, robust, callable function for all recalculations.
exports.recalculatePortfolio = onCall({ timeoutSeconds: 300, memory: '1GB' }, async (request) => {
    const userId = request.auth?.uid;
    if (!userId) {
        console.error("Recalculation called without an authenticated user.");
        throw new functions.https.HttpsError('unauthenticated', 'The function must be called while authenticated.');
    }
    console.log(`Recalculation requested for user: ${userId}`);
    try {
        await performRecalculation(userId);
        return { status: 'success', message: `Recalculation completed for ${userId}` };
    } catch (error) {
        console.error(`Error during recalculation for user ${userId}:`, error);
        throw new functions.https.HttpsError('internal', 'An error occurred during recalculation.', error.message);
    }
});

// Core calculation logic - no longer a trigger, just a function to be called.
async function performRecalculation(userId) {
    const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
    const logs = [];
    const log = (message) => {
        const timestamp = new Date().toISOString();
        logs.push(`${timestamp}: ${message}`);
        console.log(`[${userId}] ${timestamp}: ${message}`);
    };

    try {
        log("--- Recalculation triggered ---");

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
                holdingsDocRef.set({ holdings: {}, totalRealizedPL: 0, xirr: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }),
                historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
            ]);
            return;
        }

        // This function now ONLY reads from the database.
        const marketData = await getMarketDataFromDb(transactions, log);
        if (!marketData || Object.keys(marketData).length === 0) {
            // This case might happen if main.py hasn't run yet for the required symbols.
            // We should not proceed with calculation if market data is missing.
            log("Market data not found in Firestore for the required symbols. Aborting calculation.");
            return;
        }

        log("Starting final, corrected calculation...");
        const result = calculatePortfolio(transactions, userSplits, marketData, log);
        if (!result) throw new Error("Calculation function returned undefined.");

        const { holdings, totalRealizedPL, portfolioHistory, xirr } = result;
        log(`Calculation complete. Holdings: ${Object.keys(holdings).length}, Realized P/L: ${totalRealizedPL}, XIRR: ${xirr}`);

        log("Saving results...");
        await Promise.all([
            holdingsDocRef.set({ holdings, totalRealizedPL, xirr, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }),
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

async function getMarketDataFromDb(transactions, log) {
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const allSymbols = [...new Set([...symbols, "TWD=X"])];
    log(`Reading market data for symbols: [${allSymbols.join(', ')}] from Firestore.`);
    const marketData = {};

    const docRefs = allSymbols.map(symbol => {
        const collectionName = symbol === "TWD=X" ? "exchange_rates" : "price_history";
        return db.collection(collectionName).doc(symbol);
    });

    const docSnapshots = await db.getAll(...docRefs);

    for (const doc of docSnapshots) {
        if (doc.exists) {
            log(`Found ${doc.id} in Firestore.`);
            marketData[doc.id] = doc.data();
        } else {
            log(`Warning: Market data for ${doc.id} not found in Firestore.`);
            // We no longer fetch from the network here. This is the single source of truth.
        }
    }
    return marketData;
}

// =================================================================================
// === Firestore Triggers ========================================================
// =================================================================================

// This is the primary trigger for all recalculations.
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds: 300, memory: '1GB' }).firestore
    .document("users/{userId}/user_data/current_holdings")
    .onUpdate(async (change, context) => {
        const before = change.before.data();
        const after = change.after.data();

        // Check if the update was triggered by our daily script
        if (after.force_recalc_timestamp && before.force_recalc_timestamp !== after.force_recalc_timestamp) {
            console.log(`Recalculation forced for user ${context.params.userId} by daily update.`);
            await performRecalculation(context.params.userId);
        }
    });

// Trigger for Transaction changes (now just a passthrough to the main trigger)
exports.recalculateOnTransaction = functions.firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const holdingsRef = db.doc(`users/${context.params.userId}/user_data/current_holdings`);
        // Use set with merge to create the doc if it doesn't exist, or update it if it does.
        await holdingsRef.set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    });

// Trigger for Split changes (now just a passthrough to the main trigger)
exports.recalculateOnSplit = functions.firestore
    .document("users/{userId}/splits/{splitId}")
    .onWrite(async (change, context) => {
        const holdingsRef = db.doc(`users/${context.params.userId}/user_data/current_holdings`);
        // Use set with merge for robustness.
        await holdingsRef.set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    });


// =================================================================================
// === Data Fetching and Processing Functions (Unchanged) ========================
// =================================================================================



function calculateXIRR(cashflows) {
    if (cashflows.length < 2) return 0;

    // Separate values and dates
    const values = cashflows.map(cf => cf.amount);
    const dates = cashflows.map(cf => cf.date);

    // Find the time difference in years from the first transaction
    const yearFractions = dates.map(date => (date.getTime() - dates[0].getTime()) / (1000 * 60 * 60 * 24 * 365));

    // NPV function
    const npv = (rate) => {
        let result = 0;
        for (let i = 0; i < values.length; i++) {
            result += values[i] / Math.pow(1 + rate, yearFractions[i]);
        }
        return result;
    };

    // Derivative of NPV function
    const derivative = (rate) => {
        let result = 0;
        for (let i = 0; i < values.length; i++) {
            if (yearFractions[i] > 0) { // Avoid division by zero for the first transaction
                result -= values[i] * yearFractions[i] / Math.pow(1 + rate, yearFractions[i] + 1);
            }
        }
        return result;
    };

    // Newton-Raphson method to find the root (XIRR)
    let guess = 0.1; // Initial guess
    const tolerance = 1e-6;
    const maxIterations = 100;

    for (let i = 0; i < maxIterations; i++) {
        const npvValue = npv(guess);
        const derivativeValue = derivative(guess);

        if (Math.abs(derivativeValue) < 1e-9) { // Avoid division by zero
            break;
        }

        const newGuess = guess - npvValue / derivativeValue;

        if (Math.abs(newGuess - guess) < tolerance) {
            return newGuess;
        }

        guess = newGuess;
    }

    return guess; // Return the best guess if it doesn't converge
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
                
                // --- THIS IS THE CRITICAL BUG FIX ---
                // If totalCost is provided, use it to calculate the price per share.
                // Otherwise, use the provided price.
                const pricePerShare = t.totalCost ? (t.totalCost / t.quantity) : t.price;
                const totalCostTWD = (t.totalCost || t.quantity * t.price) * (t.currency === 'USD' ? rateOnDate : 1);

                if (t.type === 'buy') {
                    portfolio[symbol].lots.push({ 
                        quantity: t.quantity, 
                        pricePerShareTWD: totalCostTWD / t.quantity, // Cost per share in TWD
                        pricePerShareOriginal: pricePerShare, // Cost per share in original currency
                        date: event.date 
                    });
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
    const cashflows = createCashflows(events, portfolio, finalHoldings, marketData);
    const xirr = calculateXIRR(cashflows);

    return { holdings: finalHoldings, totalRealizedPL, portfolioHistory, xirr };
}

function createCashflows(events, marketData) {
    const cashflows = [];
    const portfolioStateForDividends = {}; // Track portfolio state just for dividends

    // Chronologically process events to build up portfolio state for dividend calculation
    const sortedEvents = [...events].sort((a, b) => new Date(a.date) - new Date(b.date));

    for (const event of sortedEvents) {
        const symbol = event.symbol.toUpperCase();
        if (!portfolioStateForDividends[symbol]) {
            portfolioStateForDividends[symbol] = { lots: [], currency: 'USD' };
        }

        switch (event.eventType) {
            case 'transaction':
                const t = event;
                portfolioStateForDividends[symbol].currency = t.currency;
                const rateHistory = marketData["TWD=X"]?.rates || {};
                const rateOnDate = findNearestDataPoint(rateHistory, t.date);
                const amount = (t.totalCost || t.quantity * t.price) * (t.currency === 'USD' ? rateOnDate : 1);
                cashflows.push({ date: new Date(t.date), amount: t.type === 'buy' ? -amount : amount });

                if (t.type === 'buy') {
                    portfolioStateForDividends[symbol].lots.push({ quantity: t.quantity });
                } else if (t.type === 'sell') {
                    let sharesToSell = t.quantity;
                    while (sharesToSell > 0 && portfolioStateForDividends[symbol].lots.length > 0) {
                        const firstLot = portfolioStateForDividends[symbol].lots[0];
                        if (firstLot.quantity <= sharesToSell) {
                            sharesToSell -= firstLot.quantity;
                            portfolioStateForDividends[symbol].lots.shift();
                        } else {
                            firstLot.quantity -= sharesToSell;
                            sharesToSell = 0;
                        }
                    }
                }
                break;

            case 'split':
                portfolioStateForDividends[symbol].lots.forEach(lot => { lot.quantity *= event.ratio; });
                break;

            case 'dividend':
                const divRateHistory = marketData["TWD=X"]?.rates || {};
                const divRateOnDate = findNearestDataPoint(divRateHistory, event.date);
                const holdingCurrency = portfolioStateForDividends[symbol]?.currency || 'USD';
                const totalSharesOnDate = portfolioStateForDividends[symbol].lots.reduce((sum, lot) => sum + lot.quantity, 0);
                const dividendAmount = event.amount * totalSharesOnDate * (holdingCurrency === 'USD' ? divRateOnDate : 1);
                if (dividendAmount > 0) {
                    cashflows.push({ date: new Date(event.date), amount: dividendAmount });
                }
                break;
        }
    }

    // Add final market value as the last cashflow
    const finalMarketValue = Object.values(calculateFinalHoldings(portfolioStateForDividends, marketData)).reduce((sum, h) => sum + h.marketValueTWD, 0);
    if (finalMarketValue > 0) {
        cashflows.push({ date: new Date(), amount: finalMarketValue });
    }

    return cashflows;
}

function calculatePortfolioHistory(events, marketData) {
    const portfolioHistory = {};
    const transactionEvents = events.filter(e => e.eventType === 'transaction');
    if (transactionEvents.length === 0) return {};
    
    const firstDate = new Date(transactionEvents[0].date);
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
    
    // 1. Filter events to the target date to get the state AT that date
    const relevantEvents = allEvents.filter(e => new Date(e.date) <= targetDate);
    
    // 2. Get all split events to adjust for future splits
    const allSplitEvents = allEvents.filter(e => e.eventType === 'split');

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
                // This logic now correctly applies splits as they happen chronologically
                portfolioState[symbol].lots.forEach(lot => { 
                    lot.quantity *= event.ratio; 
                    // We also need to adjust the cost basis to maintain correct total cost
                    if (lot.pricePerShareOriginal) lot.pricePerShareOriginal /= event.ratio;
                    if (lot.pricePerShareTWD) lot.pricePerShareTWD /= event.ratio;
                });
                break;
        }
    }

    // 3. Adjust the quantity for splits that happened AFTER the targetDate
    // This is the key change to align quantities with forward-adjusted prices
    for (const symbol in portfolioState) {
        const futureSplits = allSplitEvents.filter(s => s.symbol.toUpperCase() === symbol && new Date(s.date) > targetDate);
        for (const split of futureSplits) {
            portfolioState[symbol].lots.forEach(lot => {
                lot.quantity *= split.ratio;
            });
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
