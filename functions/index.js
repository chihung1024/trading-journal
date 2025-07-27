const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// Final version with the definitive calculation logic fix.
exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 300, memory: '1GB' }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const { userId } = context.params;
        const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
        const logs = [];

        const log = (message) => {
            const timestamp = new Date().toISOString();
            const logEntry = `${timestamp}: ${message}`;
            console.log(logEntry);
            logs.push(logEntry);
        };

        try {
            log("--- Recalculation triggered (v13 - Final Calculation Fix) ---");

            const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
            const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);

            const transactionsSnapshot = await db.collection(`users/${userId}/transactions`).get();
            const transactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

            if (transactions.length === 0) {
                log("No transactions found. Clearing all portfolio data.");
                await Promise.all([
                    holdingsDocRef.set({ holdings: {}, totalRealizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
                    historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
                ]);
                return; // Exit early
            }

            const marketData = await getMarketDataFromDb(transactions, log);

            log(`Final check before calculation. Is marketData defined? ${!!marketData}. Object keys: ${Object.keys(marketData)}`);
            if (!marketData || Object.keys(marketData).length < 2) {
                throw new Error("Market data is incomplete or undefined after fetch.");
            }

            log("Starting FIFO calculation with on-the-fly adjustment...");
            const result = calculatePortfolioAdjustOnTheFly(transactions, marketData);
            if (!result) throw new Error("Calculation function returned undefined.");

            const { holdings, totalRealizedPL, portfolioHistory } = result;
            log(`Calculation complete. Found ${Object.keys(holdings).length} holdings. Total Realized P/L: ${totalRealizedPL}`);

            log("Saving calculated data to Firestore...");
            await Promise.all([
                holdingsDocRef.set({ holdings, totalRealizedPL, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
                historyDocRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
            ]);
            log("--- Recalculation finished successfully! ---");

        } catch (error) {
            console.error("CRITICAL ERROR in recalculateHoldings:", error);
            log(`CRITICAL ERROR: ${error.message}. Stack: ${error.stack}`);
        } finally {
            await logRef.set({ entries: logs });
        }
    });

// Data fetching logic is now stable and correct.
async function getMarketDataFromDb(transactions, log) { /* ... */ }
async function fetchAndSaveMarketData(symbol, log) { /* ... */ }

// **THE FINAL FIX IS HERE: A completely rewritten calculation engine**
function calculatePortfolioAdjustOnTheFly(transactions, marketData) {
    const events = [];
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    // 1. Populate the event timeline with transactions and dividends ONLY.
    for (const t of transactions) {
        events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: 'transaction' });
    }
    for (const symbol of symbols) {
        const stockData = marketData[symbol] || {};
        Object.entries(stockData.dividends || {}).forEach(([date, amount]) => {
            events.push({ date: new Date(date), symbol, amount, eventType: 'dividend' });
        });
    }
    events.sort((a, b) => a.date - b.date);

    // 2. Process the timeline
    const portfolio = {}; // Tracks state of each holding using lots of *original* shares
    let totalRealizedPL = 0;
    const portfolioHistory = {};
    let lastProcessedDate = null;

    for (const event of events) {
        const eventDate = event.date;
        const eventDateStr = eventDate.toISOString().split('T')[0];

        // Daily snapshot generation
        if (lastProcessedDate) {
            let currentDate = new Date(lastProcessedDate);
            currentDate.setDate(currentDate.getDate() + 1);
            while (currentDate < eventDate) {
                const dateStr = currentDate.toISOString().split('T')[0];
                portfolioHistory[dateStr] = calculateDailyMarketValue(portfolio, marketData, currentDate);
                currentDate.setDate(currentDate.getDate() + 1);
            }
        }

        const symbol = event.symbol.toUpperCase();
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
                    portfolio[symbol].lots.push({
                        quantity: t.quantity, // Store original quantity
                        pricePerShareTWD: costPerShareTWD, // Store original cost in TWD
                        date: eventDate
                    });
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

            case 'dividend':
                let totalSharesOnDividendDay = portfolio[symbol].lots.reduce((sum, lot) => sum + lot.quantity, 0);
                // We must adjust the share count for future splits to match the dividend payment
                const futureSplitRatioForDividend = getFutureSplitRatio(marketData[symbol]?.splits, eventDate);
                totalSharesOnDividendDay *= futureSplitRatioForDividend;

                const dividendTWD = event.amount * totalSharesOnDividendDay * (portfolio[symbol].currency === 'USD' ? rateOnDate : 1);
                totalRealizedPL += dividendTWD;
                break;
        }
        
        portfolioHistory[eventDateStr] = calculateDailyMarketValue(portfolio, marketData, eventDate);
        lastProcessedDate = eventDate;
    }

    // Generate history from the last event to today
    if (lastProcessedDate) {
        let currentDate = new Date(lastProcessedDate);
        currentDate.setDate(currentDate.getDate() + 1);
        const today = new Date();
        while (currentDate <= today) {
            const dateStr = currentDate.toISOString().split('T')[0];
            portfolioHistory[dateStr] = calculateDailyMarketValue(portfolio, marketData, currentDate);
            currentDate.setDate(currentDate.getDate() + 1);
        }
    }

    // 4. Calculate final holdings details
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
            
            // For final market value, we use the split-adjusted price from yfinance
            const marketValueTWD = totalOriginalQuantity * latestPrice * rate;
            const unrealizedPLTWD = marketValueTWD - totalCostTWD;

            finalHoldings[symbol] = {
                symbol: symbol,
                quantity: totalOriginalQuantity,
                totalCostTWD: totalCostTWD,
                avgCost: totalCostTWD > 0 ? totalCostTWD / totalOriginalQuantity : 0,
                currency: holding.currency,
                currentPrice: latestPrice, // This is the split-adjusted price
                marketValueTWD: marketValueTWD,
                unrealizedPLTWD: unrealizedPLTWD,
                returnRate: totalCostTWD > 0 ? (unrealizedPLTWD / totalCostTWD) * 100 : 0,
            };
        }
    }

    return { holdings: finalHoldings, totalRealizedPL, portfolioHistory };
}

// New helper to get the cumulative split ratio from a certain date to the present.
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

// Rewritten helper to calculate daily market value correctly.
function calculateDailyMarketValue(portfolio, marketData, date) {
    let totalValue = 0;
    const rateOnDate = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, date);

    for (const symbol in portfolio) {
        const holding = portfolio[symbol];
        const totalOriginalQuantity = holding.lots.reduce((sum, lot) => sum + lot.quantity, 0);

        if (totalOriginalQuantity > 0) {
            const priceOnDate = findNearestDataPoint(marketData[symbol]?.prices || {}, date);
            const rate = holding.currency === 'USD' ? rateOnDate : 1;
            // The price is already split-adjusted, so we can multiply by the original quantity.
            totalValue += totalOriginalQuantity * priceOnDate * rate;
        }
    }
    return totalValue;
}

function findNearestDataPoint(history, targetDate) { /* ... same as before ... */ }
