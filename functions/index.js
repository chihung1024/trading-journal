const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// Final version with enhanced robustness for the calculation engine.
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
            log("--- Recalculation triggered (v14 - Final Robustness Fix) ---");

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
            const result = calculatePortfolioAdjustOnTheFly(transactions, marketData, log);
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

// **THE FINAL FIX IS HERE: Added robustness checks**
function calculatePortfolioAdjustOnTheFly(transactions, marketData, log) {
    const events = [];
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    // 1. Populate the event timeline
    for (const t of transactions) {
        events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: 'transaction' });
    }
    for (const symbol of symbols) {
        const stockData = marketData[symbol];
        if (!stockData) {
            log(`Warning: No market data for ${symbol} in calculation engine. Skipping its dividend events.`);
            continue;
        }
        Object.entries(stockData.dividends || {}).forEach(([date, amount]) => {
            events.push({ date: new Date(date), symbol, amount, eventType: 'dividend' });
        });
    }
    events.sort((a, b) => a.date - b.date);

    // 2. Process the timeline
    const portfolio = {};
    let totalRealizedPL = 0;
    const portfolioHistory = {};
    let lastProcessedDate = null;

    for (const event of events) {
        const eventDate = event.date;
        const symbol = event.symbol.toUpperCase();
        const stockData = marketData[symbol];

        // **ROBUSTNESS CHECK**
        if (!stockData) {
            log(`Warning: Skipping event for ${symbol} on ${eventDate.toISOString()} due to missing market data.`);
            continue;
        }

        // ... (rest of the logic is the same as the previous correct version)
        const eventDateStr = eventDate.toISOString().split('T')[0];

        if (lastProcessedDate) {
            let currentDate = new Date(lastProcessedDate);
            currentDate.setDate(currentDate.getDate() + 1);
            while (currentDate < eventDate) {
                const dateStr = currentDate.toISOString().split('T')[0];
                portfolioHistory[dateStr] = calculateDailyMarketValue(portfolio, marketData, currentDate);
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
                    portfolio[symbol].lots.push({
                        quantity: t.quantity,
                        pricePerShareTWD: costPerShareTWD,
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
                const futureSplitRatioForDividend = getFutureSplitRatio(stockData.splits, eventDate);
                totalSharesOnDividendDay *= futureSplitRatioForDividend;

                const dividendTWD = event.amount * totalSharesOnDividendDay * (portfolio[symbol].currency === 'USD' ? rateOnDate : 1);
                totalRealizedPL += dividendTWD;
                break;
        }
        
        portfolioHistory[eventDateStr] = calculateDailyMarketValue(portfolio, marketData, eventDate);
        lastProcessedDate = eventDate;
    }

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

function getFutureSplitRatio(splits, fromDate) { /* ... */ }
function calculateDailyMarketValue(portfolio, marketData, date) { /* ... */ }
function findNearestDataPoint(history, targetDate) { /* ... */ }
