const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// Main Cloud Function with the final return statement fix
exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 540, memory: '1GB' }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const { userId } = context.params;
        const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
        const logger = new Logger(logRef);

        try {
            await logger.log("--- Recalculation triggered (v5 - Return Fix) ---");

            const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
            const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);

            const transactionsSnapshot = await db.collection(`users/${userId}/transactions`).get();
            const transactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

            if (transactions.length === 0) {
                await logger.log("No transactions found. Clearing all portfolio data.");
                await Promise.all([
                    holdingsDocRef.set({ holdings: {}, totalRealizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
                    historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
                ]);
                await logger.log("--- Clearing data finished ---");
                return null;
            }

            const marketData = await getMarketDataFromDb(transactions, logger);

            await logger.log("Starting FIFO calculation with on-the-fly adjustment...");
            const { holdings, totalRealizedPL, portfolioHistory } = calculatePortfolioAdjustOnTheFly(transactions, marketData);
            await logger.log(`Calculation complete. Found ${Object.keys(holdings).length} holdings. Total Realized P/L: ${totalRealizedPL}`);

            await logger.log("Saving calculated data to Firestore...");
            await Promise.all([
                holdingsDocRef.set({
                    holdings: holdings,
                    totalRealizedPL: totalRealizedPL,
                    lastUpdated: admin.firestore.FieldValue.serverTimestamp()
                }),
                historyDocRef.set({
                    history: portfolioHistory,
                    lastUpdated: admin.firestore.FieldValue.serverTimestamp()
                })
            ]);
            await logger.log("--- Recalculation finished successfully! ---");

        } catch (error) {
            console.error("CRITICAL ERROR in recalculateHoldings:", error);
            await logger.log(`CRITICAL ERROR: ${error.message}. Stack: ${error.stack}`);
        }
        return null;
    });

// Data Fetching and Logger Class remain unchanged
async function getMarketDataFromDb(transactions, logger) { /* ... same as before ... */ }
async function fetchAndSaveMarketData(symbol, logger) { /* ... same as before ... */ }
class Logger { /* ... same as before ... */ }

// --- Calculation Engine with the crucial return statement added ---
function calculatePortfolioAdjustOnTheFly(transactions, marketData) {
    // ... all the previous correct calculation logic ...
    const events = [];
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    const cumulativeSplitRatios = {};
    for (const symbol of symbols) {
        const stockData = marketData[symbol] || {};
        const splits = Object.entries(stockData.splits || {}).sort((a, b) => new Date(a[0]) - new Date(b[0]));
        cumulativeSplitRatios[symbol] = {};
        let ratio = 1;
        if (splits.length > 0) {
            const allDates = Object.keys(stockData.prices || {}).sort();
            let splitIndex = splits.length - 1;
            for (let i = allDates.length - 1; i >= 0; i--) {
                const date = allDates[i];
                if (splitIndex >= 0 && date < splits[splitIndex][0]) {
                    ratio *= splits[splitIndex][1];
                    splitIndex--;
                }
                cumulativeSplitRatios[symbol][date] = ratio;
            }
        }
    }

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

    const portfolio = {};
    let totalRealizedPL = 0;
    const portfolioHistory = {};
    let lastProcessedDate = null;

    for (const event of events) {
        const eventDate = event.date;
        const eventDateStr = eventDate.toISOString().split('T')[0];

        if (lastProcessedDate) {
            let currentDate = new Date(lastProcessedDate);
            currentDate.setDate(currentDate.getDate() + 1);
            while (currentDate < eventDate) {
                const dateStr = currentDate.toISOString().split('T')[0];
                portfolioHistory[dateStr] = calculateDailyMarketValueAdjusted(portfolio, marketData, cumulativeSplitRatios, currentDate);
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

                const ratio = cumulativeSplitRatios[symbol]?.[eventDateStr] || 1;
                const adjustedQuantity = t.quantity * ratio;
                const adjustedPrice = t.price / ratio;
                const adjustedPriceTWD = adjustedPrice * (t.currency === 'USD' ? rateOnDate : 1);

                if (t.type === 'buy') {
                    portfolio[symbol].lots.push({
                        quantity: adjustedQuantity,
                        pricePerShareTWD: adjustedPriceTWD,
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
                const totalShares = portfolio[symbol].lots.reduce((sum, lot) => sum + lot.quantity, 0);
                const dividendTWD = event.amount * totalShares * (portfolio[symbol].currency === 'USD' ? rateOnDate : 1);
                totalRealizedPL += dividendTWD;
                break;
        }
        
        portfolioHistory[eventDateStr] = calculateDailyMarketValueAdjusted(portfolio, marketData, cumulativeSplitRatios, eventDate);
        lastProcessedDate = eventDate;
    }

    if (lastProcessedDate) {
        let currentDate = new Date(lastProcessedDate);
        currentDate.setDate(currentDate.getDate() + 1);
        const today = new Date();
        while (currentDate <= today) {
            const dateStr = currentDate.toISOString().split('T')[0];
            portfolioHistory[dateStr] = calculateDailyMarketValueAdjusted(portfolio, marketData, cumulativeSplitRatios, currentDate);
            currentDate.setDate(currentDate.getDate() + 1);
        }
    }

    const finalHoldings = {};
    const today = new Date();
    const latestRate = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, today);

    for (const symbol of Object.keys(portfolio)) {
        const holding = portfolio[symbol];
        const totalAdjustedQuantity = holding.lots.reduce((sum, lot) => sum + lot.quantity, 0);
        const todayRatio = cumulativeSplitRatios[symbol]?.[today.toISOString().split('T')[0]] || 1;
        const originalQuantity = totalAdjustedQuantity / todayRatio;

        if (originalQuantity > 1e-9) {
            const totalCostTWD = holding.lots.reduce((sum, lot) => sum + (lot.quantity * lot.pricePerShareTWD), 0);
            const priceHistory = marketData[symbol]?.prices || {};
            const latestPrice = findNearestDataPoint(priceHistory, today);
            const rate = holding.currency === 'USD' ? latestRate : 1;
            const marketValueTWD = originalQuantity * latestPrice * rate;
            const unrealizedPLTWD = marketValueTWD - totalCostTWD;

            finalHoldings[symbol] = {
                symbol: symbol,
                quantity: originalQuantity,
                totalCostTWD: totalCostTWD,
                avgCost: totalCostTWD > 0 ? totalCostTWD / originalQuantity : 0,
                currency: holding.currency,
                currentPrice: latestPrice,
                marketValueTWD: marketValueTWD,
                unrealizedPLTWD: unrealizedPLTWD,
                returnRate: totalCostTWD > 0 ? (unrealizedPLTWD / totalCostTWD) * 100 : 0,
            };
        }
    }

    // **THE FIX IS HERE**: Return the calculated results.
    return { holdings: finalHoldings, totalRealizedPL, portfolioHistory };
}

function calculateDailyMarketValueAdjusted(portfolio, marketData, cumulativeSplitRatios, date) { /* ... same as before ... */ }
function findNearestDataPoint(history, targetDate) { /* ... same as before ... */ }
