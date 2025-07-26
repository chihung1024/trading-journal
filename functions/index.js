const functions = require("firebase-functions");
const admin = require("firebase-admin");

admin.initializeApp();
const db = admin.firestore();

// Main Cloud Function triggered by any change in a user's transactions.
exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 540, memory: '1GB' }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const { userId } = context.params;
        console.log(`--- Recalculation triggered for user: ${userId} ---`);

        const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
        const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);

        // 1. Get all transactions for the user.
        const transactionsSnapshot = await db.collection(`users/${userId}/transactions`).get();
        const transactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

        // If no transactions exist, clear all data and exit.
        if (transactions.length === 0) {
            console.log(`No transactions found for user ${userId}. Clearing all portfolio data.`);
            await Promise.all([
                holdingsDocRef.set({ holdings: {}, totalRealizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
                historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
            ]);
            return null;
        }

        // 2. Get all necessary market data from our Firestore database.
        const marketData = await getMarketDataFromDb(transactions);

        // 3. Perform the core calculation using the event timeline model.
        const { holdings, totalRealizedPL, portfolioHistory } = calculatePortfolio(transactions, marketData);

        // 4. Save the newly calculated results back to Firestore.
        console.log(`Saving calculated data for user ${userId}. Holdings: ${Object.keys(holdings).length}, Realized P/L: ${totalRealizedPL}`);
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

        console.log(`--- Recalculation finished for user: ${userId} ---`);
        return null;
    });

/**
 * Fetches all required market data (prices, splits, dividends, rates) from Firestore.
 * This function no longer calls external APIs.
 */
async function getMarketDataFromDb(transactions) {
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const marketData = {};
    const promises = [];

    // Fetch stock data (prices, splits, dividends)
    for (const symbol of symbols) {
        const docRef = db.collection("price_history").doc(symbol);
        promises.push(docRef.get().then(doc => {
            if (doc.exists) {
                marketData[symbol] = doc.data();
            } else {
                console.error(`FATAL: Market data for symbol '${symbol}' not found in Firestore. Calculations may be incorrect.`);
                marketData[symbol] = { prices: {}, splits: {}, dividends: {} }; // Provide empty data to prevent crashes
            }
        }));
    }

    // Fetch exchange rate data
    const ratesDocRef = db.collection("exchange_rates").doc("TWD=X");
    promises.push(ratesDocRef.get().then(doc => {
        if (doc.exists) {
            marketData["TWD=X"] = doc.data();
        } else {
            console.error("FATAL: Exchange rate data 'TWD=X' not found in Firestore. Calculations will be incorrect.");
            marketData["TWD=X"] = { rates: {} };
        }
    }));

    await Promise.all(promises);
    console.log(`Successfully fetched market data for ${Object.keys(marketData).length} symbols from Firestore.`);
    return marketData;
}

/**
 * The core calculation engine based on an event timeline.
 */
function calculatePortfolio(transactions, marketData) {
    const events = [];
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    // 1. Populate the event timeline from transactions, splits, and dividends
    for (const t of transactions) {
        events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: 'transaction' });
    }
    for (const symbol of symbols) {
        const stockData = marketData[symbol] || {};
        Object.entries(stockData.splits || {}).forEach(([date, ratio]) => {
            events.push({ date: new Date(date), symbol, ratio, eventType: 'split' });
        });
        Object.entries(stockData.dividends || {}).forEach(([date, amount]) => {
            events.push({ date: new Date(date), symbol, amount, eventType: 'dividend' });
        });
    }
    events.sort((a, b) => a.date - b.date);

    // 2. Process the timeline
    const portfolio = {}; // Tracks state of each holding
    let totalRealizedPL = 0;
    const portfolioHistory = {};
    let lastProcessedDate = null;

    for (const event of events) {
        const eventDate = event.date;
        const eventDateStr = eventDate.toISOString().split('T')[0];

        // Generate daily portfolio snapshots for dates between events
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
            portfolio[symbol] = { quantity: 0, totalCostTWD: 0, currency: 'USD' };
        }

        const rateHistory = marketData["TWD=X"]?.rates || {};
        const rateOnDate = findNearestRate(rateHistory, eventDate);

        switch (event.eventType) {
            case 'transaction':
                const t = event;
                portfolio[symbol].currency = t.currency; // Update currency from latest transaction
                const valueOriginal = t.quantity * t.price;
                const valueTWD = valueOriginal * (t.currency === 'USD' ? rateOnDate : 1);

                if (t.type === 'buy') {
                    portfolio[symbol].quantity += t.quantity;
                    portfolio[symbol].totalCostTWD += valueTWD;
                } else if (t.type === 'sell') {
                    if (portfolio[symbol].quantity > 0) {
                        const costOfSoldShares = (portfolio[symbol].totalCostTWD / portfolio[symbol].quantity) * t.quantity;
                        const realizedPL = valueTWD - costOfSoldShares;
                        totalRealizedPL += realizedPL;
                        portfolio[symbol].totalCostTWD -= costOfSoldShares;
                        portfolio[symbol].quantity -= t.quantity;
                    }
                }
                break;

            case 'split':
                portfolio[symbol].quantity *= event.ratio;
                break;

            case 'dividend':
                const dividendTWD = event.amount * portfolio[symbol].quantity * (portfolio[symbol].currency === 'USD' ? rateOnDate : 1);
                totalRealizedPL += dividendTWD;
                break;
        }
        
        // Record portfolio value on the day of the event
        portfolioHistory[eventDateStr] = calculateDailyMarketValue(portfolio, marketData, eventDate);
        lastProcessedDate = eventDate;
    }

    // 3. Generate history from the last event to today
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
    const latestRate = findNearestRate(marketData["TWD=X"]?.rates || {}, today);

    for (const symbol of Object.keys(portfolio)) {
        const holding = portfolio[symbol];
        if (holding.quantity > 1e-9) { // Use a small epsilon for float comparison
            const priceHistory = marketData[symbol]?.prices || {};
            const latestPrice = findNearestPrice(priceHistory, today);
            const rate = holding.currency === 'USD' ? latestRate : 1;
            const marketValueTWD = holding.quantity * latestPrice * rate;
            const unrealizedPLTWD = marketValueTWD - holding.totalCostTWD;

            finalHoldings[symbol] = {
                symbol: symbol,
                quantity: holding.quantity,
                totalCostTWD: holding.totalCostTWD,
                avgCostTWD: holding.quantity > 0 ? holding.totalCostTWD / holding.quantity : 0,
                currency: holding.currency,
                currentPrice: latestPrice,
                marketValueTWD: marketValueTWD,
                unrealizedPLTWD: unrealizedPLTWD,
                returnRate: holding.totalCostTWD > 0 ? (unrealizedPLTWD / holding.totalCostTWD) * 100 : 0,
            };
        }
    }

    return { holdings: finalHoldings, totalRealizedPL, portfolioHistory };
}

function calculateDailyMarketValue(portfolio, marketData, date) {
    let totalValue = 0;
    const rateOnDate = findNearestRate(marketData["TWD=X"]?.rates || {}, date);

    for (const symbol in portfolio) {
        const holding = portfolio[symbol];
        if (holding.quantity > 0) {
            const priceOnDate = findNearestPrice(marketData[symbol]?.prices || {}, date);
            const rate = holding.currency === 'USD' ? rateOnDate : 1;
            totalValue += holding.quantity * priceOnDate * rate;
        }
    }
    return totalValue;
}

function findNearestPrice(priceHistory, targetDate) {
    return findNearestDataPoint(priceHistory, targetDate);
}

function findNearestRate(rateHistory, targetDate) {
    return findNearestDataPoint(rateHistory, targetDate);
}

/**
 * A robust function to find the closest available data point for a given date.
 * It searches backwards up to 7 days, then falls back to the closest earlier date.
 */
function findNearestDataPoint(history, targetDate) {
    if (!history || Object.keys(history).length === 0) return 1; // Return a neutral value

    const d = new Date(targetDate);
    d.setUTCHours(12, 0, 0, 0); // Normalize date to avoid timezone issues

    // Search backwards for up to 7 days
    for (let i = 0; i < 7; i++) {
        const searchDate = new Date(d);
        searchDate.setDate(searchDate.getDate() - i);
        const dateStr = searchDate.toISOString().split('T')[0];
        if (history[dateStr] !== undefined) {
            return history[dateStr];
        }
    }

    // Fallback: find the last available date before the target date
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
    return 1; // Return neutral value if no data is found
}
