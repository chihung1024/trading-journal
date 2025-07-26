const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default; // Re-import for emergency fetches

admin.initializeApp();
const db = admin.firestore();

// Main Cloud Function
exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 540, memory: '1GB' }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const { userId } = context.params;
        console.log(`--- Recalculation triggered for user: ${userId} ---`);

        const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
        const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);

        const transactionsSnapshot = await db.collection(`users/${userId}/transactions`).get();
        const transactions = transactionsSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

        if (transactions.length === 0) {
            console.log(`No transactions found for user ${userId}. Clearing all portfolio data.`);
            await Promise.all([
                holdingsDocRef.set({ holdings: {}, totalRealizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
                historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
            ]);
            return null;
        }

        // The getMarketDataFromDb function now handles emergency data fetching.
        const marketData = await getMarketDataFromDb(transactions);

        const { holdings, totalRealizedPL, portfolioHistory } = calculatePortfolio(transactions, marketData);

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
 * Fetches market data from Firestore and performs emergency fetches from Yahoo Finance if data is missing.
 */
async function getMarketDataFromDb(transactions) {
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const marketData = {};
    const symbolsToFetch = [];

    // First, try to get all data from Firestore
    const promises = symbols.map(symbol => 
        db.collection("price_history").doc(symbol).get().then(doc => {
            if (doc.exists) {
                marketData[symbol] = doc.data();
            } else {
                console.log(`Data for symbol '${symbol}' not found in Firestore. Queuing for emergency fetch.`);
                symbolsToFetch.push(symbol);
            }
        })
    );
    // Also fetch exchange rates
    promises.push(
        db.collection("exchange_rates").doc("TWD=X").get().then(doc => {
            if (doc.exists) {
                marketData["TWD=X"] = doc.data();
            } else {
                console.log(`Exchange rate data 'TWD=X' not found. Queuing for emergency fetch.`);
                symbolsToFetch.push("TWD=X");
            }
        })
    );

    await Promise.all(promises);

    // If any symbols were not found, fetch them now
    if (symbolsToFetch.length > 0) {
        console.log(`Performing emergency fetch for ${symbolsToFetch.length} symbols: ${symbolsToFetch.join(', ')}`);
        const fetchPromises = symbolsToFetch.map(symbol => fetchAndSaveMarketData(symbol));
        const fetchedData = await Promise.all(fetchPromises);
        
        // Merge fetched data into our main marketData object
        fetchedData.forEach((data, index) => {
            if (data) {
                const symbol = symbolsToFetch[index];
                marketData[symbol] = data;
            }
        });
    }

    console.log(`Successfully prepared market data for ${Object.keys(marketData).length} symbols.`);
    return marketData;
}

/**
 * Fetches full historical data for a single symbol from Yahoo Finance and saves it to Firestore.
 * This is the emergency fetch function.
 */
async function fetchAndSaveMarketData(symbol) {
    try {
        const isForex = symbol === "TWD=X";
        const collectionName = isForex ? "exchange_rates" : "price_history";
        const docRef = db.collection(collectionName).doc(symbol);

        console.log(`Fetching full history for ${symbol} from Yahoo Finance...`);
        const queryOptions = { period1: '2000-01-01', events: 'split,div' };
        const results = await yahooFinance.historical(symbol, queryOptions);

        // Standardize data format to match python script output
        const prices = {};
        results.forEach(item => {
            prices[item.date.toISOString().split('T')[0]] = item.close;
        });

        const payload = {
            prices: prices,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            dataSource: 'yahoo-finance2-emergency-fetch',
        };

        if (isForex) {
            payload.rates = prices; // For exchange rates, the key is 'rates'
            delete payload.prices;
        } else {
            const splits = {};
            (results.splits || []).forEach(item => {
                splits[item.date.toISOString().split('T')[0]] = item.numerator / item.denominator;
            });
            const dividends = {};
            (results.dividends || []).forEach(item => {
                dividends[item.date.toISOString().split('T')[0]] = item.amount;
            });
            payload.splits = splits;
            payload.dividends = dividends;
        }

        await docRef.set(payload);
        console.log(`Successfully fetched and saved emergency data for ${symbol} to Firestore.`);
        return payload;

    } catch (error) {
        console.error(`ERROR during emergency fetch for ${symbol}:`, error);
        return null; // Return null on failure
    }
}

// The core calculation engine - REMAINS THE SAME
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

// Helper functions - REMAINS THE SAME
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
