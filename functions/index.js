const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default; // For emergency fetches

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

        const marketData = await getMarketDataFromDb(transactions);

        // CORE LOGIC: Switched to FIFO-based calculation
        const { holdings, totalRealizedPL, portfolioHistory } = calculatePortfolioFIFO(transactions, marketData);

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

// --- Data Fetching (Remains the same) ---
async function getMarketDataFromDb(transactions) {
    // This function, with its emergency fetch logic, is unchanged.
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const marketData = {};
    const symbolsToFetch = [];

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

    if (symbolsToFetch.length > 0) {
        console.log(`Performing emergency fetch for ${symbolsToFetch.length} symbols: ${symbolsToFetch.join(', ')}`);
        const fetchPromises = symbolsToFetch.map(symbol => fetchAndSaveMarketData(symbol));
        const fetchedData = await Promise.all(fetchPromises);
        
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

async function fetchAndSaveMarketData(symbol) {
    // This emergency fetch function is also unchanged.
    try {
        const isForex = symbol === "TWD=X";
        const collectionName = isForex ? "exchange_rates" : "price_history";
        const docRef = db.collection(collectionName).doc(symbol);

        console.log(`Fetching full history for ${symbol} from Yahoo Finance...`);
        const queryOptions = { period1: '2000-01-01', events: 'split,div' };
        const results = await yahooFinance.historical(symbol, queryOptions);

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
            payload.rates = prices;
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
        return null;
    }
}


// --- NEW FIFO Calculation Engine ---
function calculatePortfolioFIFO(transactions, marketData) {
    const events = [];
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    // 1. Populate the event timeline (same as before)
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

    // 2. Process the timeline with FIFO logic
    const portfolio = {}; // Tracks state of each holding using lots
    let totalRealizedPL = 0;
    const portfolioHistory = {};
    let lastProcessedDate = null;

    for (const event of events) {
        const eventDate = event.date;
        const eventDateStr = eventDate.toISOString().split('T')[0];

        // Daily snapshot generation (logic remains the same)
        if (lastProcessedDate) {
            let currentDate = new Date(lastProcessedDate);
            currentDate.setDate(currentDate.getDate() + 1);
            while (currentDate < eventDate) {
                const dateStr = currentDate.toISOString().split('T')[0];
                portfolioHistory[dateStr] = calculateDailyMarketValueFIFO(portfolio, marketData, currentDate);
                currentDate.setDate(currentDate.getDate() + 1);
            }
        }

        const symbol = event.symbol.toUpperCase();
        if (!portfolio[symbol]) {
            // Initialize with the new FIFO structure
            portfolio[symbol] = { lots: [], currency: 'USD' };
        }

        const rateHistory = marketData["TWD=X"]?.rates || {};
        const rateOnDate = findNearestDataPoint(rateHistory, eventDate);

        switch (event.eventType) {
            case 'transaction':
                const t = event;
                portfolio[symbol].currency = t.currency;

                if (t.type === 'buy') {
                    const pricePerShareTWD = t.price * (t.currency === 'USD' ? rateOnDate : 1);
                    portfolio[symbol].lots.push({
                        quantity: t.quantity,
                        pricePerShareTWD: pricePerShareTWD,
                        date: eventDate
                    });
                } else if (t.type === 'sell') {
                    let sharesToSell = t.quantity;
                    const saleValueTWD = t.quantity * t.price * (t.currency === 'USD' ? rateOnDate : 1);
                    let costOfSoldSharesTWD = 0;

                    while (sharesToSell > 0 && portfolio[symbol].lots.length > 0) {
                        const firstLot = portfolio[symbol].lots[0];
                        if (firstLot.quantity <= sharesToSell) {
                            // Sell the entire first lot
                            costOfSoldSharesTWD += firstLot.quantity * firstLot.pricePerShareTWD;
                            sharesToSell -= firstLot.quantity;
                            portfolio[symbol].lots.shift(); // Remove the first lot
                        } else {
                            // Sell a partial amount from the first lot
                            costOfSoldSharesTWD += sharesToSell * firstLot.pricePerShareTWD;
                            firstLot.quantity -= sharesToSell;
                            sharesToSell = 0;
                        }
                    }
                    totalRealizedPL += saleValueTWD - costOfSoldSharesTWD;
                }
                break;

            case 'split':
                // Apply split to each lot
                portfolio[symbol].lots.forEach(lot => {
                    lot.quantity *= event.ratio;
                    lot.pricePerShareTWD /= event.ratio;
                });
                break;

            case 'dividend':
                // Dividend logic remains the same for now
                const totalShares = portfolio[symbol].lots.reduce((sum, lot) => sum + lot.quantity, 0);
                const dividendTWD = event.amount * totalShares * (portfolio[symbol].currency === 'USD' ? rateOnDate : 1);
                totalRealizedPL += dividendTWD;
                break;
        }
        
        portfolioHistory[eventDateStr] = calculateDailyMarketValueFIFO(portfolio, marketData, eventDate);
        lastProcessedDate = eventDate;
    }

    // Generate history from the last event to today (logic remains the same)
    if (lastProcessedDate) {
        let currentDate = new Date(lastProcessedDate);
        currentDate.setDate(currentDate.getDate() + 1);
        const today = new Date();
        while (currentDate <= today) {
            const dateStr = currentDate.toISOString().split('T')[0];
            portfolioHistory[dateStr] = calculateDailyMarketValueFIFO(portfolio, marketData, currentDate);
            currentDate.setDate(currentDate.getDate() + 1);
        }
    }

    // 4. Calculate final holdings details from the lots
    const finalHoldings = {};
    const today = new Date();
    const latestRate = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, today);

    for (const symbol of Object.keys(portfolio)) {
        const holding = portfolio[symbol];
        const totalQuantity = holding.lots.reduce((sum, lot) => sum + lot.quantity, 0);

        if (totalQuantity > 1e-9) {
            const totalCostTWD = holding.lots.reduce((sum, lot) => sum + (lot.quantity * lot.pricePerShareTWD), 0);
            const priceHistory = marketData[symbol]?.prices || {};
            const latestPrice = findNearestDataPoint(priceHistory, today);
            const rate = holding.currency === 'USD' ? latestRate : 1;
            const marketValueTWD = totalQuantity * latestPrice * rate;
            const unrealizedPLTWD = marketValueTWD - totalCostTWD;

            finalHoldings[symbol] = {
                symbol: symbol,
                quantity: totalQuantity,
                totalCostTWD: totalCostTWD,
                avgCostTWD: totalQuantity > 0 ? totalCostTWD / totalQuantity : 0,
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

// Helper function to calculate daily market value based on lots
function calculateDailyMarketValueFIFO(portfolio, marketData, date) {
    let totalValue = 0;
    const rateOnDate = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, date);

    for (const symbol in portfolio) {
        const holding = portfolio[symbol];
        const totalQuantity = holding.lots.reduce((sum, lot) => sum + lot.quantity, 0);

        if (totalQuantity > 0) {
            const priceOnDate = findNearestDataPoint(marketData[symbol]?.prices || {}, date);
            const rate = holding.currency === 'USD' ? rateOnDate : 1;
            totalValue += totalQuantity * priceOnDate * rate;
        }
    }
    return totalValue;
}

// findNearestDataPoint function remains the same
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
