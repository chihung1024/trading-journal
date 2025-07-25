const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();

const db = admin.firestore();

// --- Date & Sorting Helpers (Timezone-Safe) ---

/**
 * Converts a Date object to a YYYY-MM-DD string in UTC.
 * @param {Date} date The date object.
 * @returns {string} The date string in YYYY-MM-DD format.
 */
function toUtcDateString(date) {
    if (!date || !(date instanceof Date)) return '';
    return date.toISOString().split('T')[0];
}

/**
 * Creates a Date object from a YYYY-MM-DD string, interpreted as UTC midnight.
 * @param {string} dateStr The date string.
 * @returns {Date} The date object.
 */
function dateFromUtcString(dateStr) {
    return new Date(`${dateStr}T00:00:00Z`);
}

/**
 * Sorts events by UTC date, ensuring splits are processed before transactions on the same day.
 */
function sortEvents(a, b) {
    const dateA = toUtcDateString(a.date);
    const dateB = toUtcDateString(b.date);
    if (dateA !== dateB) return dateA.localeCompare(dateB);
    if (a.eventType === 'split' && b.eventType !== 'split') return -1;
    if (a.eventType !== 'split' && b.eventType === 'split') return 1;
    return 0;
}

// --- Main Cloud Function ---

exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 540 }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const { userId } = context.params;
        console.log(`Recalculating holdings for user: ${userId}`);

        const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
        const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);

        const transactionsRef = db.collection(`users/${userId}/transactions`);
        const snapshot = await transactionsRef.get();
        const transactions = snapshot.docs.map(doc => doc.data());

        if (transactions.length === 0) {
            console.log(`No transactions found for user ${userId}. Clearing all data.`);
            await Promise.all([
                holdingsDocRef.set({ holdings: {}, realizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
                historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
            ]);
            return null;
        }

        const marketData = await getMarketDataFromDb(transactions);
        const { holdings, realizedPL } = calculateCurrentHoldings(transactions, marketData);
        const portfolioHistory = calculatePortfolioHistory(transactions, marketData);

        await Promise.all([
            holdingsDocRef.set({ holdings, realizedPL, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
            historyDocRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
        ]);

        return null;
    });

// --- Data Fetching ---

async function getMarketDataFromDb(transactions) {
    const marketData = {};
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const allSymbols = [...symbols, "TWD=X"];

    for (const symbol of allSymbols) {
        const collectionName = (symbol === "TWD=X") ? "exchange_rates" : "price_history";
        const doc = await db.collection(collectionName).doc(symbol).get();

        if (doc.exists) {
            marketData[symbol] = doc.data();
        } else {
            console.log(`Market data for ${symbol} not found in DB. Fetching from API...`);
            const newData = await fetchAndSaveMarketData(symbol);
            if (newData) marketData[symbol] = newData;
        }
    }
    return marketData;
}

async function fetchAndSaveMarketData(symbol) {
    try {
        if (symbol === "TWD=X") {
            const result = await yahooFinance.historical(symbol, { period1: '2000-01-01' });
            if (!result || result.length === 0) return null;
            const rates = Object.fromEntries(result.map(item => [toUtcDateString(item.date), item.close]));
            await db.collection("exchange_rates").doc(symbol).set({ rates, lastUpdated: admin.firestore.FieldValue.serverTimestamp() });
            return { rates };
        } else {
            const [priceHistory, eventsHistory] = await Promise.all([
                yahooFinance.historical(symbol, { period1: '2000-01-01' }),
                yahooFinance.historical(symbol, { period1: '2000-01-01', events: 'split' })
            ]);
            const prices = priceHistory ? Object.fromEntries(priceHistory.map(item => [toUtcDateString(item.date), item.close])) : {};
            const splits = eventsHistory?.splits ? Object.fromEntries(eventsHistory.splits.map(item => [toUtcDateString(item.date), item.numerator / item.denominator])) : {};
            const payload = { prices, splits, lastUpdated: admin.firestore.FieldValue.serverTimestamp(), dataSource: 'yahoo-finance2-live' };
            await db.collection("price_history").doc(symbol).set(payload);
            return { prices, splits };
        }
    } catch (error) {
        console.error(`Error fetching or saving market data for ${symbol}:`, error);
        return null;
    }
}

// --- Core Calculation Engines ---

function calculateCurrentHoldings(transactions, marketData) {
    const holdings = {};
    let totalRealizedPL = 0;
    const rateHistory = marketData["TWD=X"]?.rates || {};
    const allSymbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    for (const symbol of allSymbols) {
        const symbolTransactions = transactions.filter(t => t.symbol.toUpperCase() === symbol);
        const priceHistory = marketData[symbol]?.prices || {};
        const splitHistory = marketData[symbol]?.splits || {};
        const currency = symbolTransactions[0]?.currency || 'TWD';

        const events = [];
        symbolTransactions.forEach(t => events.push({ ...t, date: t.date.toDate(), eventType: 'transaction' }));
        Object.keys(splitHistory).forEach(dateStr => events.push({ date: dateFromUtcString(dateStr), splitRatio: splitHistory[dateStr], eventType: 'split' }));
        events.sort(sortEvents);

        let currentShares = 0, totalCostOriginal = 0, totalCostTWD = 0, symbolRealizedPLTWD = 0;

        for (const event of events) {
            if (event.eventType === 'transaction') {
                const { quantity: t_shares = 0, price: t_price = 0, date: t_date, type } = event;
                const rateOnTransactionDate = currency === 'USD' ? (findPriceForDate(rateHistory, t_date) || 1) : 1;
                const valueOriginal = t_shares * t_price;
                const valueTWD = valueOriginal * rateOnTransactionDate;

                if (type === 'buy') {
                    currentShares += t_shares;
                    totalCostOriginal += valueOriginal;
                    totalCostTWD += valueTWD;
                } else if (type === 'sell') {
                    if (currentShares > 0) {
                        const proportion = t_shares / currentShares;
                        symbolRealizedPLTWD += valueTWD - (totalCostTWD * proportion);
                        totalCostOriginal *= (1 - proportion);
                        totalCostTWD *= (1 - proportion);
                        currentShares -= t_shares;
                    }
                } else if (type === 'dividend') {
                    symbolRealizedPLTWD += valueTWD;
                }
            } else if (event.eventType === 'split') {
                currentShares *= event.splitRatio;
            }
        }

        if (currentShares > 1e-9) {
            const latestPrice = findPriceForDate(priceHistory, new Date());
            if (latestPrice === null) continue;
            const latestRate = currency === 'USD' ? (findPriceForDate(rateHistory, new Date()) || 1) : 1;
            const marketValueTWD = currentShares * latestPrice * latestRate;

            holdings[symbol] = {
                symbol, quantity: currentShares, currency,
                avgCost: currentShares > 0 ? totalCostOriginal / currentShares : 0,
                totalCostTWD, currentPrice: latestPrice, marketValueTWD,
                unrealizedPLTWD: marketValueTWD - totalCostTWD,
                realizedPLTWD: symbolRealizedPLTWD,
                returnRate: totalCostTWD > 0 ? ((marketValueTWD - totalCostTWD) / totalCostTWD) * 100 : 0,
            };
        } else {
            totalRealizedPL += symbolRealizedPLTWD;
        }
    }
    return { holdings, realizedPL: totalRealizedPL };
}

function calculatePortfolioHistory(transactions, marketData) {
    if (transactions.length === 0) return {};

    const portfolioHistory = {};
    const sortedTransactions = transactions.sort((a, b) => (a.date.toDate() - b.date.toDate()));
    const firstDate = sortedTransactions[0].date.toDate();
    const allDates = getDatesBetween(firstDate, new Date());
    const rateHistory = marketData["TWD=X"]?.rates || {};
    const allSymbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    const eventsBySymbol = {};
    for (const symbol of allSymbols) {
        const symbolTransactions = transactions.filter(t => t.symbol.toUpperCase() === symbol);
        const splitHistory = marketData[symbol]?.splits || {};
        const events = [];
        symbolTransactions.forEach(t => events.push({ ...t, date: t.date.toDate(), eventType: 'transaction' }));
        Object.keys(splitHistory).forEach(dateStr => events.push({ date: dateFromUtcString(dateStr), splitRatio: splitHistory[dateStr], eventType: 'split' }));
        events.sort(sortEvents);
        eventsBySymbol[symbol] = events;
    }

    for (const date of allDates) {
        let dailyMarketValue = 0;
        const rateOnDate = findPriceForDate(rateHistory, date) || 1;

        for (const symbol of allSymbols) {
            const priceHistory = marketData[symbol]?.prices || {};
            const priceOnDate = findPriceForDate(priceHistory, date);
            if (priceOnDate === null) continue;

            const dateStr = toUtcDateString(date);
            const relevantEvents = eventsBySymbol[symbol].filter(e => toUtcDateString(e.date) <= dateStr);
            let sharesOnDate = 0;

            for (const event of relevantEvents) {
                if (event.eventType === 'transaction') {
                    if (event.type === 'buy') sharesOnDate += event.quantity;
                    else if (event.type === 'sell') sharesOnDate -= event.quantity;
                } else if (event.eventType === 'split') {
                    sharesOnDate *= event.splitRatio;
                }
            }

            if (sharesOnDate > 1e-9) {
                const currency = transactions.find(t => t.symbol.toUpperCase() === symbol)?.currency || 'TWD';
                const rate = currency === 'USD' ? rateOnDate : 1;
                dailyMarketValue += sharesOnDate * priceOnDate * rate;
            }
        }

        if (dailyMarketValue > 0) {
            portfolioHistory[toUtcDateString(date)] = dailyMarketValue;
        }
    }
    return portfolioHistory;
}

function findPriceForDate(history, targetDate) {
    if (!history || Object.keys(history).length === 0) return null;
    for (let i = 0; i < 7; i++) {
        const d = new Date(targetDate);
        d.setUTCDate(d.getUTCDate() - i);
        const d_str = toUtcDateString(d);
        if (history[d_str]) return history[d_str];
    }
    return null;
}

function getDatesBetween(startDate, endDate) {
    const dates = [];
    let currentDate = dateFromUtcString(toUtcDateString(startDate));
    const finalDate = dateFromUtcString(toUtcDateString(endDate));
    while (currentDate <= finalDate) {
        dates.push(new Date(currentDate));
        currentDate.setUTCDate(currentDate.getUTCDate() + 1);
    }
    return dates;
}
