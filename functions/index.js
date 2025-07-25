const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// --- Timezone-Safe Date Helpers ---
const toUtcDateString = (date) => date.toISOString().split('T')[0];
const dateFromUtcString = (str) => new Date(`${str}T00:00:00Z`);

/**
 * Sorts events by UTC date, ensuring splits are processed before transactions on the same day.
 */
function sortEvents(a, b) {
    const dateA = toUtcDateString(a.date);
    const dateB = toUtcDateString(b.date);
    if (dateA !== dateB) return dateA.localeCompare(dateB);
    if (a.eventType === 'split') return -1;
    if (b.eventType === 'split') return 1;
    return 0;
}

// --- Main Cloud Function Trigger ---
exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 540 }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const { userId } = context.params;
        console.log(`Recalculating holdings for user: ${userId}`);

        const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
        const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);

        const transactionsRef = db.collection(`users/${userId}/transactions`);
        const snapshot = await transactionsRef.get();
        const transactions = snapshot.docs.map(doc => ({ ...doc.data(), id: doc.id }));

        if (transactions.length === 0) {
            console.log(`No transactions for user ${userId}. Clearing data.`);
            await Promise.all([
                holdingsDocRef.set({ holdings: {}, realizedPL: 0 }),
                historyDocRef.set({ history: {} })
            ]);
            return null;
        }

        const marketData = await getMarketData(transactions);
        const allEvents = buildEventMap(transactions, marketData);

        const { holdings, realizedPL } = calculateHoldingsAtDate(new Date(), allEvents, marketData);
        const portfolioHistory = calculatePortfolioHistory(transactions, allEvents, marketData);

        await Promise.all([
            holdingsDocRef.set({ holdings, realizedPL, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
            historyDocRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
        ]);
    });

// --- Data Fetching and Preparation ---
async function getMarketData(transactions) {
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const allSymbols = [...symbols, "TWD=X"];
    const marketData = {};
    for (const symbol of allSymbols) {
        const collectionName = (symbol === "TWD=X") ? "exchange_rates" : "price_history";
        const doc = await db.collection(collectionName).doc(symbol).get();
        marketData[symbol] = doc.exists ? doc.data() : {}; // CORRECTED LINE
    }
    return marketData;
}

function buildEventMap(transactions, marketData) {
    const eventsBySymbol = {};
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

    for (const symbol of symbols) {
        const symbolTransactions = transactions.filter(t => t.symbol.toUpperCase() === symbol);
        const splitHistory = marketData[symbol]?.splits || {};
        const events = [];
        symbolTransactions.forEach(t => events.push({ ...t, date: t.date.toDate(), eventType: 'transaction' }));
        Object.keys(splitHistory).forEach(dateStr => {
            events.push({ date: dateFromUtcString(dateStr), splitRatio: splitHistory[dateStr], eventType: 'split' });
        });
        events.sort(sortEvents);
        eventsBySymbol[symbol] = events;
    }
    return eventsBySymbol;
}

// --- Unified Calculation Engine ---

/**
 * Authoritative function to calculate the complete portfolio state at a specific point in time.
 * @param {Date} targetDate The date for which to calculate the state.
 * @param {object} allEvents A map of sorted event arrays, keyed by symbol.
 * @param {object} marketData The complete market data object.
 * @returns {{holdings: object, realizedPL: number}}
 */
function calculateHoldingsAtDate(targetDate, allEvents, marketData) {
    const holdings = {};
    let totalRealizedPL = 0;
    const rateHistory = marketData["TWD=X"]?.rates || {};
    const targetDateStr = toUtcDateString(targetDate);

    for (const symbol in allEvents) {
        const priceHistory = marketData[symbol]?.prices || {};
        const relevantEvents = allEvents[symbol].filter(e => toUtcDateString(e.date) <= targetDateStr);
        if (relevantEvents.length === 0) continue;

        const currency = relevantEvents.find(e => e.currency)?.currency || 'TWD';
        let currentShares = 0, totalCostTWD = 0, symbolRealizedPLTWD = 0;

        for (const event of relevantEvents) {
            if (event.eventType === 'transaction') {
                const { quantity: t_shares = 0, price: t_price = 0, date: t_date, type } = event;
                const rateOnTransactionDate = currency === 'USD' ? (findPriceForDate(rateHistory, t_date) || 1) : 1;
                const valueTWD = t_shares * t_price * rateOnTransactionDate;

                if (type === 'buy') {
                    totalCostTWD += valueTWD;
                    currentShares += t_shares;
                } else if (type === 'sell') {
                    if (currentShares > 0) {
                        const avgCostPerShare = totalCostTWD / currentShares;
                        const costOfSoldShares = t_shares * avgCostPerShare;
                        symbolRealizedPLTWD += valueTWD - costOfSoldShares;
                        totalCostTWD -= costOfSoldShares;
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
            const latestPrice = findPriceForDate(priceHistory, targetDate);
            if (latestPrice === null) continue;
            const latestRate = currency === 'USD' ? (findPriceForDate(rateHistory, new Date()) || 1) : 1;
            const marketValueTWD = currentShares * latestPrice * latestRate;

            holdings[symbol] = {
                symbol, quantity: currentShares, currency,
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

/**
 * Calculates the historical portfolio value for the chart.
 */
function calculatePortfolioHistory(transactions, allEvents, marketData) {
    const portfolioHistory = {};
    const firstDate = transactions.sort((a, b) => a.date.toDate() - b.date.toDate())[0].date.toDate();
    const allDates = getDatesBetween(firstDate, new Date());

    for (const date of allDates) {
        const { holdings } = calculateHoldingsAtDate(date, allEvents, marketData);
        const dailyMarketValue = Object.values(holdings).reduce((sum, h) => sum + h.marketValueTWD, 0);
        if (dailyMarketValue > 0) {
            portfolioHistory[toUtcDateString(date)] = dailyMarketValue;
        }
    }
    return portfolioHistory;
}

// --- Utility Functions ---
function findPriceForDate(history, targetDate) {
    if (!history) return null;
    for (let i = 0; i < 7; i++) {
        const d = new Date(targetDate);
        d.setUTCDate(d.getUTCDate() - i);
        const d_str = toUtcDateString(d);
        if (history[d_str] !== undefined) return history[d_str];
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
