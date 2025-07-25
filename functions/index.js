const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();

const db = admin.firestore();

// This is the main trigger function.
exports.recalculateHoldings = functions
  .runWith({ timeoutSeconds: 540, memory: "1GB" })
  .firestore.document("users/{userId}/transactions/{transactionId}")
  .onWrite(async (change, context) => {
    const { userId } = context.params;
    console.log(`Starting holdings recalculation for user: ${userId}`);

    const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
    const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);

    const transactionsSnapshot = await db.collection(`users/${userId}/transactions`).get();
    const transactions = transactionsSnapshot.docs.map((doc) => {
      const data = doc.data();
      return { ...data, id: doc.id, date: data.date.toDate ? data.date.toDate() : new Date(data.date) };
    });

    if (transactions.length === 0) {
      console.log(`No transactions found for user ${userId}. Clearing all data.`);
      await Promise.all([
        holdingsDocRef.set({ holdings: {}, realizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
        historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
      ]);
      return null;
    }

    const marketData = await getMarketData(transactions);
    const { holdings, realizedPL } = calculateHoldings(transactions, marketData);
    const portfolioHistory = calculatePortfolioHistory(transactions, marketData);

    await Promise.all([
      holdingsDocRef.set({ holdings, realizedPL, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
      historyDocRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
    ]);

    console.log(`Successfully recalculated holdings for user: ${userId}`);
    return null;
  });

// Fetches all necessary market data from Firestore or Yahoo Finance.
async function getMarketData(transactions) {
  const symbols = [...new Set(transactions.map((t) => t.symbol.toUpperCase()))];
  const allSymbols = [...new Set([...symbols, "TWD=X"])];
  const marketData = {};

  for (const symbol of allSymbols) {
    const isForex = symbol.endsWith("=X");
    const collectionName = isForex ? "exchange_rates" : "price_history";
    const docRef = db.collection(collectionName).doc(symbol);
    const doc = await docRef.get();

    if (doc.exists) {
      marketData[symbol] = doc.data();
    } else {
      console.log(`Market data for ${symbol} not found in DB. Fetching...`);
      const newData = await fetchAndSaveMarketData(symbol);
      if (newData) marketData[symbol] = newData;
    }
  }
  return marketData;
}

// Fetches and saves market data from Yahoo Finance.
async function fetchAndSaveMarketData(symbol) {
  try {
    const isForex = symbol.endsWith("=X");
    const collectionName = isForex ? "exchange_rates" : "price_history";
    const docRef = db.collection(collectionName).doc(symbol);

    if (isForex) {
      const result = await yahooFinance.historical(symbol, { period1: '2000-01-01' });
      if (!result || result.length === 0) return null;
      const rates = result.reduce((acc, item) => ({ ...acc, [item.date.toISOString().split('T')[0]]: item.close }), {});
      const payload = { rates, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
      await docRef.set(payload, { merge: true });
      return { prices: rates, splits: {}, dividends: {} };
    } else {
      const [priceHistory, eventsHistory] = await Promise.all([
        yahooFinance.historical(symbol, { period1: '2000-01-01' }),
        yahooFinance.historical(symbol, { period1: '2000-01-01', events: 'split,div' })
      ]);

      const prices = priceHistory.reduce((acc, item) => ({ ...acc, [item.date.toISOString().split('T')[0]]: item.close }), {});
      const splits = eventsHistory.splits.reduce((acc, item) => ({ ...acc, [item.date.toISOString().split('T')[0]]: item.numerator / item.denominator }), {});
      const dividends = eventsHistory.dividends.reduce((acc, item) => ({ ...acc, [item.date.toISOString().split('T')[0]]: item.amount }), {});

      const payload = { prices, splits, dividends, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
      await docRef.set(payload, { merge: true });
      return payload;
    }
  } catch (error) {
    console.error(`Error fetching/saving market data for ${symbol}:`, error);
    return null;
  }
}

// Finds the nearest available value in a time-series object.
function findNearestValue(history, date) {
    if (!history) return null;
    const targetDate = new Date(date);
    targetDate.setUTCHours(0, 0, 0, 0);

    for (let i = 0; i < 7; i++) {
        const d = new Date(targetDate);
        d.setDate(d.getDate() - i);
        const dateStr = d.toISOString().split('T')[0];
        if (history[dateStr] !== undefined) return history[dateStr];
    }
    return null;
}

/**
 * Calculates the cumulative split adjustment factor for a given transaction date.
 * @param {Date} transactionDate The date of the transaction.
 * @param {Object} splitHistory The split history for the stock.
 * @returns {number} The cumulative adjustment factor.
 */
function getAdjustmentFactor(transactionDate, splitHistory) {
    let factor = 1;
    if (!splitHistory) return factor;

    const sortedSplitDates = Object.keys(splitHistory).sort();

    for (const splitDateStr of sortedSplitDates) {
        const splitDate = new Date(splitDateStr);
        if (splitDate > transactionDate) {
            factor *= splitHistory[splitDateStr];
        }
    }
    return factor;
}

// Main calculation logic based on the Average Cost Method.
function calculateHoldings(transactions, marketData) {
    const holdings = {};
    let totalRealizedPL = 0;
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const exchangeRates = marketData['TWD=X']?.prices || {};

    for (const symbol of symbols) {
        const symbolTransactions = transactions.filter(t => t.symbol.toUpperCase() === symbol);
        const priceHistory = marketData[symbol]?.prices || {};
        const splitHistory = marketData[symbol]?.splits || {};
        const dividendHistory = marketData[symbol]?.dividends || {};
        const currency = symbolTransactions[0]?.currency || 'TWD';

        // 1. Adjust all transactions for splits.
        const adjustedTransactions = symbolTransactions.map(t => {
            const factor = getAdjustmentFactor(t.date, splitHistory);
            const isCashDividend = t.transactionType === 'dividend';
            return {
                ...t,
                adjustedQuantity: isCashDividend ? t.quantity : t.quantity * factor,
                adjustedPrice: isCashDividend ? 0 : t.price / factor,
            };
        });

        // 2. Calculate total buys to determine average cost in both original currency and TWD.
        let totalAdjustedSharesBought = 0;
        let totalBuyCostOriginal = 0;
        let totalBuyCostTWD = 0;

        adjustedTransactions.filter(t => t.transactionType === 'buy').forEach(buy => {
            const rate = findNearestValue(exchangeRates, buy.date) || 1;
            totalAdjustedSharesBought += buy.adjustedQuantity;
            totalBuyCostOriginal += buy.adjustedQuantity * buy.adjustedPrice;
            totalBuyCostTWD += buy.adjustedQuantity * buy.adjustedPrice * (currency === 'USD' ? rate : 1);
        });

        const avgCostPerAdjustedShareOriginal = totalAdjustedSharesBought > 0 ? totalBuyCostOriginal / totalAdjustedSharesBought : 0;
        const avgCostPerAdjustedShareTWD = totalAdjustedSharesBought > 0 ? totalBuyCostTWD / totalAdjustedSharesBought : 0;

        // 3. Calculate realized P/L from sales using the TWD average cost.
        let realizedPLFromSales = 0;
        let totalAdjustedSharesSold = 0;
        adjustedTransactions.filter(t => t.transactionType === 'sell').forEach(sell => {
            const rate = findNearestValue(exchangeRates, sell.date) || 1;
            const proceedsTWD = sell.adjustedQuantity * sell.adjustedPrice * (currency === 'USD' ? rate : 1);
            const costOfSaleTWD = sell.adjustedQuantity * avgCostPerAdjustedShareTWD;
            realizedPLFromSales += proceedsTWD - costOfSaleTWD;
            totalAdjustedSharesSold += sell.adjustedQuantity;
        });

        // 4. Calculate dividend income.
        let dividendIncomeTWD = 0;
        // Manual dividend entries (quantity is the total amount)
        adjustedTransactions.filter(t => t.transactionType === 'dividend').forEach(div => {
            const rate = findNearestValue(exchangeRates, div.date) || 1;
            dividendIncomeTWD += div.adjustedQuantity * (currency === 'USD' ? rate : 1);
        });

        // Fetched dividend history
        const sortedDividendDates = Object.keys(dividendHistory).sort();
        for (const divDateStr of sortedDividendDates) {
            const divDate = new Date(divDateStr);
            const dividendPerShare = dividendHistory[divDateStr];
            
            let sharesOnDivDate = 0;
            const dayBeforeDiv = new Date(divDate);
            dayBeforeDiv.setDate(dayBeforeDiv.getDate() - 1);

            adjustedTransactions.filter(t => t.date <= dayBeforeDiv && t.transactionType !== 'dividend').forEach(t => {
                if (t.transactionType === 'buy') sharesOnDivDate += t.adjustedQuantity;
                if (t.transactionType === 'sell') sharesOnDivDate -= t.adjustedQuantity;
            });

            if (sharesOnDivDate > 0) {
                const rate = findNearestValue(exchangeRates, divDate) || 1;
                dividendIncomeTWD += sharesOnDivDate * dividendPerShare * (currency === 'USD' ? rate : 1);
            }
        }

        const totalRealizedPLTWD = realizedPLFromSales + dividendIncomeTWD;

        // 5. Calculate final current holdings.
        const currentAdjustedShares = totalAdjustedSharesBought - totalAdjustedSharesSold;

        if (currentAdjustedShares > 1e-9) {
            const currentCostBasisTWD = currentAdjustedShares * avgCostPerAdjustedShareTWD;
            const latestPrice = findNearestValue(priceHistory, new Date());
            const latestRate = findNearestValue(exchangeRates, new Date()) || 1;
            
            const marketValueTWD = latestPrice !== null ? currentAdjustedShares * latestPrice * (currency === 'USD' ? latestRate : 1) : 0;
            const unrealizedPLTWD = marketValueTWD - currentCostBasisTWD;

            holdings[symbol] = {
                symbol,
                quantity: currentAdjustedShares,
                avgCost: avgCostPerAdjustedShareOriginal,
                totalCostTWD: currentCostBasisTWD,
                currency,
                currentPrice: latestPrice,
                marketValueTWD,
                unrealizedPLTWD,
                realizedPLTWD: totalRealizedPLTWD,
                returnRate: currentCostBasisTWD > 0 ? (unrealizedPLTWD / currentCostBasisTWD) * 100 : 0,
            };
        } else {
            totalRealizedPL += totalRealizedPLTWD;
        }
    }

    return { holdings, realizedPL: totalRealizedPL };
}

function calculatePortfolioHistory(transactions, marketData) {
    if (transactions.length === 0) return {};

    const portfolioHistory = {};
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const exchangeRates = marketData['TWD=X']?.prices || {};

    const firstDate = new Date(Math.min(...transactions.map(t => t.date)));
    const today = new Date();
    const dateRange = [];
    for (let d = new Date(firstDate); d <= today; d.setDate(d.getDate() + 1)) {
        dateRange.push(new Date(d));
    }

    for (const date of dateRange) {
        let dailyMarketValue = 0;
        const dateStr = date.toISOString().split('T')[0];

        for (const symbol of symbols) {
            const symbolTransactions = transactions.filter(t => t.symbol.toUpperCase() === symbol && t.date <= date);
            if (symbolTransactions.length === 0) continue;

            const splitHistory = marketData[symbol]?.splits || {};
            const priceHistory = marketData[symbol]?.prices || {};
            const currency = symbolTransactions[0].currency || 'TWD';

            const adjustedTransactions = symbolTransactions.map(t => {
                const factor = getAdjustmentFactor(t.date, splitHistory);
                return { ...t, adjustedQuantity: t.quantity * factor };
            });

            let sharesOnDate = 0;
            adjustedTransactions.forEach(t => {
                if (t.transactionType === 'buy') sharesOnDate += t.adjustedQuantity;
                if (t.transactionType === 'sell') sharesOnDate -= t.adjustedQuantity;
            });

            if (sharesOnDate > 1e-9) {
                const priceOnDate = findNearestValue(priceHistory, date);
                const rateOnDate = findNearestValue(exchangeRates, date) || 1;
                if (priceOnDate !== null) {
                    dailyMarketValue += sharesOnDate * priceOnDate * (currency === 'USD' ? rateOnDate : 1);
                }
            }
        }
        if (dailyMarketValue > 0) {
            portfolioHistory[dateStr] = dailyMarketValue;
        }
    }

    return portfolioHistory;
}
