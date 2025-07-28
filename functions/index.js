const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// =================================================================================
// === Core Calculation Logic (Refactored to be reusable) ========================
// =================================================================================
async function performRecalculation(userId) {
    const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
    const logs = [];
    const log = (message) => {
        const timestamp = new Date().toISOString();
        logs.push(`${timestamp}: ${message}`);
        console.log(`[${userId}] ${timestamp}: ${message}`);
    };

    try {
        log("--- Recalculation triggered (v31 - Unified Trigger) ---");

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
                holdingsDocRef.set({ holdings: {}, totalRealizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
                historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
            ]);
            return;
        }

        const marketData = await getMarketDataFromDb(transactions, log);
        if (!marketData || Object.keys(marketData).length === 0) {
            throw new Error("Market data is empty after fetch.");
        }

        log("Starting final, corrected calculation...");
        const result = calculatePortfolio(transactions, userSplits, marketData, log);
        if (!result) throw new Error("Calculation function returned undefined.");

        const { holdings, totalRealizedPL, portfolioHistory, xirr } = result;
        log(`Calculation complete. Holdings: ${Object.keys(holdings).length}, Realized P/L: ${totalRealizedPL}, History points: ${Object.keys(portfolioHistory).length}, XIRR: ${xirr}`);

        log("Saving results...");

        // Prepare the final data, ensuring the trigger field is removed to prevent loops
        const finalData = { 
            holdings, 
            totalRealizedPL, 
            xirr, 
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            force_recalc_timestamp: admin.firestore.FieldValue.delete() // Remove the trigger field
        };

        await Promise.all([
            holdingsDocRef.set(finalData, { merge: true }), // Use merge to avoid race conditions
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

// 監聽個股歷史資料 price_history/{symbol}
exports.recalculateOnPriceUpdate = functions
  .runWith({ timeoutSeconds: 240, memory: "1GB" })     // 視計算量調整
  .firestore
  .document("price_history/{symbol}")
  .onWrite(async (change, context) => {
    const symbol = context.params.symbol.toUpperCase();
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();

    // 1. 是否真的有 meaningful 變動？避免無限迴圈
    if (before &&
        JSON.stringify(before.prices)    === JSON.stringify(after.prices) &&
        JSON.stringify(before.dividends) === JSON.stringify(after.dividends)) {
      console.log(`[${symbol}] only metadata changed, skip.`);
      return null;
    }

    console.log(`[${symbol}] market data changed, finding affected users…`);

    // 2. 找出所有持有該股票的使用者（跨 collectionGroup 搜尋）
    const txSnap = await db.collectionGroup("transactions")
                           .where("symbol", "==", symbol)
                           .get();

    if (txSnap.empty) {
      console.log(`[${symbol}] no users hold this symbol, done.`);
      return null;
    }

    const affectedUsers = new Set(
      txSnap.docs.map(d => d.ref.path.split("/")[1])   // users/{uid}/…
    );

    // 3. 對每位使用者塞 force_recalc_timestamp，讓主要計算函式動起來
    const nowTS = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(
      [...affectedUsers].map(uid =>
        db.doc(`users/${uid}/user_data/current_holdings`)
          .set({ force_recalc_timestamp: nowTS }, { merge: true })
      )
    );

    console.log(`[${symbol}] triggered ${affectedUsers.size} users to recalc.`);
    return null;
  });

// 監聽匯率檔 exchange_rates/TWD=X
exports.recalculateOnFxUpdate = functions
  .runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore
  .document("exchange_rates/TWD=X")
  .onWrite(async (change, context) => {
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();

    if (before && JSON.stringify(before.rates) === JSON.stringify(after.rates)) {
      console.log("TWD=X metadata only, skip.");
      return null;
    }

    const users = await db.collection("users").listDocuments();
    const nowTS = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(
      users.map(u =>
        db.doc(`users/${u.id}/user_data/current_holdings`)
          .set({ force_recalc_timestamp: nowTS }, { merge: true })
      )
    );
    console.log(`FX updated → triggered ${users.length} users.`);
    return null;
  });



// =================================================================================
// === Data Fetching and Processing Functions (Unchanged) ========================
// =================================================================================

async function getMarketDataFromDb(transactions, log) {
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    const allSymbols = [...new Set([...symbols, "TWD=X"])];
    log(`Required symbols: [${allSymbols.join(', ')}]`);
    const marketData = {};

    for (const symbol of allSymbols) {
        const docRef = db.collection(symbol === "TWD=X" ? "exchange_rates" : "price_history").doc(symbol);
        const doc = await docRef.get();

        if (doc.exists) {
            log(`Found ${symbol} in Firestore.`);
            marketData[symbol] = doc.data();
        } else {
            log(`Data for ${symbol} not found. Performing emergency fetch...`);
            const fetchedData = await fetchAndSaveMarketData(symbol, log);
            if (fetchedData) {
                marketData[symbol] = fetchedData;
                await docRef.set(fetchedData);
                log(`Successfully fetched and saved data for ${symbol}.`);
            }
        }
    }
    return marketData;
}

async function fetchAndSaveMarketData(symbol, log) {
    try {
        log(`[Fetch] Fetching full history for ${symbol} from Yahoo Finance...`);
        const queryOptions = { period1: '2000-01-01' };
        const hist = await yahooFinance.historical(symbol, queryOptions);

        if (!hist || hist.length === 0) {
            log(`[Fetch] Warning: No data returned for ${symbol}.`);
            return null;
        }

        log(`[Fetch] Received ${hist.length} data points for ${symbol}.`);

        const prices = {};
        hist.forEach(item => {
            prices[item.date.toISOString().split('T')[0]] = item.close;
        });

        const payload = {
            prices: prices,
            splits: {}, // We no longer store splits from yfinance
            dividends: (hist.dividends || []).reduce((acc, d) => ({ ...acc, [d.date.toISOString().split('T')[0]]: d.amount }), {}),
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            dataSource: 'emergency-fetch-user-split-model-v2'
        };

        if (symbol === 'TWD=X') {
            payload.rates = payload.prices;
            delete payload.dividends;
        }

        return payload;
    } catch (e) {
        log(`[Fetch Error] for ${symbol}: ${e.message}`);
        return null;
    }
}

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
                const costPerShareOriginal = (t.totalCost || t.price);
                const costPerShareTWD = costPerShareOriginal * (t.currency === 'USD' ? rateOnDate : 1);

                if (t.type === 'buy') {
                    portfolio[symbol].lots.push({ quantity: t.quantity, pricePerShareTWD: costPerShareTWD, pricePerShareOriginal: costPerShareOriginal, date: event.date });
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

function createCashflows(events, portfolio, finalHoldings, marketData) {
    const cashflows = [];

    // Add buy/sell transactions to cashflows
    events.filter(e => e.eventType === 'transaction').forEach(t => {
        const rateHistory = marketData["TWD=X"]?.rates || {};
        const rateOnDate = findNearestDataPoint(rateHistory, t.date);
        const amount = (t.totalCost || t.quantity * t.price) * (t.currency === 'USD' ? rateOnDate : 1);

        cashflows.push({
            date: new Date(t.date),
            amount: t.type === 'buy' ? -amount : amount
        });
    });

    // Add dividends to cashflows
    events.filter(e => e.eventType === 'dividend').forEach(d => {
        const rateHistory = marketData["TWD=X"]?.rates || {};
        const rateOnDate = findNearestDataPoint(rateHistory, d.date);
        // Correctly get the currency from the portfolio state at that time
        const holdingCurrency = portfolio[d.symbol]?.currency || 'USD'; 
        const totalSharesOnDate = portfolio[d.symbol]?.lots.reduce((sum, lot) => sum + lot.quantity, 0) || 0;
        const amount = d.amount * totalSharesOnDate * (holdingCurrency === 'USD' ? rateOnDate : 1);

        if (amount > 0) {
            cashflows.push({
                date: new Date(d.date),
                amount: amount
            });
        }
    });

    // Add current market value as the final cashflow
    const totalMarketValue = Object.values(finalHoldings).reduce((sum, h) => sum + h.marketValueTWD, 0);
    if (totalMarketValue > 0) {
        cashflows.push({
            date: new Date(),
            amount: totalMarketValue
        });
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
