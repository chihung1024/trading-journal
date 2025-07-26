const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// Final version with a simplified, ultra-reliable logging mechanism.
exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 540, memory: '1GB' }).firestore
    .document("users/{userId}/transactions/{transactionId}")
    .onWrite(async (change, context) => {
        const { userId } = context.params;
        const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
        const logs = []; // Use a simple array for logging.

        const log = (message) => {
            const timestamp = new Date().toISOString();
            const logEntry = `${timestamp}: ${message}`;
            console.log(logEntry);
            logs.push(logEntry);
        };

        try {
            log("--- Recalculation triggered (v6 - Simple Log) ---");

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
                await logRef.set({ entries: logs });
                return null;
            }

            const marketData = await getMarketDataFromDb(transactions, log);

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
            // Always write logs at the very end.
            await logRef.set({ entries: logs });
        }
        return null;
    });

async function getMarketDataFromDb(transactions, log) {
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    log(`Required symbols: [${symbols.join(', ')}, TWD=X]`);
    const marketData = {};
    const symbolsToFetch = [];

    const promises = symbols.map(symbol => 
        db.collection("price_history").doc(symbol).get().then(doc => {
            if (doc.exists) {
                marketData[symbol] = doc.data();
            } else {
                symbolsToFetch.push(symbol);
            }
        })
    );
    promises.push(
        db.collection("exchange_rates").doc("TWD=X").get().then(doc => {
            if (doc.exists) {
                marketData["TWD=X"] = doc.data();
            } else {
                symbolsToFetch.push("TWD=X");
            }
        })
    );

    await Promise.all(promises);
    log(`Found ${Object.keys(marketData).length} symbols in Firestore. Missing ${symbolsToFetch.length} symbols.`);

    if (symbolsToFetch.length > 0) {
        log(`Performing emergency fetch for: [${symbolsToFetch.join(', ')}]`);
        const fetchPromises = symbolsToFetch.map(symbol => fetchAndSaveMarketData(symbol, log));
        const fetchedData = await Promise.all(fetchPromises);
        
        fetchedData.forEach((data, index) => {
            if (data) {
                const symbol = symbolsToFetch[index];
                marketData[symbol] = data;
            }
        });
    }

    log(`Market data preparation complete.`);
    return marketData;
}

async function fetchAndSaveMarketData(symbol, log) {
    try {
        const isForex = symbol === "TWD=X";
        const collectionName = isForex ? "exchange_rates" : "price_history";
        const docRef = db.collection(collectionName).doc(symbol);

        log(`[Fetch] Fetching full history for ${symbol} from Yahoo Finance...`);
        
        const queryOptions = { period1: '2000-01-01' }; 
        const results = await yahooFinance.historical(symbol, queryOptions);

        log(`[Fetch] Received data for ${symbol}. Shaping payload...`);

        const prices = {};
        results.forEach(item => {
            prices[item.date.toISOString().split('T')[0]] = item.close;
        });

        const payload = {
            prices: prices,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            dataSource: 'yahoo-finance2-emergency-fetch-v3',
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
        log(`[Fetch] Successfully saved emergency data for ${symbol} to Firestore.`);
        return payload;

    } catch (error) {
        console.error(`ERROR during emergency fetch for ${symbol}:`, error);
        log(`[Fetch] ERROR for ${symbol}: ${error.message}`);
        return null;
    }
}

// --- Calculation Engine and Helpers (Unchanged from previous version) ---
function calculatePortfolioAdjustOnTheFly(transactions, marketData) { /* ... same logic ... */ }
function calculateDailyMarketValueAdjusted(portfolio, marketData, cumulativeSplitRatios, date) { /* ... same logic ... */ }
function findNearestDataPoint(history, targetDate) { /* ... same logic ... */ }
