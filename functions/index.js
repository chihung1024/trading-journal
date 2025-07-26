const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// Final version with the definitive async/await fix in data fetching.
exports.recalculateHoldings = functions.runWith({ timeoutSeconds: 540, memory: '1GB' }).firestore
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
            log("--- Recalculation triggered (v10 - Final Async Fix) ---");

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

            log(`Final check before calculation. Is marketData defined? ${!!marketData}.`);
            if (!marketData) throw new Error("getMarketDataFromDb returned undefined.");

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
            await logRef.set({ entries: logs });
        }
        return null;
    });

// **THE FINAL FIX IS HERE**
async function getMarketDataFromDb(transactions, log) {
    const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
    log(`Required symbols: [${symbols.join(', ')}, TWD=X]`);
    const marketData = {};
    const symbolsToFetch = [];

    // Step 1: Try to get existing data from Firestore.
    const readPromises = symbols.map(symbol => 
        db.collection("price_history").doc(symbol).get().then(doc => {
            if (doc.exists) {
                marketData[symbol] = doc.data();
            } else {
                symbolsToFetch.push(symbol);
            }
        })
    );
    readPromises.push(
        db.collection("exchange_rates").doc("TWD=X").get().then(doc => {
            if (doc.exists) {
                marketData["TWD=X"] = doc.data();
            } else {
                symbolsToFetch.push("TWD=X");
            }
        })
    );
    await Promise.all(readPromises);
    log(`Found ${Object.keys(marketData).length} symbols in Firestore. Missing ${symbolsToFetch.length} symbols.`);

    // Step 2: If any data is missing, perform emergency fetch.
    if (symbolsToFetch.length > 0) {
        log(`Performing emergency fetch for: [${symbolsToFetch.join(', ')}]`);
        // This will wait for all fetches to complete.
        const fetchedResults = await Promise.all(
            symbolsToFetch.map(symbol => fetchAndSaveMarketData(symbol, log))
        );
        
        // Step 3: Correctly merge the newly fetched data back into the main object.
        fetchedResults.forEach((data, index) => {
            if (data) {
                const symbol = symbolsToFetch[index];
                marketData[symbol] = data;
            }
        });
    }

    log(`Market data preparation complete. Final object has ${Object.keys(marketData).length} keys.`);
    return marketData; // Now returns the fully populated object.
}

// The rest of the functions are stable and correct.
async function fetchAndSaveMarketData(symbol, log) { /* ... */ }
function calculatePortfolioAdjustOnTheFly(transactions, marketData) { /* ... */ }
function calculateDailyMarketValueAdjusted(portfolio, marketData, cumulativeSplitRatios, date) { /* ... */ }
function findNearestDataPoint(history, targetDate) { /* ... */ }
