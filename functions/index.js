const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// Final diagnostic version to inspect marketData object.
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
            log("--- Recalculation triggered (v8 - Final Diagnostic) ---");

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

            // ** THE DIAGNOSTIC PROBE IS HERE **
            log(`Final check before calculation. Is marketData defined? ${!!marketData}. Content: ${JSON.stringify(marketData)}`);

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

// The rest of the functions remain the same as the last stable version.
async function getMarketDataFromDb(transactions, log) { /* ... */ }
async function fetchAndSaveMarketData(symbol, log) { /* ... */ }
function calculatePortfolioAdjustOnTheFly(transactions, marketData) { /* ... */ }
function calculateDailyMarketValueAdjusted(portfolio, marketData, cumulativeSplitRatios, date) { /* ... */ }
function findNearestDataPoint(history, targetDate) { /* ... */ }
