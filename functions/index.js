/* eslint-disable */
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// --- 基礎工具函式 ---
function getTotalCost(tx) {
  return (tx.totalCost !== undefined && tx.totalCost !== null)
    ? Number(tx.totalCost)
    : Number(tx.price || 0) * Number(tx.quantity || 0);
}

const currencyToFx = { USD: "TWD=X", HKD: "HKD=TWD", JPY: "JPY=TWD" };
const toDate = v => v.toDate ? v.toDate() : new Date(v);

function findNearest(hist, date, toleranceDays = 7) {
    if (!hist || Object.keys(hist).length === 0) return undefined;
    const tgt = date instanceof Date ? date : new Date(date);
    const tgtStr = tgt.toISOString().slice(0, 10);
    if (hist[tgtStr]) return hist[tgtStr];

    for (let i = 1; i <= toleranceDays; i++) {
        const checkDate = new Date(tgt);
        checkDate.setDate(checkDate.getDate() - i);
        const checkDateStr = checkDate.toISOString().split('T')[0];
        if (hist[checkDateStr]) return hist[checkDateStr];
    }

    const sortedDates = Object.keys(hist).sort((a,b) => new Date(b) - new Date(a));
    for (const dateStr of sortedDates) {
        if (dateStr <= tgtStr) return hist[dateStr];
    }
    return undefined;
}

function findFxRate(market, currency, date, tolerance = 15) {
  if (!currency || currency === "TWD") return 1;
  const fxSym = currencyToFx[currency];
  if (!fxSym) return 1;
  const hist = market[fxSym]?.rates || {};
  return findNearest(hist, date, tolerance) ?? 1;
}

// --- 核心計算函式 ---
function getPortfolioStateOnDate(allEvts, targetDate) {
    const state = {};
    const pastEvents = allEvts.filter(e => toDate(e.date) <= targetDate);

    for (const e of pastEvents) {
        const sym = e.symbol.toUpperCase();
        if (!state[sym]) state[sym] = { lots: [], currency: e.currency || "USD" };
        
        if (e.eventType === 'transaction') {
            state[sym].currency = e.currency;
            if (e.type === 'buy') {
                state[sym].lots.push({ quantity: e.quantity, pricePerShareTWD: e.pricePerShareTWD });
            } else {
                let sellQty = e.quantity;
                while (sellQty > 0 && state[sym].lots.length > 0) {
                    const lot = state[sym].lots[0];
                    if (lot.quantity <= sellQty) {
                        sellQty -= lot.quantity;
                        state[sym].lots.shift();
                    } else {
                        lot.quantity -= sellQty;
                        sellQty = 0;
                    }
                }
            }
        } else if (e.eventType === 'split') {
            state[sym].lots.forEach(lot => {
                lot.quantity *= e.ratio;
                lot.pricePerShareTWD /= e.ratio;
            });
        }
    }
    return state;
}

function dailyValue(state, market, date) {
    return Object.keys(state).reduce((totalValue, sym) => {
        const s = state[sym];
        const qty = s.lots.reduce((sum, lot) => sum + lot.quantity, 0);
        if (qty === 0) return totalValue;

        const price = findNearest(market[sym]?.prices, date);
        if (price === undefined) return totalValue;

        const fx = findFxRate(market, s.currency, date);
        return totalValue + (qty * price * (s.currency === "TWD" ? 1 : fx));
    }, 0);
}

function prepareEvents(txs, splits, market) {
    const firstBuyDateMap = {};
    txs.forEach(tx => {
        if (tx.type === "buy") {
            const sym = tx.symbol.toUpperCase();
            const d = toDate(tx.date);
            if (!firstBuyDateMap[sym] || d < firstBuyDateMap[sym]) firstBuyDateMap[sym] = d;
        }
    });

    const evts = [
        ...txs.map(t => ({ ...t, date: toDate(t.date), eventType: "transaction" })),
        ...splits.map(s => ({ ...s, date: toDate(s.date), eventType: "split" }))
    ];

    Object.keys(market).forEach(sym => {
        if (market[sym] && market[sym].dividends) {
            Object.entries(market[sym].dividends).forEach(([dateStr, amount]) => {
                const dividendDate = new Date(dateStr);
                if (firstBuyDateMap[sym] && dividendDate >= firstBuyDateMap[sym] && amount > 0) {
                    evts.push({ date: dividendDate, symbol: sym, amount, eventType: "dividend" });
                }
            });
        }
    });

    evts.sort((a, b) => toDate(a.date) - toDate(b.date));
    const firstTx = evts.find(e => e.eventType === 'transaction');
    return { evts, firstBuyDate: firstTx ? toDate(firstTx.date) : null, firstBuyDateMap };
}

function calculateTwrHistory(dailyPortfolioValues, evts, market, benchmarkSymbol, startDate, log) {
    const dates = Object.keys(dailyPortfolioValues).sort();
    if (!startDate || dates.length === 0) return { twrHistory: {}, benchmarkHistory: {} };

    const upperBenchmarkSymbol = benchmarkSymbol.toUpperCase();
    const benchmarkPrices = market[upperBenchmarkSymbol]?.prices || {};
    const benchmarkStartPrice = findNearest(benchmarkPrices, startDate);

    if (!benchmarkStartPrice) {
        log(`TWR_CALC_FAIL: Cannot find start price for benchmark ${upperBenchmarkSymbol} on ${startDate.toISOString().split('T')[0]}.`);
        return { twrHistory: {}, benchmarkHistory: {} };
    }

    const cashflows = evts.reduce((acc, e) => {
        const dateStr = toDate(e.date).toISOString().split('T')[0];
        let flow = 0;
        if (e.eventType === 'transaction') {
            const fx = findFxRate(market, e.currency, e.date);
            const cost = getTotalCost(e);
            flow = (e.type === 'buy' ? 1 : -1) * cost * fx;
        } else if (e.eventType === 'dividend') {
            const stateOnDate = getPortfolioStateOnDate(evts, e.date);
            const shares = stateOnDate[e.symbol.toUpperCase()]?.lots.reduce((sum, lot) => sum + lot.quantity, 0) || 0;
            if (shares > 0) {
                const fx = findFxRate(market, stateOnDate[e.symbol.toUpperCase()].currency, e.date);
                flow = -1 * e.amount * shares * fx;
            }
        }
        if (flow !== 0) {
            acc[dateStr] = (acc[dateStr] || 0) + flow;
        }
        return acc;
    }, {});
    
    const twrHistory = {};
    const benchmarkHistory = {};
    let cumulativeReturn = 1;

    for (let i = 0; i < dates.length; i++) {
        const dateStr = dates[i];
        const endValue = dailyPortfolioValues[dateStr];
        const prevDateStr = i > 0 ? dates[i - 1] : null;
        const startValue = prevDateStr ? dailyPortfolioValues[prevDateStr] : 0;
        const netCashFlow = prevDateStr ? (cashflows[dateStr] || 0) : Object.keys(cashflows).filter(d => d <= dateStr).reduce((sum, d) => sum + cashflows[d], 0);

        const denominator = startValue + netCashFlow;
        if (denominator !== 0) {
            const periodReturn = endValue / denominator;
            cumulativeReturn *= periodReturn;
        }
        twrHistory[dateStr] = (cumulativeReturn - 1);
        
        const currentBenchPrice = findNearest(benchmarkPrices, new Date(dateStr));
        if (currentBenchPrice) {
            benchmarkHistory[dateStr] = (currentBenchPrice / benchmarkStartPrice) - 1;
        }
    }
    
    // Convert to percentage at the end
    Object.keys(twrHistory).forEach(d => twrHistory[d] *= 100);
    Object.keys(benchmarkHistory).forEach(d => benchmarkHistory[d] *= 100);
    
    return { twrHistory, benchmarkHistory };
}

// Other calculation functions...
function calculateCoreMetrics(evts, market) {
    // ...
    return { holdings: {}, totalRealizedPL: 0, xirr: null, overallReturnRate: 0 };
}


// --- 主流程 & Triggers ---

async function performRecalculation(uid) {
    const logRef = db.doc(`users/${uid}/user_data/calculation_logs`);
    const logs = [];
    const log = msg => {
        const ts = new Date().toISOString();
        logs.push(`${ts}: ${msg}`);
        console.log(`[${uid}] ${ts}: ${msg}`);
    };

    try {
        log("--- Recalc start (TWR & Benchmark Final Version) ---");

        const holdingsRef = db.doc(`users/${uid}/user_data/current_holdings`);
        const histRef = db.doc(`users/${uid}/user_data/portfolio_history`);
        const holdingsSnap = await holdingsRef.get();
        const benchmarkSymbol = holdingsSnap.data()?.benchmarkSymbol || 'SPY';
        log(`Using benchmark: ${benchmarkSymbol}`);

        const [txSnap, splitSnap] = await Promise.all([
            db.collection(`users/${uid}/transactions`).get(),
            db.collection(`users/${uid}/splits`).get()
        ]);
        const txs = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
        const splits = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

        if (txs.length === 0) {
            await holdingsRef.set({
                holdings: {}, totalRealizedPL: 0, xirr: null, overallReturnRate: 0,
                benchmarkSymbol: benchmarkSymbol,
                lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            });
            await holdingsRef.update({ force_recalc_timestamp: admin.firestore.FieldValue.delete() });
            await histRef.set({ history: {}, twrHistory: {}, benchmarkHistory: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() });
            log("no tx, cleared");
            return;
        }

        const market = await getMarketDataFromDb(txs, benchmarkSymbol, log);
        const { evts, firstBuyDate } = prepareEvents(txs, splits, market);

        if (!firstBuyDate) {
            log("No transactions found, cannot start calculation.");
            return;
        }

        const portfolioResult = calculateCoreMetrics(evts, market); // Simplified call
        const dailyPortfolioValues = calculateDailyPortfolioValues(evts, market, firstBuyDate, log);
        const { twrHistory, benchmarkHistory } = calculateTwrHistory(dailyPortfolioValues, evts, market, benchmarkSymbol, firstBuyDate, log);
        
        const dataToWrite = { ...portfolioResult, benchmarkSymbol, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
        const historyData = { history: dailyPortfolioValues, twrHistory, benchmarkHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
        
        await holdingsRef.set(dataToWrite);
        await holdingsRef.update({ force_recalc_timestamp: admin.firestore.FieldValue.delete() });
        await histRef.set(historyData);
        
        log("--- Recalc done ---");
    } catch (e) {
        console.error(`[${uid}] An error occurred during calculation or write:`, e);
        logs.push(`CRITICAL: ${e.message}\n${e.stack}`);
    } finally {
        await logRef.set({ entries: logs });
    }
}

// ... Triggers ...
// (Triggers remain the same as the very last complete version I provided)
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds: 300, memory: "1GB" })
  .firestore.document("users/{uid}/user_data/current_holdings")
  .onWrite((chg, ctx) => {
    // ... logic
  });
// ... and so on for all 5 triggers
