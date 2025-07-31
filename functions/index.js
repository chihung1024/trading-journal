/* eslint-disable */
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// --- 基礎工具函式 ---
const toDate = v => v.toDate ? v.toDate() : new Date(v);
const currencyToFx = { USD: "TWD=X", HKD: "HKD=TWD", JPY: "JPY=TWD" };

function getTotalCost(tx) {
  return (tx.totalCost !== undefined && tx.totalCost !== null) ? Number(tx.totalCost) : Number(tx.price || 0) * Number(tx.quantity || 0);
}

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
    const sortedDates = Object.keys(hist).sort((a, b) => new Date(b) - new Date(a));
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
    const pastEvents = allEvts.filter(e => toDate(e.date) <= toDate(targetDate));
    const futureSplits = allEvts.filter(e => e.eventType === 'split' && toDate(e.date) > toDate(targetDate));

    for (const e of pastEvents) {
        const sym = e.symbol.toUpperCase();
        if (!state[sym]) state[sym] = { lots: [], currency: e.currency || "USD" };
        if (e.eventType === 'transaction') {
            state[sym].currency = e.currency;
            if (e.type === 'buy') {
                state[sym].lots.push({ date: toDate(e.date), quantity: e.quantity, pricePerShareTWD: getTotalCost(e) / (e.quantity || 1) });
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
    for (const sym in state) {
        futureSplits.filter(s => s.symbol.toUpperCase() === sym).forEach(split => {
            state[sym].lots.forEach(lot => { lot.quantity *= split.ratio; });
        });
    }
    return state;
}

function dailyValue(state, market, date) {
    return Object.keys(state).reduce((totalValue, sym) => {
        const s = state[sym];
        const qty = s.lots.reduce((sum, lot) => sum + lot.quantity, 0);
        if (qty < 1e-9) return totalValue;
        const price = findNearest(market[sym]?.prices, date);
        if (price === undefined) {
             const yesterday = new Date(date);
             yesterday.setDate(yesterday.getDate() - 1);
             const firstEventDate = s.lots.length > 0 ? toDate(s.lots[0].date) : date;
             if (yesterday < firstEventDate) return totalValue;
             return totalValue + dailyValue({[sym]: s}, market, yesterday);
        }
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

function calculateDailyPortfolioValues(evts, market, startDate) {
    if (!startDate) return {};
    let curDate = new Date(startDate);
    curDate.setUTCHours(0, 0, 0, 0);
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);
    const history = {};
    while (curDate <= today) {
        const dateStr = curDate.toISOString().split("T")[0];
        const stateOnDate = getPortfolioStateOnDate(evts, curDate);
        history[dateStr] = dailyValue(stateOnDate, market, curDate);
        curDate.setDate(curDate.getDate() + 1);
    }
    return history;
}

function calculateTwrHistory(dailyPortfolioValues, evts, market, benchmarkSymbol, startDate, log) {
    const dates = Object.keys(dailyPortfolioValues).sort();
    if (!startDate || dates.length === 0) return { twrHistory: {}, benchmarkHistory: {} };
    const upperBenchmarkSymbol = benchmarkSymbol.toUpperCase();
    const benchmarkPrices = market[upperBenchmarkSymbol]?.prices || {};
    const benchmarkStartPrice = findNearest(benchmarkPrices, startDate);
    if (!benchmarkStartPrice) {
        log(`TWR_CALC_FAIL: Cannot find start price for benchmark ${upperBenchmarkSymbol}.`);
        return { twrHistory: {}, benchmarkHistory: {} };
    }
    const cashflows = evts.reduce((acc, e) => {
        const dateStr = toDate(e.date).toISOString().split('T')[0];
        let flow = 0;
        const currency = e.currency || market[e.symbol.toUpperCase()]?.currency || 'USD';
        const fx = findFxRate(market, currency, toDate(e.date));
        if (e.eventType === 'transaction') {
            const cost = getTotalCost(e);
            flow = (e.type === 'buy' ? 1 : -1) * cost * (currency === 'TWD' ? 1 : fx);
        } else if (e.eventType === 'dividend') {
            const stateOnDate = getPortfolioStateOnDate(evts, toDate(e.date));
            const shares = stateOnDate[e.symbol.toUpperCase()]?.lots.reduce((sum, lot) => sum + lot.quantity, 0) || 0;
            if (shares > 0) {
                flow = -1 * e.amount * shares * fx;
            }
        }
        if (flow !== 0) acc[dateStr] = (acc[dateStr] || 0) + flow;
        return acc;
    }, {});
    let cumulativeHpr = 1, lastMarketValue = 0;
    const twrHistory = {}, benchmarkHistory = {};
    for (const dateStr of dates) {
        const MVE = dailyPortfolioValues[dateStr]; 
        const CF = cashflows[dateStr] || 0; 
        const denominator = lastMarketValue + CF;
        if (denominator !== 0) cumulativeHpr *= MVE / denominator;
        twrHistory[dateStr] = (cumulativeHpr - 1) * 100;
        lastMarketValue = MVE;
        const currentBenchPrice = findNearest(benchmarkPrices, new Date(dateStr));
        if (currentBenchPrice) benchmarkHistory[dateStr] = ((currentBenchPrice / benchmarkStartPrice) - 1) * 100;
    }
    return { twrHistory, benchmarkHistory };
}

function calculateFinalHoldings(pf, market) {
    const out = {}, today = new Date();
    for (const sym in pf) {
        const h = pf[sym], qty = h.lots.reduce((s, l) => s + l.quantity, 0);
        if (qty < 1e-9) continue;
        const totCostTWD = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareTWD, 0);
        const totCostOrg = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareOriginal, 0);
        const priceHist = market[sym]?.prices || {};
        const curPrice = findNearest(priceHist, today);
        const fx = findFxRate(market, h.currency, today);
        const mktVal = qty * (curPrice ?? 0) * (h.currency === "TWD" ? 1 : fx);
        const unreal = mktVal - totCostTWD;
        const invested = totCostTWD + h.realizedCostTWD;
        const totalRet = unreal + h.realizedPLTWD;
        const rrCurrent = totCostTWD > 0 ? (unreal / totCostTWD) * 100 : 0;
        const rrTotal = invested > 0 ? (totalRet / invested) * 100 : 0;
        out[sym] = {
            symbol: sym, quantity: qty, currency: h.currency,
            avgCostOriginal: totCostOrg > 0 ? totCostOrg / qty : 0, totalCostTWD: totCostTWD, investedCostTWD: invested,
            currentPriceOriginal: curPrice ?? null, marketValueTWD: mktVal,
            unrealizedPLTWD: unreal, realizedPLTWD: h.realizedPLTWD,
            returnRate: rrCurrent, returnRateTotal: rrTotal,
        };
    }
    return out;
}

function createCashflowsForXirr(evts, holdings, market) {
    const flows = [];
    evts.filter(e => e.eventType === "transaction").forEach(t => {
        const fx = findFxRate(market, t.currency, toDate(t.date));
        const amt = getTotalCost(t) * (t.currency === "TWD" ? 1 : fx);
        flows.push({ date: toDate(t.date), amount: t.type === "buy" ? -amt : amt });
    });
    evts.filter(e => e.eventType === "dividend").forEach(d => {
        const stateOnDate = getPortfolioStateOnDate(evts, toDate(d.date));
        const currency = stateOnDate[d.symbol.toUpperCase()]?.currency || 'USD';
        const shares = stateOnDate[d.symbol.toUpperCase()]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
        if (shares > 0) {
            const fx = findFxRate(market, currency, toDate(d.date));
            const amt = d.amount * shares * (currency === "TWD" ? 1 : fx);
            flows.push({ date: toDate(d.date), amount: amt });
        }
    });
    const totalMarketValue = Object.values(holdings).reduce((s, h) => s + h.marketValueTWD, 0);
    if (totalMarketValue > 0) flows.push({ date: new Date(), amount: totalMarketValue });
    const combined = flows.reduce((acc, flow) => {
        const dateStr = flow.date.toISOString().slice(0, 10);
        acc[dateStr] = (acc[dateStr] || 0) + flow.amount;
        return acc;
    }, {});
    return Object.entries(combined).filter(([,amount]) => Math.abs(amount) > 1e-6)
        .map(([date, amount]) => ({ date: new Date(date), amount })).sort((a, b) => a.date - b.date);
}

function calculateXIRR(flows) {
    if (flows.length < 2) return null;
    const amounts = flows.map(f => f.amount);
    if (!amounts.some(v => v < 0) || !amounts.some(v => v > 0)) return null;
    const dates = flows.map(f => f.date);
    const epoch = dates[0].getTime();
    const years = dates.map(d => (d.getTime() - epoch) / (365.25 * 24 * 60 * 60 * 1000));
    let guess = 0.1;
    for (let i = 0; i < 50; i++) {
        const npv = amounts.reduce((sum, amount, j) => sum + amount / Math.pow(1 + guess, years[j]), 0);
        if (Math.abs(npv) < 1e-6) return guess;
        const derivative = amounts.reduce((sum, amount, j) => sum - years[j] * amount / Math.pow(1 + guess, years[j] + 1), 0);
        if (Math.abs(derivative) < 1e-9) break;
        guess -= npv / derivative;
    }
    return npv < 1e-6 ? guess : null;
}

function calculateCoreMetrics(evts, market) {
    const pf = {};
    let totalRealizedPL = 0;
    for (const e of evts) {
        const sym = e.symbol.toUpperCase();
        if (!pf[sym]) pf[sym] = { lots: [], currency: e.currency || "USD", realizedPLTWD: 0, realizedCostTWD: 0 };
        switch (e.eventType) {
            case "transaction": {
                const fx = findFxRate(market, e.currency, toDate(e.date));
                const costTWD = getTotalCost(e) * (e.currency === "TWD" ? 1 : fx);
                if (e.type === "buy") {
                    pf[sym].lots.push({ date: toDate(e.date), quantity: e.quantity, pricePerShareOriginal: e.price, pricePerShareTWD: costTWD / e.quantity });
                } else {
                    let sellQty = e.quantity;
                    const saleProceedsTWD = costTWD;
                    let costOfGoodsSoldTWD = 0;
                    while (sellQty > 0 && pf[sym].lots.length > 0) {
                        const lot = pf[sym].lots[0];
                        const qtyToSell = Math.min(sellQty, lot.quantity);
                        costOfGoodsSoldTWD += qtyToSell * lot.pricePerShareTWD;
                        lot.quantity -= qtyToSell;
                        sellQty -= qtyToSell;
                        if (lot.quantity < 1e-9) pf[sym].lots.shift();
                    }
                    const realized = saleProceedsTWD - costOfGoodsSoldTWD;
                    totalRealizedPL += realized;
                    pf[sym].realizedCostTWD += costOfGoodsSoldTWD;
                    pf[sym].realizedPLTWD += realized;
                }
                break;
            }
            case "split":
                pf[sym].lots.forEach(l => {
                    l.quantity *= e.ratio; l.pricePerShareTWD /= e.ratio; l.pricePerShareOriginal /= e.ratio;
                });
                break;
            case "dividend": {
                const stateOnDate = getPortfolioStateOnDate(evts, toDate(e.date));
                const shares = stateOnDate[sym]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
                if (shares > 0) {
                    const fx = findFxRate(market, pf[sym].currency, toDate(e.date));
                    const divTWD = e.amount * shares * (pf[sym].currency === "TWD" ? 1 : fx);
                    totalRealizedPL += divTWD;
                    pf[sym].realizedPLTWD += divTWD;
                }
                break;
            }
        }
    }
    const holdings = calculateFinalHoldings(pf, market);
    const xirrFlows = createCashflowsForXirr(evts, holdings, market);
    const xirr = calculateXIRR(xirrFlows);
    const totalUnrealizedPL = Object.values(holdings).reduce((sum, h) => sum + h.unrealizedPLTWD, 0);
    const totalInvestedCost = Object.values(holdings).reduce((sum, h) => sum + h.totalCostTWD, 0) + Object.values(pf).reduce((sum, p) => sum + p.realizedCostTWD, 0);
    const totalReturnValue = totalRealizedPL + totalUnrealizedPL;
    const overallReturnRate = totalInvestedCost > 0 ? (totalReturnValue / totalInvestedCost) * 100 : 0;
    return { holdings, totalRealizedPL, xirr, overallReturnRate };
}

// --- Market Data ---
async function fetchAndSaveMarketData(symbol, startDate, log) {
    try {
        const queryDate = new Date(startDate);
        queryDate.setDate(queryDate.getDate() - 35);
        log(`Fetching FULL history for ${symbol} from ${queryDate.toISOString().split('T')[0]}`);
        const hist = await yahooFinance.historical(symbol, { period1: queryDate, interval: '1d' });
        const prices = hist.reduce((acc, cur) => { acc[cur.date.toISOString().split("T")[0]] = cur.close; return acc; }, {});
        const dividends = hist.reduce((acc, cur) => { if(cur.dividends && cur.dividends > 0) acc[cur.date.toISOString().split("T")[0]] = cur.dividends; return acc; }, {});
        const payload = { prices, dividends, splits: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp(), dataSource: "on-demand-fetch-v4" };
        if (symbol.includes("=")) { payload.rates = payload.prices; delete payload.dividends; }
        return payload;
    } catch (e) {
        log(`fetchAndSaveMarketData for ${symbol} ERROR: ${e.message}`);
        return null;
    }
}

// --- Main Flow & Triggers ---
async function performRecalculation(uid, log) {
    const holdingsRef = db.doc(`users/${uid}/user_data/current_holdings`);
    const histRef = db.doc(`users/${uid}/user_data/portfolio_history`);
    const holdingsSnap = await holdingsRef.get();
    if (!holdingsSnap.exists) { log("No holdings doc, aborting."); return; }
    const benchmarkSymbol = holdingsSnap.data()?.benchmarkSymbol || 'SPY';
    log(`Using benchmark: ${benchmarkSymbol}`);

    const [txSnap, splitSnap] = await Promise.all([
        db.collection(`users/${uid}/transactions`).get(),
        db.collection(`users/${uid}/splits`).get()
    ]);
    const txs = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
    const splits = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    if (txs.length === 0) {
        await holdingsRef.set({ holdings: {}, totalRealizedPL: 0, xirr: null, overallReturnRate: 0, benchmarkSymbol, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
        await histRef.set({ history: {}, twrHistory: {}, benchmarkHistory: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() });
        log("No transactions, cleared user data.");
        return;
    }

    const { evts, firstBuyDate } = prepareEvents(txs, splits, {});
    if (!firstBuyDate) {
        log("No buy transactions found.");
        return;
    }
    const market = await getMarketDataFromDb(txs, benchmarkSymbol, log);
    const refreshedEvents = prepareEvents(txs, splits, market).evts;
    
    const portfolioResult = calculateCoreMetrics(refreshedEvents, market);
    const dailyPortfolioValues = calculateDailyPortfolioValues(refreshedEvents, market, firstBuyDate);
    const { twrHistory, benchmarkHistory } = calculateTwrHistory(dailyPortfolioValues, refreshedEvents, market, benchmarkSymbol, firstBuyDate, log);
    
    const dataToWrite = { ...portfolioResult, benchmarkSymbol, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
    const historyData = { history: dailyPortfolioValues, twrHistory, benchmarkHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
    
    await holdingsRef.set(dataToWrite);
    await histRef.set(historyData);
    log("Recalculation done.");
}

exports.dataOrchestrator = functions.firestore
    .document('users/{uid}/transactions/{txId}')
    .onWrite(async (chg, ctx) => {
        const uid = ctx.params.uid;
        const logRef = db.doc(`users/${uid}/user_data/calculation_logs`);
        const log = (msg) => logRef.set({ entries: admin.firestore.FieldValue.arrayUnion(`${new Date().toISOString()}: ${msg}`) }, { merge: true });
        
        log(`Orchestrator triggered by transaction write.`);
        const txsSnap = await db.collection(`users/${uid}/transactions`).get();
        if (txsSnap.empty) {
            await db.doc(`users/${uid}/user_data/current_holdings`).set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
            return;
        }

        const txs = txsSnap.docs.map(d => d.data());
        const holdingsSnap = await db.doc(`users/${uid}/user_data/current_holdings`).get();
        const benchmarkSymbol = holdingsSnap.data()?.benchmarkSymbol || 'SPY';
        const globalStartDate = toDate(txs.reduce((earliest, tx) => toDate(tx.date) < earliest ? toDate(tx.date) : earliest, new Date()));
        
        const symbolsInTxs = [...new Set(txs.map(t => t.symbol.toUpperCase()))];
        const symbolsToProcess = [...new Set([...symbolsInTxs, benchmarkSymbol, "TWD=X"])];
        let wasDataFetched = false;

        for (const symbol of symbolsToProcess) {
            const isForex = symbol.includes("=");
            const collection = isForex ? "exchange_rates" : "price_history";
            const priceDocRef = db.doc(`${collection}/${symbol}`);
            const priceDoc = await priceDocRef.get();
            const prices = priceDoc.data()?.prices || priceDoc.data()?.rates || {};
            const priceDates = Object.keys(prices);
            
            const startDateForSymbol = symbol === benchmarkSymbol || isForex 
                ? globalStartDate
                : toDate(txs.filter(t => t.symbol.toUpperCase() === symbol).reduce((e, t) => toDate(t.date) < e ? toDate(t.date) : e, new Date()));

            const requiredStartDate = new Date(startDateForSymbol);
            requiredStartDate.setDate(requiredStartDate.getDate() - 30);

            if (priceDates.length === 0 || new Date(priceDates.sort()[0]) > requiredStartDate) {
                log(`Backfill needed for ${symbol}. Required since ${requiredStartDate.toISOString().split('T')[0]}`);
                const fullHistory = await fetchAndSaveMarketData(symbol, requiredStartDate, log);
                if (fullHistory) {
                    await priceDocRef.set(fullHistory);
                    wasDataFetched = true;
                }
            }
        }
        
        if (!wasDataFetched) {
            log("No backfill needed, triggering recalculation directly.");
            await db.doc(`users/${uid}/user_data/current_holdings`).set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
        } else {
            // Recalculation will be triggered by recalculateOnPriceHistoryWrite
            log("Backfill completed. Recalculation will be triggered by price_history update.");
        }
    });

exports.recalculateOnPriceHistoryWrite = functions.firestore
    .document('price_history/{symbol}')
    .onWrite(async (chg, ctx) => {
        const symbol = ctx.params.symbol.toUpperCase();
        const holdingsGroup = db.collectionGroup("current_holdings");
        const query1 = holdingsGroup.where(`holdings.${symbol}.symbol`, "==", symbol).get();
        const query2 = holdingsGroup.where("benchmarkSymbol", "==", symbol).get();
        const [holdersSnap, benchmarkUsersSnap] = await Promise.all([query1, query2]);
        const uids = new Set([...holdersSnap.docs.map(d => d.ref.path.split('/')[1]), ...benchmarkUsersSnap.docs.map(d => d.ref.path.split('/')[1])]);
        if (uids.size > 0) {
            console.log(`Price update for ${symbol} affects users: ${[...uids].join(', ')}`);
            const ts = admin.firestore.FieldValue.serverTimestamp();
            await Promise.all([...uids].map(uid => db.doc(`users/${uid}/user_data/current_holdings`).set({ force_recalc_timestamp: ts }, { merge: true })));
        }
    });
    
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds: 300, memory: "2GB" })
  .firestore.document("users/{uid}/user_data/current_holdings")
  .onWrite(async (chg, ctx) => {
    const uid = ctx.params.uid;
    const beforeData = chg.before.data();
    const afterData = chg.after.data();
    if (!afterData) return null;
    if (beforeData?.force_recalc_timestamp?.isEqual(afterData.force_recalc_timestamp)) return null;
    if (!afterData.force_recalc_timestamp) return null; // Only run if timestamp is present

    const logRef = db.doc(`users/${uid}/user_data/calculation_logs`);
    const log = (msg) => logRef.set({ entries: admin.firestore.FieldValue.arrayUnion(`${new Date().toISOString()}: ${msg}`) }, { merge: true });
    
    await performRecalculation(uid, log);
    await chg.after.ref.update({ force_recalc_timestamp: admin.firestore.FieldValue.delete() });
  });

exports.onBenchmarkUpdate = functions.firestore
    .document('users/{uid}/controls/benchmark_control')
    .onWrite(async (chg, ctx) => {
        if (!chg.after.exists) return null;
        const data = chg.after.data();
        const uid = ctx.params.uid;
        await db.doc(`users/${uid}/user_data/current_holdings`).set({
            benchmarkSymbol: data.symbol,
            force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        return chg.after.ref.delete();
    });

exports.recalculateOnSplit = functions.firestore
  .document("users/{uid}/splits/{splitId}")
  .onWrite((_, ctx) => db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }));
