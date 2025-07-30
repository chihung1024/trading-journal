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
    const pastEvents = allEvts.filter(e => toDate(e.date) <= targetDate);

    for (const e of pastEvents) {
        const sym = e.symbol.toUpperCase();
        if (!state[sym]) state[sym] = { lots: [], currency: e.currency || "USD" };
        
        if (e.eventType === 'transaction') {
            state[sym].currency = e.currency;
            const fx = 1; // 這裡的 FX 僅用於 TWD 成本計算，在 dailyValue 中會用當日匯率重算市值
            const costTWD = getTotalCost(e) * fx / (e.quantity || 1);

            if (e.type === 'buy') {
                state[sym].lots.push({ quantity: e.quantity, pricePerShareTWD: costTWD });
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
        if (qty < 1e-9) return totalValue;

        const price = findNearest(market[sym]?.prices, date);
        if (price === undefined) {
             // 如果找不到當天價格，嘗試用前一天的市值來估算，避免圖表斷點
            const yesterday = new Date(date);
            yesterday.setDate(yesterday.getDate() - 1);
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
        const fx = findFxRate(market, e.currency, e.date);

        if (e.eventType === 'transaction') {
            const cost = getTotalCost(e);
            flow = (e.type === 'buy' ? 1 : -1) * cost * (e.currency === 'TWD' ? 1 : fx);
        } else if (e.eventType === 'dividend') {
            const stateOnDate = getPortfolioStateOnDate(evts, toDate(e.date));
            const shares = stateOnDate[e.symbol.toUpperCase()]?.lots.reduce((sum, lot) => sum + lot.quantity, 0) || 0;
            if (shares > 0) {
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
    let cumulativeHpr = 1;

    for (let i = 0; i < dates.length; i++) {
        const dateStr = dates[i];
        const MVE = dailyPortfolioValues[dateStr]; // 當期期末價值
        const prevDateStr = i > 0 ? dates[i - 1] : null;
        const MVB = prevDateStr ? dailyPortfolioValues[prevDateStr] : 0; // 上期期末價值
        const CF = cashflows[dateStr] || 0; // 當期現金流

        const denominator = MVB + CF;
        if (denominator !== 0) {
            const periodReturn = MVE / denominator;
            cumulativeHpr *= periodReturn;
        }

        twrHistory[dateStr] = (cumulativeHpr - 1) * 100;
        
        const currentBenchPrice = findNearest(benchmarkPrices, new Date(dateStr));
        if (currentBenchPrice) {
            benchmarkHistory[dateStr] = ((currentBenchPrice / benchmarkStartPrice) - 1) * 100;
        }
    }
    
    return { twrHistory, benchmarkHistory };
}

function calculateCoreMetrics(evts, market, log) {
    const pf = {};
    let totalRealizedPL = 0;
    const { firstBuyDateMap } = prepareEvents([], [], market);

    for (const e of evts) {
        const sym = e.symbol.toUpperCase();
        if (!pf[sym]) pf[sym] = { lots: [], currency: e.currency || "USD", realizedPLTWD: 0, realizedCostTWD: 0 };
        
        switch (e.eventType) {
            case "transaction": {
                const fx = findFxRate(market, e.currency, e.date);
                const costTWD = getTotalCost(e) * (e.currency === "TWD" ? 1 : fx);
                if (e.type === "buy") {
                    pf[sym].lots.push({ quantity: e.quantity, pricePerShareOriginal: e.price, pricePerShareTWD: costTWD / e.quantity });
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
                    l.quantity *= e.ratio;
                    l.pricePerShareTWD /= e.ratio;
                    l.pricePerShareOriginal /= e.ratio;
                });
                break;
            case "dividend": {
                const stateOnDate = getPortfolioStateOnDate(evts, toDate(e.date));
                const shares = stateOnDate[sym]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
                if (shares > 0) {
                    const fx = findFxRate(market, pf[sym].currency, e.date);
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

    const totalInvestedCost = Object.values(pf).reduce((sum, p) => sum + p.lots.reduce((s,l) => s + l.quantity * l.pricePerShareTWD, 0) + p.realizedCostTWD, 0);
    const totalReturnValue = totalRealizedPL + Object.values(holdings).reduce((sum, h) => sum + h.unrealizedPLTWD, 0);
    const overallReturnRate = totalInvestedCost > 0 ? (totalReturnValue / totalInvestedCost) * 100 : 0;

    return { holdings, totalRealizedPL, xirr, overallReturnRate };
}

function calculateFinalHoldings(pf, market) {
  const out = {};
  const today = new Date();
  for (const sym in pf) {
    const h = pf[sym];
    const qty = h.lots.reduce((s, l) => s + l.quantity, 0);
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
      returnRateCurrent: rrCurrent, returnRateTotal: rrTotal, returnRate: rrCurrent
    };
  }
  return out;
}

function createCashflowsForXirr(evts, holdings, market) {
    const flows = [];
    evts.filter(e => e.eventType === "transaction").forEach(t => {
        const fx = findFxRate(market, t.currency, t.date);
        const amt = getTotalCost(t) * (t.currency === "TWD" ? 1 : fx);
        flows.push({ date: toDate(t.date), amount: t.type === "buy" ? -amt : amt });
    });

    evts.filter(e => e.eventType === "dividend").forEach(d => {
        const stateOnDate = getPortfolioStateOnDate(evts, toDate(d.date));
        const shares = stateOnDate[d.symbol.toUpperCase()]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
        if (shares > 0) {
            const fx = findFxRate(market, stateOnDate[d.symbol.toUpperCase()].currency, d.date);
            const amt = d.amount * shares * (d.currency === "TWD" ? 1 : fx);
            flows.push({ date: toDate(d.date), amount: amt });
        }
    });

    const totalMarketValue = Object.values(holdings).reduce((s, h) => s + h.marketValueTWD, 0);
    if (totalMarketValue > 0) {
        flows.push({ date: new Date(), amount: totalMarketValue });
    }

    const combined = flows.reduce((acc, flow) => {
        const dateStr = flow.date.toISOString().slice(0, 10);
        acc[dateStr] = (acc[dateStr] || 0) + flow.amount;
        return acc;
    }, {});

    return Object.entries(combined)
        .filter(([,amount]) => Math.abs(amount) > 1e-6)
        .map(([date, amount]) => ({ date: new Date(date), amount }))
        .sort((a, b) => a.date - b.date);
}

function calculateXIRR(flows) {
    if (flows.length < 2) return null;
    const amounts = flows.map(f => f.amount);
    if (!amounts.some(v => v < 0) || !amounts.some(v => v > 0)) return null;
    
    // Newton-Raphson method
    const dates = flows.map(f => f.date);
    const epoch = dates[0].getTime();
    const years = dates.map(d => (d.getTime() - epoch) / (365.25 * 24 * 60 * 60 * 1000));
    
    let guess = 0.1;
    for (let i = 0; i < 50; i++) {
        let npv = 0;
        let derivative = 0;
        for (let j = 0; j < amounts.length; j++) {
            npv += amounts[j] / Math.pow(1 + guess, years[j]);
            derivative += -years[j] * amounts[j] / Math.pow(1 + guess, years[j] + 1);
        }
        if (Math.abs(npv) < 1e-6) return guess;
        if (derivative === 0) break;
        guess -= npv / derivative;
    }
    return null;
}


// --- Market Data Fetching ---
async function getMarketDataFromDb(txs, benchmarkSymbol, log) {
  const syms = [...new Set(txs.map(t => t.symbol.toUpperCase()))];
  const currencies = [...new Set(txs.map(t => t.currency || "USD"))].filter(c => c !== "TWD");
  const fxSyms = currencies.map(c => currencyToFx[c]).filter(Boolean);
  const all = [...new Set([...syms, ...fxSyms, benchmarkSymbol.toUpperCase()])];

  log(`Fetching market data for: ${all.join(', ')}`);
  const out = {};
  for (const s of all) {
    if (!s) continue;
    const col = s.includes("=") ? "exchange_rates" : "price_history";
    const ref = db.collection(col).doc(s);
    const doc = await ref.get();
    if (doc.exists) {
      out[s] = doc.data();
      continue;
    }
    const fetched = await fetchAndSaveMarketData(s, log);
    if (fetched) {
      out[s] = fetched;
      await ref.set(fetched);
    } else {
      log(`fetch market data failed for symbol: ${s}, set empty object`);
      out[s] = { prices: {}, dividends: {}, splits: {} };
    }
  }
  return out;
}

async function fetchAndSaveMarketData(symbol, log) {
  try {
    const hist = await yahooFinance.historical(symbol, { period1: '2000-01-01', interval: '1d' });
    const prices = hist.reduce((acc, cur) => {
        acc[cur.date.toISOString().split("T")[0]] = cur.close;
        return acc;
    }, {});
    const dividends = hist.reduce((acc, cur) => {
        if(cur.dividends && cur.dividends > 0) acc[cur.date.toISOString().split("T")[0]] = cur.dividends;
        return acc;
    }, {});
    
    const payload = { prices, splits: {}, dividends, lastUpdated: admin.firestore.FieldValue.serverTimestamp(), dataSource: "emergency-fetch-v3" };
    if (symbol.includes("=")) {
      payload.rates = payload.prices;
      delete payload.dividends;
    }
    return payload;
  } catch (e) {
    log(`fetch ${symbol} err ${e.message}`);
    return null;
  }
}

// --- Main Recalculation Flow ---
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
            log("No buy transactions found, cannot start calculation.");
            await holdingsRef.update({ force_recalc_timestamp: admin.firestore.FieldValue.delete() });
            return;
        }

        const portfolioResult = calculateCoreMetrics(evts, market, log);
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

// --- Triggers ---
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds: 300, memory: "1GB" })
  .firestore.document("users/{uid}/user_data/current_holdings")
  .onWrite((chg, ctx) => {
    const beforeData = chg.before.data();
    const afterData = chg.after.data();
    if (beforeData && !afterData) return null;
    if (!beforeData || (afterData.force_recalc_timestamp && afterData.force_recalc_timestamp !== beforeData?.force_recalc_timestamp)) {
      return performRecalculation(ctx.params.uid);
    }
    return null;
  });

exports.recalculateOnTransaction = functions.firestore
  .document("users/{uid}/transactions/{txId}")
  .onWrite((_, ctx) => db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }));

exports.recalculateOnSplit = functions.firestore
  .document("users/{uid}/splits/{splitId}")
  .onWrite((_, ctx) => db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }));

exports.recalculateOnPriceUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore.document("price_history/{symbol}")
  .onWrite(async (chg, ctx) => {
    const s = ctx.params.symbol.toUpperCase();
    const query1 = db.collectionGroup("transactions").where("symbol", "==", s).get();
    const query2 = db.collection("users").where("benchmarkSymbol", "==", s).get();
    const [txSnap, benchmarkUsersSnap] = await Promise.all([query1, query2]);
    const usersFromTx = txSnap.docs.map(d => d.ref.path.split("/")[1]);
    const usersFromBenchmark = benchmarkUsersSnap.docs.map(d => d.id);
    const users = new Set([...usersFromTx, ...usersFromBenchmark].filter(Boolean));
    if (users.size === 0) return null;
    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all([...users].map(uid => db.doc(`users/${uid}/user_data/current_holdings`).set({ force_recalc_timestamp: ts }, { merge: true })));
    return null;
  });

exports.recalculateOnFxUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore.document("exchange_rates/{fxSym}")
  .onWrite(async (chg, ctx) => {
    const txSnap = await db.collectionGroup("transactions").get();
    if (txSnap.empty) return null;
    const users = new Set(txSnap.docs.map(d => d.ref.path.split("/")[1]).filter(Boolean));
    if (users.size === 0) return null;
    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all([...users].map(uid => db.doc(`users/${uid}/user_data/current_holdings`).set({ force_recalc_timestamp: ts }, { merge: true })));
    return null;
  });
