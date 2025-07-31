/* eslint-disable */
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// --- 基礎工具函式 ---
const toDate = v => v.toDate ? v.toDate() : new Date(v);
const currencyToFx = { USD: "TWD=X", HKD: "HKD=TWD", JPY: "JPY=TWD" };

// NEW: 新增判斷是否為台股的輔助函式
function isTwStock(symbol) {
    if (!symbol) return false;
    const upperSymbol = symbol.toUpperCase();
    return upperSymbol.endsWith('.TW') || upperSymbol.endsWith('.TWO');
}

function getTotalCost(tx) {
  return (tx.totalCost !== undefined && tx.totalCost !== null)
    ? Number(tx.totalCost)
    : Number(tx.price || 0) * Number(tx.quantity || 0);
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
            const fx = 1; 
            const costPerShareTWD = getTotalCost(e) / (e.quantity || 1) * fx;

            if (e.type === 'buy') {
                state[sym].lots.push({ quantity: e.quantity, pricePerShareTWD: costPerShareTWD });
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
        futureSplits
            .filter(s => s.symbol.toUpperCase() === sym)
            .forEach(split => {
                state[sym].lots.forEach(lot => {
                    lot.quantity *= split.ratio;
                });
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
             const firstEventDate = toDate(s.lots[0]?.date || date);
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

function calculateDailyPortfolioValues(evts, market, startDate, log) {
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
        log(`TWR_CALC_FAIL: Cannot find start price for benchmark ${upperBenchmarkSymbol} on ${startDate.toISOString().split('T')[0]}.`);
        return { twrHistory: {}, benchmarkHistory: {} };
    }

    const cashflows = evts.reduce((acc, e) => {
        const dateStr = toDate(e.date).toISOString().split('T')[0];
        let flow = 0;
        const currency = e.currency || market[e.symbol.toUpperCase()]?.currency || 'USD';
        
        let fx;
        if (e.eventType === 'transaction' && e.exchangeRate && e.currency !== 'TWD') {
            fx = e.exchangeRate; // BUG FIX: Prioritize manual exchange rate for transactions
        } else {
            fx = findFxRate(market, currency, toDate(e.date));
        }

        if (e.eventType === 'transaction') {
            const cost = getTotalCost(e);
            flow = (e.type === 'buy' ? 1 : -1) * cost * (currency === 'TWD' ? 1 : fx);
        } else if (e.eventType === 'dividend') {
            const stateOnDate = getPortfolioStateOnDate(evts, toDate(e.date));
            const shares = stateOnDate[e.symbol.toUpperCase()]?.lots.reduce((sum, lot) => sum + lot.quantity, 0) || 0;
            if (shares > 0) {
                 // FEATURE: Apply dividend withholding tax
                const taxRate = isTwStock(e.symbol) ? 0.0 : 0.30;
                const postTaxAmount = e.amount * (1 - taxRate);
                flow = -1 * postTaxAmount * shares * fx;
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
    let lastMarketValue = 0;

    for (const dateStr of dates) {
        const MVE = dailyPortfolioValues[dateStr]; 
        const CF = cashflows[dateStr] || 0; 

        const denominator = lastMarketValue + CF;
        if (denominator !== 0) {
            const periodReturn = MVE / denominator;
            cumulativeHpr *= periodReturn;
        }

        twrHistory[dateStr] = (cumulativeHpr - 1) * 100;
        lastMarketValue = MVE;
        
        const currentBenchPrice = findNearest(benchmarkPrices, new Date(dateStr));
        if (currentBenchPrice) {
            benchmarkHistory[dateStr] = ((currentBenchPrice / benchmarkStartPrice) - 1) * 100;
        }
    }
    
    return { twrHistory, benchmarkHistory };
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
        // BUG FIX: Prioritize manual exchange rate
        let fx;
        if (t.exchangeRate && t.currency !== 'TWD') {
            fx = t.exchangeRate;
        } else {
            fx = findFxRate(market, t.currency, toDate(t.date));
        }
        const amt = getTotalCost(t) * (t.currency === "TWD" ? 1 : fx);
        flows.push({ date: toDate(t.date), amount: t.type === "buy" ? -amt : amt });
    });
    evts.filter(e => e.eventType === "dividend").forEach(d => {
        const stateOnDate = getPortfolioStateOnDate(evts, toDate(d.date));
        const sym = d.symbol.toUpperCase();
        const currency = stateOnDate[sym]?.currency || 'USD';
        const shares = stateOnDate[sym]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
        if (shares > 0) {
            const fx = findFxRate(market, currency, toDate(d.date));
            // FEATURE: Apply dividend withholding tax
            const taxRate = isTwStock(sym) ? 0.0 : 0.30;
            const postTaxAmount = d.amount * (1 - taxRate);
            const amt = postTaxAmount * shares * (currency === "TWD" ? 1 : fx);
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
    const dates = flows.map(f => f.date);
    const epoch = dates[0].getTime();
    const years = dates.map(d => (d.getTime() - epoch) / (365.25 * 24 * 60 * 60 * 1000));
    
    let guess = 0.1;
    let npv; 

    for (let i = 0; i < 50; i++) {
        npv = amounts.reduce((sum, amount, j) => sum + amount / Math.pow(1 + guess, years[j]), 0);
        
        if (Math.abs(npv) < 1e-6) return guess;
        const derivative = amounts.reduce((sum, amount, j) => sum - years[j] * amount / Math.pow(1 + guess, years[j] + 1), 0);
        if (Math.abs(derivative) < 1e-9) break;
        guess -= npv / derivative;
    }
    
    return (npv && Math.abs(npv) < 1e-6) ? guess : null;
}

function calculateCoreMetrics(evts, market, log) {
    const pf = {};
    let totalRealizedPL = 0;
    for (const e of evts) {
        const sym = e.symbol.toUpperCase();
        if (!pf[sym]) pf[sym] = { lots: [], currency: e.currency || "USD", realizedPLTWD: 0, realizedCostTWD: 0 };
        switch (e.eventType) {
            case "transaction": {
                // BUG FIX: Prioritize manual exchange rate
                let fx;
                if (e.exchangeRate && e.currency !== 'TWD') {
                    fx = e.exchangeRate;
                } else {
                    fx = findFxRate(market, e.currency, toDate(e.date));
                }
                const costTWD = getTotalCost(e) * (e.currency === "TWD" ? 1 : fx);

                if (e.type === "buy") {
                    pf[sym].lots.push({ quantity: e.quantity, pricePerShareOriginal: e.price, pricePerShareTWD: costTWD / e.quantity, date: toDate(e.date) });
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
                    const fx = findFxRate(market, pf[sym].currency, toDate(e.date));
                    // FEATURE: Apply dividend withholding tax
                    const taxRate = isTwStock(sym) ? 0.0 : 0.30;
                    const postTaxAmount = e.amount * (1 - taxRate);
                    const divTWD = postTaxAmount * shares * (pf[sym].currency === "TWD" ? 1 : fx);
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

// --- 資料庫與網路請求 ---
async function fetchAndSaveMarketData(symbol, log) {
  try {
    log(`Fetching full history for ${symbol} from Yahoo Finance...`);
    const hist = await yahooFinance.historical(symbol, { period1: '2000-01-01', interval: '1d' });
    const prices = hist.reduce((acc, cur) => {
        if (cur.close) acc[cur.date.toISOString().split("T")[0]] = cur.close;
        return acc;
    }, {});
    const dividends = hist.reduce((acc, cur) => {
        if(cur.dividends && cur.dividends > 0) acc[cur.date.toISOString().split("T")[0]] = cur.dividends;
        return acc;
    }, {});
    
    const payload = { prices, splits: {}, dividends, lastUpdated: admin.firestore.FieldValue.serverTimestamp(), dataSource: "yfinance-on-demand-v5" };
    if (symbol.includes("=")) {
      payload.rates = payload.prices;
      delete payload.dividends;
    }

    const col = symbol.includes("=") ? "exchange_rates" : "price_history";
    await db.collection(col).doc(symbol).set(payload);
    log(`Successfully fetched and wrote full history for ${symbol}.`);
    return payload;

  } catch (e) {
    log(`ERROR: fetchAndSaveMarketData for ${symbol} failed. Reason: ${e.message}`);
    return null;
  }
}

async function getMarketDataFromDb(txs, benchmarkSymbol, log) {
  const syms = [...new Set(txs.map(t => t.symbol.toUpperCase()))];
  const currencies = [...new Set(txs.map(t => t.currency || "USD"))].filter(c => c !== "TWD");
  const fxSyms = currencies.map(c => currencyToFx[c]).filter(Boolean);
  const allRequiredSymbols = [...new Set([...syms, ...fxSyms, benchmarkSymbol.toUpperCase()])];

  log(`Data check for symbols: ${allRequiredSymbols.join(', ')}`);
  const marketData = {};
  
  for (const s of allRequiredSymbols) {
    if (!s) continue;
    const col = s.includes("=") ? "exchange_rates" : "price_history";
    const ref = db.collection(col).doc(s);
    const doc = await ref.get();
    
    if (doc.exists) {
      marketData[s] = doc.data();
    } else {
      log(`Data for ${s} not found in Firestore. Fetching now...`);
      const fetchedData = await fetchAndSaveMarketData(s, log);
      if (fetchedData) {
        marketData[s] = fetchedData;
      } else {
        throw new Error(`Failed to fetch critical market data for ${s}. Aborting calculation.`);
      }
    }
  }
  log("All required market data is present and loaded.");
  return marketData;
}


// --- 主計算流程 (單一、強固的觸發點) ---
async function performRecalculation(uid) {
    const logRef = db.doc(`users/${uid}/user_data/calculation_logs`);
    const logs = [];
    const log = msg => {
        const ts = new Date().toISOString();
        logs.push(`${ts}: ${msg}`);
        console.log(`[${uid}] ${ts}: ${msg}`);
    };

    try {
        log("--- Recalculation Process Start (Robust v6 - Tax/FX Fix) ---");

        const holdingsRef = db.doc(`users/${uid}/user_data/current_holdings`);
        const histRef = db.doc(`users/${uid}/user_data/portfolio_history`);
        const holdingsSnap = await holdingsRef.get();
        const benchmarkSymbol = holdingsSnap.data()?.benchmarkSymbol || 'SPY';

        const [txSnap, splitSnap] = await Promise.all([
            db.collection(`users/${uid}/transactions`).get(),
            db.collection(`users/${uid}/splits`).get()
        ]);
        const txs = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
        const splits = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

        if (txs.length === 0) {
            log("No transactions found. Clearing user data.");
            await holdingsRef.set({
                holdings: {}, totalRealizedPL: 0, xirr: null, overallReturnRate: 0,
                benchmarkSymbol: benchmarkSymbol,
                lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            });
            await histRef.set({ history: {}, twrHistory: {}, benchmarkHistory: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() });
            return;
        }

        const market = await getMarketDataFromDb(txs, benchmarkSymbol, log);
        
        const { evts, firstBuyDate } = prepareEvents(txs, splits, market);

        if (!firstBuyDate) {
            log("No buy transactions found, calculation not needed.");
            return;
        }

        const portfolioResult = calculateCoreMetrics(evts, market, log);
        const dailyPortfolioValues = calculateDailyPortfolioValues(evts, market, firstBuyDate, log);
        const { twrHistory, benchmarkHistory } = calculateTwrHistory(dailyPortfolioValues, evts, market, benchmarkSymbol, firstBuyDate, log);
        
        const dataToWrite = { ...portfolioResult, benchmarkSymbol, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
        const historyData = { history: dailyPortfolioValues, twrHistory, benchmarkHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
        
        await holdingsRef.set(dataToWrite, { merge: true });
        await histRef.set(historyData);
        
        log("--- Recalculation Process Done ---");
    } catch (e) {
        console.error(`[${uid}] CRITICAL ERROR during calculation:`, e);
        logs.push(`CRITICAL: ${e.message}\n${e.stack}`);
    } finally {
        await logRef.set({ entries: logs });
        const holdingsRef = db.doc(`users/${uid}/user_data/current_holdings`);
        await holdingsRef.update({ force_recalc_timestamp: admin.firestore.FieldValue.delete() }).catch(err => log(`Could not delete timestamp: ${err.message}`));
    }
}


// --- Triggers ---
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds: 300, memory: "1GB" })
  .firestore.document("users/{uid}/user_data/current_holdings")
  .onWrite(async (chg, ctx) => {
    const beforeData = chg.before.data();
    const afterData = chg.after.data();
    if (!afterData) return null;
    
    if (afterData.force_recalc_timestamp && afterData.force_recalc_timestamp !== beforeData?.force_recalc_timestamp) {
      await performRecalculation(ctx.params.uid);
    }
    
    return null;
  });
  
exports.onBenchmarkUpdate = functions.firestore
    .document('users/{uid}/controls/benchmark_control')
    .onWrite(async (chg, ctx) => {
        if (!chg.after.exists) return null;
        const data = chg.after.data();
        const uid = ctx.params.uid;
        const holdingsRef = db.doc(`users/${uid}/user_data/current_holdings`);
        console.log(`[${uid}] User requested benchmark update to ${data.symbol}.`);
        
        await getMarketDataFromDb([], data.symbol, (msg) => console.log(`[Benchmark Pre-fetch] ${msg}`));
        
        console.log(`[${uid}] Pre-fetch for ${data.symbol} complete. Now triggering recalculation.`);
        await holdingsRef.set({
            benchmarkSymbol: data.symbol,
            force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        
        return chg.after.ref.delete();
    });

const triggerRecalculation = (ctx) => {
    return db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
}

exports.recalculateOnTransaction = functions.firestore
  .document("users/{uid}/transactions/{txId}")
  .onWrite((_, ctx) => triggerRecalculation(ctx));

exports.recalculateOnSplit = functions.firestore
  .document("users/{uid}/splits/{splitId}")
  .onWrite((_, ctx) => triggerRecalculation(ctx));

exports.recalculateOnPriceUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore.document("price_history/{symbol}")
  .onWrite(async (chg, ctx) => {
    const s = ctx.params.symbol.toUpperCase();
    const query1 = db.collectionGroup("transactions").where("symbol", "==", s);
    const query2 = db.collectionGroup("current_holdings").where("benchmarkSymbol", "==", s);
    
    const [txSnap, benchmarkUsersSnap] = await Promise.all([query1.get(), query2.get()]);
    
    const usersFromTx = txSnap.docs.map(d => d.ref.path.split("/")[1]);
    const usersFromBenchmark = benchmarkUsersSnap.docs.map(d => d.ref.path.split("/")[1]);
    
    const users = new Set([...usersFromTx, ...usersFromBenchmark].filter(Boolean));
    if (users.size === 0) return null;

    console.log(`Price update for ${s}. Triggering recalc for ${users.size} users.`);
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

    console.log(`FX rate for ${ctx.params.fxSym} updated. Triggering recalc for all ${users.size} active users.`);
    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all([...users].map(uid => db.doc(`users/${uid}/user_data/current_holdings`).set({ force_recalc_timestamp: ts }, { merge: true })));
    return null;
  });
