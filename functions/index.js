/* eslint-disable */
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// --- 基礎工具函式 ---
const toDate = v => v.toDate ? v.toDate() : new Date(v);
const currencyToFx = { USD: "TWD=X", HKD: "HKD=TWD", JPY: "JPY=TWD" };

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

    for (const e of pastEvents) {
        const sym = e.symbol.toUpperCase();
        if (!state[sym]) state[sym] = { lots: [], currency: e.currency || "USD" };
        
        if (e.eventType === 'transaction') {
            state[sym].currency = e.currency;
            const costPerShareTWD = getTotalCost(e) / (e.quantity || 1); // Cost in original currency, FX applied later

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
    return state;
}

// **[優化]** 此函式現在只專注於計算「單一資產在某一天的價值」，不再處理遞迴
function getAssetValueOnDate(symbol, assetState, market, date) {
    const qty = assetState.lots.reduce((sum, lot) => sum + lot.quantity, 0);
    if (qty < 1e-9) return 0; // 如果沒有股數，價值為 0

    const price = findNearest(market[symbol]?.prices, date);
    // 如果找不到價格 (例如：股票已下市或數據中斷)，則當日價值視為 0
    if (price === undefined) {
        return undefined; 
    }

    const fx = findFxRate(market, assetState.currency, date);
    return qty * price * (assetState.currency === "TWD" ? 1 : fx);
}

// **[優化]** 重構此函式，使其邏輯更清晰，並能妥善處理價格空缺日
function calculateDailyPortfolioValues(evts, market, startDate, log) {
    if (!startDate) return {};

    const history = {};
    const lastKnownAssetValues = {}; // 用來儲存每個資產最後一次計算出的市值
    let curDate = new Date(startDate);
    curDate.setUTCHours(0, 0, 0, 0);
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);

    while (curDate <= today) {
        const dateStr = curDate.toISOString().split("T")[0];
        const stateOnDate = getPortfolioStateOnDate(evts, curDate);
        let totalPortfolioValue = 0;

        for (const symbol in stateOnDate) {
            const assetState = stateOnDate[symbol];
            const assetValue = getAssetValueOnDate(symbol, assetState, market, curDate);

            if (assetValue !== undefined) {
                // 如果今天能成功計算價值，則更新總價值和最後已知價值
                totalPortfolioValue += assetValue;
                lastKnownAssetValues[symbol] = assetValue;
            } else {
                // 如果今天算不出價值 (e.g., 價格缺失)，則沿用該資產的「最後已知價值」
                totalPortfolioValue += (lastKnownAssetValues[symbol] || 0);
            }
        }

        history[dateStr] = totalPortfolioValue;
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
            fx = e.exchangeRate;
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
        if (denominator !== 0 && MVE !== 0) { // 避免除以零
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
  const holdingsToUpdate = {};
  const holdingsToDelete = [];
  const today = new Date();

  for (const sym in pf) {
    const h = pf[sym];
    const qty = h.lots.reduce((s, l) => s + l.quantity, 0);
    
    if (qty > 1e-9) {
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

        holdingsToUpdate[sym] = {
          symbol: sym, quantity: qty, currency: h.currency,
          avgCostOriginal: totCostOrg > 0 ? totCostOrg / qty : 0, totalCostTWD: totCostTWD,
          currentPriceOriginal: curPrice ?? null, marketValueTWD: mktVal,
          unrealizedPLTWD: unreal, returnRate: rrCurrent
        };
    } else {
        holdingsToDelete.push(sym);
    }
  }
  return { holdingsToUpdate, holdingsToDelete };
}

function createCashflowsForXirr(evts, holdings, market) {
    const flows = [];
    evts.filter(e => e.eventType === "transaction").forEach(t => {
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
    const { holdingsToUpdate, holdingsToDelete } = calculateFinalHoldings(pf, market);
    const xirrFlows = createCashflowsForXirr(evts, holdingsToUpdate, market);
    const xirr = calculateXIRR(xirrFlows);
    const totalUnrealizedPL = Object.values(holdingsToUpdate).reduce((sum, h) => sum + h.unrealizedPLTWD, 0);
    const totalInvestedCost = Object.values(holdingsToUpdate).reduce((sum, h) => sum + h.totalCostTWD, 0) + Object.values(pf).reduce((sum, p) => sum + p.realizedCostTWD, 0);
    const totalReturnValue = totalRealizedPL + totalUnrealizedPL;
    const overallReturnRate = totalInvestedCost > 0 ? (totalReturnValue / totalInvestedCost) * 100 : 0;
    return { holdings: { holdingsToUpdate, holdingsToDelete }, totalRealizedPL, xirr, overallReturnRate };
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
    
    const payload = { prices, splits: {}, dividends, lastUpdated: admin.firestore.FieldValue.serverTimestamp(), dataSource: "yfinance-on-demand-v8" };
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
        log("--- Recalculation Process Start (Robust v8 - Refactored Daily Value) ---");

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
            const updatePayload = {
                holdings: {}, 
                totalRealizedPL: 0, 
                xirr: null, 
                overallReturnRate: 0,
            };
            if(holdingsSnap.exists && holdingsSnap.data().holdings){
                for(const symbol in holdingsSnap.data().holdings) {
                    updatePayload[`holdings.${symbol}`] = admin.firestore.FieldValue.delete();
                }
            }
            await holdingsRef.set(updatePayload, { merge: true });
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
        
        const { holdingsToUpdate, holdingsToDelete } = portfolioResult.holdings;
        
        // 使用 set 和 merge:true 來建立或更新文件，同時能夠刪除欄位
        const finalHoldingsPayload = {
            ...holdingsToUpdate
        };
        holdingsToDelete.forEach(sym => {
            finalHoldingsPayload[sym] = admin.firestore.FieldValue.delete();
        });

        await holdingsRef.set({
            holdings: finalHoldingsPayload,
            totalRealizedPL: portfolioResult.totalRealizedPL,
            xirr: portfolioResult.xirr,
            overallReturnRate: portfolioResult.overallReturnRate,
            benchmarkSymbol: benchmarkSymbol,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });

        const historyData = { history: dailyPortfolioValues, twrHistory, benchmarkHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() };
        await histRef.set(historyData);
        
        log("--- Recalculation Process Done ---");
    } catch (e) {
        console.error(`[${uid}] CRITICAL ERROR during calculation:`, e);
        logs.push(`CRITICAL: ${e.message}\n${e.stack}`);
    } finally {
        await logRef.set({ entries: logs });
        // 在 finally 區塊中安全地刪除觸發戳記
        const holdingsRef = db.doc(`users/${uid}/user_data/current_holdings`);
        await holdingsRef.set({ force_recalc_timestamp: admin.firestore.FieldValue.delete() }, { merge: true })
            .catch(err => log(`Could not delete timestamp: ${err.message}`));
    }
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
