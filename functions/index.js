/* eslint-disable */
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

/* ---------- utils ---------- */
function getTotalCost(tx) {
  return (tx.totalCost !== undefined && tx.totalCost !== null)
    ? Number(tx.totalCost)
    : Number(tx.price || 0) * Number(tx.quantity || 0);
}

const currencyToFx = {
  USD: "TWD=X",
  HKD: "HKD=TWD",
  JPY: "JPY=TWD"
};

const toDate = v => v.toDate ? v.toDate() : new Date(v);

function findNearest(hist, date, toleranceDays = 7) {
    if (!hist || Object.keys(hist).length === 0) return undefined;
    const tgt = date instanceof Date ? date : new Date(date);
    const tgtStr = tgt.toISOString().slice(0, 10);
  
    // 優先找當天
    if (hist[tgtStr]) return hist[tgtStr];
  
    // 往回找 toleranceDays
    for (let i = 1; i <= toleranceDays; i++) {
        const checkDate = new Date(tgt);
        checkDate.setDate(checkDate.getDate() - i);
        const checkDateStr = checkDate.toISOString().split('T')[0];
        if (hist[checkDateStr]) {
            return hist[checkDateStr];
        }
    }
  
    // 若都找不到，找小於等於目標日的最近一個日期
    const sortedDates = Object.keys(hist).sort();
    let closestDate = null;
    for (const dateStr of sortedDates) {
        if (dateStr <= tgtStr) {
            closestDate = dateStr;
        } else {
            break;
        }
    }
    return closestDate ? hist[closestDate] : undefined;
}

function findFxRate(market, currency, date, tolerance = 15) {
  if (!currency || currency === "TWD") return 1;
  const fxSym = currencyToFx[currency];
  if (!fxSym) return 1;
  const hist = market[fxSym]?.rates || {};
  const rate = findNearest(hist, date, tolerance);
  return rate ?? 1;
}

/* ================================================================
 * 核心計算函式
 * ================================================================ */

function getPortfolioStateOnDate(allEvts, target) {
  const st = {};
  const past = allEvts.filter(e => toDate(e.date) <= target);
  
  for (const e of past) {
    if (!e.symbol) continue;
    const sym = e.symbol.toUpperCase();
    if (!st[sym]) st[sym] = { lots: [], currency: "USD" };
    switch (e.eventType) {
      case "transaction":
        st[sym].currency = e.currency;
        if (e.type === "buy") {
          st[sym].lots.push({ quantity: e.quantity, pricePerShareTWD: e.pricePerShareTWD });
        } else {
          let q = e.quantity;
          while (q > 0 && st[sym].lots.length) {
            const lot = st[sym].lots[0];
            if (lot.quantity <= q) { q -= lot.quantity; st[sym].lots.shift(); }
            else { lot.quantity -= q; q = 0; }
          }
        }
        break;
      case "split":
        st[sym].lots.forEach(l => {
          l.quantity *= e.ratio;
          l.pricePerShareTWD /= e.ratio;
        });
        break;
    }
  }
  return st;
}

function dailyValue(state, market, date) {
  let tot = 0;
  for (const sym in state) {
    const s = state[sym];
    const qty = s.lots.reduce((sum, l) => sum + l.quantity, 0);
    if (qty === 0) continue;
    const price = findNearest((market[sym]?.prices) || {}, date);
    if (price === undefined) continue;
    const fx = findFxRate(market, s.currency, date);
    tot += qty * price * (s.currency === "TWD" ? 1 : fx);
  }
  return tot;
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
    if (!startDate || Object.keys(dailyPortfolioValues).length === 0) {
        return { twrHistory: {}, benchmarkHistory: {} };
    }

    const upperBenchmarkSymbol = benchmarkSymbol.toUpperCase();
    const benchmarkPrices = market[upperBenchmarkSymbol]?.prices || {};
    const startPrice = findNearest(benchmarkPrices, startDate);

    if (!startPrice) {
        log(`Warning: Cannot find start price for benchmark ${upperBenchmarkSymbol} on ${startDate.toISOString().split('T')[0]}. TWR chart will be incomplete.`);
        return { twrHistory: {}, benchmarkHistory: {} };
    }

    const cashflows = evts.filter(e => e.eventType === 'transaction' || e.eventType === 'dividend')
        .map(e => {
            const date = toDate(e.date);
            const dateStr = date.toISOString().split('T')[0];
            let amount = 0;
            const fx = findFxRate(market, e.currency, date);

            if (e.eventType === 'transaction') {
                const cost = getTotalCost(e);
                amount = (e.type === 'buy' ? cost : -cost) * (e.currency === 'TWD' ? 1 : fx);
            } else {
                const state = getPortfolioStateOnDate(evts, date);
                const shares = state[e.symbol.toUpperCase()]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
                amount = -1 * e.amount * shares * (e.currency === 'TWD' ? 1 : fx);
            }
            return { dateStr, amount };
        }).reduce((acc, val) => {
            acc[val.dateStr] = (acc[val.dateStr] || 0) + val.amount;
            return acc;
        }, {});

    const dates = Object.keys(dailyPortfolioValues).sort();
    const twrHistory = {};
    const benchmarkHistory = {};
    let hpr = 1;

    for (let i = 0; i < dates.length; i++) {
        const dateStr = dates[i];
        const MVE = dailyPortfolioValues[dateStr];
        const cashflowOnDate = cashflows[dateStr] || 0;
        const MVB = i > 0 ? dailyPortfolioValues[dates[i - 1]] : (MVE - cashflowOnDate);
        
        if ((MVB + (i === 0 ? 0 : cashflowOnDate)) !== 0) {
            hpr *= (MVE / (MVB + (i === 0 ? 0 : cashflowOnDate)));
        }

        twrHistory[dateStr] = (hpr - 1) * 100;

        const currentBenchPrice = findNearest(benchmarkPrices, new Date(dateStr));
        if (currentBenchPrice) {
            benchmarkHistory[dateStr] = ((currentBenchPrice / startPrice) - 1) * 100;
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
    const priceHist = (market[sym]?.prices) || {};
    const curPrice = findNearest(priceHist, today);
    const fx = findFxRate(market, h.currency, today);
    const mktVal = qty * (curPrice ?? 0) * (h.currency === "TWD" ? 1 : fx);
    const unreal = mktVal - totCostTWD;
    const invested = totCostTWD + (h.realizedCostTWD || 0);
    const totalRet = unreal + (h.realizedPLTWD || 0);
    const rrCurrent = totCostTWD > 0 ? (unreal / totCostTWD) * 100 : 0;
    const rrTotal = invested > 0 ? (totalRet / invested) * 100 : 0;
    out[sym] = {
      symbol: sym, quantity: qty, currency: h.currency,
      avgCostOriginal: totCostOrg / qty, totalCostTWD: totCostTWD, investedCostTWD: invested,
      currentPriceOriginal: curPrice ?? null, marketValueTWD: mktVal,
      unrealizedPLTWD: unreal, realizedPLTWD: h.realizedPLTWD || 0,
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
        const fx = findFxRate(market, d.currency, d.date);
        const state = getPortfolioStateOnDate(evts, d.date);
        const shares = state[d.symbol.toUpperCase()]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
        if (shares === 0) return;
        const amt = d.amount * shares * (d.currency === "TWD" ? 1 : fx);
        if (amt > 0) flows.push({ date: toDate(d.date), amount: amt });
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
        .map(([date, amount]) => ({ date: new Date(date), amount }))
        .sort((a, b) => a.date - b.date);
}

function calculateXIRR(flows) {
    if (flows.length < 2) return null;
    const amounts = flows.map(f => f.amount);
    if (!amounts.some(v => v < 0) || !amounts.some(v => v > 0)) return null;
    const dates = flows.map(f => f.date);
    const years = dates.map(d => (d - dates[0]) / (365.25 * 24 * 60 * 60 * 1000));
    const npv = r => years.reduce((sum, year, i) => sum + amounts[i] / Math.pow(1 + r, year), 0);
    let r = 0.1;
    for (let i = 0; i < 100; i++) {
        const f = npv(r);
        if (Math.abs(f) < 1e-6) return r;
        const df = years.reduce((sum, year, j) => sum - year * amounts[j] / Math.pow(1 + r, year + 1), 0);
        if (df === 0) break;
        r -= f / df;
    }
    return null;
}

function calculatePortfolio(evts, market, log) {
  const pf = {};
  let totalRealizedPL = 0;
  for (const e of evts) {
    if (!e.symbol) { log(`Warning: missing symbol in event: ${JSON.stringify(e)}`); continue; }
    const sym = e.symbol.toUpperCase();
    if (!pf[sym]) pf[sym] = { lots: [], currency: "USD", realizedPLTWD: 0, realizedCostTWD: 0 };
    switch (e.eventType) {
      case "transaction": {
        const t = e;
        pf[sym].currency = t.currency;
        const fx = findFxRate(market, t.currency, t.date);
        const cost = getTotalCost(t);
        const costTWD = cost * (t.currency === "TWD" ? 1 : fx);
        if (t.type === "buy") {
          pf[sym].lots.push({
            quantity: t.quantity,
            pricePerShareOriginal: t.price,
            pricePerShareTWD: costTWD / t.quantity
          });
        } else {
          let q = t.quantity;
          const saleTWD = costTWD;
          let costSold = 0;
          while (q > 0 && pf[sym].lots.length) {
            const lot = pf[sym].lots[0];
            const sellQty = Math.min(q, lot.quantity);
            costSold += sellQty * lot.pricePerShareTWD;
            lot.quantity -= sellQty;
            q -= sellQty;
            if (lot.quantity < 1e-9) {
              pf[sym].lots.shift();
            }
          }
          const realized = saleTWD - costSold;
          totalRealizedPL += realized;
          pf[sym].realizedCostTWD += costSold;
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
        const shares = pf[sym].lots.reduce((s, l) => s + l.quantity, 0);
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
  const xirr = calculateXIRR(createCashflowsForXirr(evts, holdings, market));
  
  const totalInvestedCost = Object.values(pf).reduce((sum, p) => sum + p.lots.reduce((s,l) => s + l.quantity * l.pricePerShareTWD, 0) + p.realizedCostTWD, 0);
  const totalReturnValue = totalRealizedPL + Object.values(holdings).reduce((sum, h) => sum + h.unrealizedPLTWD, 0);
  const overallReturnRateTotal = totalInvestedCost > 0 ? (totalReturnValue / totalInvestedCost) * 100 : 0;

  return { holdings, totalRealizedPL, xirr, overallReturnRateTotal, overallReturnRate: overallReturnRateTotal };
}

/* ================================================================
 * 主流程
 * ================================================================ */

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
        holdings: {}, totalRealizedPL: 0, xirr: null, overallReturnRateTotal: 0,
        overallReturnRate: 0, benchmarkSymbol: benchmarkSymbol,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      });
      await holdingsRef.update({ force_recalc_timestamp: admin.firestore.FieldValue.delete() });
      await histRef.set({ history: {}, twrHistory: {}, benchmarkHistory: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() });
      log("no tx, cleared");
      return;
    }

    const market = await getMarketDataFromDb(txs, benchmarkSymbol, log);
    const { evts, firstBuyDate } = prepareEvents(txs, splits, market);
    
    if(!firstBuyDate){
      log("No buy transactions found, cannot start calculation.");
      return;
    }

    const portfolioResult = calculatePortfolio(evts, market, log);
    const dailyPortfolioValues = calculateDailyPortfolioValues(evts, market, firstBuyDate, log);
    const { twrHistory, benchmarkHistory } = calculateTwrHistory(dailyPortfolioValues, evts, market, benchmarkSymbol, firstBuyDate, log);

    const dataToWrite = {
      ...portfolioResult,
      benchmarkSymbol: benchmarkSymbol,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
    };

    const historyData = {
      history: dailyPortfolioValues,
      twrHistory: twrHistory,
      benchmarkHistory: benchmarkHistory,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp()
    };
    
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

/* ================================================================
 * Market-data helpers
 * ================================================================ */
async function fetchAndSaveMarketData(symbol, log) {
  try {
    const queryOptions = { period1: '2000-01-01', interval: '1d' };
    const data = await yahooFinance.historical(symbol, queryOptions);
    const prices = data.reduce((acc, cur) => {
        acc[cur.date.toISOString().split("T")[0]] = cur.close;
        return acc;
    }, {});
    const dividends = (await yahooFinance.getQuote(symbol))?.dividends || {};

    const payload = {
      prices,
      splits: {}, // We are using user-defined splits
      dividends,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dataSource: "yfinance-v2-fetch"
    };

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

/* ================================================================
 * Triggers
 * ================================================================ */
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds: 300, memory: "1GB" })
  .firestore.document("users/{uid}/user_data/current_holdings")
  .onWrite((chg, ctx) => {
    const beforeData = chg.before.data();
    const afterData = chg.after.data();

    if (beforeData && !afterData) {
      console.log(`[${ctx.params.uid}] current_holdings document was deleted. Halting execution.`);
      return null;
    }

    if (!beforeData || (afterData.force_recalc_timestamp && afterData.force_recalc_timestamp !== beforeData?.force_recalc_timestamp)) {
      console.log(`[${ctx.params.uid}] Trigger condition met. Starting recalculation.`);
      return performRecalculation(ctx.params.uid);
    }
    
    console.log(`[${ctx.params.uid}] Write event did not meet trigger conditions. No action.`);
    return null;
  });

exports.recalculateOnTransaction = functions.firestore
  .document("users/{uid}/transactions/{txId}")
  .onWrite((_, ctx) =>
    db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
  );

exports.recalculateOnSplit = functions.firestore
  .document("users/{uid}/splits/{splitId}")
  .onWrite((_, ctx) =>
    db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
  );

exports.recalculateOnPriceUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore.document("price_history/{symbol}")
  .onWrite(async (chg, ctx) => {
    const s = ctx.params.symbol.toUpperCase();
    const before = chg.before.exists ? chg.before.data() : null;
    const after = chg.after.data();
    if (
      before &&
      JSON.stringify(before.prices) === JSON.stringify(after.prices) &&
      JSON.stringify(before.dividends) === JSON.stringify(after.dividends)
    ) return null;

    const query1 = db.collectionGroup("transactions").where("symbol", "==", s).get();
    const query2 = db.collection("users").where("benchmarkSymbol", "==", s).get();
    
    const [txSnap, benchmarkUsersSnap] = await Promise.all([query1, query2]);

    const usersFromTx = txSnap.docs.map(d => d.ref.path.split("/")[1]);
    const usersFromBenchmark = benchmarkUsersSnap.docs.map(d => d.id);
    
    const users = new Set([...usersFromTx, ...usersFromBenchmark].filter(Boolean));

    if (users.size === 0) return null;

    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all([...users].map(async uid => {
      try {
        await db.doc(`users/${uid}/user_data/current_holdings`)
          .set({ force_recalc_timestamp: ts }, { merge: true });
      } catch (e) {
        console.error(`Error updating current_holdings for user ${uid}:`, e);
      }
    }));
    return null;
  });

exports.recalculateOnFxUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore.document("exchange_rates/{fxSym}")
  .onWrite(async (chg, ctx) => {
    const b = chg.before.exists ? chg.before.data() : null;
    const a = chg.after.data();
    if (b && JSON.stringify(b.rates) === JSON.stringify(a.rates)) return null;

    console.log(`FX rate for ${ctx.params.fxSym} updated. Finding active users to trigger recalculation.`);

    const txSnap = await db.collectionGroup("transactions").get();
    if (txSnap.empty) {
      console.log("No transactions found in any user account. Halting execution.");
      return null;
    }

    const users = new Set(txSnap.docs.map(d => d.ref.path.split("/")[1]).filter(Boolean));
    if (users.size === 0) return null;

    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all([...users].map(async uid => {
      try {
        await db.doc(`users/${uid}/user_data/current_holdings`).set({ force_recalc_timestamp: ts }, { merge: true });
      } catch (e) {
        console.error(`Error updating current_holdings for user ${uid}:`, e);
      }
    }));
    return null;
  });
