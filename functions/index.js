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

const currencyToFx = { USD: "TWD=X", HKD: "HKD=TWD", JPY: "JPY=TWD" };

/* ================================================================
 * 核心： Portfolio Recalculation
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
    log("--- Recalc start (Split-Aware TWR + Dividend Tax Final Version) ---");

    const holdingsRef = db.doc(`users/${uid}/user_data/current_holdings`);
    const histRef = db.doc(`users/${uid}/user_data/portfolio_history`);

    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${uid}/transactions`).get(),
      db.collection(`users/${uid}/splits`).get()
    ]);
    const txs = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
    const splits = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    if (txs.length === 0) {
      await Promise.all([
        holdingsRef.set({ holdings: {}, totalRealizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
        histRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
      ]);
      log("no tx, cleared");
      return;
    }

    const market = await getMarketDataFromDb(txs, log);
    const result = calculatePortfolio(txs, splits, market, log);
    const {
      holdings,
      totalRealizedPL,
      portfolioHistory,
      xirr,
      overallReturnRateTotal,
      overallReturnRate
    } = result;

    const data = {
      holdings,
      totalRealizedPL,
      xirr,
      overallReturnRateTotal,
      overallReturnRate,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      force_recalc_timestamp: admin.firestore.FieldValue.delete()
    };
    
    await holdingsRef.set(data, { merge: true });
    await histRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() });
    
    log("--- Recalc done ---");
  } catch (e) {
    console.error(`[${uid}] An error occurred during calculation or write:`, e);
    logs.push(`CRITICAL: ${e.message}\n${e.stack}`);
  } finally {
    await logRef.set({ entries: logs });
  }
}

/* ================================================================
 * Triggers
 * ================================================================ */
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds: 300, memory: "1GB" }).firestore.document("users/{uid}/user_data/current_holdings").onWrite((chg, ctx) => { if (chg.before.exists && !chg.after.exists) { console.log(`[${ctx.params.uid}] Doc deleted. Halting.`); return null; } const beforeData = chg.before.data(); const afterData = chg.after.data(); if (!beforeData || (afterData.force_recalc_timestamp !== beforeData.force_recalc_timestamp)) { console.log(`[${ctx.params.uid}] Triggering recalc.`); return performRecalculation(ctx.params.uid); } console.log(`[${ctx.params.uid}] No action needed.`); return null; });
exports.recalculateOnTransaction = functions.firestore.document("users/{uid}/transactions/{txId}").onWrite((_, ctx) => db.doc(`users/${ctx.params.uid}/user_data/current_holdings`).set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }));
exports.recalculateOnSplit = functions.firestore.document("users/{uid}/splits/{splitId}").onWrite((_, ctx) => db.doc(`users/${ctx.params.uid}/user_data/current_holdings`).set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }));
exports.recalculateOnPriceUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" }).firestore.document("price_history/{symbol}").onWrite(async (chg, ctx) => { const s = ctx.params.symbol.toUpperCase(); const before = chg.before.exists ? chg.before.data() : null; const after = chg.after.data(); if (before && JSON.stringify(before.prices) === JSON.stringify(after.prices) && JSON.stringify(before.dividends) === JSON.stringify(after.dividends)) return null; const txSnap = await db.collectionGroup("transactions").where("symbol", "==", s).get(); if (txSnap.empty) return null; const users = new Set(txSnap.docs.map(d => d.ref.path.split("/")[1]).filter(uid => typeof uid === "string" && uid.length > 0)); const ts = admin.firestore.FieldValue.serverTimestamp(); await Promise.all([...users].map(async uid => { try { await db.doc(`users/${uid}/user_data/current_holdings`).set({ force_recalc_timestamp: ts }, { merge: true }); } catch (e) { console.error(`Error for user ${uid}:`, e); } })); return null; });
exports.recalculateOnFxUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" }).firestore.document("exchange_rates/{fxSym}").onWrite(async (chg, ctx) => { const b = chg.before.exists ? chg.before.data() : null; const a = chg.after.data(); if (b && JSON.stringify(b.rates) === JSON.stringify(a.rates)) return null; const users = await db.collection("users").listDocuments(); const ts = admin.firestore.FieldValue.serverTimestamp(); await Promise.all(users.map(u => db.doc(`users/${u.id}/user_data/current_holdings`).set({ force_recalc_timestamp: ts }, { merge: true }))); return null; });


/* ================================================================
 * Market-data helpers
 * ================================================================ */
async function getMarketDataFromDb(txs, log) { const syms = [...new Set(txs.map(t => t.symbol.toUpperCase()))]; const currencies = [...new Set(txs.map(t => t.currency || "USD"))].filter(c => c !== "TWD"); const fxSyms = currencies.map(c => currencyToFx[c]).filter(Boolean); const all = [...new Set([...syms, ...fxSyms])]; const out = {}; for (const s of all) { const col = s.includes("=") ? "exchange_rates" : "price_history"; const ref = db.collection(col).doc(s); const doc = await ref.get(); if (doc.exists) { out[s] = doc.data(); continue; } const fetched = await fetchAndSaveMarketData(s, log); if (fetched) { out[s] = fetched; await ref.set(fetched); } else { log(`fetch failed for ${s}, set empty`); out[s] = {}; } } return out; }
async function fetchAndSaveMarketData(symbol, log) { try { const hist = await yahooFinance.historical(symbol, { period1: "2000-01-01" }); const prices = {}; hist.forEach(i => prices[i.date.toISOString().split("T")[0]] = i.close); const payload = { prices, splits: {}, dividends: (hist.dividends || []).reduce((a, d) => ({ ...a, [d.date.toISOString().split("T")[0]]: d.amount }), {}), lastUpdated: admin.firestore.FieldValue.serverTimestamp(), dataSource: "emergency-fetch" }; if (symbol.includes("=")) { payload.rates = payload.prices; delete payload.dividends; } return payload; } catch (e) { log(`fetch ${symbol} err ${e.message}`); return null; } }


/* ================================================================
 * Portfolio calculation
 * ================================================================ */
function calculatePortfolio(txs, splits, market, log) {
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
  [...new Set(txs.map(t => t.symbol.toUpperCase()))].forEach(sym => {
    Object.entries(market[sym]?.dividends || {}).forEach(([d, amt]) => {
      if (firstBuyDateMap[sym] && new Date(d) >= firstBuyDateMap[sym])
        evts.push({ date: new Date(d), symbol: sym, amount: amt, eventType: "dividend" });
    });
  });
  evts.sort((a, b) => new Date(a.date) - new Date(b.date));

  const pf = {};
  let totalRealizedPL = 0;
  for (const e of evts) {
    if (!e.symbol) { log(`Warning: missing symbol in event: ${JSON.stringify(e)}`); continue; }
    const sym = e.symbol.toUpperCase();
    if (!pf[sym]) pf[sym] = { lots: [], currency: "USD" };
    switch (e.eventType) {
      case "transaction": {
        const t = e;
        pf[sym].currency = t.currency;
        const fx = findFxRate(market, t.currency, t.date);
        const cost = getTotalCost(t);
        const costTWD = cost * (t.currency === "TWD" ? 1 : fx);
        if (t.type === "buy") {
          pf[sym].lots.push({ quantity: t.quantity, pricePerShareOriginal: cost / t.quantity, pricePerShareTWD: costTWD / t.quantity });
        } else {
          let q = t.quantity;
          const saleTWD = costTWD;
          let costSold = 0;
          while (q > 0 && pf[sym].lots.length) {
            const lot = pf[sym].lots[0];
            if (lot.quantity <= q) { costSold += lot.quantity * lot.pricePerShareTWD; q -= lot.quantity; pf[sym].lots.shift(); }
            else { costSold += q * lot.pricePerShareTWD; lot.quantity -= q; q = 0; }
          }
          const realized = saleTWD - costSold;
          totalRealizedPL += realized;
          pf[sym].realizedCostTWD = (pf[sym].realizedCostTWD || 0) + costSold;
          pf[sym].realizedPLTWD = (pf[sym].realizedPLTWD || 0) + realized;
        }
        break;
      }
      case "split":
        pf[sym].lots.forEach(l => {
          l.quantity *= e.ratio; l.pricePerShareTWD /= e.ratio; l.pricePerShareOriginal /= e.ratio;
        });
        break;
      case "dividend": {
        const shares = pf[sym].lots.reduce((s, l) => s + l.quantity, 0);
        if (shares === 0) break;
        const fx = findFxRate(market, pf[sym].currency, e.date);
        
        // 1. 先計算稅前總股息
        const grossDividendTWD = e.amount * shares * (pf[sym].currency === "TWD" ? 1 : fx);
        
        let netDividendTWD = grossDividendTWD;

        // 2. [新增] 檢查是否為美股 (以幣別 USD 判斷)，若是則扣 30% 的稅
        if (pf[sym].currency === 'USD') {
            const taxRate = 0.30; // 30% 稅率
            netDividendTWD = grossDividendTWD * (1 - taxRate);
        }

        // 3. 使用稅後股息來更新已實現損益
        totalRealizedPL += netDividendTWD;
        pf[sym].realizedPLTWD = (pf[sym].realizedPLTWD || 0) + netDividendTWD;
        break;
      }
    }
  }

  const holdings = calculateFinalHoldings(pf, market);
  const history = calculatePortfolioHistory(evts, market);
  const xirr = calculateXIRR(createCashflows(evts, pf, holdings, market, firstBuyDateMap));

  let investedTotal = 0, totalReturnTWD = 0;
  for (const s in pf) {
    const h = pf[s];
    const realizedCost = h.realizedCostTWD || 0;
    const realizedPL = h.realizedPLTWD || 0;
    const unrealizedPL = holdings[s]?.unrealizedPLTWD || 0;
    const remainingCost = holdings[s]?.totalCostTWD || 0;
    investedTotal += remainingCost + realizedCost;
    totalReturnTWD += unrealizedPL + realizedPL;
  }
  const overallReturnRateTotal = investedTotal > 0 ? (totalReturnTWD / investedTotal) * 100 : 0;

  return { holdings, totalRealizedPL, portfolioHistory: history, xirr, overallReturnRateTotal, overallReturnRate: overallReturnRateTotal };
}

/* ================================================================
 * History Calculation - Split-aware
 * ================================================================ */
function calculatePortfolioHistory(evts, market) {
  const txEvts = evts.filter(e => e.eventType === "transaction");
  if (txEvts.length === 0) return {};

  const firstDate = new Date(txEvts[0].date);
  const today = new Date();
  let currentDate = new Date(firstDate);
  currentDate.setUTCHours(0, 0, 0, 0);

  const history = {};
  let previousDayValue = 0;
  let twrIndex = 1;
  const allSplits = evts.filter(e => e.eventType === 'split');

  while (currentDate <= today) {
    const dateKey = currentDate.toISOString().split("T")[0];

    const cashFlowToday = txEvts
      .filter(t => toDate(t.date).toISOString().split("T")[0] === dateKey)
      .reduce((sum, t) => {
        const fx = findFxRate(market, t.currency, t.date);
        const amountTWD = getTotalCost(t) * (t.currency === "TWD" ? 1 : fx);
        return sum + (t.type === "buy" ? amountTWD : -amountTWD);
      }, 0);

    const portfolioState = getPortfolioStateOnDate(evts, currentDate);
    const endOfDayValue = dailyValue(portfolioState, market, currentDate, allSplits);

    if (previousDayValue > 0) {
      const denominator = previousDayValue + cashFlowToday;
      if (denominator > 0) {
        const dailyReturnFactor = endOfDayValue / denominator;
        twrIndex = twrIndex * dailyReturnFactor;
      }
    }
    
    history[dateKey] = {
      marketValue: endOfDayValue,
      twr: twrIndex
    };
    
    previousDayValue = endOfDayValue;
    currentDate.setDate(currentDate.getDate() + 1);
  }
  return history;
}

function dailyValue(state, market, date, allSplits) {
  let totalValue = 0;
  for (const sym in state) {
    const s = state[sym];
    const qty = s.lots.reduce((sum, l) => sum + l.quantity, 0);
    if (qty === 0) continue;

    let price = findNearest((market[sym]?.prices) || {}, date);
    if (price === undefined) continue;

    const futureSplits = allSplits.filter(sp => sp.symbol.toUpperCase() === sym && new Date(sp.date) > date);
    for (const split of futureSplits) {
      price *= split.ratio;
    }

    const fx = findFxRate(market, s.currency, date);
    totalValue += qty * price * (s.currency === "TWD" ? 1 : fx);
  }
  return totalValue;
}


/* ================================================================
 * Helpers
 * ================================================================ */
function calculateFinalHoldings(pf, market) { const out = {}, today = new Date(); for (const sym in pf) { const h = pf[sym], qty = h.lots.reduce((s, l) => s + l.quantity, 0); if (qty < 1e-9) continue; const totCostTWD = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareTWD, 0); const totCostOrg = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareOriginal, 0); const priceHist = (market[sym]?.prices) || {}; const curPrice = findNearest(priceHist, today); const fx = findFxRate(market, h.currency, today); const mktVal = qty * (curPrice ?? 0) * (h.currency === "TWD" ? 1 : fx); const unreal = mktVal - totCostTWD; const invested = totCostTWD + (h.realizedCostTWD || 0); const totalRet = unreal + (h.realizedPLTWD || 0); const rrCurrent = totCostTWD > 0 ? (unreal / totCostTWD) * 100 : 0; const rrTotal = invested > 0 ? (totalRet / invested) * 100 : 0; out[sym] = { symbol: sym, quantity: qty, currency: h.currency, avgCostOriginal: totCostOrg / qty, totalCostTWD: totCostTWD, investedCostTWD: invested, currentPriceOriginal: curPrice ?? null, marketValueTWD: mktVal, unrealizedPLTWD: unreal, realizedPLTWD: h.realizedPLTWD || 0, returnRateCurrent: rrCurrent, returnRateTotal: rrTotal, returnRate: rrCurrent }; } return out; }
function createCashflows(evts, pf, holdings, market, firstBuyDateMap) { const flows = []; evts.filter(e => e.eventType === "transaction").forEach(t => { const fx = findFxRate(market, t.currency, t.date); const amt = getTotalCost(t) * (t.currency === "TWD" ? 1 : fx); flows.push({ date: toDate(t.date), amount: t.type === "buy" ? -amt : amt }); }); evts.filter(e => e.eventType === "dividend").forEach(d => { const sym = d.symbol.toUpperCase(); if (!firstBuyDateMap[sym] || new Date(d.date) < firstBuyDateMap[sym]) return; const fx = findFxRate(market, pf[sym]?.currency || "USD", d.date); const shares = pf[sym]?.lots.reduce((s, l) => s + l.quantity, 0) || 0; if (shares === 0) return; const amt = d.amount * shares * (pf[sym]?.currency === "TWD" ? 1 : fx); if (amt > 0) flows.push({ date: new Date(d.date), amount: amt }); }); const mktVal = Object.values(holdings).reduce((s, h) => s + h.marketValueTWD, 0); if (mktVal > 0) { flows.push({ date: new Date(), amount: mktVal }); } const combineByDate = {}; for (const f of flows) { const k = (f.date instanceof Date ? f.date.toISOString().slice(0, 10) : String(f.date).slice(0, 10)); combineByDate[k] = (combineByDate[k] || 0) + f.amount; } const mergedFlows = Object.entries(combineByDate).filter(([_, amt]) => Math.abs(amt) > 1e-6).map(([date, amt]) => ({ date: new Date(date), amount: amt })).sort((a, b) => a.date - b.date); return mergedFlows; }
function calculateXIRR(flows) { if (!Array.isArray(flows) || flows.length < 2) return null; const amounts = flows.map(f => f.amount); if (!amounts.some(v => v < 0) || !amounts.some(v => v > 0)) return null; const dates = flows.map(f => f.date); const years = dates.map(d => (d - dates[0]) / (365 * 24 * 60 * 60 * 1000)); const npv = r => amounts.reduce((s, v, i) => s + v / Math.pow(1 + r, years[i]), 0); const dnpv = r => amounts.reduce((s, v, i) => s - v * years[i] / Math.pow(1 + r, years[i] + 1), 0); for(let base = 0; base <= 4; base++) { let r = 0.1 + base * 0.2; for(let i = 0; i < 300; i++) { const f = npv(r); const f1 = dnpv(r); if (Math.abs(f1) < 1e-10) { r += 0.1; continue; } const nr = r - f / f1; if (!isFinite(nr)) break; if (Math.abs(nr - r) < 1e-6) return nr; r = nr; } } return null; }
function getPortfolioStateOnDate(allEvts, target) { const st = {}, past = allEvts.filter(e => new Date(e.date) <= target); for (const e of past) { if (!e.symbol) continue; const sym = e.symbol.toUpperCase(); if (!st[sym]) st[sym] = { lots: [], currency: "USD" }; switch (e.eventType) { case "transaction": st[sym].currency = e.currency; if (e.type === "buy") st[sym].lots.push({ quantity: e.quantity }); else { let q = e.quantity; while (q > 0 && st[sym].lots.length) { const lot = st[sym].lots[0]; if (lot.quantity <= q) { q -= lot.quantity; st[sym].lots.shift(); } else { lot.quantity -= q; q = 0; } } } break; case "split": st[sym].lots.forEach(l => l.quantity *= e.ratio); break; } } return st; }
function findNearest(hist, date, toleranceDays = 0) { if (!hist || Object.keys(hist).length === 0) return undefined; const keys = Object.keys(hist).sort(); const tgt = date instanceof Date ? date : new Date(date); const tgtStr = tgt.toISOString().slice(0, 10); let lo = 0, hi = keys.length - 1, cand = null; while (lo <= hi) { const mid = (lo + hi) >> 1; if (keys[mid] <= tgtStr) { cand = keys[mid]; lo = mid + 1; } else hi = mid - 1; } if (!cand) return undefined; if (toleranceDays > 0) { const diff = (new Date(tgtStr) - new Date(cand)) / 86400000; if (diff > toleranceDays) return undefined; } return hist[cand]; }
function findFxRate(market, currency, date, tolerance = 15) { if (!currency || currency === "TWD") return 1; const fxSym = currencyToFx[currency]; if (!fxSym) return 1; const hist = market[fxSym]?.rates || {}; const rate = findNearest(hist, date, tolerance); return rate ?? 1; }
const toDate = v => v.toDate ? v.toDate() : new Date(v);
