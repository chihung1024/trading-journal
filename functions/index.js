/* eslint-disable */
const functions = require("firebase-functions");
const admin      = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

/* ============================================================================
 *  Core Recalculation Entry
 * ==========================================================================*/
async function performRecalculation(userId) {
  const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
  const logs   = [];
  const log = (msg) => {
    const ts = new Date().toISOString();
    logs.push(`${ts}: ${msg}`);
    console.log(`[${userId}] ${ts}: ${msg}`);
  };

  try {
    log("--- Recalculation triggered (v33-AliasFix) ---");

    const holdingsRef = db.doc(`users/${userId}/user_data/current_holdings`);
    const histRef     = db.doc(`users/${userId}/user_data/portfolio_history`);

    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);

    const txs    = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
    const splits = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    if (txs.length === 0) {
      log("no tx → clear holdings / history");
      await Promise.all([
        holdingsRef.set({ holdings: {}, totalRealizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
        histRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
      ]);
      return;
    }

    const marketData = await getMarketDataFromDb(txs, log);
    if (!marketData || Object.keys(marketData).length === 0) throw new Error("marketData empty");

    const result = calculatePortfolio(txs, splits, marketData, log);
    const { holdings, totalRealizedPL, portfolioHistory, xirr, overallReturnRateTotal } = result;

    const finalData = {
      holdings,
      totalRealizedPL,
      xirr,
      overallReturnRateTotal,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      force_recalc_timestamp: admin.firestore.FieldValue.delete()
    };

    await Promise.all([
      holdingsRef.set(finalData, { merge: true }),
      histRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
    ]);

    log("--- Recalculation finished OK ---");
  } catch (err) {
    console.error(`[${userId}] ERR`, err);
    logs.push(`CRITICAL: ${err.message}\n${err.stack}`);
  } finally {
    await logRef.set({ entries: logs });
  }
}

/* ============================================================================
 *  Triggers – 只寫 force_recalc_timestamp
 * ==========================================================================*/
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds: 300, memory: "1GB" })
  .firestore
  .document("users/{userId}/user_data/current_holdings")
  .onUpdate((change, ctx) => {
    const before = change.before.data();
    const after  = change.after.data();
    if (after.force_recalc_timestamp && before.force_recalc_timestamp !== after.force_recalc_timestamp) {
      return performRecalculation(ctx.params.userId);
    }
    return null;
  });

exports.recalculateOnTransaction = functions.firestore
  .document("users/{userId}/transactions/{txId}")
  .onWrite((_, ctx) =>
    db.doc(`users/${ctx.params.userId}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
);

exports.recalculateOnSplit = functions.firestore
  .document("users/{userId}/splits/{splitId}")
  .onWrite((_, ctx) =>
    db.doc(`users/${ctx.params.userId}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
);

exports.recalculateOnPriceUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore
  .document("price_history/{symbol}")
  .onWrite(async (change, ctx) => {
    const symbol = ctx.params.symbol.toUpperCase();
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();

    if (before &&
        JSON.stringify(before.prices)    === JSON.stringify(after.prices) &&
        JSON.stringify(before.dividends) === JSON.stringify(after.dividends)) {
      return null;
    }

    const txSnap = await db.collectionGroup("transactions")
                           .where("symbol", "==", symbol)
                           .get();
    if (txSnap.empty) return null;

    const users = new Set(txSnap.docs.map(d => d.ref.path.split("/")[1]));
    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(
      [...users].map(uid =>
        db.doc(`users/${uid}/user_data/current_holdings`)
          .set({ force_recalc_timestamp: ts }, { merge: true })
      )
    );
    return null;
  });

exports.recalculateOnFxUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore
  .document("exchange_rates/TWD=X")
  .onWrite(async (change) => {
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();
    if (before && JSON.stringify(before.rates) === JSON.stringify(after.rates)) return null;

    const users = await db.collection("users").listDocuments();
    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(
      users.map(u =>
        db.doc(`users/${u.id}/user_data/current_holdings`)
          .set({ force_recalc_timestamp: ts }, { merge: true })
      )
    );
    return null;
  });

/* ============================================================================
 *  Market-data helpers
 * ==========================================================================*/
async function getMarketDataFromDb(transactions, log) {
  const syms = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
  const all  = [...new Set([...syms, "TWD=X"])];
  const mkt  = {};

  for (const s of all) {
    const col = s === "TWD=X" ? "exchange_rates" : "price_history";
    const docRef = db.collection(col).doc(s);
    const doc = await docRef.get();

    if (doc.exists) {
      mkt[s] = doc.data();
    } else {
      const fetched = await fetchAndSaveMarketData(s, log);
      if (fetched) {
        mkt[s] = fetched;
        await docRef.set(fetched);
      }
    }
  }
  return mkt;
}

async function fetchAndSaveMarketData(symbol, log) {
  try {
    const hist = await yahooFinance.historical(symbol, { period1: "2000-01-01" });
    const prices = {};
    hist.forEach(i => prices[i.date.toISOString().split("T")[0]] = i.close);

    const payload = {
      prices,
      splits: {},
      dividends: (hist.dividends || []).reduce(
        (acc, d) => ({ ...acc, [d.date.toISOString().split("T")[0]]: d.amount }), {}),
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dataSource: "emergency-fetch"
    };

    if (symbol === "TWD=X") {
      payload.rates = payload.prices;
      delete payload.dividends;
    }
    return payload;
  } catch (e) {
    log(`fetch ${symbol} err ${e.message}`);
    return null;
  }
}

/* ============================================================================
 *  Portfolio Calculation
 * ==========================================================================*/
function calculatePortfolio(txs, splits, marketData, log) {
  /* ---- build chronological event list ---- */
  const events = [
    ...txs.map(t => ({ ...t, date: toDate(t.date), eventType: "transaction" })),
    ...splits.map(s => ({ ...s, date: toDate(s.date), eventType: "split" }))
  ];

  // add dividends
  [...new Set(txs.map(t => t.symbol.toUpperCase()))].forEach(sym => {
    Object.entries(marketData[sym]?.dividends || {})
      .forEach(([d, amt]) =>
        events.push({ date: new Date(d), symbol: sym, amount: amt, eventType: "dividend" }));
  });

  events.sort((a, b) => new Date(a.date) - new Date(b.date));

  /* ---- iterate events ---- */
  const pf = {};        // per-symbol lot list etc.
  let totalRealizedPL = 0;

  for (const ev of events) {
    const sym = ev.symbol.toUpperCase();
    if (!pf[sym]) pf[sym] = { lots: [], currency: "USD" };

    const fxHist = marketData["TWD=X"]?.rates || {};
    const fx = findNearestDataPoint(fxHist, ev.date);

    switch (ev.eventType) {
      case "transaction": {
        const t = ev;
        pf[sym].currency = t.currency;
        const costPerShareOrig = (t.totalCost || t.price);
        const costPerShareTWD  = costPerShareOrig * (t.currency === "USD" ? fx : 1);

        if (t.type === "buy") {
          pf[sym].lots.push({
            quantity: t.quantity,
            pricePerShareOriginal: costPerShareOrig,
            pricePerShareTWD: costPerShareTWD,
            date: ev.date
          });
        } else {  // sell
          let left = t.quantity;
          const saleValueTWD = (t.totalCost || t.quantity * t.price) * (t.currency === "USD" ? fx : 1);
          let costSoldTWD = 0;

          while (left > 0 && pf[sym].lots.length) {
            const lot = pf[sym].lots[0];
            if (lot.quantity <= left) {
              costSoldTWD += lot.quantity * lot.pricePerShareTWD;
              left -= lot.quantity;
              pf[sym].lots.shift();
            } else {
              costSoldTWD += left * lot.pricePerShareTWD;
              lot.quantity -= left;
              left = 0;
            }
          }

          const realized = saleValueTWD - costSoldTWD;
          totalRealizedPL += realized;

          pf[sym].realizedCostTWD = (pf[sym].realizedCostTWD || 0) + costSoldTWD;
          pf[sym].realizedPLTWD   = (pf[sym].realizedPLTWD   || 0) + realized;
        }
        break;
      }

      case "split":
        pf[sym].lots.forEach(lot => {
          lot.quantity            *= ev.ratio;
          lot.pricePerShareTWD    /= ev.ratio;
          lot.pricePerShareOriginal /= ev.ratio;
        });
        break;

      case "dividend":
        const shares = pf[sym].lots.reduce((s, l) => s + l.quantity, 0);
        if (shares === 0) break;
        const divTWD = ev.amount * shares * (pf[sym].currency === "USD" ? fx : 1);
        totalRealizedPL += divTWD;
        pf[sym].realizedPLTWD = (pf[sym].realizedPLTWD || 0) + divTWD;
        break;
    }
  }

  /* ---- summary / history / XIRR ---- */
  const holdings = calculateFinalHoldings(pf, marketData);
  const history  = calculatePortfolioHistory(events, marketData);
  const xirr     = calculateXIRR(createCashflows(events, pf, holdings, marketData));

  // overall total return rate
  let invested = 0, totalReturn = 0;
  for (const s in holdings) {
    invested    += holdings[s].investedCostTWD || 0;
    totalReturn += holdings[s].unrealizedPLTWD + (pf[s].realizedPLTWD || 0);
  }
  const overallReturnRateTotal = invested > 0 ? (totalReturn / invested) * 100 : 0;

  return { holdings, totalRealizedPL, portfolioHistory: history, xirr, overallReturnRateTotal };
}

/* ============================================================================
 *  Sub-helpers
 * ==========================================================================*/
function calculateFinalHoldings(pf, marketData) {
  const final = {};
  const today = new Date();
  const fxToday = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, today);

  for (const sym in pf) {
    const h = pf[sym];
    const qty = h.lots.reduce((s, l) => s + l.quantity, 0);
    if (qty < 1e-9) continue;

    const totalCostTWD  = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareTWD, 0);
    const totalCostOrig = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareOriginal, 0);
    const realizedCostTWD = h.realizedCostTWD || 0;
    const realizedPLTWD   = h.realizedPLTWD   || 0;

    const priceHist  = marketData[sym]?.prices || {};
    const curPrice   = findNearestDataPoint(priceHist, today);
    const fx         = h.currency === "USD" ? fxToday : 1;

    const mktValueTWD    = qty * curPrice * fx;
    const unrealizedPL   = mktValueTWD - totalCostTWD;

    const investedCost   = totalCostTWD + realizedCostTWD;
    const totalReturnTWD = unrealizedPL + realizedPLTWD;

    const returnRateCurrent = totalCostTWD  > 0 ? (unrealizedPL  / totalCostTWD) * 100 : 0;
    const returnRateTotal   = investedCost  > 0 ? (totalReturnTWD / investedCost) * 100 : 0;

    final[sym] = {
      symbol: sym,
      quantity: qty,
      currency: h.currency,
      avgCostOriginal: totalCostOrig / qty,
      totalCostTWD,
      investedCostTWD: investedCost,
      currentPriceOriginal: curPrice,
      marketValueTWD: mktValueTWD,
      unrealizedPLTWD: unrealizedPL,
      realizedPLTWD,
      returnRateCurrent,
      returnRateTotal,
      returnRate: returnRateCurrent           // ← 向下相容給舊前端
    };
  }
  return final;
}

function calculatePortfolioHistory(events, marketData) {
  const txEvents = events.filter(e => e.eventType === "transaction");
  if (txEvents.length === 0) return {};

  const first = new Date(txEvents[0].date);
  const today = new Date();
  let cur = new Date(first);
  cur.setUTCHours(0,0,0,0);

  const history = {};
  while (cur <= today) {
    const key = cur.toISOString().split("T")[0];
    const state = getPortfolioStateOnDate(events, cur);
    history[key] = calculateDailyMarketValue(state, marketData, cur);
    cur.setDate(cur.getDate() + 1);
  }
  return history;
}

function createCashflows(events, pf, holdings, marketData) {
  const flows = [];
  const fxHist = marketData["TWD=X"]?.rates || {};

  events.filter(e => e.eventType === "transaction").forEach(t => {
    const fx = findNearestDataPoint(fxHist, t.date);
    const amt = (t.totalCost || t.quantity * t.price) * (t.currency === "USD" ? fx : 1);
    flows.push({ date: toDate(t.date), amount: t.type === "buy" ? -amt : amt });
  });

  events.filter(e => e.eventType === "dividend").forEach(d => {
    const fx = findNearestDataPoint(fxHist, d.date);
    const shares = pf[d.symbol]?.lots.reduce((s,l)=>s+l.quantity,0) || 0;
    const amt = d.amount * shares * (pf[d.symbol]?.currency === "USD" ? fx : 1);
    if (amt > 0) flows.push({ date: toDate(d.date), amount: amt });
  });

  const mkt = Object.values(holdings).reduce((s,h)=>s+h.marketValueTWD,0);
  if (mkt > 0) flows.push({ date: new Date(), amount: mkt });

  return flows;
}

function calculateXIRR(flows) {
  if (flows.length < 2) return 0;

  const vals  = flows.map(f => f.amount);
  const dates = flows.map(f => f.date);
  const yrs   = dates.map(d => (d - dates[0]) / (1000*60*60*24*365));

  const npv  = r => vals.reduce((s,v,i)=>s+v/Math.pow(1+r,yrs[i]),0);
  const dnpv = r => vals.reduce((s,v,i)=>yrs[i]>0 ? s - v*yrs[i]/Math.pow(1+r,yrs[i]+1) : s,0);

  let g = 0.1;
  for (let i=0;i<100;i++) {
    const f = npv(g), f1 = dnpv(g);
    if (Math.abs(f1) < 1e-9) break;
    const ng = g - f/f1;
    if (Math.abs(ng-g) < 1e-6) return ng;
    g = ng;
  }
  return g;
}

function calculateDailyMarketValue(state, marketData, date) {
  let total = 0;
  const fxHist = marketData["TWD=X"]?.rates || {};
  const fx = findNearestDataPoint(fxHist, date);

  for (const sym in state) {
    const s = state[sym];
    const qty = s.lots.reduce((sum,l)=>sum+l.quantity,0);
    if (qty === 0) continue;

    const price = findNearestDataPoint(marketData[sym]?.prices || {}, date);
    const rate  = s.currency === "USD" ? fx : 1;
    total += qty * price * rate;
  }
  return total;
}

function getPortfolioStateOnDate(allEvents, target) {
  const st = {};
  const past = allEvents.filter(e => new Date(e.date) <= target);
  const futureSplits = allEvents.filter(e => e.eventType === "split" && new Date(e.date) > target);

  for (const ev of past) {
    const sym = ev.symbol.toUpperCase();
    if (!st[sym]) st[sym] = { lots: [], currency: "USD" };

    switch (ev.eventType) {
      case "transaction":
        st[sym].currency = ev.currency;
        if (ev.type === "buy") {
          st[sym].lots.push({ quantity: ev.quantity });
        } else {
          let q = ev.quantity;
          while (q > 0 && st[sym].lots.length) {
            const lot = st[sym].lots[0];
            if (lot.quantity <= q) {
              q -= lot.quantity;
              st[sym].lots.shift();
            } else {
              lot.quantity -= q;
              q = 0;
            }
          }
        }
        break;
      case "split":
        st[sym].lots.forEach(l => l.quantity *= ev.ratio);
        break;
    }
  }

  // forward adjust
  for (const s in st) {
    futureSplits.filter(sp => sp.symbol.toUpperCase() === s)
                .forEach(sp => st[s].lots.forEach(l => l.quantity *= sp.ratio));
  }
  return st;
}

/* ============================================================================
 *  Utilities
 * ==========================================================================*/
function findNearestDataPoint(hist, date) {
  if (!hist || Object.keys(hist).length === 0) return 1;
  const d = new Date(date); d.setUTCHours(12,0,0,0);

  for (let i=0;i<7;i++) {
    const s = new Date(d); s.setDate(s.getDate()-i);
    const k = s.toISOString().split("T")[0];
    if (hist[k] !== undefined) return hist[k];
  }

  const keys = Object.keys(hist).sort();
  const tgt  = d.toISOString().split("T")[0];
  let closest = null;
  for (const k of keys) {
    if (k <= tgt) closest = k; else break;
  }
  return closest ? hist[closest] : 1;
}

const toDate = (v) => v.toDate ? v.toDate() : new Date(v);
