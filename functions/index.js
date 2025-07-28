/* eslint-disable */
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// =================================================================================
// === Core Calculation Logic ======================================================
// =================================================================================
async function performRecalculation(userId) {
  const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
  const logs = [];
  const log = (msg) => {
    const ts = new Date().toISOString();
    logs.push(`${ts}: ${msg}`);
    console.log(`[${userId}] ${ts}: ${msg}`);
  };

  try {
    log("--- Recalculation triggered (v32-ReturnRateFix) ---");

    const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
    const historyDocRef  = db.doc(`users/${userId}/user_data/portfolio_history`);

    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);

    const transactions = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
    const userSplits   = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    if (transactions.length === 0) {
      log("No transactions, clearing.");
      await Promise.all([
        holdingsDocRef.set({ holdings: {}, totalRealizedPL: 0, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
        historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
      ]);
      return;
    }

    const marketData = await getMarketDataFromDb(transactions, log);
    if (!marketData || Object.keys(marketData).length === 0) throw new Error("empty market data");

    log("Start calc …");
    const result = calculatePortfolio(transactions, userSplits, marketData, log);
    if (!result) throw new Error("calc returned undefined");

    const { holdings, totalRealizedPL, portfolioHistory, xirr, overallReturnRateTotal } = result;

    log(`Done. holdings=${Object.keys(holdings).length}, realPL=${totalRealizedPL}, XIRR=${xirr}`);

    const finalData = {
      holdings,
      totalRealizedPL,
      xirr,
      overallReturnRateTotal,           // ← 新增
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      force_recalc_timestamp: admin.firestore.FieldValue.delete()
    };

    await Promise.all([
      holdingsDocRef.set(finalData, { merge: true }),
      historyDocRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
    ]);

    log("--- Recalculation finished OK ---");
  } catch (err) {
    console.error(`[${userId}] CRITICAL:`, err);
    log(`CRITICAL: ${err.message}\n${err.stack}`);
  } finally {
    await logRef.set({ entries: logs });
  }
}

// =================================================================================
// === Firestore Triggers ==========================================================
// =================================================================================

// 主入口：current_holdings 裡的 force_recalc_timestamp
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds: 300, memory: "1GB" })
  .firestore
  .document("users/{userId}/user_data/current_holdings")
  .onUpdate(async (change, context) => {
    const before = change.before.data();
    const after  = change.after.data();
    if (after.force_recalc_timestamp && before.force_recalc_timestamp !== after.force_recalc_timestamp) {
      await performRecalculation(context.params.userId);
    }
  });

// 任何交易 / 拆股 / 價格 / 匯率異動 → 只寫入 trigger 欄位
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

// price_history/{symbol}
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
      console.log(`[${symbol}] metadata only, skip.`);
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
    console.log(`[${symbol}] triggered ${users.size} users`);
    return null;
  });

// 匯率
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
    console.log(`FX updated → triggered ${users.length} users`);
    return null;
  });


// =================================================================================
// === Data Fetching & Helper Functions ===========================================
// =================================================================================

async function getMarketDataFromDb(transactions, log) {
  const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
  const all = [...new Set([...symbols, "TWD=X"])];
  log(`need symbols: [${all.join(", ")}]`);
  const market = {};

  for (const sym of all) {
    const col = sym === "TWD=X" ? "exchange_rates" : "price_history";
    const docRef = db.collection(col).doc(sym);
    const doc = await docRef.get();

    if (doc.exists) {
      market[sym] = doc.data();
    } else {
      const fetched = await fetchAndSaveMarketData(sym, log);
      if (fetched) {
        market[sym] = fetched;
        await docRef.set(fetched);
      }
    }
  }
  return market;
}

async function fetchAndSaveMarketData(symbol, log) {
  try {
    log(`[Fetch] ${symbol} …`);
    const hist = await yahooFinance.historical(symbol, { period1: "2000-01-01" });
    if (!hist || hist.length === 0) return null;

    const prices = {};
    hist.forEach(i => prices[i.date.toISOString().split("T")[0]] = i.close);

    const payload = {
      prices,
      splits: {},
      dividends: (hist.dividends || []).reduce(
        (acc, d) => ({ ...acc, [d.date.toISOString().split("T")[0]]: d.amount }), {}),
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dataSource: "emergency-fetch-user-split-model-v2"
    };

    if (symbol === "TWD=X") {
      payload.rates = payload.prices;
      delete payload.dividends;
    }
    return payload;
  } catch (e) {
    log(`[Fetch error] ${symbol}: ${e.message}`);
    return null;
  }
}

// =================================================================================
// === Portfolio Calculation =======================================================
// =================================================================================

function calculatePortfolio(transactions, userSplits, marketData, log) {
  const events = [];

  // assemble events
  transactions.forEach(t =>
    events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: "transaction" }));
  userSplits.forEach(s =>
    events.push({ ...s, date: s.date.toDate ? s.date.toDate() : new Date(s.date), eventType: "split" }));
  [...new Set(transactions.map(t => t.symbol.toUpperCase()))].forEach(sym => {
    const divs = marketData[sym]?.dividends || {};
    Object.entries(divs).forEach(([d, amt]) =>
      events.push({ date: new Date(d), symbol: sym, amount: amt, eventType: "dividend" }));
  });
  events.sort((a, b) => new Date(a.date) - new Date(b.date));

  const portfolio = {};
  let totalRealizedPL = 0;

  for (const ev of events) {
    const sym = ev.symbol.toUpperCase();
    if (!portfolio[sym]) portfolio[sym] = { lots: [], currency: "USD" };

    const rateHist = marketData["TWD=X"]?.rates || {};
    const fx = findNearestDataPoint(rateHist, ev.date);

    switch (ev.eventType) {
      case "transaction": {
        const t = ev;
        portfolio[sym].currency = t.currency;
        const costPerShareOrig = (t.totalCost || t.price);
        const costPerShareTWD  = costPerShareOrig * (t.currency === "USD" ? fx : 1);

        if (t.type === "buy") {
          portfolio[sym].lots.push({
            quantity: t.quantity,
            pricePerShareTWD: costPerShareTWD,
            pricePerShareOriginal: costPerShareOrig,
            date: ev.date
          });
        } else if (t.type === "sell") {
          let qty = t.quantity;
          const saleValueTWD = (t.totalCost || t.quantity * t.price) * (t.currency === "USD" ? fx : 1);
          let costSoldTWD = 0;

          while (qty > 0 && portfolio[sym].lots.length > 0) {
            const lot = portfolio[sym].lots[0];
            if (lot.quantity <= qty) {
              costSoldTWD += lot.quantity * lot.pricePerShareTWD;
              qty -= lot.quantity;
              portfolio[sym].lots.shift();
            } else {
              costSoldTWD += qty * lot.pricePerShareTWD;
              lot.quantity -= qty;
              qty = 0;
            }
          }

          const realized = saleValueTWD - costSoldTWD;
          totalRealizedPL += realized;

          // 新增：累積個股已實現損益 / 成本
          portfolio[sym].realizedCostTWD = (portfolio[sym].realizedCostTWD || 0) + costSoldTWD;
          portfolio[sym].realizedPLTWD   = (portfolio[sym].realizedPLTWD   || 0) + realized;
        }
        break;
      }

      case "split":
        portfolio[sym].lots.forEach(lot => {
          lot.quantity            *= ev.ratio;
          lot.pricePerShareTWD    /= ev.ratio;
          lot.pricePerShareOriginal /= ev.ratio;
        });
        break;

      case "dividend": {
        const totalShares = portfolio[sym].lots.reduce((s, l) => s + l.quantity, 0);
        if (totalShares === 0) break;

        const divTWD = ev.amount * totalShares * (portfolio[sym].currency === "USD" ? fx : 1);
        totalRealizedPL += divTWD;

        // 個股層級也加到 realizedPL
        portfolio[sym].realizedPLTWD = (portfolio[sym].realizedPLTWD || 0) + divTWD;
        break;
      }
    }
  }

  const finalHoldings = calculateFinalHoldings(portfolio, marketData);
  const portfolioHistory = calculatePortfolioHistory(events, marketData);
  const cashflows = createCashflows(events, portfolio, finalHoldings, marketData);
  const xirr = calculateXIRR(cashflows);

  // ===== overall 累積報酬率 =====
  let investedTotal = 0;
  let totalReturnTWD = 0;

  for (const s in finalHoldings) {
    investedTotal  += finalHoldings[s].investedCostTWD || 0;
    totalReturnTWD += finalHoldings[s].unrealizedPLTWD +
                      (portfolio[s].realizedPLTWD || 0);
  }

  const overallReturnRateTotal = investedTotal > 0 ? (totalReturnTWD / investedTotal) * 100 : 0;

  return {
    holdings: finalHoldings,
    totalRealizedPL,
    portfolioHistory,
    xirr,
    overallReturnRateTotal    // ← 回傳給前端整體「總報酬率」
  };
}

// =================================================================================
// === Helpers: 持股 / 歷史 / XIRR 等 ==============================================
// =================================================================================

function calculateFinalHoldings(portfolio, marketData) {
  const final = {};
  const today = new Date();
  const fxToday = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, today);

  for (const sym in portfolio) {
    const h = portfolio[sym];
    const qty = h.lots.reduce((s, l) => s + l.quantity, 0);
    if (qty < 1e-9) continue;

    const totalCostTWD     = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareTWD, 0);
    const totalCostOrig    = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareOriginal, 0);
    const realizedCostTWD  = h.realizedCostTWD || 0;
    const realizedPLTWD    = h.realizedPLTWD   || 0;

    const priceHist        = marketData[sym]?.prices || {};
    const curPriceOrig     = findNearestDataPoint(priceHist, today);
    const fx               = h.currency === "USD" ? fxToday : 1;

    const mktValueTWD      = qty * curPriceOrig * fx;
    const unrealizedPLTWD  = mktValueTWD - totalCostTWD;

    const investedCostTWD  = totalCostTWD + realizedCostTWD;
    const totalReturnTWD   = unrealizedPLTWD + realizedPLTWD;

    const returnRateCurrent = totalCostTWD    > 0 ? (unrealizedPLTWD / totalCostTWD)   * 100 : 0;
    const returnRateTotal   = investedCostTWD > 0 ? (totalReturnTWD  / investedCostTWD) * 100 : 0;

    final[sym] = {
      symbol: sym,
      quantity: qty,
      currency: h.currency,
      avgCostOriginal: totalCostOrig / qty,
      totalCostTWD,
      investedCostTWD,        // 新增
      currentPriceOriginal: curPriceOrig,
      marketValueTWD: mktValueTWD,
      unrealizedPLTWD,
      realizedPLTWD,          // 新增
      returnRateCurrent,      // 未實現
      returnRateTotal         // 含已實現
    };
  }
  return final;
}

function calculatePortfolioHistory(events, marketData) {
  const history = {};
  const txEvents = events.filter(e => e.eventType === "transaction");
  if (txEvents.length === 0) return history;

  const first = new Date(txEvents[0].date);
  const today = new Date();
  let cur = new Date(first);
  cur.setUTCHours(0,0,0,0);

  while (cur <= today) {
    const dStr = cur.toISOString().split("T")[0];
    const state = getPortfolioStateOnDate(events, cur);
    history[dStr] = calculateDailyMarketValue(state, marketData, cur);
    cur.setDate(cur.getDate() + 1);
  }
  return history;
}

function createCashflows(events, portfolio, finalHoldings, marketData) {
  const flows = [];

  events.filter(e => e.eventType === "transaction").forEach(t => {
    const fx = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, t.date);
    const amt = (t.totalCost || t.quantity * t.price) * (t.currency === "USD" ? fx : 1);
    flows.push({ date: new Date(t.date), amount: t.type === "buy" ? -amt : amt });
  });

  events.filter(e => e.eventType === "dividend").forEach(d => {
    const fx = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, d.date);
    const holdingCurrency = portfolio[d.symbol]?.currency || "USD";
    const shares = portfolio[d.symbol]?.lots.reduce((s,l)=>s+l.quantity,0) || 0;
    const amt = d.amount * shares * (holdingCurrency === "USD" ? fx : 1);
    if (amt > 0) flows.push({ date: new Date(d.date), amount: amt });
  });

  const totalMkt = Object.values(finalHoldings).reduce((s,h)=>s+h.marketValueTWD,0);
  if (totalMkt > 0) flows.push({ date: new Date(), amount: totalMkt });

  return flows;
}

function calculateDailyMarketValue(portfolioState, marketData, date) {
  let total = 0;
  const fx = findNearestDataPoint(marketData["TWD=X"]?.rates || {}, date);

  for (const sym in portfolioState) {
    const h = portfolioState[sym];
    const qty = h.lots.reduce((s,l)=>s+l.quantity,0);
    if (qty === 0) continue;

    const price = findNearestDataPoint(marketData[sym]?.prices || {}, date);
    const rate  = h.currency === "USD" ? fx : 1;
    total += qty * price * rate;
  }
  return total;
}

function getPortfolioStateOnDate(allEvents, target) {
  const state = {};
  const rel = allEvents.filter(e => new Date(e.date) <= target);
  const splitsAll = allEvents.filter(e => e.eventType === "split");

  for (const ev of rel) {
    const sym = ev.symbol.toUpperCase();
    if (!state[sym]) state[sym] = { lots: [], currency: "USD" };

    switch (ev.eventType) {
      case "transaction":
        state[sym].currency = ev.currency;
        if (ev.type === "buy") {
          state[sym].lots.push({ quantity: ev.quantity, date: ev.date });
        } else {
          let q = ev.quantity;
          while (q > 0 && state[sym].lots.length) {
            const lot = state[sym].lots[0];
            if (lot.quantity <= q) {
              q -= lot.quantity;
              state[sym].lots.shift();
            } else {
              lot.quantity -= q;
              q = 0;
            }
          }
        }
        break;

      case "split":
        state[sym].lots.forEach(l => l.quantity *= ev.ratio);
        break;
    }
  }

  // forward adjust for future splits
  for (const sym in state) {
    const futureSplits = splitsAll.filter(s => s.symbol.toUpperCase() === sym && new Date(s.date) > target);
    futureSplits.forEach(s => state[sym].lots.forEach(l => l.quantity *= s.ratio));
  }
  return state;
}

function calculateXIRR(cashflows) {
  if (cashflows.length < 2) return 0;

  const vals = cashflows.map(c => c.amount);
  const dates = cashflows.map(c => c.date);
  const yrs = dates.map(d => (d - dates[0]) / (1000*60*60*24*365));

  const npv = r => vals.reduce((s,v,i)=>s+v/Math.pow(1+r,yrs[i]),0);
  const dNpv = r => vals.reduce((s,v,i)=> yrs[i]>0 ? s - v*yrs[i]/Math.pow(1+r,yrs[i]+1) : s, 0);

  let guess = 0.1;
  for (let i=0;i<100;i++) {
    const f = npv(guess);
    const f1 = dNpv(guess);
    if (Math.abs(f1) < 1e-9) break;
    const newG = guess - f/f1;
    if (Math.abs(newG - guess) < 1e-6) return newG;
    guess = newG;
  }
  return guess;
}

function findNearestDataPoint(history, date) {
  if (!history || Object.keys(history).length === 0) return 1;
  const d = new Date(date);
  d.setUTCHours(12,0,0,0);

  for (let i=0;i<7;i++) {
    const s = new Date(d);
    s.setDate(s.getDate() - i);
    const key = s.toISOString().split("T")[0];
    if (history[key] !== undefined) return history[key];
  }

  const sorted = Object.keys(history).sort();
  const target = d.toISOString().split("T")[0];
  let closest = null;
  for (const k of sorted) {
    if (k <= target) closest = k; else break;
  }
  return closest ? history[closest] : 1;
}
// =================================================================================
