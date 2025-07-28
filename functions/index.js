const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// === 全域參數 ===
const LOOKBACK_DAYS = process.env.LOOKBACK_DAYS
  ? parseInt(process.env.LOOKBACK_DAYS)
  : 7;                        // 回溯天數（可由 env 設定）
const LOCK_DURATION_MS = 5 * 60 * 1000; // 5 分鐘內視為同一次計算

// =================================================================================
// === 核心計算函式（可重複使用））===============================================
// =================================================================================
async function performRecalculation(userId) {
  const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
  const logs = [];
  const log = (message) => {
    const timestamp = new Date().toISOString();
    logs.push(`${timestamp}: ${message}`);
    console.log(`[${userId}] ${timestamp}: ${message}`);
  };

  const lockRef = db.doc(`users/${userId}/user_data/recalc_lock`);
  let lockAcquired = false;

  try {
    // 1. 取得鎖（如果已有鎖且未過期，就跳過重算）
    await db.runTransaction(async tx => {
      const snap = await tx.get(lockRef);
      const now = Date.now();
      if (snap.exists) {
        const lockedAt = snap.data().lockedAt.toMillis();
        if (now - lockedAt < LOCK_DURATION_MS) {
          log("Another recalculation in progress → skip.");
          throw new Error("Recalc in progress");
        }
      }
      tx.set(lockRef, { lockedAt: admin.firestore.Timestamp.now() });
    });
    lockAcquired = true;

    log("--- Recalculation triggered (with lock) ---");

    // 2. 取交易＆拆股
    const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
    const historyDocRef = db.doc(`users/${userId}/user_data/portfolio_history`);
    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);
    const transactions = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
    const userSplits   = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    if (transactions.length === 0) {
      log("No transactions → clear holdings & history.");
      await Promise.all([
        holdingsDocRef.set({
          holdings: {},
          totalRealizedPL: 0,
          totalUnrealizedPL: 0,
          lastUpdated: admin.firestore.FieldValue.serverTimestamp()
        }),
        historyDocRef.set({
          history: {},
          lastUpdated: admin.firestore.FieldValue.serverTimestamp()
        })
      ]);
      return;
    }

    // 3. 取市值 & 匯率
    const marketData = await getMarketDataFromDb(transactions, log);
    if (!marketData || Object.keys(marketData).length === 0) {
      throw new Error("Market data empty after fetch.");
    }

    // 4. 最終計算
    log("Starting portfolio calculation...");
    const result = calculatePortfolio(transactions, userSplits, marketData, log);
    if (!result) throw new Error("calculatePortfolio returned undefined");

    const {
      holdings,
      totalRealizedPL,
      totalUnrealizedPL,
      portfolioHistory,
      xirr
    } = result;

    log(`Calc done → holdings: ${Object.keys(holdings).length}, RealizedPL: ${totalRealizedPL.toFixed(2)}, ` +
        `UnrealizedPL: ${totalUnrealizedPL.toFixed(2)}, History pts: ${Object.keys(portfolioHistory).length}, ` +
        `XIRR: ${xirr == null ? "N/A" : (xirr * 100).toFixed(2) + "%"}`);

    // 5. 寫回 Firestore（merge 避免覆寫 trigger 欄位）
    const finalData = {
      holdings,
      totalRealizedPL,
      totalUnrealizedPL,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      force_recalc_timestamp: admin.firestore.FieldValue.delete()
    };
    if (xirr != null) {
      finalData.xirr = xirr;
    }
    await Promise.all([
      holdingsDocRef.set(finalData, { merge: true }),
      historyDocRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
    ]);

    log("--- Recalculation finished successfully! ---");

  } catch (err) {
    if (err.message === "Recalc in progress") {
      // 只是跳過，不算錯誤
      return;
    }
    console.error(`[${userId}] CRITICAL ERROR:`, err);
    log(`CRITICAL ERROR: ${err.message}`);
  } finally {
    // 釋放鎖
    if (lockAcquired) {
      await lockRef.delete().catch(() => {/* ignore */});
    }
    // 寫日誌
    await logRef.set({ entries: logs });
  }
}

// =================================================================================
// === Firestore 觸發器（API 路徑 & 參數不變）=======================================
// =================================================================================
exports.recalculatePortfolio = functions
  .runWith({ timeoutSeconds: 300, memory: '1GB' })
  .firestore
  .document("users/{userId}/user_data/current_holdings")
  .onUpdate(async (change, context) => {
    const before = change.before.data();
    const after  = change.after.data();
    if (after.force_recalc_timestamp && before.force_recalc_timestamp !== after.force_recalc_timestamp) {
      console.log(`Recalc forced for user ${context.params.userId}`);
      await performRecalculation(context.params.userId);
    }
  });

exports.recalculateOnTransaction = functions.firestore
  .document("users/{userId}/transactions/{transactionId}")
  .onWrite(async (change, context) => {
    const ref = db.doc(`users/${context.params.userId}/user_data/current_holdings`);
    await ref.set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
  });

exports.recalculateOnSplit = functions.firestore
  .document("users/{userId}/splits/{splitId}`)
  .onWrite(async (change, context) => {
    const ref = db.doc(`users/${context.params.userId}/user_data/current_holdings`);
    await ref.set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
  });

exports.recalculateOnPriceUpdate = functions
  .runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore
  .document("price_history/{symbol}")
  .onWrite(async (change, context) => {
    const symbol = context.params.symbol.toUpperCase();
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();
    if (before
        && JSON.stringify(before.prices) === JSON.stringify(after.prices)
        && JSON.stringify(before.dividends) === JSON.stringify(after.dividends)) {
      console.log(`[${symbol}] only metadata changed, skip.`);
      return null;
    }
    console.log(`[${symbol}] market data changed, finding users…`);
    const txSnap = await db.collectionGroup("transactions").where("symbol", "==", symbol).get();
    if (txSnap.empty) {
      console.log(`[${symbol}] no holders.`);
      return null;
    }
    const uids = [...new Set(txSnap.docs.map(d => d.ref.path.split("/")[1]))];
    const nowTS = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(uids.map(uid =>
      db.doc(`users/${uid}/user_data/current_holdings`)
        .set({ force_recalc_timestamp: nowTS }, { merge: true })
    ));
    console.log(`[${symbol}] triggered ${uids.length} users.`);
    return null;
  });

exports.recalculateOnFxUpdate = functions
  .runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore
  .document("exchange_rates/TWD=X")
  .onWrite(async (change, context) => {
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();
    if (before && JSON.stringify(before.rates) === JSON.stringify(after.rates)) {
      console.log("TWD=X metadata only, skip.");
      return null;
    }
    const users = await db.collection("users").listDocuments();
    const nowTS = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(
      users.map(u =>
        db.doc(`users/${u.id}/user_data/current_holdings`)
          .set({ force_recalc_timestamp: nowTS }, { merge: true })
      )
    );
    console.log(`FX updated → triggered ${users.length} users.`);
    return null;
  });

// =================================================================================
// === 市場資料撈取 ================================================================
// =================================================================================
async function getMarketDataFromDb(transactions, log) {
  const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
  const allSymbols = [...new Set([...symbols, "TWD=X"])];
  log(`Required symbols: [${allSymbols.join(", ")}]`);
  const marketData = {};
  for (const symbol of allSymbols) {
    const col = symbol === "TWD=X" ? "exchange_rates" : "price_history";
    const docRef = db.collection(col).doc(symbol);
    const doc = await docRef.get();
    if (doc.exists) {
      log(`Found ${symbol} in Firestore.`);
      marketData[symbol] = doc.data();
    } else {
      log(`Missing ${symbol} → emergency fetch.`);
      const fetched = await fetchAndSaveMarketData(symbol, log);
      if (fetched) {
        marketData[symbol] = fetched;
        await docRef.set(fetched);
        log(`Saved ${symbol} after emergency fetch.`);
      }
    }
  }
  return marketData;
}

async function fetchAndSaveMarketData(symbol, log) {
  try {
    log(`[Fetch] Fetching ${symbol} from Yahoo Finance...`);
    const opts = { period1: "2000-01-01" };
    const hist = await yahooFinance.historical(symbol, opts);
    if (!hist || hist.length === 0) {
      log(`[Fetch] Warning: no data for ${symbol}`);
      return null;
    }
    const prices = {};
    hist.forEach(item => {
      prices[item.date.toISOString().slice(0,10)] = item.close;
    });
    const dividends = (hist.dividends || []).reduce((acc,d) => {
      acc[d.date.toISOString().slice(0,10)] = d.amount;
      return acc;
    }, {});
    const payload = {
      prices,
      splits: {}, // 不再存 splits from yfinance
      dividends,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dataSource: "emergency-fetch-v2"
    };
    if (symbol === "TWD=X") {
      payload.rates = { ...prices };
      delete payload.dividends;
    }
    return payload;
  } catch (e) {
    log(`[Fetch Error] ${symbol}: ${e.message}`);
    return null;
  }
}

// =================================================================================
// === 幫助函式 & 計算邏輯 ========================================================
// =================================================================================

// 過濾 0 元現金流 & 缺乏正／負向流時回傳空
function sanitizeCashflows(cashflows, log) {
  const valid = cashflows.filter(cf =>
    Number.isFinite(cf.amount) && Math.abs(cf.amount) > 1e-9
  );
  const hasPos = valid.some(cf => cf.amount > 0);
  const hasNeg = valid.some(cf => cf.amount < 0);
  if (!hasPos || !hasNeg) {
    log("⚠️  cashflow lacks inflow or outflow → XIRR skip");
    return [];
  }
  return valid;
}

// XIRR + Newton-Raphson + 動態初值 + 收斂失敗回 null
function calculateXIRR(cashflows, log) {
  if (cashflows.length < 2) return null;
  const values = cashflows.map(cf => cf.amount);
  const dates  = cashflows.map(cf => cf.date);
  const years  = dates.map(d => (d.getTime() - dates[0].getTime()) / (1000*60*60*24*365));
  const npv = rate => values.reduce((sum, v, i) => sum + v / Math.pow(1+rate, years[i]), 0);
  const dNpv = rate => values.reduce((sum, v, i) => {
    if (years[i] > 0) return sum - v * years[i] / Math.pow(1+rate, years[i]+1);
    return sum;
  }, 0);

  let rate = 0.1 * Math.sign(values[0] || 1);
  const tol = 1e-6, maxIter = 100;
  for (let i=0; i<maxIter; i++) {
    const f = npv(rate), df = dNpv(rate);
    if (Math.abs(df) < 1e-9) break;
    const next = rate - f/df;
    if (Math.abs(next - rate) < tol) return next;
    rate = next;
  }
  log("⚠️  XIRR failed to converge");
  return null;
}

function calculatePortfolio(transactions, userSplits, marketData, log) {
  // 1. 整理所有事件
  const events = [];
  transactions.forEach(t => events.push({
    ...t,
    date: t.date.toDate ? t.date.toDate() : new Date(t.date),
    eventType: "transaction"
  }));
  userSplits.forEach(s => events.push({
    ...s,
    date: s.date.toDate ? s.date.toDate() : new Date(s.date),
    eventType: "split"
  }));
  Object.entries(marketData)
    .filter(([sym]) => sym !== "TWD=X")
    .forEach(([sym, data]) => {
      Object.entries(data.dividends || {}).forEach(([date, amt]) => {
        events.push({ symbol: sym, date: new Date(date), amount: amt, eventType: "dividend" });
      });
    });
  events.sort((a,b) => a.date - b.date);

  // 2. 計算持倉 & 已實現損益
  const portfolio = {};
  let totalRealizedPL = 0;
  events.forEach(evt => {
    const sym = evt.symbol.toUpperCase();
    if (!portfolio[sym]) portfolio[sym] = { lots: [], currency: "USD" };
    const rateOnDate = findNearestDataPoint(marketData["TWD=X"]?.rates, evt.date);

    switch(evt.eventType) {
      case "transaction":
        const qty = evt.quantity;
        const costOrig = evt.totalCost || (evt.price * qty);
        const costTWD  = costOrig * (evt.currency==="USD"?rateOnDate:1);
        portfolio[sym].currency = evt.currency;
        if (evt.type==="buy") {
          portfolio[sym].lots.push({ quantity: qty, pricePerShareTWD: costTWD/qty, pricePerShareOriginal: costOrig/qty, date: evt.date });
        } else {
          let toSell = qty, costSold=0, saleVal = costTWD;
          while(toSell>0 && portfolio[sym].lots.length>0) {
            const lot = portfolio[sym].lots[0];
            if (lot.quantity<=toSell) {
              costSold += lot.quantity * lot.pricePerShareTWD;
              toSell -= lot.quantity;
              portfolio[sym].lots.shift();
            } else {
              costSold += toSell * lot.pricePerShareTWD;
              lot.quantity -= toSell;
              toSell = 0;
            }
          }
          totalRealizedPL += (saleVal - costSold);
        }
        break;

      case "split":
        portfolio[sym].lots.forEach(lot => {
          lot.quantity *= evt.ratio;
          lot.pricePerShareOriginal /= evt.ratio;
          lot.pricePerShareTWD /= evt.ratio;
        });
        break;

      case "dividend":
        const shares = portfolio[sym].lots.reduce((s,l)=>s+l.quantity,0);
        const divTWD = evt.amount * shares * (portfolio[sym].currency==="USD"?rateOnDate:1);
        totalRealizedPL += divTWD;
        evt.dividendTWD = divTWD; // 存下來給 XIRR 重用
        log(`Dividend ${sym} on ${evt.date.toISOString().slice(0,10)} = ${divTWD.toFixed(2)} TWD`);
        break;
    }
  });

  // 3. 計算期末持倉、未實現損益
  const finalHoldings = calculateFinalHoldings(portfolio, marketData);
  const totalUnrealizedPL = Object.values(finalHoldings)
    .reduce((sum,h)=>(sum + (h.unrealizedPLTWD||0)), 0);

  // 4. 歷史、市值、XIRR
  const portfolioHistory = calculatePortfolioHistory(events, marketData);
  let cashflows = createCashflows(events, portfolio, finalHoldings, marketData);
  cashflows = sanitizeCashflows(cashflows, log);
  const xirr = calculateXIRR(cashflows, log);

  return { holdings: finalHoldings, totalRealizedPL, totalUnrealizedPL, portfolioHistory, xirr };
}

// XIRR 用現金流：交易 + 股息 + 期末市值
function createCashflows(events, portfolio, finalHoldings, marketData) {
  const cf = [];
  // 交易
  events.filter(e=>e.eventType==="transaction").forEach(t => {
    const rate = findNearestDataPoint(marketData["TWD=X"]?.rates, t.date);
    const amt = (t.totalCost || t.price*t.quantity) * (t.currency==="USD"?rate:1);
    cf.push({ date: new Date(t.date), amount: t.type==="buy" ? -amt : amt });
  });
  // 股息
  events.filter(e=>e.eventType==="dividend").forEach(d => {
    const amt = d.dividendTWD||0;
    if (Math.abs(amt)>1e-9) cf.push({ date: new Date(d.date), amount: amt });
  });
  // 期末市值
  const totalMV = Object.values(finalHoldings)
    .reduce((s,h)=>s + h.marketValueTWD, 0);
  if (totalMV>1e-9) {
    cf.push({ date: new Date(), amount: totalMV });
  }
  return cf;
}

function calculatePortfolioHistory(events, marketData) {
  const history = {};
  const txs = events.filter(e=>e.eventType==="transaction");
  if (!txs.length) return history;
  const start = new Date(txs[0].date);
  start.setUTCHours(0,0,0,0);
  const today = new Date();
  let cursor = new Date(start);
  while (cursor <= today) {
    const ds = cursor.toISOString().slice(0,10);
    const state = getPortfolioStateOnDate(events, cursor);
    history[ds] = calculateDailyMarketValue(state, marketData, cursor);
    cursor.setDate(cursor.getDate()+1);
  }
  return history;
}

function getPortfolioStateOnDate(allEvents, targetDate) {
  const state = {};
  const splits = allEvents.filter(e=>e.eventType==="split");
  const relevant = allEvents.filter(e=>e.date<=targetDate);
  relevant.forEach(evt => {
    const sym = evt.symbol.toUpperCase();
    if (!state[sym]) state[sym] = { lots: [], currency:"USD" };
    switch(evt.eventType) {
      case "transaction":
        state[sym].currency = evt.currency;
        if (evt.type==="buy") {
          state[sym].lots.push({ quantity: evt.quantity, date: evt.date });
        } else {
          let toSell=evt.quantity;
          while(toSell>0 && state[sym].lots.length>0) {
            const lot = state[sym].lots[0];
            if (lot.quantity<=toSell) {
              toSell -= lot.quantity;
              state[sym].lots.shift();
            } else {
              lot.quantity -= toSell;
              toSell=0;
            }
          }
        }
        break;
      case "split":
        state[sym].lots.forEach(lot => {
          lot.quantity *= evt.ratio;
        });
        break;
    }
  });
  // 未來 split forward-adjust
  Object.keys(state).forEach(sym => {
    splits.filter(s=>s.symbol.toUpperCase()===sym && s.date>targetDate)
      .forEach(s => {
        state[sym].lots.forEach(lot => { lot.quantity*=s.ratio; });
      });
  });
  return state;
}

function calculateDailyMarketValue(portfolio, marketData, date) {
  let total = 0;
  const rate = findNearestDataPoint(marketData["TWD=X"]?.rates, date);
  Object.entries(portfolio).forEach(([sym, h]) => {
    const qty = h.lots.reduce((s,l)=>s+l.quantity,0);
    if (qty>0) {
      const price = findNearestDataPoint(marketData[sym]?.prices, date);
      total += qty * price * (h.currency==="USD"?rate:1);
    }
  });
  return total;
}

function calculateFinalHoldings(portfolio, marketData) {
  const result = {};
  const today = new Date();
  const rate = findNearestDataPoint(marketData["TWD=X"]?.rates, today);
  Object.entries(portfolio).forEach(([sym, h]) => {
    const qty = h.lots.reduce((s,l)=>s+l.quantity,0);
    if (qty>1e-9) {
      const totalCostTWD = h.lots.reduce((s,l)=>s + l.quantity*l.pricePerShareTWD,0);
      const totalCostOrig = h.lots.reduce((s,l)=>s + l.quantity*l.pricePerShareOriginal,0);
      const price = findNearestDataPoint(marketData[sym]?.prices, today);
      const mvTWD = qty * price * (h.currency==="USD"?rate:1);
      const unrealized = mvTWD - totalCostTWD;
      result[sym] = {
        symbol: sym,
        quantity: qty,
        avgCostOriginal: totalCostOrig/qty,
        totalCostTWD: totalCostTWD,
        currency: h.currency,
        currentPriceOriginal: price,
        marketValueTWD: mvTWD,
        unrealizedPLTWD: unrealized,
        returnRate: totalCostTWD>0 ? unrealized/totalCostTWD*100 : 0
      };
    }
  });
  return result;
}

// 增強版：回溯 LOOKBACK_DAYS 天內找不到就退到最近且警告
function findNearestDataPoint(history, targetDate) {
  if (!history || typeof history !== "object") return 1;
  const d = new Date(targetDate);
  d.setUTCHours(12,0,0,0);
  for (let i=0; i<LOOKBACK_DAYS; i++) {
    const dd = new Date(d); dd.setDate(dd.getDate()-i);
    const key = dd.toISOString().slice(0,10);
    if (history[key] != null) return history[key];
  }
  // fallback
  const dates = Object.keys(history).sort();
  const todayKey = d.toISOString().slice(0,10);
  let pick = null;
  for (const dt of dates) {
    if (dt <= todayKey) pick = dt;
    else break;
  }
  if (pick) {
    console.warn(`Fallback ${targetDate.toISOString().slice(0,10)} → using ${pick}`);
    return history[pick];
  }
  console.warn(`No data for ${targetDate.toISOString().slice(0,10)}, default to 1`);
  return 1;
}
