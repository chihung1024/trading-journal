/*  index.js  ── 只改這一檔即可  */
const functions = require("firebase-functions");
const admin     = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

/* ------------------------------------------------------------------ *
 *  0. 參數（硬編碼，避免依賴其他檔案）                                *
 * ------------------------------------------------------------------ */
const LOOKBACK_DAYS      = 7;                 // 股價 / 匯率回溯天數
const LOCK_DURATION_MS   = 5 * 60 * 1000;     // 同一使用者 5 分鐘內只跑一次

/* ------------------------------------------------------------------ *
 *  1. 主計算流程                                                     *
 * ------------------------------------------------------------------ */
async function performRecalculation (userId) {
  const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
  const lockRef = db.doc(`users/${userId}/user_data/recalc_lock`);
  const logs = [];
  const log = (msg) => {
    const ts = new Date().toISOString();
    logs.push(`${ts}: ${msg}`);
    console.log(`[${userId}] ${ts}: ${msg}`);
  };

  let lockAcquired = false;

  try {
    /* ----- 1-1. 取得分散式鎖 ------------------------------------ */
    await db.runTransaction(async tx => {
      const snap = await tx.get(lockRef);
      const now  = Date.now();
      if (snap.exists) {
        const lockedAt = snap.data().lockedAt.toMillis();
        if (now - lockedAt < LOCK_DURATION_MS) {
          throw new Error("Recalculation already in progress");
        }
      }
      tx.set(lockRef, { lockedAt: admin.firestore.Timestamp.now() });
      lockAcquired = true;
    });

    log("=== Recalculation started ===");

    /* ----- 1-2. 讀取交易與拆股 ---------------------------------- */
    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);

    const transactions = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
    const userSplits   = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    if (transactions.length === 0) {
      log("No transactions → 清空資料");
      await Promise.all([
        db.doc(`users/${userId}/user_data/current_holdings`).set({
          holdings           : {},
          totalRealizedPL    : 0,
          totalUnrealizedPL  : 0,
          lastUpdated        : admin.firestore.FieldValue.serverTimestamp()
        }),
        db.doc(`users/${userId}/user_data/portfolio_history`).set({
          history    : {},
          lastUpdated: admin.firestore.FieldValue.serverTimestamp()
        })
      ]);
      return;
    }

    /* ----- 1-3. 取得市場資料 ------------------------------------ */
    const marketData = await getMarketDataFromDb(transactions, log);
    if (!marketData || Object.keys(marketData).length === 0) {
      throw new Error("Market data empty");
    }

    /* ----- 1-4. 計算投資組合 ------------------------------------ */
    const {
      holdings,
      totalRealizedPL,
      totalUnrealizedPL,
      portfolioHistory,
      xirr
    } = calculatePortfolio(transactions, userSplits, marketData, log);

    log(`完畢 → Realized ${totalRealizedPL.toFixed(2)}, `
        + `Unrealized ${totalUnrealizedPL.toFixed(2)}, `
        + `XIRR ${xirr == null ? "N/A" : (xirr*100).toFixed(2)+"%"}`);

    /* ----- 1-5. 寫回 Firestore ---------------------------------- */
    const holdingsRef = db.doc(`users/${userId}/user_data/current_holdings`);
    const historyRef  = db.doc(`users/${userId}/user_data/portfolio_history`);

    const payload = {
      holdings,
      totalRealizedPL,
      totalUnrealizedPL,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      force_recalc_timestamp: admin.firestore.FieldValue.delete()
    };
    if (xirr != null) payload.xirr = xirr;

    await Promise.all([
      holdingsRef.set(payload, { merge: true }),
      historyRef.set({
        history: portfolioHistory,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
      })
    ]);

    log("=== Recalculation finished ===");
  }
  catch (e) {
    if (e.message !== "Recalculation already in progress")
      console.error(`[${userId}] ERROR`, e);
    log(`ERROR: ${e.message}`);
  }
  finally {
    if (lockAcquired) await lockRef.delete().catch(()=>{});
    await logRef.set({ entries: logs });
  }
}

/* ------------------------------------------------------------------ *
 *  2. Firestore 觸發器（與原路徑完全一致）                           *
 * ------------------------------------------------------------------ */
exports.recalculatePortfolio = functions
  .runWith({ timeoutSeconds: 300, memory: "1GB" })
  .firestore
  .document("users/{userId}/user_data/current_holdings")
  .onUpdate(async (change, ctx) => {
    const before = change.before.data();
    const after  = change.after.data();
    if (after.force_recalc_timestamp
        && before.force_recalc_timestamp !== after.force_recalc_timestamp) {
      await performRecalculation(ctx.params.userId);
    }
  });

exports.recalculateOnTransaction = functions.firestore
  .document("users/{userId}/transactions/{transactionId}")
  .onWrite(async (change, ctx) => {
    const ref = db.doc(`users/${ctx.params.userId}/user_data/current_holdings`);
    await ref.set(
      { force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() },
      { merge: true }
    );
  });

exports.recalculateOnSplit = functions.firestore
  .document("users/{userId}/splits/{splitId}")
  .onWrite(async (change, ctx) => {
    const ref = db.doc(`users/${ctx.params.userId}/user_data/current_holdings`);
    await ref.set(
      { force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() },
      { merge: true }
    );
  });

exports.recalculateOnPriceUpdate = functions
  .runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore
  .document("price_history/{symbol}")
  .onWrite(async (change, ctx) => {
    const symbol = ctx.params.symbol.toUpperCase();
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();

    if (before
        && JSON.stringify(before.prices)    === JSON.stringify(after.prices)
        && JSON.stringify(before.dividends) === JSON.stringify(after.dividends)) {
      return null;        // 只有 metadata 變化
    }

    const txSnap = await db.collectionGroup("transactions")
                           .where("symbol", "==", symbol)
                           .get();
    if (txSnap.empty) return null;

    const uids = [...new Set(txSnap.docs.map(d => d.ref.path.split("/")[1]))];
    const ts   = admin.firestore.FieldValue.serverTimestamp();

    await Promise.all(
      uids.map(uid =>
        db.doc(`users/${uid}/user_data/current_holdings`)
          .set({ force_recalc_timestamp: ts }, { merge: true })
      )
    );
    return null;
  });

exports.recalculateOnFxUpdate = functions
  .runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore
  .document("exchange_rates/TWD=X")
  .onWrite(async (change) => {
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();
    if (before && JSON.stringify(before.rates) === JSON.stringify(after.rates))
      return null;

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

/* ------------------------------------------------------------------ *
 *  3. 市場資料讀取                                                   *
 * ------------------------------------------------------------------ */
async function getMarketDataFromDb (transactions, log) {
  const symbols     = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
  const allSymbols  = [...new Set([...symbols, "TWD=X"])];
  const marketData  = {};

  for (const sym of allSymbols) {
    const col    = sym === "TWD=X" ? "exchange_rates" : "price_history";
    const docRef = db.collection(col).doc(sym);
    const snap   = await docRef.get();

    if (snap.exists) {
      marketData[sym] = snap.data();
    } else {
      log(`Missing ${sym} → fetch from YF`);
      const fetched = await fetchAndSaveMarketData(sym, log);
      if (fetched) {
        marketData[sym] = fetched;
        await docRef.set(fetched);
      }
    }
  }
  return marketData;
}

async function fetchAndSaveMarketData (symbol, log) {
  try {
    const hist = await yahooFinance.historical(symbol, { period1: "2000-01-01" });
    if (!hist || !hist.length) return null;

    const prices = {};
    hist.forEach(i => { prices[i.date.toISOString().slice(0,10)] = i.close; });

    const dividends = (hist.dividends || []).reduce((acc, d) => {
      acc[d.date.toISOString().slice(0,10)] = d.amount; return acc;
    }, {});

    const payload = {
      prices,
      dividends,
      splits: {},
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dataSource : "emergency-fetch"
    };

    if (symbol === "TWD=X") {
      payload.rates = prices;
      delete payload.dividends;
    }
    return payload;
  } catch (e) {
    log(`Fetch error ${symbol}: ${e.message}`);
    return null;
  }
}

/* ------------------------------------------------------------------ *
 *  4. 投資組合計算                                                   *
 * ------------------------------------------------------------------ */
function calculatePortfolio (transactions, splits, marketData, log) {
  /* 4-1. 整理事件 */
  const events = [];

  transactions.forEach(t =>
    events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType:"transaction" })
  );
  splits.forEach(s =>
    events.push({ ...s, date: s.date.toDate ? s.date.toDate() : new Date(s.date), eventType:"split" })
  );
  Object.entries(marketData)
    .filter(([s]) => s !== "TWD=X")
    .forEach(([sym, data]) => {
      Object.entries(data.dividends || {}).forEach(([d, amt]) => {
        events.push({ symbol:sym, date:new Date(d), amount:amt, eventType:"dividend" });
      });
    });

  events.sort((a,b) => a.date - b.date);

  /* 4-2. 持倉與已實現損益 */
  const portfolio = {};
  let totalRealizedPL = 0;

  events.forEach(evt => {
    const s = evt.symbol.toUpperCase();
    if (!portfolio[s]) portfolio[s] = { lots:[], currency:"USD" };
    const fx = findNearestDataPoint(marketData["TWD=X"]?.rates, evt.date);

    switch (evt.eventType) {
      case "transaction": {
        const qty       = evt.quantity;
        const costOrig  = evt.totalCost || evt.price * qty;
        const costTWD   = costOrig * (evt.currency==="USD"?fx:1);
        portfolio[s].currency = evt.currency;

        if (evt.type === "buy") {
          portfolio[s].lots.push({
            quantity           : qty,
            pricePerShareTWD   : costTWD/qty,
            pricePerShareOrig  : costOrig/qty,
            date               : evt.date
          });
        } else { // sell
          let remain = qty, costSold = 0;
          while (remain > 0 && portfolio[s].lots.length) {
            const lot = portfolio[s].lots[0];
            if (lot.quantity <= remain) {
              costSold += lot.quantity * lot.pricePerShareTWD;
              remain   -= lot.quantity;
              portfolio[s].lots.shift();
            } else {
              costSold += remain * lot.pricePerShareTWD;
              lot.quantity -= remain;
              remain = 0;
            }
          }
          totalRealizedPL += costTWD - costSold;
        }
        break;
      }
      case "split":
        portfolio[s].lots.forEach(lot => {
          lot.quantity          *= evt.ratio;
          lot.pricePerShareOrig /= evt.ratio;
          lot.pricePerShareTWD  /= evt.ratio;
        });
        break;
      case "dividend": {
        const shares = portfolio[s].lots.reduce((sum,l)=>sum+l.quantity,0);
        const amtTWD = evt.amount * shares * (portfolio[s].currency==="USD"?fx:1);
        totalRealizedPL += amtTWD;
        evt.dividendTWD = amtTWD;            // 傳下去給 XIRR 使用
        break;
      }
    }
  });

  /* 4-3. 期末持倉與未實現損益 */
  const holdings        = calculateFinalHoldings(portfolio, marketData);
  const totalUnrealized = Object.values(holdings)
                                 .reduce((s,h)=>s+h.unrealizedPLTWD, 0);

  /* 4-4. 歷史 & XIRR */
  const history   = calculatePortfolioHistory(events, marketData);
  let cashflows   = createCashflows(events, holdings, marketData);
  cashflows       = sanitizeCashflows(cashflows, log);
  const xirr      = calculateXIRR(cashflows, log);

  return { holdings, totalRealizedPL, totalUnrealizedPL: totalUnrealized, portfolioHistory:history, xirr };
}

function createCashflows (events, holdings, marketData) {
  const cf = [];

  // 交易
  events.filter(e=>e.eventType==="transaction")
        .forEach(t => {
          const fx  = findNearestDataPoint(marketData["TWD=X"]?.rates, t.date);
          const amt = (t.totalCost || t.price*t.quantity) * (t.currency==="USD"?fx:1);
          cf.push({ date:new Date(t.date), amount: t.type==="buy" ? -amt : amt });
        });

  // 股息
  events.filter(e=>e.eventType==="dividend")
        .forEach(d => {
          const amt = d.dividendTWD || 0;
          if (Math.abs(amt) > 1e-9)
            cf.push({ date:new Date(d.date), amount: amt });
        });

  // 期末市值
  const totalMV = Object.values(holdings).reduce((s,h)=>s+h.marketValueTWD,0);
  if (totalMV > 1e-9) cf.push({ date:new Date(), amount: totalMV });

  return cf;
}

/* ------------------------------------------------------------------ *
 *  5. 工具函式                                                       *
 * ------------------------------------------------------------------ */

/* -- XIRR ----------------------------------------------------------- */
function sanitizeCashflows (arr, log) {
  const valid = arr.filter(c => Number.isFinite(c.amount) && Math.abs(c.amount) > 1e-9);
  const hasPos = valid.some(c=>c.amount>0), hasNeg = valid.some(c=>c.amount<0);
  if (!hasPos || !hasNeg) {
    log("Cashflow lacks inflow or outflow → skip XIRR");
    return [];
  }
  return valid;
}
function calculateXIRR (cashflows, log) {
  if (cashflows.length < 2) return null;

  const vals  = cashflows.map(c=>c.amount);
  const dates = cashflows.map(c=>c.date);
  const years = dates.map(d=>(d - dates[0]) / (1000*60*60*24*365));

  const npv = r => vals.reduce((s,v,i)=>s+v/Math.pow(1+r,years[i]),0);
  const dnpv= r => vals.reduce((s,v,i)=>s - v*years[i]/Math.pow(1+r,years[i]+1),0);

  let r = 0.1 * Math.sign(vals[0]||1);
  for (let i=0;i<100;i++) {
    const f = npv(r), df=dnpv(r);
    if (Math.abs(df)<1e-9) break;
    const next = r - f/df;
    if (Math.abs(next-r) < 1e-6) return next;
    r = next;
  }
  log("XIRR failed to converge");
  return null;
}

/* -- 持倉終值 ------------------------------------------------------ */
function calculateFinalHoldings (portfolio, marketData) {
  const out  = {};
  const today= new Date();
  const fx   = findNearestDataPoint(marketData["TWD=X"]?.rates, today);

  Object.entries(portfolio).forEach(([sym, p]) => {
    const qty = p.lots.reduce((s,l)=>s+l.quantity,0);
    if (qty <= 1e-9) return;

    const totalCostTWD  = p.lots.reduce((s,l)=>s+l.quantity*l.pricePerShareTWD ,0);
    const totalCostOrig = p.lots.reduce((s,l)=>s+l.quantity*l.pricePerShareOrig,0);
    const priceOrig     = findNearestDataPoint(marketData[sym]?.prices, today);
    const mvTWD         = qty * priceOrig * (p.currency==="USD"?fx:1);
    const unrl          = mvTWD - totalCostTWD;

    out[sym] = {
      symbol            : sym,
      quantity          : qty,
      avgCostOriginal   : totalCostOrig/qty,
      totalCostTWD      : totalCostTWD,
      currency          : p.currency,
      currentPriceOriginal: priceOrig,
      marketValueTWD    : mvTWD,
      unrealizedPLTWD   : unrl,
      returnRate        : totalCostTWD>0 ? unrl/totalCostTWD*100 : 0
    };
  });
  return out;
}

/* -- 歷史曲線 ------------------------------------------------------ */
function calculatePortfolioHistory (events, marketData) {
  const txs = events.filter(e=>e.eventType==="transaction");
  if (!txs.length) return {};
  const history = {};
  const start = new Date(txs[0].date); start.setUTCHours(0,0,0,0);
  const today = new Date();
  for (let d=new Date(start); d<=today; d.setDate(d.getDate()+1)) {
    const key = d.toISOString().slice(0,10);
    const state = getPortfolioStateOnDate(events, d);
    history[key] = calculateDailyMarketValue(state, marketData, d);
  }
  return history;
}

function getPortfolioStateOnDate (allEvents, date) {
  const state = {};
  const splits = allEvents.filter(e=>e.eventType==="split");
  allEvents.filter(e=>e.date<=date).forEach(evt => {
    const s = evt.symbol.toUpperCase();
    if (!state[s]) state[s] = { lots:[], currency:"USD" };

    switch (evt.eventType) {
      case "transaction":
        state[s].currency = evt.currency;
        if (evt.type==="buy") {
          state[s].lots.push({ quantity: evt.quantity });
        } else {
          let rem = evt.quantity;
          while (rem>0 && state[s].lots.length) {
            const lot = state[s].lots[0];
            if (lot.quantity<=rem) {
              rem -= lot.quantity;
              state[s].lots.shift();
            } else {
              lot.quantity -= rem; rem=0;
            }
          }
        }
        break;
      case "split":
        state[s].lots.forEach(lot => { lot.quantity *= evt.ratio; });
        break;
    }
  });

  Object.keys(state).forEach(sym => {
    splits.filter(sp=>sp.symbol.toUpperCase()===sym && sp.date>date)
          .forEach(sp => state[sym].lots.forEach(lot => { lot.quantity*=sp.ratio; }));
  });
  return state;
}

function calculateDailyMarketValue (portfolio, marketData, date) {
  let total = 0;
  const fx = findNearestDataPoint(marketData["TWD=X"]?.rates, date);
  Object.entries(portfolio).forEach(([sym, h]) => {
    const qty = h.lots.reduce((s,l)=>s+l.quantity,0);
    if (qty<=0) return;
    const price = findNearestDataPoint(marketData[sym]?.prices, date);
    total += qty * price * (h.currency==="USD"?fx:1);
  });
  return total;
}

/* -- 找最近價格或匯率 -------------------------------------------- */
function findNearestDataPoint (history, date) {
  if (!history) return 1;
  const d = new Date(date); d.setUTCHours(12,0,0,0);
  for (let i=0;i<LOOKBACK_DAYS;i++) {
    const k = new Date(d); k.setDate(k.getDate()-i);
    const key = k.toISOString().slice(0,10);
    if (history[key]!=null) return history[key];
  }
  // fallback 到歷史最近值
  const keys = Object.keys(history).sort();
  const todayKey = d.toISOString().slice(0,10);
  let pick=null;
  for (const k of keys) if (k<=todayKey) pick=k; else break;
  return pick ? history[pick] : 1;
}
