/* functions/index.js ─ 完整版本 */

const functions = require("firebase-functions");
const admin     = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// ============================================================================
//  Core calculation
// ============================================================================
async function performRecalculation(userId) {
  const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
  const logs   = [];
  const log = (msg) => {
    const ts = new Date().toISOString();
    logs.push(`${ts}: ${msg}`);
    console.log(`[${userId}] ${ts}: ${msg}`);
  };

  try {
    log('--- Recalculation triggered (v32) ---');

    const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
    const historyDocRef  = db.doc(`users/${userId}/user_data/portfolio_history`);

    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);

    const transactions = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
    const userSplits   = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    if (transactions.length === 0) {
      log('No transactions found. Clearing data.');
      await Promise.all([
        holdingsDocRef.set({
          holdings: {},
          totalRealizedPL: 0,
          xirr: 0,
          lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
          force_recalc_timestamp: admin.firestore.FieldValue.delete()
        }),
        historyDocRef.set({ history: {}, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
      ]);
      return;
    }

    const marketData = await getMarketDataFromDb(transactions, log);
    if (!marketData || Object.keys(marketData).length === 0) {
      throw new Error('Market data empty.');
    }

    const { holdings, totalRealizedPL, portfolioHistory, xirr } =
      calculatePortfolio(transactions, userSplits, marketData, log);

    log(`Calc done. Holdings ${Object.keys(holdings).length}, RealizedPL ${totalRealizedPL}, XIRR ${xirr}`);

    await Promise.all([
      holdingsDocRef.set({
        holdings,
        totalRealizedPL,
        xirr,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        force_recalc_timestamp: admin.firestore.FieldValue.delete()
      }, { merge: true }),
      historyDocRef.set({
        history: portfolioHistory,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
      })
    ]);

    log('--- Recalculation finished ---');
  } catch (err) {
    console.error(`[${userId}] ERROR`, err);
    log(`ERROR: ${err.message}`);
  } finally {
    await logRef.set({ entries: logs });
  }
}

// ============================================================================
//  Firestore triggers
// ============================================================================

// 1. 主要持股文件 ─ onWrite (create & update 都觸發)
exports.recalculatePortfolio = functions
  .runWith({ timeoutSeconds: 300, memory: '1GB' })
  .firestore
  .document('users/{userId}/user_data/current_holdings')
  .onWrite(async (change, context) => {
    const userId = context.params.userId;

    // 刪除文件不處理
    if (!change.after.exists) return null;

    const isCreated = !change.before.exists;
    const before    = change.before.exists ? change.before.data() : {};
    const after     = change.after.data();

    if (isCreated) {
      console.log(`[${userId}] current_holdings created → first-time recalc`);
      await performRecalculation(userId);
      return null;
    }

    const tsChanged =
      after.force_recalc_timestamp &&
      (!before.force_recalc_timestamp ||
       after.force_recalc_timestamp.toMillis() !==
       before.force_recalc_timestamp.toMillis());

    if (tsChanged) {
      console.log(`[${userId}] timestamp changed → recalc`);
      await performRecalculation(userId);
    }
    return null;
  });

// 2. 交易、拆股 → 寫 timestamp 觸發
exports.recalculateOnTransaction = functions.firestore
  .document('users/{userId}/transactions/{tid}')
  .onWrite(async (_, ctx) => {
    await db.doc(`users/${ctx.params.userId}/user_data/current_holdings`)
            .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
  });

exports.recalculateOnSplit = functions.firestore
  .document('users/{userId}/splits/{sid}')
  .onWrite(async (_, ctx) => {
    await db.doc(`users/${ctx.params.userId}/user_data/current_holdings`)
            .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
  });

// 3. 價格／配息文件更新 → 找出受影響用戶
exports.recalculateOnPriceUpdate = functions
  .runWith({ timeoutSeconds: 240, memory: '1GB' })
  .firestore
  .document('price_history/{symbol}')
  .onWrite(async (change, ctx) => {
    const symbol = ctx.params.symbol.toUpperCase();
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();

    if (before &&
        JSON.stringify(before.prices)    === JSON.stringify(after.prices) &&
        JSON.stringify(before.dividends) === JSON.stringify(after.dividends)) {
      console.log(`[${symbol}] metadata only, skip`);
      return null;
    }

    const txSnap = await db.collectionGroup('transactions')
                           .where('symbol', '==', symbol).get();
    if (txSnap.empty) {
      console.log(`[${symbol}] no affected users`);
      return null;
    }

    const nowTS = admin.firestore.FieldValue.serverTimestamp();
    const users = [...new Set(txSnap.docs.map(d => d.ref.path.split('/')[1]))];

    await Promise.all(
      users.map(uid =>
        db.doc(`users/${uid}/user_data/current_holdings`)
          .set({ force_recalc_timestamp: nowTS }, { merge: true })
      )
    );
    console.log(`[${symbol}] triggered ${users.length} users`);
    return null;
  });

// 4. 匯率文件更新 → 全體用戶重算
exports.recalculateOnFxUpdate = functions
  .runWith({ timeoutSeconds: 240, memory: '1GB' })
  .firestore
  .document('exchange_rates/TWD=X')
  .onWrite(async (change) => {
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();

    if (before && JSON.stringify(before.rates) === JSON.stringify(after.rates)) {
      console.log('TWD=X metadata only, skip');
      return null;
    }

    const users = await db.collection('users').listDocuments();
    const nowTS = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(
      users.map(u =>
        db.doc(`users/${u.id}/user_data/current_holdings`)
          .set({ force_recalc_timestamp: nowTS }, { merge: true })
      )
    );
    console.log(`FX updated → triggered ${users.length} users`);
    return null;
  });

// ============================================================================
//  Market-data helpers
// ============================================================================
async function getMarketDataFromDb(transactions, log) {
  const symbols    = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
  const allSymbols = [...new Set([...symbols, 'TWD=X'])];
  log(`Required symbols: [${allSymbols.join(', ')}]`);

  const marketData = {};
  for (const symbol of allSymbols) {
    const col = symbol === 'TWD=X' ? 'exchange_rates' : 'price_history';
    const doc = await db.collection(col).doc(symbol).get();

    if (doc.exists) {
      log(`Found ${symbol} in Firestore`);
      marketData[symbol] = doc.data();
    } else {
      log(`${symbol} missing → emergency fetch`);
      const fetched = await fetchAndSaveMarketData(symbol, log);
      if (fetched) {
        marketData[symbol] = fetched;
        await db.collection(col).doc(symbol).set(fetched);
      }
    }
  }
  return marketData;
}

async function fetchAndSaveMarketData(symbol, log) {
  try {
    log(`[Fetch] ${symbol}`);
    const hist = await yahooFinance.historical(symbol, { period1: '2000-01-01' });
    if (!hist || hist.length === 0) return null;

    const prices = {};
    hist.forEach(p => { prices[p.date.toISOString().split('T')[0]] = p.close; });

    const payload = {
      prices,
      splits: {},
      dividends: (hist.dividends || []).reduce(
        (acc, d) => ({ ...acc, [d.date.toISOString().split('T')[0]]: d.amount }), {}),
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dataSource: 'emergency-fetch'
    };
    if (symbol === 'TWD=X') {
      payload.rates = payload.prices;
      delete payload.dividends;
    }
    return payload;
  } catch (e) {
    log(`[Fetch ERROR] ${symbol} ${e.message}`);
    return null;
  }
}

// ============================================================================
//  Portfolio / XIRR helpers
// ============================================================================
function calculateXIRR(cashflows) {
  if (cashflows.length < 2) return 0;

  const values = cashflows.map(c => c.amount);
  const dates  = cashflows.map(c => c.date);

  const MS_PER_YEAR = 1000 * 60 * 60 * 24 * 365;
  const base = dates[0].getTime();
  const yrs  = dates.map(d => (d.getTime() - base) / MS_PER_YEAR);

  const npv = r => values.reduce((s, v, i) => s + v / Math.pow(1 + r, yrs[i]), 0);
  const dNp = r => values.reduce((s, v, i) => s - v * yrs[i] / Math.pow(1 + r, yrs[i] + 1), 0);

  let r = 0.1;
  for (let i = 0; i < 100; i++) {
    const f  = npv(r);
    const fp = dNp(r);
    if (Math.abs(fp) < 1e-9) return 0;
    const r2 = r - f / fp;
    if (Math.abs(r2 - r) < 1e-6) return r2;
    r = r2;
  }
  return r;
}

function calculatePortfolio(transactions, userSplits, marketData, log) {
  const events   = [];
  const symbols  = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

  transactions.forEach(t =>
    events.push({ ...t, date: t.date.toDate ? t.date.toDate() : new Date(t.date), eventType: 'transaction' }));
  userSplits.forEach(s =>
    events.push({ ...s, date: s.date.toDate ? s.date.toDate() : new Date(s.date), eventType: 'split' }));

  symbols.forEach(sym => {
    const data = marketData[sym];
    if (!data) return;
    Object.entries(data.dividends || {}).forEach(([d, amt]) =>
      events.push({ date: new Date(d), symbol: sym, amount: amt, eventType: 'dividend' }));
  });

  events.sort((a, b) => new Date(a.date) - new Date(b.date));

  const portfolio = {};
  let totalRealizedPL = 0;

  for (const ev of events) {
    const sym = ev.symbol.toUpperCase();
    if (!portfolio[sym]) portfolio[sym] = { lots: [], currency: 'USD' };

    const rateHist = marketData['TWD=X']?.rates || {};
    const rateOnDt = findNearestDataPoint(rateHist, ev.date);

    switch (ev.eventType) {
      case 'transaction': {
        const t = ev;
        portfolio[sym].currency = t.currency;
        const costOriginal = t.totalCost || t.price;
        const costTWD      = costOriginal * (t.currency === 'USD' ? rateOnDt : 1);

        if (t.type === 'buy') {
          portfolio[sym].lots.push({
            quantity: t.quantity,
            pricePerShareOriginal: costOriginal,
            pricePerShareTWD: costTWD,
            date: ev.date
          });
        } else {
          // sell
          let qty = t.quantity;
          const saleTWD = (t.totalCost || t.quantity * t.price) *
                          (t.currency === 'USD' ? rateOnDt : 1);
          let costSoldTWD = 0;
          while (qty > 0 && portfolio[sym].lots.length) {
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
          totalRealizedPL += saleTWD - costSoldTWD;
        }
        break;
      }

      case 'split':
        portfolio[sym].lots.forEach(lot => {
          lot.quantity            *= ev.ratio;
          lot.pricePerShareTWD    /= ev.ratio;
          lot.pricePerShareOriginal/= ev.ratio;
        });
        break;

      case 'dividend': {
        const shares = portfolio[sym].lots.reduce((s, l) => s + l.quantity, 0);
        const divTWD = ev.amount * shares *
                       (portfolio[sym].currency === 'USD' ? rateOnDt : 1);
        totalRealizedPL += divTWD;
        break;
      }
    }
  }

  const finalHoldings   = calculateFinalHoldings(portfolio, marketData);
  const portfolioHistory= calculatePortfolioHistory(events, marketData);
  const cashflows       = createCashflows(events, portfolio, finalHoldings, marketData);
  const xirr            = calculateXIRR(cashflows);

  return { holdings: finalHoldings, totalRealizedPL, portfolioHistory, xirr };
}

function createCashflows(events, portfolio, finalHoldings, marketData) {
  const cashflows = [];

  // transactions
  events.filter(e => e.eventType === 'transaction').forEach(t => {
    const rateHist = marketData['TWD=X']?.rates || {};
    const rateOnDt = findNearestDataPoint(rateHist, t.date);

    const signed = t.type === 'buy' ? -1 : 1;
    const raw    = t.totalCost || t.quantity * t.price;
    const amt    = Math.abs(raw) * (t.currency === 'USD' ? rateOnDt : 1) * signed;

    cashflows.push({ date: new Date(t.date), amount: amt });
  });

  // dividends
  events.filter(e => e.eventType === 'dividend').forEach(d => {
    const rateHist = marketData['TWD=X']?.rates || {};
    const rateOnDt = findNearestDataPoint(rateHist, d.date);

    const cur = portfolio[d.symbol]?.currency || 'USD';
    const shares = portfolio[d.symbol]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
    const amt = d.amount * shares * (cur === 'USD' ? rateOnDt : 1);

    if (amt !== 0) cashflows.push({ date: new Date(d.date), amount: amt });
  });

  // final market value (could be 0)
  const mv = Object.values(finalHoldings).reduce((s, h) => s + h.marketValueTWD, 0);
  cashflows.push({ date: new Date(), amount: mv });

  return cashflows;
}

// ===== Helpers for daily values =================================================
function calculatePortfolioHistory(events, marketData) {
  const history = {};
  const txEvents = events.filter(e => e.eventType === 'transaction');
  if (txEvents.length === 0) return history;

  const first = new Date(txEvents[0].date);
  const today = new Date();
  let cur = new Date(first);
  cur.setUTCHours(0,0,0,0);

  while (cur <= today) {
    const dStr = cur.toISOString().split('T')[0];
    const state = getPortfolioStateOnDate(events, cur);
    history[dStr] = calculateDailyMarketValue(state, marketData, cur);
    cur.setDate(cur.getDate() + 1);
  }
  return history;
}

function getPortfolioStateOnDate(events, target) {
  const state = {};
  const relevant = events.filter(e => new Date(e.date) <= target);
  const allSplits= events.filter(e => e.eventType === 'split');

  for (const ev of relevant) {
    const sym = ev.symbol.toUpperCase();
    if (!state[sym]) state[sym] = { lots: [], currency: 'USD' };

    switch (ev.eventType) {
      case 'transaction': {
        const t = ev;
        state[sym].currency = t.currency;
        if (t.type === 'buy') {
          state[sym].lots.push({ quantity: t.quantity });
        } else {
          let qty = t.quantity;
          while (qty > 0 && state[sym].lots.length) {
            const lot = state[sym].lots[0];
            if (lot.quantity <= qty) {
              qty -= lot.quantity;
              state[sym].lots.shift();
            } else {
              lot.quantity -= qty;
              qty = 0;
            }
          }
        }
        break;
      }
      case 'split':
        state[sym].lots.forEach(l => { l.quantity *= ev.ratio; });
        break;
    }
  }

  // future splits adjustment
  for (const sym in state) {
    const futSpl = allSplits.filter(s => s.symbol.toUpperCase() === sym && new Date(s.date) > target);
    futSpl.forEach(spl => {
      state[sym].lots.forEach(l => { l.quantity *= spl.ratio; });
    });
  }
  return state;
}

function calculateDailyMarketValue(portfolio, marketData, date) {
  let total = 0;
  const fx = findNearestDataPoint(marketData['TWD=X']?.rates || {}, date);

  for (const sym in portfolio) {
    const qty = portfolio[sym].lots.reduce((s, l) => s + l.quantity, 0);
    if (qty === 0) continue;
    const priceHist = marketData[sym]?.prices || {};
    const price = findNearestDataPoint(priceHist, date);
    const rate  = portfolio[sym].currency === 'USD' ? fx : 1;
    total += qty * price * rate;
  }
  return total;
}

function calculateFinalHoldings(portfolio, marketData) {
  const res = {};
  const today = new Date();
  const fx = findNearestDataPoint(marketData['TWD=X']?.rates || {}, today);

  for (const sym in portfolio) {
    const qty = portfolio[sym].lots.reduce((s, l) => s + l.quantity, 0);
    if (qty < 1e-9) continue;

    const totalCostTWD = portfolio[sym].lots
      .reduce((s, l) => s + l.quantity * l.pricePerShareTWD, 0);
    const totalCostOrig = portfolio[sym].lots
      .reduce((s, l) => s + l.quantity * l.pricePerShareOriginal, 0);

    const priceHist = marketData[sym]?.prices || {};
    const lastPrice = findNearestDataPoint(priceHist, today);
    const rate = portfolio[sym].currency === 'USD' ? fx : 1;
    const mvTWD = qty * lastPrice * rate;

    res[sym] = {
      symbol: sym,
      quantity: qty,
      avgCostOriginal: totalCostOrig / qty,
      totalCostTWD,
      currency: portfolio[sym].currency,
      currentPriceOriginal: lastPrice,
      marketValueTWD: mvTWD,
      unrealizedPLTWD: mvTWD - totalCostTWD,
      returnRate: totalCostTWD ? (mvTWD - totalCostTWD) / totalCostTWD * 100 : 0
    };
  }
  return res;
}

// 找最近資料點（往前 7 天）
function findNearestDataPoint(history, targetDate) {
  if (!history || Object.keys(history).length === 0) return 1;
  const d = new Date(targetDate);
  d.setUTCHours(12,0,0,0);

  for (let i = 0; i < 7; i++) {
    const s = new Date(d);
    s.setDate(s.getDate() - i);
    const key = s.toISOString().split('T')[0];
    if (history[key] !== undefined) return history[key];
  }

  const keys = Object.keys(history).sort();
  const tgt  = d.toISOString().split('T')[0];
  let closest = null;
  for (const k of keys) {
    if (k <= tgt) closest = k; else break;
  }
  return closest ? history[closest] : 1;
}
