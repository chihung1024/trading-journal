/*  ──────────────────────────────────────────────
 *  index.js  —  Cloud Functions for Portfolio Calc
 *  Version : v32  (2025-07-28)
 *  Author  : <you>
 *  ────────────────────────────────────────────── */

const functions     = require('firebase-functions');
const admin         = require('firebase-admin');
const yahooFinance  = require('yahoo-finance2').default;

admin.initializeApp();
const db = admin.firestore();

/* ──────────────────────────────  UTILITIES  ────────────────────────────── */

/** 安全寫入強制重算時間，避免 2 秒內重複寫入觸發無限迴圈 */
async function markRecalc(uid, prevTS = null) {
  const ref   = db.doc(`users/${uid}/user_data/current_holdings`);
  const nowTS = admin.firestore.Timestamp.now();
  if (
    prevTS &&
    Math.abs(nowTS.seconds - prevTS.seconds) < 2     // 2 秒內視為重複
  ) return;

  await ref.set({ force_recalc_timestamp: nowTS }, { merge: true });
}

/** 依 firestore Timestamp 判斷是否同一筆寫入 */
function sameTimestamp(a, b) {
  return a?.seconds === b?.seconds && a?.nanoseconds === b?.nanoseconds;
}

/* ─────────────────────────  CORE RECALC ENTRY  ────────────────────────── */

exports.recalculatePortfolio = functions
  .runWith({ timeoutSeconds: 300, memory: '1GB' })
  .firestore
  .document('users/{userId}/user_data/current_holdings')
  .onUpdate(async (change, context) => {
    const { userId } = context.params;
    const beforeTS   = change.before.data()?.force_recalc_timestamp;
    const afterTS    = change.after.data()?.force_recalc_timestamp;

    if (!afterTS || sameTimestamp(beforeTS, afterTS)) {
      // 不是我們預期的 “有效” 強制重算
      return null;
    }

    console.log(`[${userId}] recalc triggered @${afterTS.toDate().toISOString()}`);
    await performRecalculation(userId);
    return null;
  });

/* ────────────────  PASSTHROUGH TRIGGERS (Tx / Split / Price / FX) ─────────────── */

exports.recalculateOnTransaction = functions.firestore
  .document('users/{userId}/transactions/{txId}')
  .onWrite(async (snap, ctx) => markRecalc(ctx.params.userId));

exports.recalculateOnSplit = functions.firestore
  .document('users/{userId}/splits/{splitId}')
  .onWrite(async (snap, ctx) => markRecalc(ctx.params.userId));

exports.recalculateOnPriceUpdate = functions
  .runWith({ timeoutSeconds: 240, memory: '1GB' })
  .firestore
  .document('price_history/{symbol}')
  .onWrite(async (change, context) => {
    const symbol = context.params.symbol.toUpperCase();
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();

    if (
      before &&
      JSON.stringify(before.prices)    === JSON.stringify(after.prices) &&
      JSON.stringify(before.dividends) === JSON.stringify(after.dividends)
    ) {
      console.log(`[${symbol}] metadata only → skip`);
      return null;
    }

    // 找出所有持有該股票的使用者
    const txSnap = await db.collectionGroup('transactions')
                           .where('symbol', '==', symbol)
                           .get();
    if (txSnap.empty) {
      console.log(`[${symbol}] no user holds it`);
      return null;
    }

    const uids = [
      ...new Set(txSnap.docs.map(d => d.ref.path.split('/')[1]))
    ];
    await Promise.all(uids.map(uid => markRecalc(uid)));
    console.log(`[${symbol}] triggered ${uids.length} user(s)`);
    return null;
  });

exports.recalculateOnFxUpdate = functions
  .runWith({ timeoutSeconds: 240, memory: '1GB' })
  .firestore
  .document('exchange_rates/TWD=X')
  .onWrite(async (change) => {
    const before = change.before.exists ? change.before.data() : null;
    const after  = change.after.data();

    if (before && JSON.stringify(before.rates) === JSON.stringify(after.rates)) {
      console.log('TWD=X metadata only → skip');
      return null;
    }

    const users = await db.collection('users').listDocuments();
    await Promise.all(users.map(u => markRecalc(u.id)));
    console.log(`FX updated → ${users.length} user(s) queued`);
    return null;
  });

/* ──────────────────────────────  MAIN LOGIC  ───────────────────────────── */

async function performRecalculation(userId) {
  const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
  const logs   = [];
  const log = (msg) => {
    const ts = new Date().toISOString();
    const s  = `[${userId}] ${ts}: ${msg}`;
    logs.push(s);
    console.log(s);
  };

  try {
    log('─── Recalculation start (v32) ───');

    /* 1. 讀取使用者資料 --------------------------------------------------- */
    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);

    const transactions = txSnap.docs .map(d => ({ id: d.id,   ...d.data() }));
    const userSplits   = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    const holdingsDocRef = db.doc(`users/${userId}/user_data/current_holdings`);
    const historyDocRef  = db.doc(`users/${userId}/user_data/portfolio_history`);

    if (!transactions.length) {
      log('No transactions. Wiping holdings/history.');
      await Promise.all([
        holdingsDocRef.set({
          holdings: {},
          totalRealizedPL: 0,
          xirr: null,
          lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
          force_recalc_timestamp: admin.firestore.FieldValue.delete()
        }),
        historyDocRef.set({
          history: {},
          lastUpdated: admin.firestore.FieldValue.serverTimestamp()
        })
      ]);
      return;
    }

    /* 2. 抓行情 (批次 & 有快取) ------------------------------------------- */
    const marketData = await getMarketDataFromDb(transactions, log);
    if (!Object.keys(marketData).length) throw new Error('marketData empty');

    /* 3. 計算 ------------------------------------------------------------- */
    const { holdings, totalRealizedPL, portfolioHistory, xirr } =
      calculatePortfolio(transactions, userSplits, marketData, log);

    log(`Holdings=${Object.keys(holdings).length}  Realized=${totalRealizedPL}  XIRR=${xirr}`);

    /* 4. 儲存 ------------------------------------------------------------- */
    const payload = {
      holdings,
      totalRealizedPL,
      xirr,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      force_recalc_timestamp: admin.firestore.FieldValue.delete()
    };

    await Promise.all([
      holdingsDocRef.set(payload, { merge: true }),
      historyDocRef.set({
        history: portfolioHistory,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
      })
    ]);

    log('─── Recalculation OK ───');
  } catch (err) {
    console.error(`[${userId}] CRITICAL`, err);
    logs.push(`CRITICAL ERROR: ${err.message}\n${err.stack}`);
  } finally {
    await logRef.set({ entries: logs });
  }
}

/* ───────────────────────  MARKET DATA (批次抓＋Searcher) ─────────────────────── */

async function getMarketDataFromDb(transactions, log) {
  const symbols  = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
  const requests = [...new Set([...symbols, 'TWD=X'])];

  const CHUNK = 10;                            // Firestore `in` 陣列上限 10
  const chunks = [];
  for (let i = 0; i < requests.length; i += CHUNK)
    chunks.push(requests.slice(i, i + CHUNK));

  const results = {};

  for (const chunk of chunks) {
    const priceColl = db.collection('price_history');
    const fxColl    = db.collection('exchange_rates');

    const priceIds = chunk.filter(s => s !== 'TWD=X');
    const fxIds    = chunk.filter(s => s === 'TWD=X');

    // parallel fetch both collections
    const [priceSnap, fxSnap] = await Promise.all([
      priceIds.length
        ? priceColl.where(admin.firestore.FieldPath.documentId(), 'in', priceIds).get()
        : Promise.resolve({ docs: [] }),
      fxIds.length
        ? fxColl.where(admin.firestore.FieldPath.documentId(), 'in', fxIds).get()
        : Promise.resolve({ docs: [] })
    ]);

    [...priceSnap.docs, ...fxSnap.docs].forEach(doc => {
      const d = doc.data();
      results[doc.id] = attachSearchers(d);
    });

    // emergency fetch for missing ones
    const missing = chunk.filter(s => !results[s]);
    for (const sym of missing) {
      log(`Fetch missing ${sym}`);
      const d = await fetchAndSaveMarketData(sym, log);
      if (d) {
        results[sym] = attachSearchers(d);
        const coll = sym === 'TWD=X' ? 'exchange_rates' : 'price_history';
        await db.collection(coll).doc(sym).set(d);
      }
    }
  }
  return results;
}

/** 給 price / rates 轉成搜尋器，回傳擴充物件 */
function attachSearchers(data) {
  const out = { ...data };
  if (data.prices) out.priceSearch = buildSearcher(data.prices);
  if (data.rates)  out.rateSearch  = buildSearcher(data.rates);
  return out;
}

/* emergency Yahoo fetch */
async function fetchAndSaveMarketData(symbol, log) {
  try {
    const options = { period1: '2000-01-01' };
    const hist    = await yahooFinance.historical(symbol, options);
    if (!hist?.length) return null;

    const prices = {};
    hist.forEach(h => {
      prices[h.date.toISOString().slice(0, 10)] = h.close;
    });

    const payload = {
      prices,
      dividends: {},      // yfinance dividends array 另行實作
      splits: {},
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dataSource: 'yahoo-auto'
    };

    if (symbol === 'TWD=X') {
      payload.rates = prices;
      delete payload.prices;
    }
    return payload;
  } catch (e) {
    log(`[FetchErr] ${symbol} ${e.message}`);
    return null;
  }
}

/* ────────────────────────  SEARCHER (O(log N)) ───────────────────────── */

function buildSearcher(obj) {
  if (!obj || !Object.keys(obj).length) {
    return () => 1;                    // fallback
  }
  const arr = Object.entries(obj)
    .map(([d, v]) => [new Date(d).getTime(), v])
    .sort((a, b) => a[0] - b[0]);

  return (date) => {
    const ts = (date instanceof Date ? date : new Date(date)).getTime();
    let l = 0, r = arr.length - 1, best = arr[0][1];
    while (l <= r) {
      const m = (l + r) >> 1;
      if (arr[m][0] <= ts) {
        best = arr[m][1];
        l = m + 1;
      } else r = m - 1;
    }
    return best;
  };
}

/* ────────────────────────  PORTFOLIO CALC  ─────────────────────────── */

function calculatePortfolio(transactions, userSplits, marketData, log) {
  /* 1. 組合所有事件 (Tx / Split / Dividend) */
  const events = [];

  // transactions
  transactions.forEach(t => events.push({
    ...t,
    date: t.date.toDate ? t.date.toDate() : new Date(t.date),
    eventType: 'transaction'
  }));

  // user-defined splits
  userSplits.forEach(s => events.push({
    ...s,
    date: s.date.toDate ? s.date.toDate() : new Date(s.date),
    eventType: 'split'
  }));

  // dividends from price_history
  const txSyms = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
  txSyms.forEach(sym => {
    const divs = marketData[sym]?.dividends || {};
    Object.entries(divs).forEach(([d, amt]) => events.push({
      date: new Date(d),
      symbol: sym,
      amount: amt,
      eventType: 'dividend'
    }));
  });

  // chronological order
  events.sort((a, b) => new Date(a.date) - new Date(b.date));

  /* 2. 逐事件模擬倉位 (FIFO lots) & runningShares 供股息現金流使用 */
  const lotsMap  = {};          // symbol → [{qty, costOrig, costTWD}]
  const currency = {};          // symbol → 'USD' | 'TWD'
  const runSharesPerEvent = []; // [{date, shares:{sym:qty}}]

  events.forEach(evt => {
    const s   = evt.symbol?.toUpperCase();
    const isUSD = evt.currency === 'USD' || currency[s] === 'USD';
    switch (evt.eventType) {
      case 'transaction': {
        if (!lotsMap[s]) lotsMap[s] = [];
        currency[s] = evt.currency || 'USD';

        const rateFn  = marketData['TWD=X']?.rateSearch || (() => 1);
        const fxRate  = isUSD ? rateFn(evt.date) : 1;
        const costPS  = evt.totalCost ? evt.totalCost / evt.quantity : evt.price;
        const costTWD = costPS * fxRate;

        if (evt.type === 'buy') {
          lotsMap[s].push({
            qty: evt.quantity,
            costOrig: costPS,
            costTWD
          });
        } else if (evt.type === 'sell') {
          let remain = evt.quantity;
          while (remain > 0 && lotsMap[s].length) {
            const lot = lotsMap[s][0];
            if (lot.qty <= remain) {
              remain -= lot.qty;
              lotsMap[s].shift();
            } else {
              lot.qty -= remain;
              remain = 0;
            }
          }
        }
        break;
      }
      case 'split': {
        if (!lotsMap[s]) break;
        lotsMap[s].forEach(lot => {
          lot.qty      *= evt.ratio;
          lot.costOrig /= evt.ratio;
          lot.costTWD  /= evt.ratio;
        });
        break;
      }
    }

    // record running shares snapshot (for dividend later)
    const snapshot = {};
    Object.keys(lotsMap).forEach(sym => {
      snapshot[sym] = lotsMap[sym].reduce((sum, l) => sum + l.qty, 0);
    });
    runSharesPerEvent.push({ date: evt.date, shares: snapshot });
  });

  /* 3. 建立現金流 (唯一真相) */
  const cashflows = [];

  // helper to find shares of a symbol at given date
  function sharesAt(symbol, date) {
    let last = 0;
    for (const snap of runSharesPerEvent) {
      if (snap.date <= date && snap.shares[symbol] !== undefined) {
        last = snap.shares[symbol];
      } else if (snap.date > date) break;
    }
    return last;
  }

  events.forEach(evt => {
    const s = evt.symbol?.toUpperCase();
    const rateFn = marketData['TWD=X']?.rateSearch || (() => 1);

    switch (evt.eventType) {
      case 'transaction': {
        const fx  = (evt.currency === 'USD') ? rateFn(evt.date) : 1;
        const amt = (evt.totalCost || evt.quantity * evt.price) * fx;
        cashflows.push({
          date: new Date(evt.date),
          amount: evt.type === 'buy' ? -amt : amt
        });
        break;
      }
      case 'dividend': {
        const qty  = sharesAt(s, evt.date);
        if (!qty) break;
        const cur  = currency[s] || 'USD';
        const fx   = cur === 'USD' ? rateFn(evt.date) : 1;
        const amt  = evt.amount * qty * fx;
        if (amt) cashflows.push({ date: new Date(evt.date), amount: amt });
        break;
      }
    }
  });

  /* 4. 計算最終市值、Realized P/L */
  const today       = new Date();
  const fxLatest    = marketData['TWD=X']?.rateSearch(today) || 1;
  const finalHoldings = {};
  let   marketValue   = 0;

  Object.keys(lotsMap).forEach(sym => {
    const qty   = lotsMap[sym].reduce((s, l) => s + l.qty, 0);
    if (qty < 1e-9) return;

    const costTWD = lotsMap[sym].reduce((s, l) => s + l.costTWD * l.qty, 0);
    const costOrig= lotsMap[sym].reduce((s, l) => s + l.costOrig * l.qty, 0);

    const priceFn = marketData[sym]?.priceSearch || (() => 0);
    const pxOrig  = priceFn(today);
    const fx      = (currency[sym] === 'USD') ? fxLatest : 1;
    const mvTWD   = pxOrig * qty * fx;
    marketValue  += mvTWD;

    finalHoldings[sym] = {
      symbol: sym,
      quantity: qty,
      currency: currency[sym],
      avgCostOriginal: costOrig / qty,
      totalCostTWD: costTWD,
      currentPriceOriginal: pxOrig,
      marketValueTWD: mvTWD,
      unrealizedPLTWD: mvTWD - costTWD,
      returnRate: costTWD ? ((mvTWD - costTWD) / costTWD) * 100 : 0
    };
  });

  // 把今日市值加到現金流最後，用來算 XIRR
  if (marketValue) cashflows.push({ date: today, amount: marketValue });

  /* 5. XIRR */
  const xirr = calculateXIRR(cashflows);

  /* 6. Portfolio History (每日市值) */
  const portfolioHistory = calculatePortfolioHistory(events, marketData);

  /* 7. Realized P/L = 正現金流 - 負現金流 - 現市值 */
  const pos = cashflows.filter(c => c.amount > 0 && c.date !== today)
                       .reduce((s, c) => s + c.amount, 0);
  const neg = cashflows.filter(c => c.amount < 0)
                       .reduce((s, c) => s + Math.abs(c.amount), 0);
  const totalRealizedPL = pos - neg - marketValue;

  return { holdings: finalHoldings, totalRealizedPL, portfolioHistory, xirr };
}

/* ─────────────────────────  PORTFOLIO HISTORY  ───────────────────────── */

function calculatePortfolioHistory(events, marketData) {
  const txEvents = events.filter(e => e.eventType === 'transaction');
  if (!txEvents.length) return {};

  const firstDate = new Date(txEvents[0].date);
  const today     = new Date();
  const history   = {};

  for (
    let d = new Date(firstDate.setUTCHours(0,0,0,0));
    d <= today;
    d.setDate(d.getDate() + 1)
  ) {
    const state  = getPortfolioStateOnDate(events, d);
    history[d.toISOString().slice(0,10)] = calculateDailyMarketValue(state, marketData, d);
  }
  return history;
}

function getPortfolioStateOnDate(events, target) {
  const lots = {};
  events.forEach(e => {
    if (new Date(e.date) > target) return;

    const s = e.symbol?.toUpperCase();
    if (!lots[s]) lots[s] = [];

    switch (e.eventType) {
      case 'transaction':
        if (e.type === 'buy') lots[s].push({ qty: e.quantity });
        else if (e.type === 'sell') {
          let remain = e.quantity;
          while (remain && lots[s].length) {
            const lot = lots[s][0];
            if (lot.qty <= remain) { remain -= lot.qty; lots[s].shift(); }
            else { lot.qty -= remain; remain = 0; }
          }
        }
        break;
      case 'split':
        lots[s].forEach(lot => lot.qty *= e.ratio);
        break;
    }
  });

  // adjust future splits
  events
    .filter(e => e.eventType === 'split' && new Date(e.date) > target)
    .forEach(spl => {
      const s = spl.symbol.toUpperCase();
      lots[s]?.forEach(lot => lot.qty *= spl.ratio);
    });
  return lots;
}

function calculateDailyMarketValue(portfolioLots, marketData, date) {
  let total = 0;
  const rateFn = marketData['TWD=X']?.rateSearch || (() => 1);

  Object.entries(portfolioLots).forEach(([sym, lots]) => {
    const qty = lots.reduce((s, l) => s + l.qty, 0);
    if (!qty) return;
    const priceFn = marketData[sym]?.priceSearch || (() => 0);
    const pxOrig  = priceFn(date);
    const fx      = 'USD' === 'USD' ? rateFn(date) : 1; // 如果之後支援多幣，請調整
    total += qty * pxOrig * fx;
  });
  return total;
}

/* ──────────────────────────  XIRR (Hybrid)  ─────────────────────────── */

function calculateXIRR(cashflows) {
  if (cashflows.length < 2) return null;

  const values = cashflows.map(c => c.amount);
  const dates  = cashflows.map(c => c.date);
  const t0     = dates[0].getTime();
  const yrs    = dates.map(d => (d.getTime() - t0) / 3.15576e10);

  const npv = (r) => values.reduce(
    (s, v, i) => s + v / Math.pow(1 + r, yrs[i]), 0
  );
  const dnpv = (r) => values.reduce(
    (s, v, i) => s - yrs[i] * v / Math.pow(1 + r, yrs[i] + 1), 0
  );

  // 初始猜測：簡易 IRR
  const cashIn  = values.filter(v => v < 0).reduce((s, v) => s + (-v), 0);
  const cashOut = values.filter(v => v > 0).reduce((s, v) => s + v, 0);
  let guess     = Math.pow(cashOut / cashIn, 1 / (yrs[yrs.length - 1] || 1)) - 1;
  if (!isFinite(guess)) guess = 0.1;

  const maxIter = 100, tol = 1e-6;
  for (let i = 0; i < maxIter; i++) {
    const f  = npv(guess);
    const df = dnpv(guess);
    if (Math.abs(df) < 1e-9) break;
    const g1 = guess - f / df;
    if (Math.abs(g1 - guess) < tol) return g1;
    if (g1 <= -0.999) break;    // invalid
    guess = g1;
  }

  /* 失敗改用 Brent 夾逼法 (簡化版) */
  let a = -0.999, b = 10, fa = npv(a), fb = npv(b);
  if (fa * fb > 0) return null;
  for (let i = 0; i < 100; i++) {
    const c = (a * fb - b * fa) / (fb - fa);
    const fc = npv(c);
    if (Math.abs(fc) < tol) return c;
    if (fa * fc < 0) { b = c; fb = fc; }
    else { a = c; fa = fc; }
  }
  return null;
}

/* ─────────────────────────  EXPORT COMPLETE  ───────────────────────── */
