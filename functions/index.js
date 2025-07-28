/**
 * Firebase Cloud Functions － trading-journal
 * 完整修正版（2025-07-30）
 *
 * 解決的問題：
 * 1. 第一次輸入交易就會抓股價 / 匯率
 * 2. Node.js emergency fetch 同時抓到「股價 + 配息」
 * 3. XIRR 方向及收斂 bug 修正
 * 4. 拆股寫入當下立即重新計算
 * 5. price_history / exchange_rates 變更 → 自動觸發受影響使用者重算
 */

const functions    = require('firebase-functions');
const admin        = require('firebase-admin');
const yahooFinance = require('yahoo-finance2').default;

admin.initializeApp();
const db = admin.firestore();

/* ────────────────────────────── 1. 共用工具 ────────────────────────────── */

function findNearestDataPoint(history, targetDate) {
  if (!history || Object.keys(history).length === 0) return 1;
  const d = new Date(targetDate);
  d.setUTCHours(12, 0, 0, 0);

  // 往前找 7 天
  for (let i = 0; i < 7; i++) {
    const s = new Date(d); s.setDate(s.getDate() - i);
    const key = s.toISOString().split('T')[0];
    if (history[key] !== undefined) return history[key];
  }
  // 往更早的歷史找最後一筆
  const keys = Object.keys(history).sort();
  const tgt  = d.toISOString().split('T')[0];
  let closest = null;
  for (const k of keys) { if (k <= tgt) closest = k; else break; }
  return closest ? history[closest] : 1;
}

/* ────────────────────────────── 2. 市場資料 ────────────────────────────── */

async function fetchAndSaveMarketData(symbol, log) {
  try {
    log(`[Fetch] ${symbol}`);
    // (1) 股價
    const priceRows = await yahooFinance.historical(symbol,
      { period1: '2000-01-01' });
    if (!priceRows || priceRows.length === 0) {
      log(`[Fetch] no price for ${symbol}`);
      return null;
    }
    const prices = {};
    priceRows.forEach(r => { prices[r.date.toISOString().slice(0, 10)] = r.close; });

    // (2) 配息（股票才抓）
    let dividends = {};
    if (symbol !== 'TWD=X') {
      const divRows = await yahooFinance.historical(symbol,
        { period1: '2000-01-01', events: 'div' });
      dividends = {};
      (divRows || []).forEach(r => {
        if (r.dividends)
          dividends[r.date.toISOString().slice(0, 10)] = r.dividends;
      });
    }

    const payload = {
      prices,
      dividends,
      splits: {},                   // 不用 yfinance 的拆股資料
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dataSource: 'emergency-fetch'
    };

    if (symbol === 'TWD=X') {
      payload.rates = payload.prices;
      delete payload.dividends;
    }

    const col = symbol === 'TWD=X' ? 'exchange_rates' : 'price_history';
    await db.collection(col).doc(symbol).set(payload);
    return payload;
  } catch (e) {
    log(`[Fetch ERROR] ${symbol} ${e.message}`);
    return null;
  }
}

async function getMarketDataFromDb(transactions, log) {
  const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
  const all = [...new Set([...symbols, 'TWD=X'])];
  const mkt = {};

  for (const sym of all) {
    const col = sym === 'TWD=X' ? 'exchange_rates' : 'price_history';
    const snap = await db.collection(col).doc(sym).get();
    if (snap.exists) {
      mkt[sym] = snap.data();
    } else {
      const fetched = await fetchAndSaveMarketData(sym, log);
      if (fetched) mkt[sym] = fetched;
    }
  }
  return mkt;
}

/* ────────────────────────────── 3. XIRR ────────────────────────────── */

function calculateXIRR(cash) {
  if (cash.length < 2) return 0;
  const v = cash.map(c => c.amount);
  const d = cash.map(c => c.date);
  const MS_YEAR = 365 * 24 * 60 * 60 * 1000;
  const yrs = d.map(t => (t - d[0]) / MS_YEAR);

  const f  = r => v.reduce((s, x, i) => s + x / Math.pow(1 + r, yrs[i]), 0);
  const fp = r => v.reduce((s, x, i) => s - x * yrs[i] / Math.pow(1 + r, yrs[i] + 1), 0);

  let r = 0.1;
  for (let i = 0; i < 100; i++) {
    const y = f(r);
    const yd = fp(r);
    if (Math.abs(yd) < 1e-9) return 0;
    const r2 = r - y / yd;
    if (Math.abs(r2 - r) < 1e-6) return r2;
    r = r2;
  }
  return r;
}

/* ────────────────────────────── 4. 現金流 ────────────────────────────── */

function createCashflows(events, portfolio, finalHoldings, marketData) {
  const cash = [];

  // 交易
  events.filter(e => e.eventType === 'transaction').forEach(t => {
    const rate = findNearestDataPoint(marketData['TWD=X']?.rates || {}, t.date);
    const dir = t.type === 'buy' ? -1 : 1;
    const raw = t.totalCost || t.quantity * t.price;
    const amt = Math.abs(raw) * (t.currency === 'USD' ? rate : 1) * dir;
    cash.push({ date: new Date(t.date), amount: amt });
  });

  // 配息
  events.filter(e => e.eventType === 'dividend').forEach(dv => {
    const rate = findNearestDataPoint(marketData['TWD=X']?.rates || {}, dv.date);
    const cur  = portfolio[dv.symbol]?.currency || 'USD';
    const shares = portfolio[dv.symbol]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
    const amt = dv.amount * shares * (cur === 'USD' ? rate : 1);
    if (amt !== 0) cash.push({ date: new Date(dv.date), amount: amt });
  });

  // 期末市值（可為 0）
  const mv = Object.values(finalHoldings).reduce((s, h) => s + h.marketValueTWD, 0);
  cash.push({ date: new Date(), amount: mv });

  return cash;
}

/* ────────────────────────────── 5. 投資組合計算 ────────────────────────────── */

function calculatePortfolio(transactions, splits, marketData, log) {
  // 組事件
  const ev = [];
  transactions.forEach(t => ev.push({
    ...t,
    date: t.date.toDate ? t.date.toDate() : new Date(t.date),
    eventType: 'transaction'
  }));
  splits.forEach(s => ev.push({
    ...s,
    date: s.date.toDate ? s.date.toDate() : new Date(s.date),
    eventType: 'split'
  }));

  // 股票配息事件
  const syms = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
  syms.forEach(sym => {
    const divs = marketData[sym]?.dividends || {};
    Object.entries(divs).forEach(([d, amt]) =>
      ev.push({ date: new Date(d), symbol: sym, amount: amt, eventType: 'dividend' }));
  });

  ev.sort((a, b) => new Date(a.date) - new Date(b.date));   // 時序

  // 模擬持股
  const portfolio = {};
  let realized = 0;

  for (const e of ev) {
    const sym = e.symbol.toUpperCase();
    if (!portfolio[sym]) portfolio[sym] = { lots: [], currency: 'USD' };

    const rateHist = marketData['TWD=X']?.rates || {};
    const fx = findNearestDataPoint(rateHist, e.date);

    switch (e.eventType) {
      case 'transaction': {
        const costOrig = e.totalCost || e.price;
        const costTWD  = costOrig * (e.currency === 'USD' ? fx : 1);
        portfolio[sym].currency = e.currency;

        if (e.type === 'buy') {
          portfolio[sym].lots.push({
            quantity: e.quantity,
            pricePerShareOriginal: costOrig,
            pricePerShareTWD: costTWD,
            date: e.date
          });
        } else {
          let qty = e.quantity;
          const saleTWD = (e.totalCost || e.quantity * e.price) *
                          (e.currency === 'USD' ? fx : 1);
          let costTWD = 0;
          while (qty > 0 && portfolio[sym].lots.length) {
            const lot = portfolio[sym].lots[0];
            if (lot.quantity <= qty) {
              costTWD += lot.quantity * lot.pricePerShareTWD;
              qty -= lot.quantity;
              portfolio[sym].lots.shift();
            } else {
              costTWD += qty * lot.pricePerShareTWD;
              lot.quantity -= qty;
              qty = 0;
            }
          }
          realized += saleTWD - costTWD;
        }
        break;
      }

      case 'split':
        if (typeof e.ratio !== 'number' || e.ratio <= 0) {
          log(`Bad split ratio for ${sym}`);
          break;
        }
        portfolio[sym].lots.forEach(lot => {
          lot.quantity            *= e.ratio;
          lot.pricePerShareTWD    /= e.ratio;
          lot.pricePerShareOriginal/= e.ratio;
        });
        break;

      case 'dividend': {
        const shares = portfolio[sym].lots.reduce((s,l)=>s+l.quantity,0);
        const amtTWD = e.amount * shares *
                       (portfolio[sym].currency === 'USD' ? fx : 1);
        realized += amtTWD;
        break;
      }
    }
  }

  // 期末持股
  const holdings = calculateFinalHoldings(portfolio, marketData);
  const hist     = calculatePortfolioHistory(ev, marketData);
  const xirr     = calculateXIRR(
                     createCashflows(ev, portfolio, holdings, marketData));

  return { holdings, totalRealizedPL: realized, portfolioHistory: hist, xirr };
}

function calculateFinalHoldings(portfolio, marketData) {
  const res = {};
  const today = new Date();
  const fx = findNearestDataPoint(marketData['TWD=X']?.rates || {}, today);

  for (const sym in portfolio) {
    const qty = portfolio[sym].lots.reduce((s,l)=>s+l.quantity,0);
    if (qty < 1e-9) continue;

    const costTWD = portfolio[sym].lots
      .reduce((s,l)=>s+l.quantity*l.pricePerShareTWD,0);
    const costOrig= portfolio[sym].lots
      .reduce((s,l)=>s+l.quantity*l.pricePerShareOriginal,0);

    const priceHist = marketData[sym]?.prices || {};
    const price = findNearestDataPoint(priceHist, today);
    const rate  = portfolio[sym].currency === 'USD' ? fx : 1;
    const mvTWD = qty * price * rate;

    res[sym] = {
      symbol: sym,
      quantity: qty,
      avgCostOriginal: costOrig / qty,
      totalCostTWD: costTWD,
      currency: portfolio[sym].currency,
      currentPriceOriginal: price,
      marketValueTWD: mvTWD,
      unrealizedPLTWD: mvTWD - costTWD,
      returnRate: costTWD ? (mvTWD - costTWD) / costTWD * 100 : 0
    };
  }
  return res;
}

/* 產生每日市值歷史（給前端走線圖） */
function calculatePortfolioHistory(events, marketData) {
  const txEv = events.filter(e => e.eventType === 'transaction');
  if (txEv.length === 0) return {};

  const first = new Date(txEv[0].date);
  const today = new Date();
  let cur = new Date(first);
  cur.setUTCHours(0,0,0,0);

  const hist = {};
  while (cur <= today) {
    const dayStr = cur.toISOString().slice(0,10);
    const state  = getPortfolioStateOnDate(events, cur);
    hist[dayStr] = calculateDailyMarketValue(state, marketData, cur);
    cur.setDate(cur.getDate() + 1);
  }
  return hist;
}

function getPortfolioStateOnDate(events, date) {
  const state = {};
  const rel = events.filter(e => new Date(e.date) <= date);
  const allSplits = events.filter(e => e.eventType === 'split');

  for (const e of rel) {
    const sym = e.symbol.toUpperCase();
    if (!state[sym]) state[sym] = { lots: [], currency: 'USD' };

    switch (e.eventType) {
      case 'transaction':
        state[sym].currency = e.currency;
        if (e.type === 'buy') {
          state[sym].lots.push({ quantity: e.quantity });
        } else {
          let qty = e.quantity;
          while (qty > 0 && state[sym].lots.length) {
            const lot = state[sym].lots[0];
            if (lot.quantity <= qty) {
              qty -= lot.quantity;
              state[sym].lots.shift();
            } else {
              lot.quantity -= qty; qty = 0;
            }
          }
        }
        break;

      case 'split':
        state[sym].lots.forEach(l => { l.quantity *= e.ratio; });
        break;
    }
  }

  // 把未來拆股 forward-adjust，讓數量對上調整後股價
  for (const sym in state) {
    const fut = allSplits.filter(s =>
      s.symbol.toUpperCase() === sym && new Date(s.date) > date);
    fut.forEach(s => state[sym].lots.forEach(l => l.quantity *= s.ratio));
  }
  return state;
}

function calculateDailyMarketValue(portfolio, marketData, date) {
  let total = 0;
  const fx = findNearestDataPoint(marketData['TWD=X']?.rates || {}, date);

  for (const sym in portfolio) {
    const qty = portfolio[sym].lots.reduce((s,l)=>s+l.quantity,0);
    if (!qty) continue;
    const price = findNearestDataPoint(marketData[sym]?.prices || {}, date);
    const rate  = portfolio[sym].currency === 'USD' ? fx : 1;
    total += qty * price * rate;
  }
  return total;
}

/* ────────────────────────────── 6. Recalc 執行 ────────────────────────────── */

async function performRecalculation(userId) {
  const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
  const logs=[]; const log=m=>{const ts=new Date().toISOString();logs.push(`${ts} ${m}`);console.log(`[${userId}] ${m}`);};

  try{
    log('--- Recalc start ---');
    const [txSnap, spSnap] = await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);
    const tx = txSnap.docs.map(d=>({id:d.id,...d.data()}));
    const sp = spSnap.docs.map(d=>({id:d.id,...d.data()}));

    if(tx.length===0){
      await db.doc(`users/${userId}/user_data/current_holdings`)
              .set({holdings:{},totalRealizedPL:0,xirr:0,
                    lastUpdated:admin.firestore.FieldValue.serverTimestamp(),
                    force_recalc_timestamp:admin.firestore.FieldValue.delete()});
      log('no tx, clear holdings');
      return;
    }

    const mkt=await getMarketDataFromDb(tx,log);
    const {holdings,totalRealizedPL,portfolioHistory,xirr}=calculatePortfolio(tx,sp,mkt,log);

    await Promise.all([
      db.doc(`users/${userId}/user_data/current_holdings`).set({
        holdings,totalRealizedPL,xirr,
        lastUpdated:admin.firestore.FieldValue.serverTimestamp(),
        force_recalc_timestamp:admin.firestore.FieldValue.delete()
      },{merge:true}),
      db.doc(`users/${userId}/user_data/portfolio_history`).set({
        history:portfolioHistory,
        lastUpdated:admin.firestore.FieldValue.serverTimestamp()
      })
    ]);
    log('--- Recalc done ---');
  }catch(err){console.error(err);logs.push(`ERR ${err.message}`);}
  finally{await logRef.set({entries:logs});}
}

/* ────────────────────────────── 7. Cloud Function 觸發器 ────────────────────────────── */

// 7-1 current_holdings onWrite：第一次建立或 timestamp 變動都計算
exports.recalculatePortfolio = functions
  .runWith({timeoutSeconds:300, memory:'1GB'})
  .firestore.document('users/{uid}/user_data/current_holdings')
  .onWrite((chg,ctx)=>{
    if(!chg.after.exists) return null;
    const created = !chg.before.exists;
    const before  = chg.before.data()||{};
    const after   = chg.after.data()||{};
    const tsChanged = after.force_recalc_timestamp &&
      (!before.force_recalc_timestamp ||
       after.force_recalc_timestamp.toMillis() !==
       before.force_recalc_timestamp.toMillis());

    if(created || tsChanged) return performRecalculation(ctx.params.uid);
    return null;
  });

// 7-2 交易：寫 timestamp
exports.recalculateOnTransaction = functions.firestore
  .document('users/{uid}/transactions/{tid}')
  .onWrite((_,ctx)=>
    db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({force_recalc_timestamp:admin.firestore.FieldValue.serverTimestamp()},{merge:true})
  );

// 7-3 拆股：直接計算 + timestamp
exports.recalculateOnSplit = functions.firestore
  .document('users/{uid}/splits/{sid}')
  .onWrite((_,ctx)=>{
    const uid=ctx.params.uid;
    db.doc(`users/${uid}/user_data/current_holdings`)
      .set({force_recalc_timestamp:admin.firestore.FieldValue.serverTimestamp()},{merge:true});
    return performRecalculation(uid);
  });

// 7-4 price_history/{symbol}
exports.recalculateOnPriceUpdate = functions
  .runWith({timeoutSeconds:240,memory:'1GB'})
  .firestore.document('price_history/{symbol}')
  .onWrite(async (chg,ctx)=>{
    const sym = ctx.params.symbol.toUpperCase();
    const bef = chg.before.exists ? chg.before.data() : null;
    const aft = chg.after.data();
    if (bef &&
        JSON.stringify(bef.prices)    === JSON.stringify(aft.prices) &&
        JSON.stringify(bef.dividends) === JSON.stringify(aft.dividends)) return null;

    const txSnap = await db.collectionGroup('transactions').where('symbol','==',sym).get();
    const users  = [...new Set(txSnap.docs.map(d=>d.ref.path.split('/')[1]))];
    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(users.map(u=>
      db.doc(`users/${u}/user_data/current_holdings`)
        .set({force_recalc_timestamp:ts},{merge:true})
    ));
    return null;
  });

// 7-5 匯率
exports.recalculateOnFxUpdate = functions
  .runWith({timeoutSeconds:240,memory:'1GB'})
  .firestore.document('exchange_rates/TWD=X')
  .onWrite(async (chg)=>{
    const bef=chg.before.exists?chg.before.data():null;
    const aft=chg.after.data();
    if (bef && JSON.stringify(bef.rates) === JSON.stringify(aft.rates)) return null;
    const users = await db.collection('users').listDocuments();
    const ts=admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(users.map(u=>
      db.doc(`users/${u.id}/user_data/current_holdings`)
        .set({force_recalc_timestamp:ts},{merge:true})
    ));
    return null;
  });
