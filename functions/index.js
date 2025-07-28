/*  ────────────────────────────────────────────────────────────────
 *  index.js  —  Cloud Functions for Portfolio Calc
 *  Version : v33  (2025-07-28)
 *  Author  : <you>
 *  變更重點：
 *    1. 兩階段鎖  _calc_lock
 *    2. recalc_queue 取代 force_recalc_timestamp
 *    3. O(N) 現金流計算，刪除 runSharesPerEvent
 *    4. 幣別判斷正確、Realized P/L 修正
 *    5. FIFO oversell 防呆
 *  ─────────────────────────────────────────────────────────────── */

'use strict';

const functions     = require('firebase-functions');
const admin         = require('firebase-admin');
const yahooFinance  = require('yahoo-finance2').default;

admin.initializeApp();
const db = admin.firestore();

/* ──────────────────────────── Util ────────────────────────────── */

/** 佇列＋1，確保「至少一次」重算 (de-bounce 免 2 秒規則) */
async function markRecalc(uid) {
  const ref = db.doc(`users/${uid}/user_data/current_holdings`);
  await ref.set(
    { recalc_queue: admin.firestore.FieldValue.increment(1) },
    { merge: true }
  );
}

/* ────────────────────────── Triggers ─────────────────────────── */

exports.recalculatePortfolio = functions
  .runWith({ timeoutSeconds: 300, memory: '1GB' })
  .firestore
  .document('users/{userId}/user_data/current_holdings')
  .onUpdate(async (change, context) => {
    const { userId }  = context.params;
    const beforeCnt   = change.before.data()?.recalc_queue ?? 0;
    const afterCnt    = change.after.data()?.recalc_queue ?? 0;

    if (afterCnt <= beforeCnt) return null;   // 無新增佇列
    console.log(`[${userId}] queue +${afterCnt - beforeCnt}`);

    await performRecalculation(userId);
    return null;
  });

exports.recalculateOnTransaction = functions.firestore
  .document('users/{userId}/transactions/{txId}')
  .onWrite((_, ctx) => markRecalc(ctx.params.userId));

exports.recalculateOnSplit = functions.firestore
  .document('users/{userId}/splits/{splitId}')
  .onWrite((_, ctx) => markRecalc(ctx.params.userId));

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

    const txSnap = await db.collectionGroup('transactions')
                           .where('symbol', '==', symbol)
                           .get();
    if (txSnap.empty) { console.log(`[${symbol}] no holder`); return null; }

    const uids = [...new Set(txSnap.docs.map(d => d.ref.path.split('/')[1]))];
    await Promise.all(uids.map(markRecalc));
    console.log(`[${symbol}] queued ${uids.length} user(s)`);
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

/* ───────────────────────  Main Recalc  ───────────────────────── */

async function performRecalculation(userId) {
  const busyRef = db.doc(`users/${userId}/user_data/_calc_lock`);
  const lockDoc = await busyRef.get();
  if (lockDoc.exists) {
    console.log(`[${userId}] already running, skip`);
    return;
  }
  await busyRef.set({ startedAt: admin.firestore.Timestamp.now() });

  const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
  const logs = []; const log = (m) => { console.log(m); logs.push(m); };

  try {
    log(`─── Recalc v33 start (${userId}) ───`);

    /* 1. 讀取資料 */
    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);

    const transactions = txSnap.docs .map(d => ({ id: d.id,   ...d.data() }));
    const userSplits   = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    const holdingsRef = db.doc(`users/${userId}/user_data/current_holdings`);
    const historyRef  = db.doc(`users/${userId}/user_data/portfolio_history`);

    if (!transactions.length) {
      log('No transactions → wipe');
      await Promise.all([
        holdingsRef.set({
          holdings: {}, totalRealizedPL: 0, xirr: null,
          lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
          recalc_queue: 0
        }),
        historyRef.set({
          history: {},
          lastUpdated: admin.firestore.FieldValue.serverTimestamp()
        })
      ]);
      return;
    }

    /* 2. 行情 */
    const marketData = await getMarketDataFromDb(transactions, log);
    if (!Object.keys(marketData).length) throw new Error('marketData empty');

    /* 3. 計算 */
    const {
      holdings, totalRealizedPL, portfolioHistory, xirr
    } = calculatePortfolio(transactions, userSplits, marketData, log);

    /* 4. 寫回 */
    const payload = {
      holdings, totalRealizedPL, xirr,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      recalc_queue: 0
    };
    await Promise.all([
      holdingsRef.set(payload, { merge: true }),
      historyRef.set({
        history: portfolioHistory,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
      })
    ]);

    log('─── Recalc OK ───');
  } catch (err) {
    console.error(`[${userId}]`, err);
    logs.push(`ERROR: ${err.message}\n${err.stack}`);
  } finally {
    await busyRef.delete().catch(()=>{});
    await logRef.set({ entries: logs });
  }
}

/* ───────────────── Market Data (with cache) ─────────────────── */

async function getMarketDataFromDb(transactions, log) {
  const symbols  = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];
  const requests = [...new Set([...symbols, 'TWD=X'])];

  const CHUNK = 10, chunks = [];
  for (let i = 0; i < requests.length; i += CHUNK)
    chunks.push(requests.slice(i, i + CHUNK));

  const results = {};
  for (const chunk of chunks) {
    const priceIds = chunk.filter(s => s !== 'TWD=X');
    const fxIds    = chunk.filter(s => s === 'TWD=X');

    const [priceSnap, fxSnap] = await Promise.all([
      priceIds.length
        ? db.collection('price_history')
            .where(admin.firestore.FieldPath.documentId(), 'in', priceIds).get()
        : Promise.resolve({ docs: [] }),
      fxIds.length
        ? db.collection('exchange_rates')
            .where(admin.firestore.FieldPath.documentId(), 'in', fxIds).get()
        : Promise.resolve({ docs: [] })
    ]);

    [...priceSnap.docs, ...fxSnap.docs].forEach(doc => {
      results[doc.id] = attachSearchers(doc.data());
    });

    /* emergency fetch */
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

function attachSearchers(data) {
  const out = { ...data };
  if (data.prices) out.priceSearch = buildSearcher(data.prices);
  if (data.rates)  out.rateSearch  = buildSearcher(data.rates);
  return out;
}

async function fetchAndSaveMarketData(symbol, log) {
  try {
    const options = { period1: '2000-01-01' };
    const hist    = await yahooFinance.historical(symbol, options);
    if (!hist?.length) return null;

    const obj = {};
    hist.forEach(h => { obj[h.date.toISOString().slice(0,10)] = h.close; });

    const payload = symbol === 'TWD=X'
      ? { rates: obj }
      : { prices: obj, dividends: {}, splits: {} };

    payload.lastUpdated = admin.firestore.FieldValue.serverTimestamp();
    payload.dataSource  = 'yahoo-auto';
    return payload;
  } catch (e) {
    log(`[FetchErr] ${symbol} ${e.message}`);
    return null;
  }
}

/* ─────────────────────── Searcher  O(log N) ──────────────────── */

function buildSearcher(obj) {
  if (!obj || !Object.keys(obj).length) return () => 1;
  const arr = Object.entries(obj)
    .map(([d,v]) => [new Date(d).getTime(), v])
    .sort((a,b) => a[0]-b[0]);

  return (date) => {
    const ts = (date instanceof Date ? date : new Date(date)).getTime();
    let l=0,r=arr.length-1,best=arr[0][1];
    while (l<=r) {
      const m=(l+r)>>1;
      if (arr[m][0] <= ts) { best = arr[m][1]; l = m+1; }
      else r = m-1;
    }
    return best;
  };
}

/* ──────────────────── Portfolio Calculation ─────────────────── */

function calculatePortfolio(transactions, userSplits, marketData, log) {
  /* 1. 組合事件 (含股息) */
  const events = [];

  transactions.forEach(t => events.push({
    ...t,
    date: t.date.toDate ? t.date.toDate() : new Date(t.date),
    eventType: 'transaction'
  }));

  userSplits.forEach(s => events.push({
    ...s,
    date: s.date.toDate ? s.date.toDate() : new Date(s.date),
    eventType: 'split'
  }));

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

  events.sort((a,b) => a.date - b.date);

  /* 2. 模擬持股 FIFO */
  const lotsMap   = {};   // sym -> [{qty,costOrig,costTWD}]
  const curMap    = {};   // sym -> 'USD' | 'TWD'
  const cashflows = [];

  const rateFn = marketData['TWD=X']?.rateSearch || (()=>1);

  events.forEach(evt => {
    const sym = evt.symbol?.toUpperCase();
    switch (evt.eventType) {

      /* ── 交易 ── */
      case 'transaction': {
        if (!lotsMap[sym]) lotsMap[sym] = [];
        curMap[sym] = evt.currency || 'USD';

        const isUSD  = curMap[sym] === 'USD';
        const fxRate = isUSD ? rateFn(evt.date) : 1;
        const costPS = evt.totalCost ? evt.totalCost/evt.quantity : evt.price;
        const costTW = costPS * fxRate;

        /* 現金流 (買負賣正) */
        const amount = (evt.totalCost || evt.quantity*evt.price) * fxRate;
        cashflows.push({
          date: new Date(evt.date),
          amount: evt.type === 'buy' ? -amount : amount
        });

        if (evt.type === 'buy') {
          lotsMap[sym].push({ qty: evt.quantity, costOrig: costPS, costTWD: costTW });
        } else { // sell
          let remain = evt.quantity;
          while (remain > 0 && lotsMap[sym].length) {
            const lot = lotsMap[sym][0];
            if (lot.qty <= remain) { remain -= lot.qty; lotsMap[sym].shift(); }
            else { lot.qty -= remain; remain = 0; }
          }
          if (remain > 0) throw new Error(`Sell > position for ${sym}`);
        }
        break;
      }

      /* ── 拆股 ── */
      case 'split': {
        if (!lotsMap[sym]) break;
        lotsMap[sym].forEach(l => {
          l.qty      *= evt.ratio;
          l.costOrig /= evt.ratio;
          l.costTWD  /= evt.ratio;
        });
        break;
      }

      /* ── 股息 ── */
      case 'dividend': {
        const lots = lotsMap[sym] || [];
        const qty  = lots.reduce((s,l)=>s+l.qty,0);
        if (!qty) break;

        const isUSD = (curMap[sym] || 'USD') === 'USD';
        const fx    = isUSD ? rateFn(evt.date) : 1;
        const amt   = qty * evt.amount * fx;
        if (amt) cashflows.push({ date: new Date(evt.date), amount: amt });
        break;
      }
    }
  });

  /* 3. 期末持股市值 */
  const today    = new Date();
  const fxLatest = rateFn(today);
  const holdings = {};
  let   mktValue = 0;

  Object.entries(lotsMap).forEach(([sym,lots]) => {
    const qty = lots.reduce((s,l)=>s+l.qty,0);
    if (qty < 1e-9) return;

    const costTWD = lots.reduce((s,l)=>s+l.costTWD*l.qty,0);
    const costOrg = lots.reduce((s,l)=>s+l.costOrig*l.qty,0);
    const pxOrg   = marketData[sym]?.priceSearch(today) || 0;
    const fx      = (curMap[sym]==='USD') ? fxLatest : 1;
    const mvTWD   = qty * pxOrg * fx;
    mktValue     += mvTWD;

    holdings[sym] = {
      symbol: sym,
      quantity: qty,
      currency: curMap[sym],
      avgCostOriginal: costOrg/qty,
      totalCostTWD: costTWD,
      currentPriceOriginal: pxOrg,
      marketValueTWD: mvTWD,
      unrealizedPLTWD: mvTWD - costTWD,
      returnRate: costTWD ? (mvTWD-costTWD)/costTWD*100 : 0
    };
  });

  /* 4. XIRR */
  if (mktValue) cashflows.push({ date: today, amount: mktValue });
  const xirr = calculateXIRR(cashflows);

  /* 5. 實現損益 (不再扣市值) */
  const pos = cashflows.filter(c=>c.amount>0 && c.date!==today)
                       .reduce((s,c)=>s+c.amount,0);
  const neg = cashflows.filter(c=>c.amount<0)
                       .reduce((s,c)=>s+Math.abs(c.amount),0);
  const totalRealizedPL = pos - neg;

  /* 6. 歷史淨值 */
  const portfolioHistory = calculatePortfolioHistory(
    events, marketData, curMap
  );

  log(`Holdings: ${Object.keys(holdings).length}  XIRR=${xirr}`);

  return { holdings, totalRealizedPL, portfolioHistory, xirr };
}

/* ─────────────── Portfolio History (daily) ─────────────────── */

function calculatePortfolioHistory(events, marketData, curMap) {
  const txEvents = events.filter(e=>e.eventType==='transaction');
  if (!txEvents.length) return {};

  const first = new Date(txEvents[0].date);
  first.setUTCHours(0,0,0,0);
  const today = new Date(); today.setUTCHours(0,0,0,0);

  const history = {};
  const lotsMap = {};
  const rateFn  = marketData['TWD=X']?.rateSearch || (()=>1);

  /* 先依序處理至第一天前，建立初始 lotsMap 空 */
  let idx=0;
  for (let d = new Date(first); d <= today; d.setDate(d.getDate()+1)) {

    /* 把 <= d 的事件都吃掉 */
    while (idx < events.length && events[idx].date <= d) {
      const e = events[idx]; const sym = e.symbol?.toUpperCase();
      switch (e.eventType) {
        case 'transaction':
          if (!lotsMap[sym]) lotsMap[sym]=[];
          if (e.type==='buy') lotsMap[sym].push({qty:e.quantity});
          else {
            let rem=e.quantity;
            while(rem&&lotsMap[sym].length){
              const lot=lotsMap[sym][0];
              if(lot.qty<=rem){rem-=lot.qty;lotsMap[sym].shift();}
              else{lot.qty-=rem;rem=0;}
            }
          }
          break;
        case 'split':
          lotsMap[sym]?.forEach(lot=>{ lot.qty*=e.ratio; });
          break;
      }
      idx++;
    }

    /* 計算當日市值 */
    let total=0;
    Object.entries(lotsMap).forEach(([sym,lots])=>{
      const qty=lots.reduce((s,l)=>s+l.qty,0);
      if(!qty) return;
      const pxOrg=marketData[sym]?.priceSearch(d)||0;
      const fx=(curMap[sym]==='USD')?rateFn(d):1;
      total += qty*pxOrg*fx;
    });
    history[d.toISOString().slice(0,10)] = total;
  }
  return history;
}

/* ────────────────────────  XIRR  ───────────────────────────── */

function calculateXIRR(cashflows) {
  if (cashflows.length<2) return null;

  const vals  = cashflows.map(c=>c.amount);
  const dates = cashflows.map(c=>c.date);
  const t0    = dates[0].getTime();
  const yrs   = dates.map(d=>(d.getTime()-t0)/3.15576e10);

  const npv  = r => vals.reduce((s,v,i)=>s+v/Math.pow(1+r,yrs[i]),0);
  const dnpv = r => vals.reduce((s,v,i)=>s-yrs[i]*v/Math.pow(1+r,yrs[i]+1),0);

  /* Newton 迭代 */
  let guess=0.1, tol=1e-6;
  for(let i=0;i<50;i++){
    const f=npv(guess), df=dnpv(guess);
    if(Math.abs(df)<1e-10) break;
    const g1=guess - f/df;
    if(Math.abs(g1-guess)<tol) return g1;
    if(g1<=-0.999) break;
    guess=g1;
  }

  /* Brent 簡化 */
  let a=-0.999,b=10,fa=npv(a),fb=npv(b);
  if(fa*fb>0) return null;
  for(let i=0;i<100;i++){
    const c=(a*fb - b*fa)/(fb-fa);
    const fc=npv(c);
    if(Math.abs(fc)<tol) return c;
    if(fa*fc<0){b=c;fb=fc;} else {a=c;fa=fc;}
  }
  return null;
}

/* ─────────────────────────── End ───────────────────────────── */
