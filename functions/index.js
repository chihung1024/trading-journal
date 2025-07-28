/*  ────────────────────────────────────────────────────────────────
 *  index.js  —  Cloud Functions for Portfolio Calc
 *  Version : v35  (2025-07-28)
 *  Author  : <you>
 *
 *  變更摘要 v35
 *    1. markRecalc 改寫：1 秒內僅丟棄、佇列永遠遞增
 *    2. releaseLock 直接 delete 文件，避免殘骸
 *    3. 拆股邏輯修正：僅調 qty / 單位成本，總成本不變
 *    4. Oversell Map：回傳缺口數量與 txId
 *    5. getMarketData 快取 → getAll 批次抓取
 *    6. _calc_lock 新增 stage，前端可顯示進度
 *    7. 其它小幅優化（函式名稱、型別檢查 …）
 *  ─────────────────────────────────────────────────────────────── */

'use strict';

const functions    = require('firebase-functions');
const admin        = require('firebase-admin');
const yahooFinance = require('yahoo-finance2').default;
const zlib         = require('zlib');

admin.initializeApp();
const db = admin.firestore();

/* ──────────────────────────── Util ────────────────────────────── */

/** 交易或股價異動後「至少一次」排入重算佇列（1 秒節流） */
async function markRecalc(uid) {
  const ref = db.doc(`users/${uid}/user_data/current_holdings`);
  const now = Date.now();

  await db.runTransaction(async txn => {
    const snap = await txn.get(ref);
    const data = snap.exists ? snap.data() : {};
    const last = data.__lastMarked ?? 0;

    // 1 秒以內丟棄重複請求
    if (now - last < 1_000) return;

    txn.set(
      ref,
      {
        recalc_queue: admin.firestore.FieldValue.increment(1),
        __lastMarked: now
      },
      { merge: true }
    );
  });
}

/* ──────────────── 計算鎖：逾時 + stage ───────────────────────── */

async function acquireLock(uid) {
  const ref      = db.doc(`users/${uid}/user_data/_calc_lock`);
  const now      = admin.firestore.Timestamp.now();
  const expireAt = admin.firestore.Timestamp.fromMillis(
    now.toMillis() + 10 * 60 * 1_000
  );

  return db.runTransaction(async txn => {
    const doc = await txn.get(ref);
    if (doc.exists) {
      const d = doc.data();
      if (!d.finishedAt && d.expireAt?.toMillis() > now.toMillis()) {
        return false; // 仍被鎖定
      }
    }
    txn.set(ref, { startedAt: now, expireAt, stage: 'init' });
    return true;
  });
}

async function setStage(uid, stage) {
  return db.doc(`users/${uid}/user_data/_calc_lock`)
           .update({ stage })
           .catch(() => {});
}

async function releaseLock(uid) {
  // 直接刪除，省去殘骸文件
  await db.doc(`users/${uid}/user_data/_calc_lock`).delete().catch(() => {});
}

/* ────────────────────────── Triggers ─────────────────────────── */

exports.recalculatePortfolio = functions
  .runWith({ timeoutSeconds: 300, memory: '1GB' })
  .firestore
  .document('users/{userId}/user_data/current_holdings')
  .onUpdate(async (change, context) => {
    const { userId } = context.params;
    const beforeCnt  = change.before.data()?.recalc_queue ?? 0;
    const afterCnt   = change.after.data()?.recalc_queue ?? 0;

    if (afterCnt <= beforeCnt) return null;          // 佇列並未增加
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
  if (!(await acquireLock(userId))) {
    console.log(`[${userId}] busy, skip`);
    return;
  }

  try {
    await setStage(userId, 'loading');

    const holdingsRef = db.doc(`users/${userId}/user_data/current_holdings`);
    const historyRef  = db.doc(`users/${userId}/user_data/portfolio_history`);
    const brokenRef   = db.doc(`users/${userId}/user_data/broken_tx`);
    const logRef      = db.doc(`users/${userId}/user_data/calculation_logs`);

    let   logs   = [];
    const log    = (m) => { console.log(m); logs.push(m); };
    const oversellMap = new Map();

    log(`─── Recalc v35 start (${userId}) ───`);

    /* 1. 讀取交易 / 拆股資料 */
    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);

    const transactions = txSnap.docs .map(d => ({ id: d.id,   ...d.data() }));
    const userSplits   = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    if (!transactions.length) {
      log('No transactions → wipe');
      const batch = db.batch();
      batch.set(holdingsRef, {
        holdings: {}, totalRealizedPL: 0, xirr: null,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        recalc_queue: 0
      });
      batch.set(historyRef, {
        history: {},
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
      });
      batch.delete(brokenRef);
      await batch.commit();
      return;
    }

    /* 2. 行情 */
    await setStage(userId, 'pricing');
    const marketData = await getMarketDataFromDb(transactions, log);
    if (!Object.keys(marketData).length) throw new Error('marketData empty');

    /* 3. 計算 */
    await setStage(userId, 'calculating');
    const {
      holdings, totalRealizedPL, portfolioHistory, xirr
    } = calculatePortfolio(transactions, userSplits, marketData, oversellMap, log);

    /* 4. 寫回 (Batch) */
    await setStage(userId, 'writing');
    const batch = db.batch();

    batch.set(holdingsRef, {
      holdings, totalRealizedPL, xirr,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      recalc_queue: 0
    }, { merge: true });

    batch.set(historyRef, {
      history: portfolioHistory,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp()
    });

    if (oversellMap.size) {
      batch.set(brokenRef, {
        items: [...oversellMap.entries()].map(([s,o]) => ({
          symbol:  s,
          missing: o.missing,
          txId:    o.txId
        })),
        updatedAt: admin.firestore.FieldValue.serverTimestamp()
      });
    } else {
      batch.delete(brokenRef);
    }

    // gzip 壓縮日誌
    const zipped = zlib.gzipSync(Buffer.from(logs.join('\n'))).toString('base64');
    batch.set(logRef, { entries: zipped, encoding: 'gzip' });

    await batch.commit();

    log('─── Recalc OK ───');
  } catch (err) {
    console.error(`[${userId}]`, err);
    const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
    const body   = `ERROR: ${err.message}\n${err.stack}`;
    const zipped = zlib.gzipSync(Buffer.from(body)).toString('base64');
    await logRef.set({ entries: zipped, encoding: 'gzip' });
  } finally {
    await releaseLock(userId);
  }
}

/* ───────────────── Market Data (with cache) ─────────────────── */

async function getMarketDataFromDb(transactions, log) {
  // 1. 股票代號
  const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

  // 2. 貨幣代號（目前僅支援 USD，其餘視為 TWD = 1）
  const currencies = [...new Set(transactions
                      .map(t => (t.currency || 'USD').toUpperCase())
                      .filter(c => c !== 'TWD' && c !== 'USD'))];

  const priceIds = symbols;                 // price_history/*
  const fxIds    = ['TWD=X'];               // exchange_rates/*  (此處可擴充其他匯率)

  const results  = {};

  // Firestore getAll => 一次 round-trip
  const priceRefs = priceIds.map(id => db.collection('price_history').doc(id));
  const fxRefs    = fxIds   .map(id => db.collection('exchange_rates').doc(id));
  const snaps     = await db.getAll(...priceRefs, ...fxRefs);

  snaps.forEach(snap => { if (snap.exists) results[snap.id] = attachSearchers(snap.data()); });

  // emergency fetch
  const missing = [...priceIds, ...fxIds].filter(id => !results[id]);
  for (const id of missing) {
    log(`Fetch missing ${id}`);
    const d = await fetchAndSaveMarketData(id, log);
    if (d) {
      results[id] = attachSearchers(d);
      const coll = id.endsWith('=X') ? 'exchange_rates' : 'price_history';
      await db.collection(coll).doc(id).set(d);
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

    const payload = symbol.endsWith('=X')
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

function calculatePortfolio(transactions, userSplits, marketData, oversellMap, log) {
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

  /* 2. FIFO 模擬 (lot 級別) */
  const lotsMap   = {};   // sym -> { lots: [{qty,currency,cOrig,cTwd}], qty,cOrig,cTwd }
  const cashflows = [];
  const rateFn    = marketData['TWD=X']?.rateSearch || (()=>1);

  events.forEach(evt => {
    const sym = evt.symbol?.toUpperCase();
    switch (evt.eventType) {

      /* ── 交易 ── */
      case 'transaction': {
        if (typeof evt.quantity !== 'number' || evt.quantity <= 0) {
          log(`[SkipBadTx] ${evt.id}`);
          break;
        }

        if (!lotsMap[sym])
          lotsMap[sym] = { lots: [], qty:0, costOrig:0, costTwd:0 };

        const isBuy   = evt.type === 'buy';
        const cur     = (evt.currency || 'USD').toUpperCase();
        const fxRate  = (cur === 'USD') ? rateFn(evt.date) : 1;
        const costPS  = evt.totalCost ? evt.totalCost / evt.quantity : evt.price;
        const costTW  = costPS * fxRate;

        /* 現金流 (買負賣正) */
        const amount = (evt.totalCost || evt.quantity*evt.price) * fxRate;
        cashflows.push({
          date: new Date(evt.date),
          amount: isBuy ? -amount : amount
        });

        if (isBuy) {
          lotsMap[sym].lots.push({
            qty: evt.quantity,
            currency: cur,
            costOrig: costPS,
            costTwd:  costTW
          });
          lotsMap[sym].qty      += evt.quantity;
          lotsMap[sym].costOrig += costPS * evt.quantity;
          lotsMap[sym].costTwd  += costTW * evt.quantity;
        } else {
          let remain = evt.quantity;
          while (remain > 0 && lotsMap[sym].lots.length) {
            const lot  = lotsMap[sym].lots[0];
            const take = Math.min(lot.qty, remain);

            lot.qty          -= take;
            lotsMap[sym].qty -= take;

            const origVal = lot.costOrig * take;
            const twdVal  = lot.costTwd  * take;
            lotsMap[sym].costOrig -= origVal;
            lotsMap[sym].costTwd  -= twdVal;

            if (lot.qty < 1e-9) lotsMap[sym].lots.shift();
            remain -= take;
          }
          if (remain > 0) {
            oversellMap.set(sym, { missing: remain, txId: evt.id });
            log(`[Oversell] ${sym} qty ${remain}`);
          }
        }
        break;
      }

      /* ── 拆股 ── */
      case 'split': {
        const lm = lotsMap[sym];
        if (!lm) break;

        lm.lots.forEach(l => {
          l.qty      *= evt.ratio;
          l.costOrig /= evt.ratio;
          l.costTwd  /= evt.ratio;
        });

        lm.qty *= evt.ratio;
        // lm.costOrig & lm.costTwd 保持不變（總成本不應受影響）
        break;
      }

      /* ── 股息 ── */
      case 'dividend': {
        const lm = lotsMap[sym];
        if (!lm || lm.qty < 1e-9) break;

        const isUSD = (lm.lots[0]?.currency || 'USD') === 'USD';
        const fx    = isUSD ? rateFn(evt.date) : 1;
        const amt   = lm.qty * evt.amount * fx;
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

  Object.entries(lotsMap).forEach(([sym, lm]) => {
    if (lm.qty < 1e-9) return;

    const pxOrg  = marketData[sym]?.priceSearch(today) || 0;
    const cur    = lm.lots[0]?.currency || 'USD';
    const fx     = cur === 'USD' ? fxLatest : 1;
    const mvTwd  = lm.qty * pxOrg * fx;

    mktValue += mvTwd;

    holdings[sym] = {
      symbol: sym,
      quantity: lm.qty,
      currency: cur,
      avgCostOriginal: lm.costOrig / lm.qty,
      totalCostTWD:    lm.costTwd,
      currentPriceOriginal: pxOrg,
      marketValueTWD:  mvTwd,
      unrealizedPLTWD: mvTwd - lm.costTwd,
      returnRate:      lm.costTwd ? (mvTwd - lm.costTwd) / lm.costTwd * 100 : 0
    };
  });

  /* 4. XIRR */
  if (mktValue) cashflows.push({ date: today, amount: mktValue });
  const xirr = calculateXIRR(cashflows);

  /* 5. 實現損益 */
  const pos = cashflows.filter(c => c.amount > 0 && c.date !== today)
                       .reduce((s,c)=>s+c.amount,0);
  const neg = cashflows.filter(c => c.amount < 0)
                       .reduce((s,c)=>s+Math.abs(c.amount),0);
  const totalRealizedPL = pos - neg;

  /* 6. 歷史淨值 */
  const portfolioHistory = calculatePortfolioHistory(events, marketData, lotsMap);

  log(`Holdings: ${Object.keys(holdings).length}  XIRR=${xirr}`);

  return { holdings, totalRealizedPL, portfolioHistory, xirr };
}

/* ─────────────── Portfolio History (daily) ─────────────────── */

function calculatePortfolioHistory(events, marketData, initLotsMap) {
  const txEvents = events.filter(e => e.eventType === 'transaction');
  if (!txEvents.length) return {};

  const firstDay = new Date(txEvents[0].date);
  firstDay.setUTCHours(0,0,0,0);
  const today = new Date(); today.setUTCHours(0,0,0,0);

  const history = {};

  // 深拷貝 lotsMap 用於重播
  const lotsMap = JSON.parse(JSON.stringify(initLotsMap));
  const rateFn  = marketData['TWD=X']?.rateSearch || (()=>1);

  // 重新跑事件索引
  let idx = 0;
  for (let d = new Date(firstDay); d <= today; d.setDate(d.getDate()+1)) {

    // 處理 <= 當日事件
    while (idx < events.length && events[idx].date <= d) {
      const e   = events[idx];
      const sym = e.symbol?.toUpperCase();

      switch (e.eventType) {
        case 'transaction':
          if (!lotsMap[sym])
            lotsMap[sym] = { lots: [], qty: 0 };

          const isBuy = e.type === 'buy';
          if (isBuy) {
            lotsMap[sym].lots.push({ qty: e.quantity, costDummy:1 }); // 僅需數量
            lotsMap[sym].qty += e.quantity;
          } else {
            let rem = e.quantity;
            while (rem && lotsMap[sym].lots.length) {
              const lot  = lotsMap[sym].lots[0];
              const take = Math.min(lot.qty, rem);
              lot.qty   -= take;
              lotsMap[sym].qty -= take;
              if (lot.qty < 1e-9) lotsMap[sym].lots.shift();
              rem -= take;
            }
          }
          break;

        case 'split':
          lotsMap[sym]?.lots.forEach(l => { l.qty *= e.ratio; });
          lotsMap[sym].qty *= e.ratio;
          break;
      }
      idx++;
    }

    // 市值
    let total = 0;
    Object.entries(lotsMap).forEach(([sym, lm]) => {
      if (lm.qty < 1e-9) return;
      const pxOrg = marketData[sym]?.priceSearch(d) || 0;
      const fx    = rateFn(d);     // 僅 USD→TWD
      total += lm.qty * pxOrg * fx;
    });
    history[d.toISOString().slice(0,10)] = total;
  }
  return history;
}

/* ────────────────────────  XIRR  ───────────────────────────── */

function calculateXIRR(cashflows) {
  if (cashflows.length < 2) return null;

  const vals  = cashflows.map(c => c.amount);
  const dates = cashflows.map(c => c.date);
  const t0    = dates[0].getTime();
  const yrs   = dates.map(d => (d.getTime() - t0) / 3.15576e10);

  const npv  = r => vals.reduce((s,v,i)=>s + v / Math.pow(1 + r, yrs[i]), 0);
  const dnpv = r => vals.reduce((s,v,i)=>s - yrs[i] * v / Math.pow(1 + r, yrs[i] + 1), 0);

  /* 先掃描找符號反轉區間 */
  let a=-0.9, b=1, fa=npv(a), fb=npv(b);
  if (fa*fb > 0) return null;                     // 無解

  /* Newton */
  let guess = 0.1, tol = 1e-6;
  for (let i=0;i<50;i++) {
    const f  = npv(guess), df = dnpv(guess);
    if (Math.abs(df) < 1e-10) break;
    const g1 = guess - f / df;
    if (g1 <= -0.999) break;
    if (Math.abs(g1 - guess) < tol) return g1;
    guess = g1;
  }

  /* Brent (簡版) */
  for (let i=0;i<100;i++) {
    const c  = (a*fb - b*fa) / (fb - fa);
    const fc = npv(c);
    if (Math.abs(fc) < tol) return c;
    if (fa*fc < 0) { b = c; fb = fc; }
    else           { a = c; fa = fc; }
  }
  return null;
}

/* ─────────────────────────── End ───────────────────────────── */
