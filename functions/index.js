/* ──────────────────────────────────────────
 * Firebase Cloud Functions
 *（Node 18 - TypeScript 無需，純 JS）
 * 1. onTransactionCreate
 *    ‧ 抓 prices + dividends 寫入 price_history
 *    ‧ 打 user 專屬 force_recalc_timestamp
 * 2. recalcOnPriceUpdate
 *    ‧ 只要有 dividends 變動 → 對所有 user 打 recalc
 * 3. computeXirrOnHoldingsUpdate   ★ NEW
 *    ‧ 當 current_holdings 有異動時計算 XIRR
 * - 依賴：firebase-admin、firebase-functions、yahoo-finance2、xirr
 * ────────────────────────────────────────── */

const functions   = require('firebase-functions');
const admin       = require('firebase-admin');
const { default: yf } = require('yahoo-finance2');
const { xirr }    = require('xirr');

admin.initializeApp();
const db = admin.firestore();

/* ========== 1. 新增／編輯交易時 ========== */
exports.onTransactionCreate = functions.firestore
  .document('users/{userId}/transactions/{txnId}')
  .onCreate(async (snap, context) => {
    const data   = snap.data();
    const symbol = (data.symbol || '').toUpperCase();
    if (!symbol) return null;

    try {
      /* 1-1 抓股價與配息 */
      const hist = await yf.historical(symbol, {
        period1 : '2000-01-01',
        interval: '1d',
        events  : 'history,div'
      });

      const prices    = {};
      const dividends = {};
      hist.forEach(r => {
        const d = r.date.toISOString().slice(0,10);
        if (r.close !== undefined) prices[d] = r.close;
        if (r.dividends !== undefined && r.dividends !== 0)
          dividends[d] = r.dividends;
      });

      /* 1-2 寫入 price_history（merge=true） */
      await db.collection('price_history').doc(symbol).set({
        prices,
        dividends,
        splits     : {},
        dataSource : 'yahoo-finance2',
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
      }, { merge:true });

      /* 1-3 打旗標讓持倉彙總 / XIRR 重算 */
      await db.collection('users').doc(context.params.userId)
        .collection('user_data').doc('current_holdings')
        .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() },
             { merge:true });

      console.log(`[onTransactionCreate] ${symbol} done for user ${context.params.userId}`);
    } catch (e) {
      console.error(`[onTransactionCreate] ${symbol} error:`, e);
    }
    return null;
  });


/* ========== 2. 價格／配息更新時 ========== */
exports.recalcOnPriceUpdate = functions.firestore
  .document('price_history/{symbol}')
  .onUpdate(async (chg, ctx) => {
    const beforeDiv = chg.before.get('dividends') || {};
    const afterDiv  = chg.after.get('dividends')  || {};
    if (JSON.stringify(beforeDiv) === JSON.stringify(afterDiv)) return null;

    const users = await db.collection('users').get();
    const batch = db.batch();
    users.forEach(u => {
      const ref = db.collection('users').doc(u.id)
                    .collection('user_data').doc('current_holdings');
      batch.set(ref,
        { force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() },
        { merge:true });
    });
    await batch.commit();
    console.log(`[recalcOnPriceUpdate] ${ctx.params.symbol} → triggered ${users.size} users`);
    return null;
  });



/* ========== 3. 持倉文件異動 → 計算 XIRR ========== */
exports.computeXirrOnHoldingsUpdate = functions.firestore
  .document('users/{userId}/user_data/current_holdings')
  .onUpdate(async (chg, ctx) => {
    const after = chg.after.data() || {};
    const totalAssets = after.totalAssetsTwd || 0;          // 期末市值
    if (!totalAssets) return null;                          // 尚未算出市值先跳過

    const userId = ctx.params.userId;

    /* 3-1 收集交易現金流 */
    const txSnap = await db.collection('users').doc(userId)
                     .collection('transactions').get();

    const cashFlows = [];
    txSnap.forEach(d => {
      const t   = d.data();
      const when = (t.date && t.date.toDate) ? t.date.toDate()
                   : new Date(t.date);
      const amt  = (t.type === 'BUY' ? -1 : 1) * Number(t.totalTwd || 0);
      if (amt) cashFlows.push({ when, amount: amt });
    });

    /* 3-2 收集配息現金流（可選） */
    const divSnap = await db.collection('users').doc(userId)
                       .collection('dividends').get();
    divSnap.forEach(d => {
      const rec = d.data();
      const amt = Number(rec.amountTwd || 0);
      if (amt) cashFlows.push({
        when   : new Date(rec.payDate),
        amount : amt
      });
    });

    /* 3-3 加入期末市值 */
    cashFlows.push({ when: new Date(), amount: Number(totalAssets) });

    /* 至少需要一負一正 */
    const hasNeg = cashFlows.some(f => f.amount < 0);
    const hasPos = cashFlows.some(f => f.amount > 0);
    if (!hasNeg || !hasPos) {
      await chg.after.ref.set({ xirr: admin.firestore.FieldValue.delete() }, { merge:true });
      console.log(`[XIRR] user ${userId} 只有同號現金流 → N/A`);
      return null;
    }

    /* 3-4 計算 XIRR */
    let irr = null;
    try { irr = xirr(cashFlows); } catch(e) { irr = null; }

    /* 3-5 若值變動才回寫，避免無限遞迴 */
    const oldIrr = chg.before.get('xirr');
    if (typeof irr === 'number' && Math.abs((oldIrr ?? 0) - irr) < 1e-8) {
      return null;
    }

    await chg.after.ref.set({ xirr: irr }, { merge:true });
    console.log(`[XIRR] user ${userId} = ${irr}`);
    return null;
  });
