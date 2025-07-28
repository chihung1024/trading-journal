/* functions/index.js  ─ 2025-07-28 split-fix */

const functions = require("firebase-functions");
const admin     = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// ============== 1. 主計算 =========================================
async function performRecalculation(userId) {
  const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
  const logs   = [];
  const L = (m)=>{ const ts=new Date().toISOString(); logs.push(`${ts} ${m}`); console.log(`[${userId}] ${m}`); };

  try {
    L('--- Recalculation triggered (split-fix) ---');

    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);
    const txs    = txSnap.docs.map(d=>({id:d.id, ...d.data()}));
    const splits = splitSnap.docs.map(d=>({id:d.id, ...d.data()}));

    if (txs.length===0){
      await db.doc(`users/${userId}/user_data/current_holdings`)
              .set({holdings:{},totalRealizedPL:0,xirr:0,
                    lastUpdated:admin.firestore.FieldValue.serverTimestamp(),
                    force_recalc_timestamp:admin.firestore.FieldValue.delete()});
      L('No transactions, cleared holdings.');
      return;
    }

    const mkt = await getMarketDataFromDb(txs,L);
    const {holdings,totalRealizedPL,portfolioHistory,xirr} =
          calculatePortfolio(txs,splits,mkt,L);

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
    L('--- Recalculation finished ---');
  } catch(e){
    console.error(`[${userId}]`,e);
    logs.push(`ERROR ${e.message}`);
  } finally {
    await logRef.set({entries:logs});
  }
}

// ============== 2. 觸發器 =========================================
// 2-1 current_holdings onWrite
exports.recalculatePortfolio = functions.runWith({timeoutSeconds:300,memory:'1GB'})
  .firestore.document('users/{uid}/user_data/current_holdings')
  .onWrite(async(change,ctx)=>{
    const uid = ctx.params.uid;
    if(!change.after.exists) return null;

    const created = !change.before.exists;
    if(created){
      console.log(`[${uid}] current_holdings created → recalc`);
      return performRecalculation(uid);
    }

    const before = change.before.data()||{};
    const after  = change.after.data()||{};
    const tsChanged = after.force_recalc_timestamp &&
                      (!before.force_recalc_timestamp ||
                       after.force_recalc_timestamp.toMillis() !==
                       before.force_recalc_timestamp.toMillis());
    if(tsChanged){
      console.log(`[${uid}] timestamp changed → recalc`);
      return performRecalculation(uid);
    }
    return null;
  });

// 2-2 Transaction → 打 timestamp
exports.recalculateOnTransaction = functions.firestore
  .document('users/{uid}/transactions/{tid}')
  .onWrite((_,ctx)=>
    db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({force_recalc_timestamp:admin.firestore.FieldValue.serverTimestamp()},{merge:true})
  );

// 2-3 Split → 直接重算 + 備援 timestamp
exports.recalculateOnSplit = functions.firestore
  .document('users/{uid}/splits/{sid}')
  .onWrite(async (_,ctx)=>{
    const uid = ctx.params.uid;
    console.log(`[${uid}] split added/updated → immediate recalc`);
    // 備援 timestamp（前端如果同時監聽 holdings 也能即時刷新）
    await db.doc(`users/${uid}/user_data/current_holdings`)
            .set({force_recalc_timestamp:admin.firestore.FieldValue.serverTimestamp()},{merge:true});
    // 直接重算，避免兩層觸發時差
    return performRecalculation(uid);
  });

// 2-4 price_history / FX 觸發（同之前版本，略）

// ============== 3. 計算核心（只列改到的 split 區段） ===============
function calculatePortfolio(transactions,splits,marketData,L){
  const events=[];
  transactions.forEach(t=>events.push({...t,date:t.date.toDate? t.date.toDate():new Date(t.date),eventType:'transaction'}));
  splits.forEach(s=>events.push({...s,date:s.date.toDate? s.date.toDate():new Date(s.date),eventType:'split'}));

  // …（dividend 與 sort 省略）

  const portfolio={}; let totalRealizedPL=0;
  for(const ev of events){
    const sym=ev.symbol.toUpperCase();
    if(!portfolio[sym]) portfolio[sym]={lots:[],currency:'USD'};
    const rateHist=marketData['TWD=X']?.rates||{};
    const rateOnDt=findNearestDataPoint(rateHist,ev.date);

    switch(ev.eventType){
      case 'split':
        // 如果 ratio 異常就 log 並跳過，避免 NaN
        if(typeof ev.ratio!=='number' || !isFinite(ev.ratio) || ev.ratio<=0){
          L(`Bad split ratio for ${sym} id=${ev.id||''}`);
          break;
        }
        portfolio[sym].lots.forEach(l=>{
          l.quantity*=ev.ratio;
          if(l.pricePerShareTWD)     l.pricePerShareTWD/=ev.ratio;
          if(l.pricePerShareOriginal)l.pricePerShareOriginal/=ev.ratio;
        });
        break;

      // buy/sell/dividend 分支與之前相同 …
    }
  }

  // …產生 finalHoldings / history / cashflows / xirr
  return {holdings:calculateFinalHoldings(portfolio,marketData),
          totalRealizedPL,
          portfolioHistory:calculatePortfolioHistory(events,marketData),
          xirr:calculateXIRR(createCashflows(events,portfolio,calculateFinalHoldings(portfolio,marketData),marketData))};
}

// 其餘 helper 與上一版相同，略…
