/**
 * Firebase Cloud Functions － trading-journal   (2025-07-30 fixed)
 *
 * 修改重點
 * 1. current_holdings 觸發器改用 onWrite：第一次建立文件也會計算
 * 2. 拆股觸發器立即呼叫 performRecalculation
 * 3. emergency fetch 同時抓「股價＋配息」
 * 4. createCashflows 修正買負賣正、配息現金流
 * 5. XIRR 牛頓法斜率≈0 回傳 0，年分計算改寫
 */

const functions    = require('firebase-functions');
const admin        = require('firebase-admin');
const yahooFinance = require('yahoo-finance2').default;

admin.initializeApp();
const db = admin.firestore();

/* ─────────────────────── 共用工具 ─────────────────────── */

function findNearestDataPoint(history, targetDate) {
  if (!history || Object.keys(history).length === 0) return 1;
  const d = new Date(targetDate);
  d.setUTCHours(12, 0, 0, 0);

  for (let i = 0; i < 7; i++) {
    const s = new Date(d);
    s.setDate(s.getDate() - i);
    const k = s.toISOString().slice(0, 10);
    if (history[k] !== undefined) return history[k];
  }

  const keys = Object.keys(history).sort();
  const tgt  = d.toISOString().slice(0, 10);
  let closest = null;
  for (const k of keys) {
    if (k <= tgt) closest = k; else break;
  }
  return closest ? history[closest] : 1;
}

/* ─────────────────────── 市場資料 ─────────────────────── */

async function fetchAndSaveMarketData(symbol, log) {
  try {
    log(`[Fetch] ${symbol}`);
    const priceRows = await yahooFinance.historical(symbol,
      { period1: '2000-01-01' });
    if (!priceRows || priceRows.length === 0) return null;

    const prices = {};
    priceRows.forEach(r => {
      prices[r.date.toISOString().slice(0, 10)] = r.close;
    });

    /* 另外抓配息 */
    let dividends = {};
    if (symbol !== 'TWD=X') {
      const divRows = await yahooFinance.historical(symbol,
        { period1: '2000-01-01', events: 'div' });
      (divRows || []).forEach(r => {
        if (r.dividends)
          dividends[r.date.toISOString().slice(0, 10)] = r.dividends;
      });
    }

    const payload = {
      prices,
      dividends,
      splits: {},
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
  const data = {};

  for (const sym of all) {
    const col = sym === 'TWD=X' ? 'exchange_rates' : 'price_history';
    const snap = await db.collection(col).doc(sym).get();
    if (snap.exists) {
      data[sym] = snap.data();
    } else {
      const fetched = await fetchAndSaveMarketData(sym, log);
      if (fetched) data[sym] = fetched;
    }
  }
  return data;
}

/* ─────────────────────── XIRR ─────────────────────── */

function calculateXIRR(cashflows) {
  if (cashflows.length < 2) return 0;
  const v   = cashflows.map(c => c.amount);
  const dts = cashflows.map(c => c.date);
  const MSY = 365 * 24 * 60 * 60 * 1000;
  const yrs = dts.map(d => (d - dts[0]) / MSY);

  const f  = r => v.reduce((s,x,i)=>s + x / Math.pow(1+r, yrs[i]), 0);
  const fp = r => v.reduce((s,x,i)=>s - x*yrs[i] / Math.pow(1+r, yrs[i]+1), 0);

  let r = 0.1;
  for (let i=0;i<100;i++) {
    const y  = f(r);
    const yd = fp(r);
    if (Math.abs(yd) < 1e-9) return 0;
    const r2 = r - y / yd;
    if (Math.abs(r2 - r) < 1e-6) return r2;
    r = r2;
  }
  return r;
}

/* ─────────────────────── 組合計算 ─────────────────────── */

function createCashflows(events, portfolio, finalHoldings, marketData) {
  const cf = [];
  // 交易
  events.filter(e=>e.eventType==='transaction').forEach(t=>{
    const fx = findNearestDataPoint(marketData['TWD=X']?.rates||{}, t.date);
    const sign = t.type==='buy'? -1 : 1;
    const raw  = t.totalCost || t.quantity * t.price;
    const amt  = Math.abs(raw) * (t.currency==='USD'?fx:1) * sign;
    cf.push({date:new Date(t.date), amount:amt});
  });
  // 配息
  events.filter(e=>e.eventType==='dividend').forEach(d=>{
    const fx = findNearestDataPoint(marketData['TWD=X']?.rates||{}, d.date);
    const cur = portfolio[d.symbol]?.currency || 'USD';
    const shares = portfolio[d.symbol]?.lots.reduce((s,l)=>s+l.quantity,0)||0;
    const amt = d.amount * shares * (cur==='USD'?fx:1);
    if (amt!==0) cf.push({date:new Date(d.date), amount:amt});
  });
  // 期末
  const mv = Object.values(finalHoldings).reduce((s,h)=>s+h.marketValueTWD,0);
  cf.push({date:new Date(), amount:mv});
  return cf;
}

function calculateFinalHoldings(portfolio, marketData){
  const res = {};
  const today = new Date();
  const fx = findNearestDataPoint(marketData['TWD=X']?.rates||{}, today);

  for(const sym in portfolio){
    const qty = portfolio[sym].lots.reduce((s,l)=>s+l.quantity,0);
    if(qty<1e-9) continue;

    const costTWD = portfolio[sym].lots.reduce((s,l)=>s+l.quantity*l.pricePerShareTWD,0);
    const costOrg = portfolio[sym].lots.reduce((s,l)=>s+l.quantity*l.pricePerShareOriginal,0);
    const price = findNearestDataPoint(marketData[sym]?.prices||{}, today);
    const rate  = portfolio[sym].currency==='USD'?fx:1;
    const mvTWD = qty*price*rate;

    res[sym] = {
      symbol:sym, quantity:qty,
      avgCostOriginal: costOrg/qty,
      totalCostTWD: costTWD,
      currency: portfolio[sym].currency,
      currentPriceOriginal: price,
      marketValueTWD: mvTWD,
      unrealizedPLTWD: mvTWD - costTWD,
      returnRate: costTWD? (mvTWD-costTWD)/costTWD*100 : 0
    };
  }
  return res;
}

function calculatePortfolioHistory(events, marketData){
  const tx = events.filter(e=>e.eventType==='transaction');
  if(tx.length===0) return {};
  const first = new Date(tx[0].date), today = new Date();
  let cur = new Date(first); cur.setUTCHours(0,0,0,0);

  const hist = {};
  while(cur<=today){
    const day = cur.toISOString().slice(0,10);
    const state = getPortfolioStateOnDate(events, cur);
    hist[day] = calculateDailyMarketValue(state, marketData, cur);
    cur.setDate(cur.getDate()+1);
  }
  return hist;
}

function getPortfolioStateOnDate(events, date){
  const state={};
  const rel = events.filter(e=>new Date(e.date)<=date);
  const allSplits = events.filter(e=>e.eventType==='split');

  for(const e of rel){
    const sym = e.symbol.toUpperCase();
    if(!state[sym]) state[sym]={lots:[],currency:'USD'};
    switch(e.eventType){
      case 'transaction':
        state[sym].currency = e.currency;
        if(e.type==='buy'){
          state[sym].lots.push({quantity:e.quantity});
        }else{
          let qty=e.quantity;
          while(qty>0&&state[sym].lots.length){
            const lot=state[sym].lots[0];
            if(lot.quantity<=qty){qty-=lot.quantity;state[sym].lots.shift();}
            else{lot.quantity-=qty;qty=0;}
          }
        }
        break;
      case 'split':
        state[sym].lots.forEach(l=>{l.quantity*=e.ratio;});
        break;
    }
  }
  // future splits forward-adjust
  for(const sym in state){
    const fut = allSplits.filter(s=>s.symbol.toUpperCase()===sym && new Date(s.date)>date);
    fut.forEach(s=>state[sym].lots.forEach(l=>l.quantity*=s.ratio));
  }
  return state;
}

function calculateDailyMarketValue(portfolio, marketData, date){
  let tot=0;
  const fx = findNearestDataPoint(marketData['TWD=X']?.rates||{}, date);
  for(const sym in portfolio){
    const qty = portfolio[sym].lots.reduce((s,l)=>s+l.quantity,0);
    if(!qty) continue;
    const price = findNearestDataPoint(marketData[sym]?.prices||{}, date);
    const rate  = portfolio[sym].currency==='USD'?fx:1;
    tot += qty*price*rate;
  }
  return tot;
}

function calculatePortfolio(transactions,splits,marketData,log){
  const ev=[];
  transactions.forEach(t=>ev.push({...t,date:t.date.toDate? t.date.toDate():new Date(t.date),eventType:'transaction'}));
  splits.forEach(s=>ev.push({...s,date:s.date.toDate? s.date.toDate():new Date(s.date),eventType:'split'}));

  const syms=[...new Set(transactions.map(t=>t.symbol.toUpperCase()))];
  syms.forEach(sym=>{
    const divs = marketData[sym]?.dividends||{};
    Object.entries(divs).forEach(([d,amt])=>
      ev.push({date:new Date(d),symbol:sym,amount:amt,eventType:'dividend'}));
  });
  ev.sort((a,b)=>new Date(a.date)-new Date(b.date));

  const pf={}; let realized=0;
  for(const e of ev){
    const sym=e.symbol.toUpperCase();
    if(!pf[sym]) pf[sym]={lots:[],currency:'USD'};
    const fx=findNearestDataPoint(marketData['TWD=X']?.rates||{}, e.date);

    switch(e.eventType){
      case 'transaction':{
        const costO = e.totalCost||e.price;
        const costT = costO*(e.currency==='USD'?fx:1);
        pf[sym].currency=e.currency;

        if(e.type==='buy'){
          pf[sym].lots.push({quantity:e.quantity,pricePerShareOriginal:costO,pricePerShareTWD:costT});
        }else{
          let qty=e.quantity, costSold=0;
          const saleTWD=(e.totalCost||e.quantity*e.price)*(e.currency==='USD'?fx:1);
          while(qty>0&&pf[sym].lots.length){
            const lot=pf[sym].lots[0];
            if(lot.quantity<=qty){costSold+=lot.quantity*lot.pricePerShareTWD;qty-=lot.quantity;pf[sym].lots.shift();}
            else{costSold+=qty*lot.pricePerShareTWD;lot.quantity-=qty;qty=0;}
          }
          realized += saleTWD - costSold;
        }
        break;}
      case 'split':
        if(typeof e.ratio!=='number'||e.ratio<=0){log(`bad split ${sym}`);break;}
        pf[sym].lots.forEach(l=>{l.quantity*=e.ratio;l.pricePerShareTWD/=e.ratio;l.pricePerShareOriginal/=e.ratio;});
        break;
      case 'dividend':
        const shares = pf[sym].lots.reduce((s,l)=>s+l.quantity,0);
        realized += e.amount*shares*(pf[sym].currency==='USD'?fx:1);
        break;
    }
  }

  const holdings = calculateFinalHoldings(pf,marketData);
  const hist     = calculatePortfolioHistory(ev,marketData);
  const xirr     = calculateXIRR(createCashflows(ev,pf,holdings,marketData));

  return {holdings,totalRealizedPL:realized,portfolioHistory:hist,xirr};
}

/* ─────────────────────── Recalc 主程式 ─────────────────────── */

async function performRecalculation(userId){
  const logRef = db.doc(`users/${userId}/user_data/calculation_logs`);
  const logs=[];const log=m=>{const ts=new Date().toISOString();logs.push(`${ts} ${m}`);console.log(`[${userId}] ${m}`);};

  try{
    const [txSnap,spSnap]=await Promise.all([
      db.collection(`users/${userId}/transactions`).get(),
      db.collection(`users/${userId}/splits`).get()
    ]);
    const tx=txSnap.docs.map(d=>({id:d.id,...d.data()}));
    const sp=spSnap.docs.map(d=>({id:d.id,...d.data()}));

    if(tx.length===0){
      await db.doc(`users/${userId}/user_data/current_holdings`)
        .set({holdings:{},totalRealizedPL:0,xirr:0,
              lastUpdated:admin.firestore.FieldValue.serverTimestamp(),
              force_recalc_timestamp:admin.firestore.FieldValue.delete()});
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
  }catch(e){console.error(e);logs.push(`ERR ${e.message}`);}
  finally{await logRef.set({entries:logs});}
}

/* ─────────────────────── Firestore 觸發器 ─────────────────────── */

// 1. current_holdings onWrite
exports.recalculatePortfolio = functions
  .runWith({timeoutSeconds:300,memory:'1GB'})
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
    if(created||tsChanged) return performRecalculation(ctx.params.uid);
    return null;
  });

// 2. 交易
exports.recalculateOnTransaction = functions.firestore
  .document('users/{uid}/transactions/{tid}')
  .onWrite((_,ctx)=>
    db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({force_recalc_timestamp:admin.firestore.FieldValue.serverTimestamp()},{merge:true})
  );

// 3. 拆股
exports.recalculateOnSplit = functions.firestore
  .document('users/{uid}/splits/{sid}')
  .onWrite((_,ctx)=>{
    db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({force_recalc_timestamp:admin.firestore.FieldValue.serverTimestamp()},{merge:true});
    return performRecalculation(ctx.params.uid);
  });

// 4. 價格／配息文件
exports.recalculateOnPriceUpdate = functions
  .runWith({timeoutSeconds:240,memory:'1GB'})
  .firestore.document('price_history/{symbol}')
  .onWrite(async (chg,ctx)=>{
    const sym = ctx.params.symbol.toUpperCase();
    const bef = chg.before.exists ? chg.before.data() : null;
    const aft = chg.after.data();
    if(bef &&
       JSON.stringify(bef.prices)===JSON.stringify(aft.prices) &&
       JSON.stringify(bef.dividends)===JSON.stringify(aft.dividends)) return null;

    const txSnap = await db.collectionGroup('transactions').where('symbol','==',sym).get();
    const users=[...new Set(txSnap.docs.map(d=>d.ref.path.split('/')[1]))];
    const ts=admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(users.map(u=>
      db.doc(`users/${u}/user_data/current_holdings`)
        .set({force_recalc_timestamp:ts},{merge:true})
    ));
    return null;
  });

// 5. 匯率
exports.recalculateOnFxUpdate = functions
  .runWith({timeoutSeconds:240,memory:'1GB'})
  .firestore.document('exchange_rates/TWD=X')
  .onWrite(async (chg)=>{
    const bef=chg.before.exists?chg.before.data():null;
    const aft=chg.after.data();
    if(bef && JSON.stringify(bef.rates)===JSON.stringify(aft.rates)) return null;
    const users=await db.collection('users').listDocuments();
    const ts=admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(users.map(u=>
      db.doc(`users/${u.id}/user_data/current_holdings`)
        .set({force_recalc_timestamp:ts},{merge:true})
    ));
    return null;
  });
