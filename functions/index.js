/* eslint-disable */
const functions   = require("firebase-functions");
const admin       = require("firebase-admin");
const yahooFinance= require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

/* =======================================================================
 *  Recalculation entry
 * ===================================================================== */
async function performRecalculation(uid) {
  const logRef = db.doc(`users/${uid}/user_data/calculation_logs`);
  const logs   = [];
  const log = msg => {
    const ts = new Date().toISOString();
    logs.push(`${ts}: ${msg}`);
    console.log(`[${uid}] ${ts}: ${msg}`);
  };

  try {
    log("--- Recalc start (v34-OverallFix) ---");

    const holdingsRef = db.doc(`users/${uid}/user_data/current_holdings`);
    const histRef     = db.doc(`users/${uid}/user_data/portfolio_history`);

    const [txSnap, splitSnap] = await Promise.all([
      db.collection(`users/${uid}/transactions`).get(),
      db.collection(`users/${uid}/splits`).get()
    ]);
    const txs    = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
    const splits = splitSnap.docs.map(d => ({ id: d.id, ...d.data() }));

    if (txs.length === 0) {
      await Promise.all([
        holdingsRef.set({ holdings:{}, totalRealizedPL:0, lastUpdated:admin.firestore.FieldValue.serverTimestamp() }),
        histRef.set({ history:{}, lastUpdated:admin.firestore.FieldValue.serverTimestamp() })
      ]);
      log("no tx, cleared");
      return;
    }

    const market = await getMarketDataFromDb(txs, log);
    const result = calculatePortfolio(txs, splits, market, log);

    const {
      holdings,
      totalRealizedPL,
      portfolioHistory,
      xirr,
      overallReturnRateTotal,
      overallReturnRate      // alias
    } = result;

    const data = {
      holdings,
      totalRealizedPL,
      xirr,
      overallReturnRateTotal,
      overallReturnRate,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      force_recalc_timestamp: admin.firestore.FieldValue.delete()
    };

    await Promise.all([
      holdingsRef.set(data, { merge:true }),
      histRef.set({ history: portfolioHistory, lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
    ]);

    log("--- Recalc done ---");
  } catch (e) {
    console.error(`[${uid}]`, e);
    logs.push(`CRITICAL: ${e.message}\n${e.stack}`);
  } finally {
    await logRef.set({ entries: logs });
  }
}

/* =======================================================================
 *  Triggers – 只寫 force_recalc_timestamp
 * ===================================================================== */
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds:300, memory:"1GB" })
  .firestore
  .document("users/{uid}/user_data/current_holdings")
  .onUpdate((chg, ctx)=>{
    const b = chg.before.data(), a = chg.after.data();
    if (a.force_recalc_timestamp && a.force_recalc_timestamp !== b.force_recalc_timestamp)
      return performRecalculation(ctx.params.uid);
    return null;
  });

exports.recalculateOnTransaction = functions.firestore
  .document("users/{uid}/transactions/{txId}")
  .onWrite((_,ctx)=>
    db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge:true })
  );

exports.recalculateOnSplit = functions.firestore
  .document("users/{uid}/splits/{splitId}")
  .onWrite((_,ctx)=>
    db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge:true })
  );

exports.recalculateOnPriceUpdate = functions.runWith({ timeoutSeconds:240, memory:"1GB" })
  .firestore
  .document("price_history/{symbol}")
  .onWrite(async (chg,ctx)=>{
    const s = ctx.params.symbol.toUpperCase();
    const before = chg.before.exists ? chg.before.data():null;
    const after  = chg.after.data();
    if (before &&
        JSON.stringify(before.prices)    === JSON.stringify(after.prices) &&
        JSON.stringify(before.dividends) === JSON.stringify(after.dividends)) return null;

    const txSnap = await db.collectionGroup("transactions").where("symbol","==",s).get();
    if (txSnap.empty) return null;

    const users = new Set(txSnap.docs.map(d=>d.ref.path.split("/")[1]));
    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all([...users].map(uid =>
      db.doc(`users/${uid}/user_data/current_holdings`)
        .set({ force_recalc_timestamp: ts }, { merge:true })
    ));
    return null;
  });

exports.recalculateOnFxUpdate = functions.runWith({ timeoutSeconds:240, memory:"1GB" })
  .firestore
  .document("exchange_rates/TWD=X")
  .onWrite(async (chg)=>{
    const b = chg.before.exists ? chg.before.data():null;
    const a = chg.after.data();
    if (b && JSON.stringify(b.rates) === JSON.stringify(a.rates)) return null;

    const users = await db.collection("users").listDocuments();
    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(users.map(u=>
      db.doc(`users/${u.id}/user_data/current_holdings`)
        .set({ force_recalc_timestamp: ts }, { merge:true })
    ));
    return null;
  });

/* =======================================================================
 *  Market-data helpers
 * ===================================================================== */
async function getMarketDataFromDb(txs, log){
  const syms=[...new Set(txs.map(t=>t.symbol.toUpperCase()))];
  const all =[...new Set([...syms,"TWD=X"])];
  const out ={};
  for(const s of all){
    const col = s==="TWD=X"?"exchange_rates":"price_history";
    const ref = db.collection(col).doc(s);
    const doc = await ref.get();
    if(doc.exists){ out[s]=doc.data(); continue; }
    const fetched = await fetchAndSaveMarketData(s,log);
    if(fetched){ out[s]=fetched; await ref.set(fetched); }
  }
  return out;
}

async function fetchAndSaveMarketData(symbol,log){
  try{
    const hist=await yahooFinance.historical(symbol,{period1:"2000-01-01"});
    const prices={}; hist.forEach(i=>prices[i.date.toISOString().split("T")[0]]=i.close);
    const payload={
      prices,
      splits:{},
      dividends:(hist.dividends||[]).reduce((a,d)=>({...a,[d.date.toISOString().split("T")[0]]:d.amount}),{}),
      lastUpdated:admin.firestore.FieldValue.serverTimestamp(),
      dataSource:"emergency-fetch"
    };
    if(symbol==="TWD=X"){ payload.rates=payload.prices; delete payload.dividends; }
    return payload;
  }catch(e){ log(`fetch ${symbol} err ${e.message}`); return null;}
}

/* =======================================================================
 *  Portfolio calculation
 * ===================================================================== */
function calculatePortfolio(txs,splits,market,log){
  /* events */
  const evts=[
    ...txs.map(t=>({...t,date:toDate(t.date),eventType:"transaction"})),
    ...splits.map(s=>({...s,date:toDate(s.date),eventType:"split"}))
  ];
  [...new Set(txs.map(t=>t.symbol.toUpperCase()))].forEach(sym=>{
    Object.entries(market[sym]?.dividends||{}).forEach(([d,amt])=>
      evts.push({date:new Date(d),symbol:sym,amount:amt,eventType:"dividend"}));
  });
  evts.sort((a,b)=>new Date(a.date)-new Date(b.date));

  /* iterate */
  const pf={};
  let totalRealizedPL=0;
  for(const e of evts){
    const sym=e.symbol.toUpperCase();
    if(!pf[sym]) pf[sym]={lots:[],currency:"USD"};
    const fxHist=market["TWD=X"]?.rates||{};
    const fx=findNearest(fxHist,e.date);

    switch(e.eventType){
      case"transaction":{
        const t=e;
        pf[sym].currency=t.currency;
        const cpsOrig=(t.totalCost||t.price);
        const cpsTWD =cpsOrig*(t.currency==="USD"?fx:1);

        if(t.type==="buy"){
          pf[sym].lots.push({quantity:t.quantity,pricePerShareOriginal:cpsOrig,pricePerShareTWD:cpsTWD});
        }else{
          let q=t.quantity;
          const saleTWD=(t.totalCost||t.quantity*t.price)*(t.currency==="USD"?fx:1);
          let costSold=0;
          while(q>0&&pf[sym].lots.length){
            const lot=pf[sym].lots[0];
            if(lot.quantity<=q){ costSold+=lot.quantity*lot.pricePerShareTWD; q-=lot.quantity; pf[sym].lots.shift(); }
            else{ costSold+=q*lot.pricePerShareTWD; lot.quantity-=q; q=0; }
          }
          const realized=saleTWD-costSold;
          totalRealizedPL+=realized;
          pf[sym].realizedCostTWD=(pf[sym].realizedCostTWD||0)+costSold;
          pf[sym].realizedPLTWD  =(pf[sym].realizedPLTWD  ||0)+realized;
        }
        break;
      }
      case"split":
        pf[sym].lots.forEach(l=>{l.quantity*=e.ratio;l.pricePerShareTWD/=e.ratio;l.pricePerShareOriginal/=e.ratio;});
        break;
      case"dividend":
        const shares=pf[sym].lots.reduce((s,l)=>s+l.quantity,0);
        if(shares===0)break;
        const divTWD=e.amount*shares*(pf[sym].currency==="USD"?fx:1);
        totalRealizedPL+=divTWD;
        pf[sym].realizedPLTWD=(pf[sym].realizedPLTWD||0)+divTWD;
        break;
    }
  }

  const holdings = calculateFinalHoldings(pf,market);
  const history  = calculatePortfolioHistory(evts,market);
  const xirr     = calculateXIRR(createCashflows(evts,pf,holdings,market));

  /* overall rate —— now include all symbols (even sold-out) */
  let investedTotal=0,totalReturnTWD=0;
  for(const s in pf){
    const realizedCost = pf[s].realizedCostTWD||0;
    const realizedPL   = pf[s].realizedPLTWD  ||0;
    const unrealizedPL = holdings[s]?.unrealizedPLTWD||0;
    const remainingCost= holdings[s]?.totalCostTWD   ||0;

    investedTotal  += remainingCost + realizedCost;
    totalReturnTWD += unrealizedPL  + realizedPL;
  }
  const overallReturnRateTotal = investedTotal>0 ? (totalReturnTWD/investedTotal)*100 : 0;

  return {
    holdings,
    totalRealizedPL,
    portfolioHistory:history,
    xirr,
    overallReturnRateTotal,
    overallReturnRate: overallReturnRateTotal   // alias for front-end
  };
}

/* =======================================================================
 *  Helpers
 * ===================================================================== */
function calculateFinalHoldings(pf,market){
  const out={}, today=new Date(), fxToday=findNearest(market["TWD=X"]?.rates||{},today);
  for(const sym in pf){
    const h=pf[sym], qty=h.lots.reduce((s,l)=>s+l.quantity,0);
    if(qty<1e-9) continue;

    const totCostTWD=h.lots.reduce((s,l)=>s+l.quantity*l.pricePerShareTWD,0);
    const totCostOrg=h.lots.reduce((s,l)=>s+l.quantity*l.pricePerShareOriginal,0);
    const priceHist=market[sym]?.prices||{};
    const curPrice=findNearest(priceHist,today);
    const fx=h.currency==="USD"?fxToday:1;
    const mktVal=qty*curPrice*fx;
    const unreal=mktVal-totCostTWD;

    const invested=totCostTWD+(h.realizedCostTWD||0);
    const totalRet=unreal+(h.realizedPLTWD||0);

    const rrCurrent=totCostTWD>0 ? (unreal/totCostTWD)*100 : 0;
    const rrTotal  =invested>0   ? (totalRet/invested)*100 : 0;

    out[sym]={
      symbol:sym,
      quantity:qty,
      currency:h.currency,
      avgCostOriginal:totCostOrg/qty,
      totalCostTWD:totCostTWD,
      investedCostTWD:invested,
      currentPriceOriginal:curPrice,
      marketValueTWD:mktVal,
      unrealizedPLTWD:unreal,
      realizedPLTWD:h.realizedPLTWD||0,
      returnRateCurrent:rrCurrent,
      returnRateTotal:rrTotal,
      returnRate:rrCurrent          // alias
    };
  }
  return out;
}

function calculatePortfolioHistory(evts,market){
  const txEvts=evts.filter(e=>e.eventType==="transaction");
  if(txEvts.length===0) return {};
  const first=new Date(txEvts[0].date), today=new Date();
  let cur=new Date(first); cur.setUTCHours(0,0,0,0);
  const hist={};
  while(cur<=today){
    const key=cur.toISOString().split("T")[0];
    const state=getPortfolioStateOnDate(evts,cur);
    hist[key]=dailyValue(state,market,cur);
    cur.setDate(cur.getDate()+1);
  }
  return hist;
}

function createCashflows(evts,pf,holdings,market){
  const flows=[], fxHist=market["TWD=X"]?.rates||{};
  evts.filter(e=>e.eventType==="transaction").forEach(t=>{
    const fx=findNearest(fxHist,t.date);
    const amt=(t.totalCost||t.quantity*t.price)*(t.currency==="USD"?fx:1);
    flows.push({date:toDate(t.date),amount:t.type==="buy"?-amt:amt});
  });
  evts.filter(e=>e.eventType==="dividend").forEach(d=>{
    const fx=findNearest(fxHist,d.date);
    const shares=pf[d.symbol]?.lots.reduce((s,l)=>s+l.quantity,0)||0;
    const amt=d.amount*shares*(pf[d.symbol]?.currency==="USD"?fx:1);
    if(amt>0) flows.push({date:new Date(d.date),amount:amt});
  });
  const mkt=Object.values(holdings).reduce((s,h)=>s+h.marketValueTWD,0);
  if(mkt>0) flows.push({date:new Date(),amount:mkt});
  return flows;
}

function calculateXIRR(flows){
  if(flows.length<2)return 0;
  const v=flows.map(f=>f.amount), d=flows.map(f=>f.date),
        y=d.map(dt=>(dt-d[0])/(1000*60*60*24*365));
  const npv=r=>v.reduce((s,val,i)=>s+val/Math.pow(1+r,y[i]),0);
  const dnpv=r=>v.reduce((s,val,i)=> y[i]>0 ? s-val*y[i]/Math.pow(1+r,y[i]+1):s,0);
  let g=0.1;
  for(let i=0;i<100;i++){
    const f=npv(g), f1=dnpv(g); if(Math.abs(f1)<1e-9)break;
    const ng=g-f/f1; if(Math.abs(ng-g)<1e-6)return ng; g=ng;
  }
  return g;
}

function dailyValue(state,market,date){
  let tot=0, fxHist=market["TWD=X"]?.rates||{}, fx=findNearest(fxHist,date);
  for(const sym in state){
    const s=state[sym], qty=s.lots.reduce((sum,l)=>sum+l.quantity,0);
    if(qty===0)continue;
    const price=findNearest(market[sym]?.prices||{},date);
    const rate=s.currency==="USD"?fx:1;
    tot+=qty*price*rate;
  }
  return tot;
}

function getPortfolioStateOnDate(allEvts,target){
  const st={}, past=allEvts.filter(e=>new Date(e.date)<=target),
        fSplits=allEvts.filter(e=>e.eventType==="split"&&new Date(e.date)>target);
  for(const e of past){
    const sym=e.symbol.toUpperCase();
    if(!st[sym])st[sym]={lots:[],currency:"USD"};
    switch(e.eventType){
      case"transaction":
        st[sym].currency=e.currency;
        if(e.type==="buy") st[sym].lots.push({quantity:e.quantity});
        else{
          let q=e.quantity;
          while(q>0&&st[sym].lots.length){
            const lot=st[sym].lots[0];
            if(lot.quantity<=q){ q-=lot.quantity; st[sym].lots.shift();}
            else{ lot.quantity-=q; q=0;}
          }
        }
        break;
      case"split": st[sym].lots.forEach(l=>l.quantity*=e.ratio); break;
    }
  }
  for(const sym in st){
    fSplits.filter(sp=>sp.symbol.toUpperCase()===sym)
           .forEach(sp=>st[sym].lots.forEach(l=>l.quantity*=sp.ratio));
  }
  return st;
}

function findNearest(hist,date){
  if(!hist||Object.keys(hist).length===0)return 1;
  const d=new Date(date); d.setUTCHours(12,0,0,0);
  for(let i=0;i<7;i++){
    const s=new Date(d); s.setDate(s.getDate()-i);
    const k=s.toISOString().split("T")[0];
    if(hist[k]!==undefined) return hist[k];
  }
  const keys=Object.keys(hist).sort(), tgt=d.toISOString().split("T")[0];
  let close=null; for(const k of keys){ if(k<=tgt) close=k; else break; }
  return close?hist[close]:1;
}
const toDate=v=>v.toDate?v.toDate():new Date(v);
