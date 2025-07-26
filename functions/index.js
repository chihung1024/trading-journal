/*  Cloud Function：onWrite 重新計算持股 & 淨值
 *  重點：
 *  1. 使用 price_history.{pricesAdj, cumSplitRatio}
 *  2. 每筆交易先依 cumSplitRatio 調整股數 / 單價
 *  3. 匯率只在最後乘上 USD→TWD
 *  4. 不再插入 split 事件
 */
const functions   = require("firebase-functions");
const admin       = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

//────── 入口 ────────────────────────────────────────────────────────────
exports.recalculateHoldings = functions
  .runWith({ timeoutSeconds: 540 })
  .firestore
  .document("users/{uid}/transactions/{tid}")
  .onWrite(async (_change, ctx) => {

  const uid = ctx.params.uid;

  // 1. 讀取該使用者所有交易
  const txSnap = await db.collection(`users/${uid}/transactions`).get();
  const txs = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));
  const holdingsRef = db.doc(`users/${uid}/user_data/current_holdings`);
  const historyRef  = db.doc(`users/${uid}/user_data/portfolio_history`);

  // 若已無任何交易 → 直接清空
  if (txs.length === 0) {
    await Promise.all([
      holdingsRef.set({ holdingsAdj: {}, realizedPL: 0,
                        lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
      historyRef.set({ historyAdj: {},
                       lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
    ]);
    return null;
  }

  // 2. 取得所有必需的市場資料（股票＋匯率）
  const symbols = [...new Set(txs.map(t => t.symbol.toUpperCase()))];
  const marketData = {};
  for (const s of [...symbols, "TWD=X"]) {
    const col = (s === "TWD=X") ? "exchange_rates" : "price_history";
    let doc = await db.collection(col).doc(s).get();
    if (!doc.exists) {
      await fetchAndSaveMarketData(s);          // 即時補料
      doc = await db.collection(col).doc(s).get();
    }
    marketData[s] = doc.data();
  }

  // 3. 計算
  const { holdings, realizedPL } = calcHoldings(txs, marketData);
  const historyAdj              = calcHistory(txs, marketData);

  // 4. 寫回 Firestore
  await Promise.all([
    holdingsRef.set({
      holdingsAdj: holdings,
      realizedPL:  realizedPL,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp()
    }),
    historyRef.set({
      historyAdj:  historyAdj,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp()
    })
  ]);

  return null;
});
//───────────────────────────────────────────────────────────────────────


//====================  即時抓價 (股票 / 匯率)  ==========================
async function fetchAndSaveMarketData(symbol) {
  const col = (symbol === "TWD=X") ? "exchange_rates" : "price_history";

  if (symbol === "TWD=X") {                   // 匯率
    const hist = await yahooFinance.historical(symbol, { period1: "2000-01-01" });
    const rates = {};
    hist.forEach(h => { rates[h.date.toISOString().split("T")[0]] = h.close; });
    await db.collection(col).doc(symbol).set({
      rates, lastUpdated: admin.firestore.FieldValue.serverTimestamp()
    });
    return;
  }

  // 股票：前復權價格(直接抓 adjusted)，再算累積拆股倍率
  const priceHist  = await yahooFinance.historical(symbol,
                     { period1: "2000-01-01", adjusted: true });
  const splitHist  = await yahooFinance.historical(symbol,
                     { period1: "2000-01-01", events: "split" });

  const pricesAdj = {};
  priceHist.forEach(p => {
    pricesAdj[p.date.toISOString().split("T")[0]] = p.close;
  });

  let cum = 1;
  const cumSplitRatio = {};
  splitHist.forEach(s => {
    const r = s.numerator / s.denominator;   // 例如 1/10 = 0.1
    cum *= r;
    cumSplitRatio[s.date.toISOString().split("T")[0]] = cum;
  });

  await db.collection(col).doc(symbol).set({
    pricesAdj, cumSplitRatio,
    lastUpdated: admin.firestore.FieldValue.serverTimestamp()
  });
}

//====================  核心工具函式  ====================================
const dayStr = d => (new Date(d)).toISOString().split("T")[0];

function getCumRatio(cumMap, dStr) {
  if (!cumMap) return 1;
  const dates = Object.keys(cumMap).sort();
  let ratio = 1;
  for (const dt of dates) {
    if (dt <= dStr) ratio = cumMap[dt]; else break;
  }
  return ratio;
}

function findPrice(map, d) {
  if (!map) return null;
  const probe = new Date(d);  probe.setUTCHours(probe.getUTCHours() + 12);
  for (let i = 0; i < 7; i++) {
    const k = dayStr(new Date(probe.getTime() - i * 86400000));
    if (map[k] !== undefined) return map[k];
  }
  return null;
}

//====================  計算持股 ========================================
function calcHoldings(txs, md) {
  const rateHist = md["TWD=X"].rates;
  txs.sort((a, b) =>
    (a.date.toDate ? a.date.toDate() : new Date(a.date)) -
    (b.date.toDate ? b.date.toDate() : new Date(b.date)));

  const shares = {}, costOrig = {}, costTWD = {}, currency = {};
  let realized = 0;

  for (const t of txs) {
    const d      = t.date.toDate ? t.date.toDate() : new Date(t.date);
    const sym    = t.symbol.toUpperCase();
    const mData  = md[sym] || {};
    const ratio  = getCumRatio(mData.cumSplitRatio, dayStr(d));  // 拆股倍率
    const qtyAdj = t.quantity * ratio;
    const pxAdj  = t.price    / ratio;
    const valOri = qtyAdj * pxAdj;
    const rate   = (t.currency === "USD")
                     ? (findPrice(rateHist, d) || 1) : 1;
    const valTWD = valOri * rate;

    if (!shares[sym]) { shares[sym]=0; costOrig[sym]=0; costTWD[sym]=0; }
    if (!currency[sym]) currency[sym] = t.currency || "TWD";

    if (t.type === "buy") {
      shares[sym]   += qtyAdj;
      costOrig[sym] += valOri;
      costTWD[sym]  += valTWD;

    } else if (t.type === "sell") {
      const prop = qtyAdj / shares[sym];
      const costSoldTWD = costTWD[sym] * prop;
      realized        += valTWD - costSoldTWD;
      shares[sym]     -= qtyAdj;
      costOrig[sym]   *= (1 - prop);
      costTWD[sym]    *= (1 - prop);

    } else if (t.type === "dividend") {
      realized += valTWD;
    }
  }

  // 產生 holdings 物件
  const holdings = {};
  for (const sym of Object.keys(shares)) {
    if (shares[sym] <= 1e-9) continue;
    const latestPx   = findPrice((md[sym] || {}).pricesAdj, new Date());
    const latestRate = (currency[sym] === "USD")
                         ? (findPrice(rateHist, new Date()) || 1) : 1;
    const mktValue   = shares[sym] * latestPx * latestRate;

    holdings[sym] = {
      symbol:           sym,
      quantity:         shares[sym],
      avgCost:          costOrig[sym] / shares[sym],
      totalCostTWD:     costTWD[sym],
      currentPrice:     latestPx,
      marketValueTWD:   mktValue,
      unrealizedPLTWD:  mktValue - costTWD[sym],
      returnRate:       costTWD[sym] > 0 ? (mktValue - costTWD[sym]) /
                                           costTWD[sym] * 100 : 0
    };
  }
  return { holdings, realizedPL: realized };
}

//====================  計算歷史淨值曲線 ================================
function calcHistory(txs, md) {
  if (txs.length === 0) return {};
  txs.sort((a, b) =>
    (a.date.toDate ? a.date.toDate() : new Date(a.date)) -
    (b.date.toDate ? b.date.toDate() : new Date(b.date)));

  const firstDate = txs[0].date.toDate ? txs[0].date.toDate()
                                       : new Date(txs[0].date);
  const today = new Date();
  const rateHist = md["TWD=X"].rates;

  const history = {};
  for (let cur = new Date(firstDate); cur <= today;
       cur.setDate(cur.getDate() + 1)) {

    const curStr = dayStr(cur);
    let pv = 0;

    // 先算到今天為止各股票「調整後股數」
    const shares = {};
    for (const t of txs) {
      const d = t.date.toDate ? t.date.toDate() : new Date(t.date);
      if (d > cur) continue;

      const sym   = t.symbol.toUpperCase();
      const ratio = getCumRatio((md[sym]||{}).cumSplitRatio, curStr);
      const qAdj  = t.quantity * ratio;

      if (!shares[sym]) shares[sym] = 0;
      if (t.type === "buy")  shares[sym] += qAdj;
      if (t.type === "sell") shares[sym] -= qAdj;
    }

    // 市值累加
    for (const sym of Object.keys(shares)) {
      if (shares[sym] <= 1e-9) continue;
      const px = findPrice((md[sym]||{}).pricesAdj, cur);
      if (px === null) continue;
      const rate = (txs.find(x => x.symbol.toUpperCase() === sym).currency === "USD")
                     ? (findPrice(rateHist, cur) || 1) : 1;
      pv += shares[sym] * px * rate;
    }
    history[curStr] = pv;
  }
  return history;
}
