/**
 * Cloud Function : recalculateHoldings
 * 1. 讀取使用者全部交易 → 取得/補抓市場資料
 * 2. 以「前復權股價 pricesAdj + cumSplitRatio」計算
 * 3. 寫回 users/{uid}/user_data/current_holdings.history
 * ※ 若完全無交易則清空文件
 */
const functions    = require("firebase-functions");
const admin        = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

//────────────────────────────────────────
// 入口
//────────────────────────────────────────
exports.recalculateHoldings = functions
  .runWith({ timeoutSeconds: 540 })
  .firestore
  .document("users/{uid}/transactions/{tid}")
  .onWrite(async (_change, ctx) => {

  const uid = ctx.params.uid;
  const txSnap = await db.collection(`users/${uid}/transactions`).get();
  const txs = txSnap.docs.map(d => ({ id: d.id, ...d.data() }));

  const holdRef = db.doc(`users/${uid}/user_data/current_holdings`);
  const histRef = db.doc(`users/${uid}/user_data/portfolio_history`);

  // 無交易 → 清空並結束
  if (txs.length === 0) {
    await Promise.all([
      holdRef.set({ holdingsAdj: {}, realizedPL: 0,
                    lastUpdated: admin.firestore.FieldValue.serverTimestamp() }),
      histRef.set({ historyAdj: {},
                    lastUpdated: admin.firestore.FieldValue.serverTimestamp() })
    ]);
    return null;
  }

  //──────────────────────────────────────
  // 取得（或即時補抓）股價／匯率
  //──────────────────────────────────────
  const syms = [...new Set(txs.map(t => t.symbol.toUpperCase()))];
  const md   = {};                                     // marketData
  for (const s of [...syms, "TWD=X"]) {
    const col = (s === "TWD=X") ? "exchange_rates" : "price_history";
    let doc   = await db.collection(col).doc(s).get();
    if (!doc.exists) {              // 即時補抓
      await fetchAndSaveMarketData(s);
      doc = await db.collection(col).doc(s).get();
    }
    md[s] = doc.data();
  }

  //──────────────────────────────────────
  // 計算持股 & 歷史
  //──────────────────────────────────────
  const { holdings, realizedPL } = calcHoldings(txs, md);
  const historyAdj               = calcHistory(txs, md);

  //──────────────────────────────────────
  // 寫回 Firestore
  //──────────────────────────────────────
  await Promise.all([
    holdRef.set({
      holdingsAdj : holdings,
      realizedPL  : realizedPL,
      lastUpdated : admin.firestore.FieldValue.serverTimestamp()
    }),
    histRef.set({
      historyAdj  : historyAdj,
      lastUpdated : admin.firestore.FieldValue.serverTimestamp()
    })
  ]);

  return null;
});


//────────────────────────────────────────
// 即時抓價（股票／匯率）
//────────────────────────────────────────
async function fetchAndSaveMarketData(symbol) {
  const isForex = symbol === "TWD=X";
  const col     = isForex ? "exchange_rates" : "price_history";

  if (isForex) {
    const hist = await yahooFinance.historical(symbol, { period1: "2000-01-01" });
    const rates = {};
    hist.forEach(h => { rates[fmtDate(h.date)] = h.close; });
    await db.collection(col).doc(symbol).set({
      rates, lastUpdated: admin.firestore.FieldValue.serverTimestamp()
    });
    return;
  }

  // 股票：前復權價格 & 拆股倍率
  const priceHist = await yahooFinance.historical(symbol,
                     { period1: "2000-01-01", adjusted: true });
  const splitHist = await yahooFinance.historical(symbol,
                     { period1: "2000-01-01", events: "split" });

  const pricesAdj = {};
  priceHist.forEach(p => { pricesAdj[fmtDate(p.date)] = p.close; });

  let cum = 1;
  const cumSplitRatio = {};
  splitHist.forEach(s => {
    const r = s.numerator / s.denominator;
    cum *= r;
    cumSplitRatio[fmtDate(s.date)] = cum;
  });

  await db.collection(col).doc(symbol).set({
    pricesAdj, cumSplitRatio,
    lastUpdated: admin.firestore.FieldValue.serverTimestamp()
  });
}

//────────────────────────────────────────
// 工具函式
//────────────────────────────────────────
const fmtDate = d => new Date(d).toISOString().split("T")[0];

function getCumRatio(map = {}, dStr) {
  let ratio = 1;
  for (const k of Object.keys(map).sort())
    if (k <= dStr) ratio = map[k]; else break;
  return ratio;
}

function findPrice(map = {}, d) {
  const probe = new Date(d); probe.setUTCHours(probe.getUTCHours() + 12);
  for (let i = 0; i < 7; i++) {
    const key = fmtDate(new Date(probe.getTime() - i * 86400000));
    if (map[key] !== undefined) return map[key];
  }
  return null;
}

//────────────────────────────────────────
// 核心：計算持股
//────────────────────────────────────────
function calcHoldings(txs, md) {
  const rateHist = md["TWD=X"].rates || {};
  txs.sort((a, b) => (a.date.toDate ? a.date.toDate() : new Date(a.date)) -
                     (b.date.toDate ? b.date.toDate() : new Date(b.date)));

  const shares = {}, costOri = {}, costTWD = {}, cur = {};
  let realized = 0;

  for (const t of txs) {
    const d   = t.date.toDate ? t.date.toDate() : new Date(t.date);
    const sym = t.symbol.toUpperCase();
    const m   = md[sym] || {};
    const ratio = getCumRatio(m.cumSplitRatio, fmtDate(d));
    const qAdj  = t.quantity * ratio;
    const pAdj  = t.price / ratio;
    const val0  = qAdj * pAdj;
    const rt    = (t.currency === "USD") ?
                  (findPrice(rateHist, d) || 1) : 1;
    const valT  = val0 * rt;

    if (!shares[sym]) { shares[sym]=0; costOri[sym]=0; costTWD[sym]=0; }
    if (!cur[sym]) cur[sym] = t.currency || "TWD";

    if (t.type === "buy") {
      shares[sym]   += qAdj;
      costOri[sym]  += val0;
      costTWD[sym]  += valT;
    } else if (t.type === "sell") {
      const prop = qAdj / shares[sym];
      realized   += valT - costTWD[sym] * prop;
      shares[sym] -= qAdj;
      costOri[sym] *= (1 - prop);
      costTWD[sym] *= (1 - prop);
    } else if (t.type === "dividend") {
      realized += valT;
    }
  }

  // 彙整
  const holdings = {};
  for (const sym of Object.keys(shares)) {
    if (shares[sym] <= 1e-9) continue;
    const px = findPrice((md[sym]||{}).pricesAdj, new Date());
    const rt = (cur[sym] === "USD") ? (findPrice(rateHist, new Date())||1) : 1;
    const mv = shares[sym] * px * rt;
    holdings[sym] = {
      symbol          : sym,
      quantity        : shares[sym],
      avgCost         : costOri[sym] / shares[sym],
      totalCostTWD    : costTWD[sym],
      currentPrice    : px,
      marketValueTWD  : mv,
      unrealizedPLTWD : mv - costTWD[sym],
      returnRate      : costTWD[sym] > 0 ? (mv - costTWD[sym]) / costTWD[sym] * 100 : 0
    };
  }
  return { holdings, realizedPL: realized };
}

//────────────────────────────────────────
// 計算每日淨值
//────────────────────────────────────────
function calcHistory(txs, md) {
  if (!txs.length) return {};
  txs.sort((a, b) => (a.date.toDate ? a.date.toDate() : new Date(a.date)) -
                     (b.date.toDate ? b.date.toDate() : new Date(b.date)));

  const first = txs[0].date.toDate ? txs[0].date.toDate() : new Date(txs[0].date);
  const today = new Date();
  const rateHist = md["TWD=X"].rates || {};
  const history  = {};

  for (let cur = new Date(first); cur <= today; cur.setDate(cur.getDate()+1)) {
    const dStr = fmtDate(cur);
    let pv = 0;

    for (const t of txs) {
      const d0 = t.date.toDate ? t.date.toDate() : new Date(t.date);
      if (d0 > cur) continue;
      const sym = t.symbol.toUpperCase();
      const ratio = getCumRatio((md[sym]||{}).cumSplitRatio, dStr);
      const qAdj  = t.quantity * ratio * (t.type === "sell" ? -1 : (t.type === "buy" ? 1 : 0));
      history[sym] = (history[sym] || 0) + qAdj;
    }

    for (const sym of Object.keys(history)) {
      if (history[sym] <= 1e-9) continue;
      const px = findPrice((md[sym]||{}).pricesAdj, cur);
      if (px === null) continue;
      const rt = txs.find(x => x.symbol.toUpperCase() === sym).currency === "USD"
                 ? (findPrice(rateHist, cur) || 1) : 1;
      pv += history[sym] * px * rt;
    }
    history[dStr] = pv;
  }
  return history;
}
