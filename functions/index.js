/* eslint-disable */
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

/* ---------- utils ---------- */
function getTotalCost(tx) {
  return (tx.totalCost !== undefined && tx.totalCost !== null)
    ? Number(tx.totalCost)
    : Number(tx.price || 0) * Number(tx.quantity || 0);
}

/* 幣別對照表 */
const currencyToFx = {
  USD: "TWD=X",
  HKD: "HKD=TWD",
  JPY: "JPY=TWD"
  // TWD 省略
};

/* ================================================================
 * 核心： Portfolio Recalculation
 * ================================================================ */
// ==================================================================
// === 偵錯專用版 performRecalculation，用來測試最單純的寫入 ===
// ==================================================================
async function performRecalculation(uid) {
  const logRef = db.doc(`users/${uid}/user_data/calculation_logs`);
  const logs = [];
  const log = msg => {
    const ts = new Date().toISOString();
    logs.push(`${ts}: ${msg}`);
    console.log(`[${uid}] ${ts}: ${msg}`);
  };

  try {
    log("--- [DEBUG] Running simplified test write ---");
    const holdingsRef = db.doc(`users/${uid}/user_data/current_holdings`);

    // 建立一個絕對合法的、最簡單的測試資料物件
    const testData = {
      debug_status: "SUCCESS",
      message: "This is a simple test write.",
      test_timestamp: new Date() // 使用標準 Date 物件
    };

    console.log(`[DEBUG] Attempting to write simple test data to path: ${holdingsRef.path}`);
    console.log("[DEBUG] Test data content:", testData);

    // 執行最單純的 set 操作
    await holdingsRef.set(testData);

    log("[SUCCESS] Simplified test write appears to have completed!");

  } catch (e) {
    console.error(`[CRITICAL] The simple test write FAILED:`, e);
    logs.push(`CRITICAL (simple test): ${e.message}\n${e.stack}`);
  } finally {
    await logRef.set({ entries: logs });
  }
}

/* ================================================================
 * Triggers
 * ================================================================ */
exports.recalculatePortfolio = functions.runWith({ timeoutSeconds: 300, memory: "1GB" })
  .firestore.document("users/{uid}/user_data/current_holdings")
  .onWrite((chg, ctx) => {
    const beforeData = chg.before.data();
    const afterData = chg.after.data();

    // [最終修正] 這是最重要的防護。如果文件被刪除，afterData會不存在。
    // 這條規則會立即停止函式，防止因刪除而觸發不必要的計算。
    if (beforeData && !afterData) {
      console.log(`[${ctx.params.uid}] current_holdings document was deleted. Halting execution.`);
      return null;
    }

    // 只有在文件是新建的，或是時間戳有變動時，才執行重度計算
    if (!beforeData || (afterData.force_recalc_timestamp !== beforeData.force_recalc_timestamp)) {
      console.log(`[${ctx.params.uid}] Trigger condition met. Starting recalculation.`);
      return performRecalculation(ctx.params.uid);
    }
    
    // 其他所有情況 (例如：函式自己寫入結果時的觸發)，不執行任何操作
    console.log(`[${ctx.params.uid}] Write event did not meet trigger conditions. No action.`);
    return null;
  });

exports.recalculateOnTransaction = functions.firestore
  .document("users/{uid}/transactions/{txId}")
  .onWrite((_, ctx) =>
    db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
  );

exports.recalculateOnSplit = functions.firestore
  .document("users/{uid}/splits/{splitId}")
  .onWrite((_, ctx) =>
    db.doc(`users/${ctx.params.uid}/user_data/current_holdings`)
      .set({ force_recalc_timestamp: admin.firestore.FieldValue.serverTimestamp() }, { merge: true })
  );

exports.recalculateOnPriceUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore.document("price_history/{symbol}")
  .onWrite(async (chg, ctx) => {
    const s = ctx.params.symbol.toUpperCase();
    const before = chg.before.exists ? chg.before.data() : null;
    const after = chg.after.data();
    if (
      before &&
      JSON.stringify(before.prices) === JSON.stringify(after.prices) &&
      JSON.stringify(before.dividends) === JSON.stringify(after.dividends)
    ) return null;

    const txSnap = await db.collectionGroup("transactions").where("symbol", "==", s).get();
    if (txSnap.empty) return null;
    const users = new Set(
      txSnap.docs.map(d => d.ref.path.split("/")[1])
        .filter(uid => typeof uid === "string" && uid.length > 0)
    );
    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all([...users].map(async uid => {
      try {
        await db.doc(`users/${uid}/user_data/current_holdings`)
          .set({ force_recalc_timestamp: ts }, { merge: true });
      } catch (e) {
        console.error(`Error updating current_holdings for user ${uid}:`, e);
      }
    }));
    return null;
  });

exports.recalculateOnFxUpdate = functions.runWith({ timeoutSeconds: 240, memory: "1GB" })
  .firestore.document("exchange_rates/{fxSym}")
  .onWrite(async (chg, ctx) => {
    const b = chg.before.exists ? chg.before.data() : null;
    const a = chg.after.data();
    if (b && JSON.stringify(b.rates) === JSON.stringify(a.rates)) return null;
    const users = await db.collection("users").listDocuments();
    const ts = admin.firestore.FieldValue.serverTimestamp();
    await Promise.all(users.map(u =>
      db.doc(`users/${u.id}/user_data/current_holdings`)
        .set({ force_recalc_timestamp: ts }, { merge: true })
    ));
    return null;
  });

/* ================================================================
 * Market-data helpers
 * ================================================================ */
async function getMarketDataFromDb(txs, log) {
  const syms = [...new Set(txs.map(t => t.symbol.toUpperCase()))];
  const currencies = [...new Set(txs.map(t => t.currency || "USD"))].filter(c => c !== "TWD");
  const fxSyms = currencies.map(c => currencyToFx[c]).filter(Boolean);
  const all = [...new Set([...syms, ...fxSyms])];
  const out = {};
  for (const s of all) {
    const col = s.includes("=") ? "exchange_rates" : "price_history";
    const ref = db.collection(col).doc(s);
    const doc = await ref.get();
    if (doc.exists) {
      out[s] = doc.data();
      continue;
    }
    const fetched = await fetchAndSaveMarketData(s, log);
    if (fetched) {
      out[s] = fetched;
      await ref.set(fetched);
    } else {
      log(`fetch market data failed for symbol: ${s}, set empty object`);
      out[s] = {};
    }
  }
  return out;
}

async function fetchAndSaveMarketData(symbol, log) {
  try {
    const hist = await yahooFinance.historical(symbol, { period1: "2000-01-01" });
    const prices = {};
    hist.forEach(i => prices[i.date.toISOString().split("T")[0]] = i.close);
    const payload = {
      prices,
      splits: {},
      dividends: (hist.dividends || []).reduce((a, d) => ({ ...a, [d.date.toISOString().split("T")[0]]: d.amount }), {}),
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      dataSource: "emergency-fetch"
    };
    if (symbol.includes("=")) {
      payload.rates = payload.prices;
      delete payload.dividends;
    }
    return payload;
  } catch (e) {
    log(`fetch ${symbol} err ${e.message}`);
    return null;
  }
}

/* ================================================================
 * Portfolio calculation
 * ================================================================ */
function calculatePortfolio(txs, splits, market, log) {
  // 計算每支股票首次持有日
  const firstBuyDateMap = {};
  txs.forEach(tx => {
    if (tx.type === "buy") {
      const sym = tx.symbol.toUpperCase();
      const d = toDate(tx.date);
      if (!firstBuyDateMap[sym] || d < firstBuyDateMap[sym]) firstBuyDateMap[sym] = d;
    }
  });

  // --- 組事件佇列 ---
  const evts = [
    ...txs.map(t => ({ ...t, date: toDate(t.date), eventType: "transaction" })),
    ...splits.map(s => ({ ...s, date: toDate(s.date), eventType: "split" }))
  ];
  // 加進配息，但要過濾早於首次持有日的現金流
  [...new Set(txs.map(t => t.symbol.toUpperCase()))].forEach(sym => {
    Object.entries(market[sym]?.dividends || {}).forEach(([d, amt]) => {
      if (firstBuyDateMap[sym] && new Date(d) >= firstBuyDateMap[sym])
        evts.push({ date: new Date(d), symbol: sym, amount: amt, eventType: "dividend" });
      // 早於首次持有日會被排除
    });
  });
  evts.sort((a, b) => new Date(a.date) - new Date(b.date));

  // --- 模擬 FIFO ---
  const pf = {};
  let totalRealizedPL = 0;
  for (const e of evts) {
    if (!e.symbol) { log(`Warning: missing symbol in event: ${JSON.stringify(e)}`); continue; }
    const sym = e.symbol.toUpperCase();
    if (!pf[sym]) pf[sym] = { lots: [], currency: "USD" };
    switch (e.eventType) {
      case "transaction": {
        const t = e;
        pf[sym].currency = t.currency;
        const fx = findFxRate(market, t.currency, t.date);
        const cost = getTotalCost(t);
        const costTWD = cost * (t.currency === "TWD" ? 1 : fx);
        if (t.type === "buy") {
          pf[sym].lots.push({
            quantity: t.quantity,
            pricePerShareOriginal: cost / t.quantity,
            pricePerShareTWD: costTWD / t.quantity
          });
        } else {
          let q = t.quantity;
          const saleTWD = costTWD;
          let costSold = 0;
          while (q > 0 && pf[sym].lots.length) {
            const lot = pf[sym].lots[0];
            if (lot.quantity <= q) { costSold += lot.quantity * lot.pricePerShareTWD; q -= lot.quantity; pf[sym].lots.shift(); }
            else { costSold += q * lot.pricePerShareTWD; lot.quantity -= q; q = 0; }
          }
          const realized = saleTWD - costSold;
          totalRealizedPL += realized;
          pf[sym].realizedCostTWD = (pf[sym].realizedCostTWD || 0) + costSold;
          pf[sym].realizedPLTWD = (pf[sym].realizedPLTWD || 0) + realized;
        }
        break;
      }
      case "split":
        pf[sym].lots.forEach(l => {
          l.quantity *= e.ratio;
          l.pricePerShareTWD /= e.ratio;
          l.pricePerShareOriginal /= e.ratio;
        });
        break;
      case "dividend": {
        const shares = pf[sym].lots.reduce((s, l) => s + l.quantity, 0);
        if (shares === 0) break;
        const fx = findFxRate(market, pf[sym].currency, e.date);
        const divTWD = e.amount * shares * (pf[sym].currency === "TWD" ? 1 : fx);
        totalRealizedPL += divTWD;
        pf[sym].realizedPLTWD = (pf[sym].realizedPLTWD || 0) + divTWD;
        break;
      }
    }
  }

  const holdings = calculateFinalHoldings(pf, market);
  const history = calculatePortfolioHistory(evts, market);
  const xirr = calculateXIRR(createCashflows(evts, pf, holdings, market, firstBuyDateMap));

  let investedTotal = 0, totalReturnTWD = 0;
  for (const s in pf) {
    const h = pf[s];
    const realizedCost = h.realizedCostTWD || 0;
    const realizedPL = h.realizedPLTWD || 0;
    const unrealizedPL = holdings[s]?.unrealizedPLTWD || 0;
    const remainingCost = holdings[s]?.totalCostTWD || 0;
    investedTotal += remainingCost + realizedCost;
    totalReturnTWD += unrealizedPL + realizedPL;
  }
  const overallReturnRateTotal = investedTotal > 0 ? (totalReturnTWD / investedTotal) * 100 : 0;

  return {
    holdings,
    totalRealizedPL,
    portfolioHistory: history,
    xirr,
    overallReturnRateTotal,
    overallReturnRate: overallReturnRateTotal
  };
}

/* ================================================================
 * Helpers
 * ================================================================ */
// [最終修正] 修正後的 calculateFinalHoldings 函式
function calculateFinalHoldings(pf, market) {
  const out = {}, today = new Date();
  for (const sym in pf) {
    const h = pf[sym], qty = h.lots.reduce((s, l) => s + l.quantity, 0);
    if (qty < 1e-9) continue;
    const totCostTWD = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareTWD, 0);
    const totCostOrg = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareOriginal, 0);
    const priceHist = (market[sym]?.prices) || {};
    const curPrice = findNearest(priceHist, today);
    const fx = findFxRate(market, h.currency, today);
    const mktVal = qty * (curPrice ?? 0) * (h.currency === "TWD" ? 1 : fx);
    const unreal = mktVal - totCostTWD;
    const invested = totCostTWD + (h.realizedCostTWD || 0);
    const totalRet = unreal + (h.realizedPLTWD || 0);
    const rrCurrent = totCostTWD > 0 ? (unreal / totCostTWD) * 100 : 0;
    const rrTotal = invested > 0 ? (totalRet / invested) * 100 : 0;
    out[sym] = {
      symbol: sym,
      quantity: qty,
      currency: h.currency,
      avgCostOriginal: totCostOrg / qty,
      totalCostTWD: totCostTWD,
      investedCostTWD: invested,
      // 這是關鍵修正：確保 currentPriceOriginal 永遠不會是 undefined
      currentPriceOriginal: curPrice ?? null,
      marketValueTWD: mktVal,
      unrealizedPLTWD: unreal,
      realizedPLTWD: h.realizedPLTWD || 0,
      returnRateCurrent: rrCurrent,
      returnRateTotal: rrTotal,
      returnRate: rrCurrent
    };
  }
  return out;
}

function calculatePortfolioHistory(evts, market) {
  const txEvts = evts.filter(e => e.eventType === "transaction");
  if (txEvts.length === 0) return {};
  const first = new Date(txEvts[0].date), today = new Date();
  let cur = new Date(first); cur.setUTCHours(0, 0, 0, 0);
  const hist = {};
  while (cur <= today) {
    const key = cur.toISOString().split("T")[0];
    const state = getPortfolioStateOnDate(evts, cur);
    hist[key] = dailyValue(state, market, cur);
    cur.setDate(cur.getDate() + 1);
  }
  return hist;
}

function createCashflows(evts, pf, holdings, market, firstBuyDateMap) {
  const flows = [];
  // 1. 交易
  evts.filter(e => e.eventType === "transaction").forEach(t => {
    const fx = findFxRate(market, t.currency, t.date);
    const amt = getTotalCost(t) * (t.currency === "TWD" ? 1 : fx);
    flows.push({ date: toDate(t.date), amount: t.type === "buy" ? -amt : amt });
  });

  // 2. 股利
  evts.filter(e => e.eventType === "dividend").forEach(d => {
    const sym = d.symbol.toUpperCase();
    // 僅保留在持有日以後的配息
    if (!firstBuyDateMap[sym] || new Date(d.date) < firstBuyDateMap[sym]) return;
    const fx = findFxRate(market, pf[sym]?.currency || "USD", d.date);
    const shares = pf[sym]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
    if (shares === 0) return;
    const amt = d.amount * shares * (pf[sym]?.currency === "TWD" ? 1 : fx);
    if (amt > 0) flows.push({ date: new Date(d.date), amount: amt });
  });

  // 3. 期末清算價值（只推一筆正現金流！）
  const mktVal = Object.values(holdings).reduce((s, h) => s + h.marketValueTWD, 0);
  if (mktVal > 0) {
    const today = new Date();
    flows.push({ date: today, amount: mktVal }); // 只留下正數
  }

  // 4. 合併同日現金流
  const combineByDate = {};
  for (const f of flows) {
    const k = (f.date instanceof Date ? f.date.toISOString().slice(0, 10) : String(f.date).slice(0, 10));
    combineByDate[k] = (combineByDate[k] || 0) + f.amount;
  }
  const mergedFlows = Object.entries(combineByDate)
    .filter(([_, amt]) => Math.abs(amt) > 1e-6)
    .map(([date, amt]) => ({ date: new Date(date), amount: amt }))
    .sort((a, b) => a.date - b.date);

  // Debug log
  console.table(mergedFlows.map(f => ({
    date: f.date.toISOString().slice(0, 10),
    amt: f.amount
  })));
  return mergedFlows;
}


function calculateXIRR(flows) {
  if (!Array.isArray(flows) || flows.length < 2) return null;
  const amounts = flows.map(f => f.amount);
  if (!amounts.some(v => v < 0) || !amounts.some(v => v > 0)) return null;
  const dates = flows.map(f => f.date);
  const years = dates.map(d => (d - dates[0]) / (365 * 24 * 60 * 60 * 1000));
  const npv = r => amounts.reduce((s, v, i) => s + v / Math.pow(1 + r, years[i]), 0);
  const dnpv = r => amounts.reduce((s, v, i) => s - v * years[i] / Math.pow(1 + r, years[i] + 1), 0);

  // 用多初始值增加穩定性
  for(let base = 0; base <= 4; base++) {
    let r = 0.1 + base * 0.2;       // 多組起點，0.1, 0.3, 0.5, ...
    for(let i = 0; i < 300; i++) {   // 迭代數加多
      const f = npv(r);
      const f1 = dnpv(r);
      if (Math.abs(f1) < 1e-10) {
        r += 0.1;
        continue;                    // 碰到平坦區就換新猜值
      }
      const nr = r - f / f1;
      if (!isFinite(nr)) break;
      if (Math.abs(nr - r) < 1e-6) return nr;  // 收斂即回值
      r = nr;
    }
  }
  return null;
}


function dailyValue(state, market, date) {
  let tot = 0;
  for (const sym in state) {
    const s = state[sym], qty = s.lots.reduce((sum, l) => sum + l.quantity, 0);
    if (qty === 0) continue;
    const price = findNearest((market[sym]?.prices) || {}, date);
    if (price === undefined) continue;
    const fx = findFxRate(market, s.currency, date);
    tot += qty * price * (s.currency === "TWD" ? 1 : fx);
  }
  return tot;
}

function getPortfolioStateOnDate(allEvts, target) {
  const st = {}, past = allEvts.filter(e => new Date(e.date) <= target),
    fSplits = allEvts.filter(e => e.eventType === "split" && new Date(e.date) > target);
  for (const e of past) {
    if (!e.symbol) continue;
    const sym = e.symbol.toUpperCase();
    if (!st[sym]) st[sym] = { lots: [], currency: "USD" };
    switch (e.eventType) {
      case "transaction":
        st[sym].currency = e.currency;
        if (e.type === "buy") st[sym].lots.push({ quantity: e.quantity });
        else {
          let q = e.quantity;
          while (q > 0 && st[sym].lots.length) {
            const lot = st[sym].lots[0];
            if (lot.quantity <= q) { q -= lot.quantity; st[sym].lots.shift(); }
            else { lot.quantity -= q; q = 0; }
          }
        }
        break;
      case "split": st[sym].lots.forEach(l => l.quantity *= e.ratio); break;
    }
  }
  for (const sym in st) {
    fSplits.filter(sp => sp.symbol.toUpperCase() === sym)
      .forEach(sp => st[sym].lots.forEach(l => l.quantity *= sp.ratio));
  }
  return st;
}

/* ---------- 匯率／價格最近值 ---------- */
function findNearest(hist, date, toleranceDays = 0) {
  if (!hist || Object.keys(hist).length === 0) return undefined;
  const keys = Object.keys(hist).sort();
  const tgt = date instanceof Date ? date : new Date(date);
  const tgtStr = tgt.toISOString().slice(0, 10);
  let lo = 0, hi = keys.length - 1, cand = null;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (keys[mid] <= tgtStr) { cand = keys[mid]; lo = mid + 1; }
    else hi = mid - 1;
  }
  if (!cand) return undefined;
  if (toleranceDays > 0) {
    const diff = (new Date(tgtStr) - new Date(cand)) / 86400000;
    if (diff > toleranceDays) return undefined;
  }
  return hist[cand];
}
function findFxRate(market, currency, date, tolerance = 15) {
  if (!currency || currency === "TWD") return 1;
  const fxSym = currencyToFx[currency];
  if (!fxSym) return 1;
  const hist = market[fxSym]?.rates || {};
  const rate = findNearest(hist, date, tolerance);
  return rate ?? 1;
}
const toDate = v => v.toDate ? v.toDate() : new Date(v);
