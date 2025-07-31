/* eslint-disable */
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const yahooFinance = require("yahoo-finance2").default;

admin.initializeApp();
const db = admin.firestore();

// --- 基礎工具函式 ---
const toDate = v => v.toDate ? v.toDate() : new Date(v);
const currencyToFx = { USD: "TWD=X", HKD: "HKD=TWD", JPY: "JPY=TWD" };

function isTwStock(symbol) {
    if (!symbol) return false;
    const upperSymbol = symbol.toUpperCase();
    return upperSymbol.endsWith('.TW') || upperSymbol.endsWith('.TWO');
}

function getTotalCost(tx) {
  return (tx.totalCost !== undefined && tx.totalCost !== null)
    ? Number(tx.totalCost)
    : Number(tx.price || 0) * Number(tx.quantity || 0);
}

function findNearest(hist, date, toleranceDays = 7) {
    if (!hist || Object.keys(hist).length === 0) return undefined;
    const tgt = date instanceof Date ? date : new Date(date);
    const tgtStr = tgt.toISOString().slice(0, 10);
    if (hist[tgtStr]) return hist[tgtStr];
    for (let i = 1; i <= toleranceDays; i++) {
        const checkDate = new Date(tgt);
        checkDate.setDate(checkDate.getDate() - i);
        const checkDateStr = checkDate.toISOString().split('T')[0];
        if (hist[checkDateStr]) return hist[checkDateStr];
    }
    const sortedDates = Object.keys(hist).sort((a, b) => new Date(b) - new Date(a));
    for (const dateStr of sortedDates) {
        if (dateStr <= tgtStr) return hist[dateStr];
    }
    return undefined;
}

function findFxRate(market, currency, date, tolerance = 15) {
  if (!currency || currency === "TWD") return 1;
  const fxSym = currencyToFx[currency];
  if (!fxSym) return 1;
  const hist = market[fxSym]?.rates || {};
  return findNearest(hist, date, tolerance) ?? 1;
}

// --- 核心計算函式 (此區塊無變動) ---
// ... (此處省略未變動的 calculate... 系列函式，以節省篇幅，實際貼上時請包含所有函式)
// getPortfolioStateOnDate, dailyValue, prepareEvents, calculateDailyPortfolioValues, 
// calculateTwrHistory, createCashflowsForXirr, calculateXIRR, calculateCoreMetrics ...

// FINAL FIX: 修改此函式以區分要更新和要刪除的持股
function calculateFinalHoldings(pf, market) {
  const holdingsToUpdate = {};
  const holdingsToDelete = [];
  const today = new Date();

  for (const sym in pf) {
    const h = pf[sym];
    const qty = h.lots.reduce((s, l) => s + l.quantity, 0);
    
    if (qty > 1e-9) {
        const totCostTWD = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareTWD, 0);
        const totCostOrg = h.lots.reduce((s, l) => s + l.quantity * l.pricePerShareOriginal, 0);
        const priceHist = market[sym]?.prices || {};
        const curPrice = findNearest(priceHist, today);
        const fx = findFxRate(market, h.currency, today);
        const mktVal = qty * (curPrice ?? 0) * (h.currency === "TWD" ? 1 : fx);
        const unreal = mktVal - totCostTWD;
        const invested = totCostTWD + h.realizedCostTWD;
        const totalRet = unreal + h.realizedPLTWD;
        const rrCurrent = totCostTWD > 0 ? (unreal / totCostTWD) * 100 : 0;
        const rrTotal = invested > 0 ? (totalRet / invested) * 100 : 0;
        holdingsToUpdate[sym] = {
          symbol: sym, quantity: qty, currency: h.currency,
          avgCostOriginal: totCostOrg > 0 ? totCostOrg / qty : 0, totalCostTWD: totCostTWD, investedCostTWD: invested,
          currentPriceOriginal: curPrice ?? null, marketValueTWD: mktVal,
          unrealizedPLTWD: unreal, realizedPLTWD: h.realizedPLTWD,
          returnRateCurrent: rrCurrent, returnRateTotal: rrTotal, returnRate: rrCurrent
        };
    } else {
        holdingsToDelete.push(sym);
    }
  }
  return { holdingsToUpdate, holdingsToDelete };
}

function calculateCoreMetrics(evts, market, log) {
    const pf = {};
    let totalRealizedPL = 0;
    for (const e of evts) {
        const sym = e.symbol.toUpperCase();
        if (!pf[sym]) pf[sym] = { lots: [], currency: e.currency || "USD", realizedPLTWD: 0, realizedCostTWD: 0 };
        switch (e.eventType) {
            case "transaction": {
                let fx;
                if (e.exchangeRate && e.currency !== 'TWD') {
                    fx = e.exchangeRate;
                } else {
                    fx = findFxRate(market, e.currency, toDate(e.date));
                }
                const costTWD = getTotalCost(e) * (e.currency === "TWD" ? 1 : fx);

                if (e.type === "buy") {
                    pf[sym].lots.push({ quantity: e.quantity, pricePerShareOriginal: e.price, pricePerShareTWD: costTWD / e.quantity, date: toDate(e.date) });
                } else {
                    let sellQty = e.quantity;
                    const saleProceedsTWD = costTWD;
                    let costOfGoodsSoldTWD = 0;
                    while (sellQty > 0 && pf[sym].lots.length > 0) {
                        const lot = pf[sym].lots[0];
                        const qtyToSell = Math.min(sellQty, lot.quantity);
                        costOfGoodsSoldTWD += qtyToSell * lot.pricePerShareTWD;
                        lot.quantity -= qtyToSell;
                        sellQty -= qtyToSell;
                        if (lot.quantity < 1e-9) pf[sym].lots.shift();
                    }
                    const realized = saleProceedsTWD - costOfGoodsSoldTWD;
                    totalRealizedPL += realized;
                    pf[sym].realizedCostTWD += costOfGoodsSoldTWD;
                    pf[sym].realizedPLTWD += realized;
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
                const stateOnDate = getPortfolioStateOnDate(evts, toDate(e.date));
                const shares = stateOnDate[sym]?.lots.reduce((s, l) => s + l.quantity, 0) || 0;
                if (shares > 0) {
                    const fx = findFxRate(market, pf[sym].currency, toDate(e.date));
                    const taxRate = isTwStock(sym) ? 0.0 : 0.30;
                    const postTaxAmount = e.amount * (1 - taxRate);
                    const divTWD = postTaxAmount * shares * (pf[sym].currency === "TWD" ? 1 : fx);
                    totalRealizedPL += divTWD;
                    pf[sym].realizedPLTWD += divTWD;
                }
                break;
            }
        }
    }
    const { holdingsToUpdate, holdingsToDelete } = calculateFinalHoldings(pf, market);
    const xirrFlows = createCashflowsForXirr(evts, holdingsToUpdate, market);
    const xirr = calculateXIRR(xirrFlows);
    const totalUnrealizedPL = Object.values(holdingsToUpdate).reduce((sum, h) => sum + h.unrealizedPLTWD, 0);
    const totalInvestedCost = Object.values(holdingsToUpdate).reduce((sum, h) => sum + h.totalCostTWD, 0) + Object.values(pf).reduce((sum, p) => sum + p.realizedCostTWD, 0);
    const totalReturnValue = totalRealizedPL + totalUnrealizedPL;
    const overallReturnRate = totalInvestedCost > 0 ? (totalReturnValue / totalInvestedCost) * 100 : 0;
    return { holdings: { holdingsToUpdate, holdingsToDelete }, totalRealizedPL, xirr, overallReturnRate };
}


// --- 資料庫與網路請求 (已整合 Metadata) ---

// MODIFIED: fetchAndSaveMarketData 現在接受一個起始日期
async function fetchAndSaveMarketData(symbol, startDate, log) {
  try {
    let startDateString = '2000-01-01'; // 預設值
    if(startDate) {
        const d = toDate(startDate);
        d.setMonth(d.getMonth() - 1); // 往前推一個月作為緩衝
        startDateString = d.toISOString().split('T')[0];
    }

    log(`Fetching history for ${symbol} from ${startDateString} from Yahoo Finance...`);
    
    const hist = await yahooFinance.historical(symbol, { period1: startDateString, interval: '1d' });
    const prices = hist.reduce((acc, cur) => {
        if (cur.close) acc[cur.date.toISOString().split("T")[0]] = cur.close;
        return acc;
    }, {});
    const dividends = hist.reduce((acc, cur) => {
        if(cur.dividends && cur.dividends > 0) acc[cur.date.toISOString().split("T")[0]] = cur.dividends;
        return acc;
    }, {});
    
    const payload = { prices, splits: {}, dividends, lastUpdated: admin.firestore.FieldValue.serverTimestamp(), dataSource: "yfinance-on-demand-v8" };
    if (symbol.includes("=")) {
      payload.rates = payload.prices;
      delete payload.dividends;
    }

    const col = symbol.includes("=") ? "exchange_rates" : "price_history";
    // 使用 set merge，以合併方式寫入，避免覆蓋掉使用者手動輸入的 splits
    await db.collection(col).doc(symbol).set(payload, { merge: true });
    log(`Successfully fetched and wrote history for ${symbol}.`);
    return payload;

  } catch (e) {
    log(`ERROR: fetchAndSaveMarketData for ${symbol} failed. Reason: ${e.message}`);
    return null;
  }
}

// MODIFIED: getMarketDataFromDb 現在會讀取 metadata
async function getMarketDataFromDb(txs, benchmarkSymbol, log) {
  const syms = [...new Set(txs.map(t => t.symbol.toUpperCase()))];
  const currencies = [...new Set(txs.map(t => t.currency || "USD"))].filter(c => c !== "TWD");
  const fxSyms = currencies.map(c => currencyToFx[c]).filter(Boolean);
  const allRequiredSymbols = [...new Set([...syms, ...fxSyms, benchmarkSymbol.toUpperCase()])];

  log(`Data check for symbols: ${allRequiredSymbols.join(', ')}`);
  const marketData = {};
  
  for (const s of allRequiredSymbols) {
    if (!s) continue;
    const col = s.includes("=") ? "exchange_rates" : "price_history";
    const ref = db.collection(col).doc(s);
    const doc = await ref.get();
    
    if (doc.exists) {
      marketData[s] = doc.data();
    } else {
      log(`Data for ${s} not found in Firestore. Fetching now...`);
      // 讀取 metadata 決定起始日期
      const metadataRef = db.collection('stock_metadata').doc(s);
      const metadataDoc = await metadataRef.get();
      const earliestTxDate = metadataDoc.data()?.earliestTxDate;

      const fetchedData = await fetchAndSaveMarketData(s, earliestTxDate, log);
      if (fetchedData) {
        marketData[s] = fetchedData;
      } else {
        throw new Error(`Failed to fetch critical market data for ${s}. Aborting calculation.`);
      }
    }
  }
  log("All required market data is present and loaded.");
  return marketData;
}


// --- 主計算流程 ---
async function performRecalculation(uid) {
    // ... (此函式除了最末尾的寫入資料庫部分外，無變動)
    // FINAL FIX: 使用 update() 和 dot notation 來進行精確的更新與刪除
    const { holdingsToUpdate, holdingsToDelete } = portfolioResult.holdings;
    const updatePayload = {
        // 注意：這裡不再直接寫入 holdingsToUpdate
        totalRealizedPL: portfolioResult.totalRealizedPL,
        xirr: portfolioResult.xirr,
        overallReturnRate: portfolioResult.overallReturnRate,
        benchmarkSymbol: benchmarkSymbol,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp()
    };

    // 分別設定要更新的持股
    for (const symbol in holdingsToUpdate) {
        updatePayload[`holdings.${symbol}`] = holdingsToUpdate[symbol];
    }

    // 分別設定要刪除的持股
    for (const symbolToDelete of holdingsToDelete) {
        updatePayload[`holdings.${symbolToDelete}`] = admin.firestore.FieldValue.delete();
    }
    
    // 如果 holdingsToUpdate 為空且 holdingsToDelete 為空，表示清空所有持股
    if (Object.keys(holdingsToUpdate).length === 0 && holdingsToDelete.length === 0) {
        updatePayload.holdings = {};
    }

    // 執行一次精確的更新
    await holdingsRef.update(updatePayload);
}


// --- Triggers ---
// ... (recalculatePortfolio, onBenchmarkUpdate, triggerRecalculation 等觸發器無變動)

// NEW: 維護 stock_metadata 的新觸發器
exports.updateStockMetadata = functions.firestore
  .document("users/{uid}/transactions/{txId}")
  .onWrite(async (change, context) => {
    // 找出受影響的股票代碼
    const beforeData = change.before.data();
    const afterData = change.after.data();
    const symbols = new Set();
    if (beforeData) symbols.add(beforeData.symbol.toUpperCase());
    if (afterData) symbols.add(afterData.symbol.toUpperCase());

    console.log(`Transaction changed. Updating metadata for symbols: ${Array.from(symbols).join(', ')}`);

    for (const symbol of symbols) {
      const metadataRef = db.collection("stock_metadata").doc(symbol);
      
      // 使用 collectionGroup 查詢，高效找出該股票目前的最早交易日期
      const query = db.collectionGroup("transactions")
        .where("symbol", "==", symbol)
        .orderBy("date", "asc")
        .limit(1);
      
      const snapshot = await query.get();
      
      if (snapshot.empty) {
        // 如果已沒有任何交易，則刪除 metadata
        console.log(`No transactions left for ${symbol}. Deleting metadata.`);
        await metadataRef.delete();
      } else {
        // 否則，更新為找到的最早日期
        const earliestDate = snapshot.docs[0].data().date;
        console.log(`Earliest transaction for ${symbol} is now ${toDate(earliestDate).toISOString()}. Updating metadata.`);
        await metadataRef.set({
          earliestTxDate: earliestDate,
          symbol: symbol,
          lastUpdated: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
      }
    }
    return null;
  });
