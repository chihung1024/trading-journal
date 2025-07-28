const functions = require("firebase-functions");
const admin = require("firebase-admin");
admin.initializeApp();
const db = admin.firestore();

exports.onTransactionChange = functions.firestore
  .document("users/{userId}/transactions/{txId}")
  .onWrite(async (change, context) => {
    const userId = context.params.userId;
    const snapshot = await db
      .collection("users")
      .doc(userId)
      .collection("transactions")
      .get();

    const transactions = snapshot.docs.map(doc => doc.data());
    const splitsSnapshot = await db
      .collection("users")
      .doc(userId)
      .collection("splits")
      .get();
    const userSplits = splitsSnapshot.docs.map(doc => doc.data());

    const marketSnapshot = await db.collection("price_history").get();
    const marketData = {};
    marketSnapshot.docs.forEach(doc => {
      marketData[doc.id.toUpperCase()] = doc.data();
    });

    const exchangeSnapshot = await db.collection("exchange_rates").get();
    exchangeSnapshot.docs.forEach(doc => {
      marketData[doc.id.toUpperCase()] = doc.data();
    });

    const portfolio = calculatePortfolio(transactions, userSplits, marketData);
    await db
      .collection("users")
      .doc(userId)
      .collection("user_data")
      .doc("current_holdings")
      .set(portfolio, { merge: true });
  });

function calculatePortfolio(transactions, userSplits, marketData) {
  const events = [];
  const symbols = [...new Set(transactions.map(t => t.symbol.toUpperCase()))];

  for (const t of transactions) {
    events.push({
      ...t,
      date: new Date(t.date),
      eventType: "transaction",
    });
  }
  for (const split of userSplits) {
    events.push({
      ...split,
      date: new Date(split.date),
      eventType: "split",
    });
  }

  for (const symbol of symbols) {
    const stockData = marketData[symbol];
    if (!stockData) continue;
    const dividends = stockData.dividends || {};
    for (const [date, amount] of Object.entries(dividends)) {
      events.push({
        date: new Date(date),
        symbol,
        amount,
        eventType: "dividend",
      });
    }
  }

  events.sort((a, b) => new Date(a.date) - new Date(b.date));

  const portfolio = {};
  let totalRealizedPL = 0;

  for (const event of events) {
    const symbol = event.symbol.toUpperCase();
    if (!portfolio[symbol]) {
      portfolio[symbol] = { lots: [], currency: "USD" };
    }

    const rateHistory = marketData["TWD=X"]?.rates || {};
    const rateOnDate = findNearestDataPoint(rateHistory, event.date);

    switch (event.eventType) {
      case "transaction":
        const t = event;
        portfolio[symbol].currency = t.currency;
        const costPerShareOriginal = t.totalCost || t.price;
        const costPerShareTWD =
          costPerShareOriginal * (t.currency === "USD" ? rateOnDate : 1);

        if (t.type === "buy") {
          portfolio[symbol].lots.push({
            quantity: t.quantity,
            pricePerShareTWD: costPerShareTWD,
            pricePerShareOriginal: costPerShareOriginal,
            date: event.date,
          });
        } else if (t.type === "sell") {
          let sharesToSell = t.quantity;
          const saleValueTWD =
            (t.totalCost || t.quantity * t.price) *
            (t.currency === "USD" ? rateOnDate : 1);
          let costOfSoldSharesTWD = 0;

          while (sharesToSell > 0 && portfolio[symbol].lots.length > 0) {
            const lot = portfolio[symbol].lots[0];
            if (lot.quantity <= sharesToSell) {
              costOfSoldSharesTWD += lot.quantity * lot.pricePerShareTWD;
              sharesToSell -= lot.quantity;
              portfolio[symbol].lots.shift();
            } else {
              costOfSoldSharesTWD += sharesToSell * lot.pricePerShareTWD;
              lot.quantity -= sharesToSell;
              sharesToSell = 0;
            }
          }
          totalRealizedPL += saleValueTWD - costOfSoldSharesTWD;
        }
        break;

      case "split":
        portfolio[symbol].lots.forEach(lot => {
          lot.quantity *= event.ratio;
          lot.pricePerShareOriginal /= event.ratio;
          lot.pricePerShareTWD /= event.ratio;
        });
        break;

      case "dividend":
        const totalShares = portfolio[symbol].lots.reduce(
          (sum, lot) => sum + lot.quantity,
          0
        );
        const dividendTWD =
          event.amount * totalShares * (portfolio[symbol].currency === "USD" ? rateOnDate : 1);
        totalRealizedPL += dividendTWD;
        break;
    }
  }

  const finalHoldings = {}; // 省略詳細持股呈現
  const cashflows = createCashflows(events, portfolio, marketData);
  const xirr = calculateXIRR(cashflows);

  return {
    totalRealizedPL,
    xirr: isNaN(xirr) ? null : xirr,
    lastUpdated: new Date().toISOString(),
  };
}

function createCashflows(events, portfolio, marketData) {
  const cashflows = [];

  const rateHistory = marketData["TWD=X"]?.rates || {};
  for (const e of events) {
    const rateOnDate = findNearestDataPoint(rateHistory, e.date);
    const currency = portfolio[e.symbol]?.currency || "USD";
    const rate = currency === "USD" ? rateOnDate : 1;

    if (e.eventType === "transaction") {
      const amount =
        (e.totalCost || e.quantity * e.price) * (e.type === "buy" ? -1 : 1) * rate;
      cashflows.push({
        date: new Date(e.date),
        amount,
      });
    }

    if (e.eventType === "dividend") {
      const totalShares = portfolio[e.symbol]?.lots.reduce(
        (sum, lot) => sum + lot.quantity,
        0
      ) || 0;
      const amount = e.amount * totalShares * rate;
      if (amount > 0) {
        cashflows.push({
          date: new Date(e.date),
          amount,
        });
      }
    }
  }

  return cashflows;
}

function calculateXIRR(cashflows) {
  if (cashflows.length < 2) return NaN;
  const values = cashflows.map(c => c.amount);
  const dates = cashflows.map(c => new Date(c.date));
  const yearFractions = dates.map(
    d => (d - dates[0]) / (1000 * 60 * 60 * 24 * 365)
  );

  const npv = rate =>
    values.reduce((sum, val, i) => sum + val / Math.pow(1 + rate, yearFractions[i]), 0);

  const derivative = rate =>
    values.reduce((sum, val, i) => {
      return (
        sum -
        (yearFractions[i] > 0
          ? (val * yearFractions[i]) / Math.pow(1 + rate, yearFractions[i] + 1)
          : 0)
      );
    }, 0);

  let guess = 0.1;
  const tolerance = 1e-6;
  const maxIterations = 100;

  for (let i = 0; i < maxIterations; i++) {
    const f = npv(guess);
    const d = derivative(guess);
    if (Math.abs(d) < 1e-9) break;
    const newGuess = guess - f / d;
    if (Math.abs(newGuess - guess) < tolerance) return newGuess;
    guess = newGuess;
  }

  return guess;
}

function findNearestDataPoint(history, targetDate) {
  if (!history || Object.keys(history).length === 0) return 1;

  const d = new Date(targetDate);
  d.setUTCHours(12, 0, 0, 0);

  for (let i = 0; i < 7; i++) {
    const searchDate = new Date(d);
    searchDate.setDate(searchDate.getDate() - i);
    const dateStr = searchDate.toISOString().split("T")[0];
    if (history[dateStr] !== undefined) {
      return history[dateStr];
    }
  }

  return 1;
}
