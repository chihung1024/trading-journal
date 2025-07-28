const functions = require("firebase-functions");
const admin = require("firebase-admin");
const axios = require("axios");
admin.initializeApp();
const db = admin.firestore();

exports.onTransactionChange = functions.firestore
  .document("users/{userId}/transactions/{txId}")
  .onWrite(async (change, context) => {
    const userId = context.params.userId;
    const symbol =
      change.after.exists && change.after.data().symbol
        ? change.after.data().symbol.toUpperCase()
        : change.before.data().symbol.toUpperCase();

    // 🔁 嘗試觸發 GitHub Action 以更新股價與配息
    try {
      await triggerUpdateStockData(symbol);
      console.log(`Triggered GitHub Action for symbol: ${symbol}`);
    } catch (err) {
      console.error(`Failed to trigger GitHub Action: ${err.message}`);
    }

    // 🔁 繼續計算 portfolio
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

// ⏩ 新增這段：呼叫 GitHub repository_dispatch 事件
async function triggerUpdateStockData(symbol) {
  const GITHUB_REPO = "你的帳號/你的Repo名稱"; // ⬅ 替換為你的 Repo 名稱
  const GITHUB_TOKEN = "ghp_xxx..."; // ⬅ 使用你的 GitHub Personal Access Token

  await axios.post(
    `https://api.github.com/repos/${GITHUB_REPO}/dispatches`,
    {
      event_type: "update-stock",
      client_payload: {
        symbol: symbol,
      },
    },
    {
      headers: {
        Authorization: `Bearer ${GITHUB_TOKEN}`,
        Accept: "application/vnd.github.everest-preview+json",
      },
    }
  );
}
