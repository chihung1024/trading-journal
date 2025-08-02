# 股票分析系統：雲原生混合架構遷移計畫書
**版本**: 1.0
**日期**: 2025-08-02

## 1. 專案目標

本計畫旨在將現有的股票分析系統後端，從 Firebase/GCP 生態系完整遷移至一個以 Cloudflare 為核心，結合 Vercel 與 GitHub Actions 的「微服務混合雲」架構。核心目標是在實現**近乎零成本**營運的同時，**完整保留**現有的即時、流暢的使用者體驗。

## 2. 核心架構總覽

我們將採用「各司其職」的策略，讓每個平台發揮其最大優勢：

| 組件 | 平台 | 核心職責 |
| :--- | :--- | :--- |
| **前端介面 (UI)** | Cloudflare Pages | 使用者互動與數據呈現。 |
| **核心資料庫 (DB)** | Cloudflare D1 | 所有業務數據的儲存中心。 |
| **API/即時通知** | Cloudflare Workers | **系統總管**：API 閘道、WebSocket 即時通道、後端服務協調器。 |
| **即時數據抓取** | Vercel Functions | **偵察兵**：執行 Node.js `yahoo-finance2`，應對緊急的數據需求。 |
| **批次數據與計算** | GitHub Actions | **重裝部隊**：執行 Node.js 核心演算法、每日批次維護數據。 |

---

## 3. 組件詳細解析與設定

### 3.1 Cloudflare Pages & D1
* **職責**: 部署前端靜態檔案 (`index.html`) 並提供核心資料庫。
* **設定**:
    1.  在 Cloudflare 建立一個 Pages 專案，連接到您的 GitHub 倉庫。
    2.  在 Cloudflare 儀表板建立 D1 資料庫，並執行先前提供的 SQL 腳本完成建表與數據遷移。
    3.  在 Pages 專案的設定中，將 D1 資料庫綁定到您的 Pages Function (即 Worker)。

### 3.2 Cloudflare Worker
* **職責**: 作為系統的總管與交通樞紐。
* **關鍵程式碼 (`worker.js`)**: 需要處理三種類型的請求：
    1.  `HTTP API` 請求 (例如 `/api/transactions`)。
    2.  `WebSocket` 升級請求 (例如 `/ws`)。
    3.  來自 GHA 的 `HTTP 回呼`請求 (例如 `/api/calculation-complete`)。
* **設定檔 (`wrangler.toml`)**:
    ```toml
    name = "stock-portfolio-worker"
    main = "src/worker.js" # 您的 Worker 程式碼入口
    compatibility_date = "2024-07-25"

    # 綁定 D1 資料庫
    [[d1_databases]]
    binding = "DB" # 在 Worker 中用 env.DB 存取
    database_name = "your-d1-database-name"
    database_id = "your-d1-database-id"

    # 設定環境變數 (Secrets)，用於存放 API 金鑰
    [vars]
    VERCEL_API_URL = "[https://your-vercel-project.vercel.app/api/fetch-price](https://your-vercel-project.vercel.app/api/fetch-price)"
    GITHUB_API_TOKEN_SECRET = "your-github-pat" # 建議用 Secret
    GITHUB_REPO_URL = "https/[api.github.com/repos/your-user/your-repo](https://api.github.com/repos/your-user/your-repo)"
    ```

### 3.3 Vercel Function
* **職責**: 擔任 Node.js `yahoo-finance2` 的專用執行器。
* **專案結構**:
    ```
    /yfinance-api-wrapper
    ├── /api
    │   └── fetch-price.js  # Serverless Function
    ├── package.json
    └── vercel.json
    ```
* **設定檔 (`vercel.json`)**:
    ```json
    {
      "functions": {
        "api/fetch-price.js": {
          "maxDuration": 30 // 設定最長執行時間為 30 秒，以應對慢速的 API 回應
        }
      }
    }
    ```

### 3.4 GitHub Action
* **職責**: 執行耗時的核心計算與每日數據維護。
* **執行環境**: **Node.js v18** (與您原始的 `index.js` 環境一致)。
* **設定檔 (`.github/workflows/recalculate.yml`)**:
    ```yaml
    name: Portfolio Recalculation

    on:
      repository_dispatch:
        types: [recalculate-portfolio] # 由 Worker 觸發

    jobs:
      calculate:
        runs-on: ubuntu-latest
        steps:
          - name: Checkout repository
            uses: actions/checkout@v4

          - name: Set up Node.js 18
            uses: actions/setup-node@v4
            with:
              node-version: 18

          - name: Cache Node.js modules
            uses: actions/cache@v4
            with:
              path: ~/.npm
              key: ${{ runner.os }}-node-${{ hashFiles('**/package-lock.json') }}
              restore-keys: |
                ${{ runner.os }}-node-

          - name: Install dependencies
            run: npm install

          - name: Run calculation script
            env:
              # 所有密鑰和設定都從 GitHub Secrets 傳入
              D1_DATABASE_ID: ${{ secrets.D1_DATABASE_ID }}
              D1_API_TOKEN: ${{ secrets.D1_API_TOKEN }}
              CF_WORKER_URL: ${{ secrets.CF_WORKER_URL }}
              CF_CALLBACK_SECRET: ${{ secrets.CF_CALLBACK_SECRET }}
              # 從觸發事件中獲取 userId
              USER_ID: ${{ github.event.client_payload.userId }}
            run: node ./path/to/your/calculation-script.js
    ```

---

## 4. 優化後的核心工作流程 (並行觸發)

這是整合了您優化建議的最終流程：

1.  **[T=0s] 前端**：呼叫 **CF Worker** 的 API 端點。
2.  **[T=1s] CF Worker**：收到請求後，**同時並行**處理兩件事：
    * **任務 A**: 向 **Vercel Function** 發出 `fetch` 請求，抓取即時數據。
    * **任務 B**: 向 **GitHub API** 發送 `repository_dispatch` 事件，命令 **GitHub Action** 開始啟動並安裝環境。
3.  **[T=~8s] Vercel**：完成數據抓取，將結果返回給 **CF Worker**。
4.  **[T=~9s] CF Worker**：接收到 Vercel 的數據，將其寫入 **D1 資料庫**，並立刻給**前端**返回「已受理」的訊息。
5.  **[T=~90s] GitHub Action**：此時，它的環境已準備就緒。它直接從 **D1 資料庫**讀取剛剛由 Vercel 存入的數據以及所有歷史數據。
6.  **[T=~120s] GitHub Action**：完成所有核心計算，將結果寫回 **D1**，並向 **CF Worker** 發送回呼。
7.  **[T=~121s] CF Worker**：透過 WebSocket 向**前端**推送「計算完成」的通知。
8.  **[T=~122s] 前端**：收到通知，自動從 Worker 請求 D1 中的最新結果，並刷新 UI。

這個流程透過並行處理，將總體等待時間從「Vercel 時間 + GHA 時間」變成了「Max(Vercel 時間, GHA 時間)」，極大地提升了效率。

---

## 5. 結論

本計畫書提出了一個兼顧**零成本、高效能、優質體驗**的現代化混合雲架構。它將原始設計中的優點（即時數據獲取、非同步計算）在一個全新的、免費的技術棧上進行了還原與升級。雖然架構涉及多個組件，但每個組件職責單一、清晰，遵循了微服務的最佳實踐，將使您的專案更具彈性、更易於維護和擴展。
