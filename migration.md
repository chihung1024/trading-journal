# 股票分析系統：雲原生混合架構遷移計畫書

**版本**: 1.1 (最終版)
**日期**: 2025-08-02
**提案人**: Gemini

## 1. 摘要與前言

本提案旨在為現有的「股票交易紀錄與資產分析系統」制定一套完整的後端遷移計畫。原始專案架構深度依賴 Google Cloud Platform (GCP) 的 Firebase 生態系，雖然功能完善，但隨著數據量與使用者的增長，存在潛在的營運成本風險。

核心目標是將系統遷移至一個**成本近乎為零、數據主權獨立、且使用者體驗不降級**的現代化「微服務混合雲」架構。本計畫將詳細闡述最終的架構設計、每個組件的職責、詳細的設定指南，以及一個分四階段執行的清晰行動藍圖，確保遷移過程平順、穩健、可控。

## 2. 核心目標與設計原則

新的架構基於以下四大設計原則，以確保專案的長期健康與成功：

* **零成本營運 (Zero-Cost Operation)**
    我們將策略性地組合 Cloudflare、Vercel、Google Cloud 及 GitHub 的慷慨永久免費額度，確保在可預見的未來，即使使用者和數據量大幅增長，系統的營運成本依然為零。

* **數據主權與獨立性 (Data Sovereignty & Independence)**
    將核心資料庫從封閉的 Firestore 遷移至開放的 Cloudflare D1 (基於 SQL)，讓您的數據完全獨立，不再被單一平台綁定，未來具備更高的靈活性與掌控力。

* **極致使用者體驗 (Ultimate User Experience)**
    堅決不妥協！新架構將完整保留原始專案「30 秒內完成前端顯示」的順暢體驗。透過精巧的架構設計，使用者在新增交易後，無需等待漫長的背景計算，即可獲得即時的回饋與最終的自動化介面更新。

* **職責分離 (Separation of Concerns)**
    遵循現代雲端應用開發的最佳實踐：「讓對的工具做對的事」。我們將原先由單一服務處理的所有任務，拆解給最適合的平台執行，從而獲得最佳的性能、可靠性與成本效益。

## 3. 最終架構詳解

我們將採用一個由六個獨立服務組成的「微服務混合雲」架構，各司其職，協同工作。

| 組件 | 平台 | 核心職責 | 角色比喻 |
| :--- | :--- | :--- | :--- |
| **註冊/登入** | Cloudflare Workers | 處理使用者帳號註冊、登入，並核發 JWT 權杖。 | **大門警衛** |
| **前端 (UI)** | Cloudflare Pages | 呈現所有圖表與數據，處理使用者互動。 | **接待大廳** |
| **資料庫 (DB)** | Cloudflare D1 | 儲存所有交易、使用者及價格歷史數據。 | **中央檔案室** |
| **API/即時通知** | Cloudflare Workers | 處理前端 API 請求、管理 WebSocket 即時通道、協調後端所有服務。 | **總機與指揮中心** |
| **即時數據抓取** | Vercel Functions | 執行 `yahoo-finance2`，應對緊急的、單一股票的數據獲取需求。 | **前線偵察兵** |
| **核心計算** | Google Cloud Function | 執行耗時的核心演算法 (`index.js` 邏輯)，兼顧免費與速度。 | **計算專家** |
| **日常維護** | GitHub Actions | 每日批次抓取所有股票數據，維護資料庫完整性。 | **後勤維護部隊** |

## 4. 核心工作流程解析

### A. 即時計算流程 (當使用者新增一筆交易時)

1.  **[T=0s] 前端 (Pages)** → 呼叫 → **CF Worker** 的 `/api/transactions` 端點。
2.  **[T=1s] CF Worker** → 檢查 **D1**，若需新數據，則呼叫 → **Vercel Function**。
3.  **[T=~8s] Vercel** → 返回價格數據 → **CF Worker**。
4.  **[T=~9s] CF Worker** → 將新數據存入 **D1**，並立刻給**前端**一個「已受理」的快速回饋。
5.  **[T=~10s] CF Worker** → 透過 HTTP 請求觸發 → **GCP Cloud Function**，命令其開始核心計算。
6.  **[T=~30s] GCP Cloud Function** → 計算完畢，將結果寫回 **D1**，並向 **CF Worker** 發送回呼。
7.  **[T=~31s] CF Worker** → 透過 WebSocket 推送完成訊息 → **前端**。
8.  **[T=~32s] 前端** → 收到通知，自動抓取新結果並刷新 UI。

### B. 日常維護流程 (每日定時)

1.  **GitHub Actions** → `cron` 定時器觸發。
2.  **GHA** → 執行 Python 腳本，使用 `yfinance` 抓取**所有**使用者持股的最新數據。
3.  **GHA** → 將數據覆蓋寫入 → **Cloudflare D1 資料庫**。

## 5. 四階段遷移執行計畫

建議將整個遷移過程分解為四個清晰、可管理的階段，以確保每一步都穩固可靠。

### 第一階段：地基工程 - 資料庫遷移

* **目標**: 建立並填充新的 D1 資料庫。
* **步驟**:
    1.  **建立資料庫**: 在 Cloudflare 儀表板建立一個新的 D1 資料庫。
    2.  **定義表結構 (Schema)**: 執行以下 SQL 指令完成建表。
        ```sql
        -- 使用者表 (用於 JWT 認證)
        CREATE TABLE users (
          id TEXT PRIMARY KEY,
          email TEXT NOT NULL UNIQUE,
          password_hash TEXT NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- 交易紀錄表
        CREATE TABLE transactions (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          date TEXT NOT NULL,
          symbol TEXT NOT NULL,
          type TEXT NOT NULL CHECK(type IN ('buy', 'sell')),
          quantity REAL NOT NULL,
          price REAL NOT NULL,
          currency TEXT NOT NULL,
          exchange_rate REAL,
          total_cost REAL,
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        
        -- 其他表: splits, price_history, exchange_rates...
        ```
    3.  **遷移數據**: 在本機準備好 `serviceAccountKey.json`，執行 `migrate_from_firebase.py` 腳本。這將會生成一個 `migration_data.sql` 檔案。
    4.  **導入數據**: 將 `migration_data.sql` 檔案的全部內容，複製並貼到 D1 資料庫的控制台中執行。
* **產出**: 一個擁有您所有歷史數據、隨時可用的 D1 資料庫。

### 第二階段：後端服務部署

* **目標**: 將三個獨立的後端服務部署上線。
* **步驟**:
    1.  **部署 Vercel Function**: 建立一個新的 Node.js 專案，包含 `api/fetch-price.js`，並將其部署到 Vercel。
    2.  **部署 GCP Function**: 將原始 `index.js` 的核心計算邏輯，打包成一個可被 HTTP 觸發的 Cloud Function，並部署到 GCP。確保其擁有公開存取權限或透過 IAM 進行安全調用。
    3.  **設定 GHA 維護任務**: 修改 `update_prices.yml`，將其連接到 D1 資料庫（透過 Cloudflare API Token），並設定好每日執行的 `cron` 排程。

### 第三階段：中樞神經建立

* **目標**: 部署 Cloudflare Worker，將所有服務串連起來。
* **步驟**:
    1.  **建立 Worker 專案**: 使用 `wrangler` CLI 工具建立新專案。
    2.  **編寫核心邏輯**: 實現 API 路由、WebSocket 伺服器、JWT 認證、以及呼叫 Vercel 和 GCP 的 `fetch` 邏輯。
    3.  **綁定與設定**: 在 `wrangler.toml` 檔案中，綁定 D1 資料庫，並將 Vercel、GCP、GitHub 等服務的 URL 和 API 金鑰設定為環境變數 (Secrets)。
    4.  **部署 Worker**: 將 Worker 部署到 Cloudflare 網路。

### 第四階段：前端改造與上線

* **目標**: 讓前端應用程式與全新的後端架構對接。
* **步驟**:
    1.  **移除 Firebase SDK**: 從 `index.html` 中徹底移除所有 Firebase 相關的 `<script>` 標籤和 JavaScript 程式碼。
    2.  **實現 JWT 認證流程**: 修改登入/註冊表單，使其呼叫 Worker 的 `/api/login` 和 `/api/register` 端點，並在成功後將 JWT 存入 `localStorage`。
    3.  **改造數據操作**: 將所有 `addDoc`, `onSnapshot` 等操作，改為使用 `fetch` API，並在請求的 Header 中附上 JWT。
    4.  **整合 WebSocket**: 加入 WebSocket 連線邏輯，監聽來自後端的計算完成通知，並觸發 UI 自動刷新。
    5.  **部署上線**: 將改造後的前端專案部署到 Cloudflare Pages，並進行端到端的完整測試。

## 6. 成本效益分析

本架構經過精心設計，能最大化利用各大平台的永久免費額度。

* **Cloudflare (Pages, D1, Workers)**: 免費方案提供每日數十萬次的請求、大量的數據庫讀寫與儲存空間，完全滿足專案需求。
* **Vercel (Functions)**: Hobby 方案提供每月大量的無伺服器函式執行時間，足以應付所有即時數據抓取。
* **Google Cloud (Functions)**: 永久免費額度提供每月 200 萬次調用，對於計算觸發頻率不高的本專案而言，成本為零。
* **GitHub (Actions)**: 公開專案每月提供 2000 分鐘的執行時間，遠超每日維護任務所需。

**結論：本架構的每月營運成本，在可預見的未來將穩定為 0 元。**

## 7. 總結

本提案為您的股票分析系統擘畫了一條從「單一平台綁定」邁向「現代化、高彈性、零成本混合雲」的清晰路徑。它不僅解決了您對潛在成本的憂慮，更在技術上進行了全面的升級，採用了微服務、邊緣運算、非同步處理等業界最佳實踐。

雖然遷移過程需要投入時間與精力，但完成後，您將擁有一個技術先進、架構穩固、體驗流暢且無後顧之憂的個人專案，為其未來的發展奠定了最堅實的基礎。
