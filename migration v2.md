股票分析系統：雲原生混合架構遷移計畫書
版本: 7.0 (最終執行藍圖)

日期: 2025-08-02

文件目的: 本文件是一份以行動為導向的執行手冊，旨在指導「股票分析系統」從 GCP/Firebase 後端，完整遷移至一個成本可控、體驗不降級、且具備長期可維運性的現代化混合雲架構。

1. 總覽 (Overview)
1.1 專案目標與設計原則
核心目標: 將系統遷移至一個成本可控、數據主權獨立、且使用者體驗不降級的「微服務混合雲」架構。
設計原則:
成本可控營運 (Cost-Controlled Operation): 接受小額投資 (建議 Cloudflare Workers Paid Plan, $5/月) 以換取巨大的穩定性與擴展性。
數據主權與獨立性 (Data Sovereignty & Independence): 以 Cloudflare D1 為數據核心，擺脫平台鎖定。
極致使用者體驗 (Ultimate User Experience): 保留「30 秒內完成前端顯示」的順暢體驗。
職責分離 (Separation of Concerns): 讓對的工具做對的事，提升系統彈性與可維運性。
1.2 最終架構藍圖
組件	基礎架構平台	長期優化建議平台	角色比喻
註冊/登入	Cloudflare Workers	Cloudflare Zero Trust	大門警衛
前端 (UI)	Cloudflare Pages	(維持不變)	接待大廳
資料庫 (DB)	Cloudflare D1	(維持不變)	中央檔案室
API/即時通知	Cloudflare Workers	(維持不變)	總機與指揮中心
即時數據抓取	Vercel Functions	Cloudflare Workers (搭配 Hyperdrive)	前線偵察兵
核心計算	Google Cloud Function	Cloudflare Workers (Unbound 模式)	計算專家
日常維護	GitHub Actions	Cloudflare Cron Triggers	後勤維護部隊
2. 企業級考量與長期演進
在執行遷移前，必須充分理解並規劃以下企業級策略，以確保專案的長期成功。

2.1 成本與配額：再細分層級
Workers 計價陷阱: 雖然 $5/月的 Paid Plan 能移除多數硬性限制，但仍有 10ms CPU、網路 egress 及 KV/D1 讀寫的分項計價。
行動要點: 在 PoC 階段即建立一張「單一請求成本表」，明列各項細項，並透過 Wrangler 的 `--profile` 或 OpenTelemetry 自動彙整，以「每 100 RPS」為單位做預估，量化未來放量後的月費。
Vercel 與 GCP 的冷啟動差異: GCP Cloud Functions 首次呼叫常見 1–2 秒冷啟動，若計算路徑對 FCP 影響大。
行動要點: 考慮改用 Cloudflare Workers Unbound 統一執行環境（配合 30 秒 CPU 上限），或在 Vercel/GCP 層前加一層 Request Coalescing（將同一 symbol 的重複請求合併為一次）。
2.2 D1 資料庫：結構與擴充策略
風險	建議措施	觸發條件
單區域寫入延遲	啟用 Hyperdrive + R2 快取關鍵查詢結果。	讀 QPS >3,000 或跨洲流量占比 >30%
交易表爆量	依 user_id % N 水平分庫；寫入 API 端做路由。	單表 >50 M rows
大型批次查詢	將歷史查價改用 R2 + Parquet，結合 Workers WASM 做向量掃描。	連續查價區間 >5 年
2.3 CI/CD 與 IaC：落地細節
Mono‐repo + Turborepo: 建議將 Workers、Pages、ETL、IaC 等所有相關專案置於同一個 Mono-repo 中，以 Turborepo 串聯測試與部署，加速跨服務的型號一致性與依賴管理。
Preview Stacks: 每個 PR 應自動以 Terraform Workspace + Pages Preview 生成完全隔離的預覽環境（包含獨立的 D1 Fork），確保 QA 測試不污染正式庫。
2.4 風險矩陣與 TCO 模型
風險矩陣: 應建立一份機率 (Probability) × 衝擊 (Impact) 的二維風險矩陣，將所有潛在風險（如 D1 爆量、Vercel 超額）視覺化，讓決策者能快速判讀優先級。
三年滾動 TCO 模型: 建立一個試算表模型，包含「保守／基準／樂觀」三種用戶與數據量的成長曲線，透過公式 月費估算 = (D1 Reads × $0.001) + (Writes × $0.005) + ...，明確預估在不同情境下的年度總擁有成本 (TCO)。
2.5 長期演進路線圖
時程	目標	關鍵指標
M+6	全面淘汰 Firebase；95% 請求落在 Workers/D1。	通常延遲 ≤150 ms (95th percentile)。
M+9	引入 Edge AI Inference（TensorFlow.js + WASM）做情緒分析。	新指標：單 inference 成本 <0.05¢。
M+12	部署多雲容器化計算層（Kubernetes + Karpenter）取代 GCP Function。	批次計算成本降低 30%。
3. 階段性部署策略與行動要點
本計畫將遷移過程分解為六個主要階段，每個階段都包含其主要工作、風險控管措施、及可量化的驗收指標 (KPI)。

階段 0：概念驗證 (PoC) & 基準測試
目標: 在投入大規模遷移前，驗證核心技術的可行性與性能瓶頸。
主要工作:
以真實的 1-3 GB Firestore 資料樣本，執行 ETL 腳本並導入一個測試用的 D1 資料庫。
編寫一個簡單的 Cloudflare Worker，對此 D1 資料庫進行高強度的讀寫壓力測試。
驗收 KPI:
產出包含讀寫延遲 (p95, p99) 與吞吐量 (RPS) 的基準測試報告。
根據報告，初步判斷觸發水平分片策略的性能閾值。
階段 1：地基工程 - 資料庫與數據層
目標: 建立穩固、可靠、且可擴展的數據基礎。
主要工作:
D1 資料庫建立 與 Schema 設計部署 (含複合索引)。
ETL 執行 與 數據導入。
風險控管與強化措施:
[資料一致性]: 執行隨機抽樣比對腳本，驗證 Firestore 與 D1 的數據差異。
[容量與流量]: 規劃冷熱分離策略，並使用 EXPLAIN QUERY PLAN 進行查詢治理。
[備份與還原]: 設定每日將 D1 的增量 WAL 備份至 R2，確保 RPO 在可接受範圍內。
驗收 KPI:
數據比對差異率 < 0.1%。
遷移後，D1 資料庫初始體積 < 300 MB。
所有核心查詢的 EXPLAIN 結果均顯示 USING INDEX。
階段 2：後端服務部署與安全強化
目標: 將所有獨立的後端計算與抓價服務部署上線，並建立安全連接。
主要工作:
Vercel/GCP Function 部署 與 GHA 維護任務設定。
IaC 導入: 使用 Terraform 或 Pulumi 將所有雲端資源配置程式碼化。
風險控管與強化措施:
[成本控制]: 在 Vercel、GCP、GitHub 平台啟用用量分析與自動化警報。
[跨雲安全]: 建立 JWKS 端點並實施金鑰輪換策略 (≤7 天存活期，24 小時雙簽發過渡)。
[SSRF 防護]: 為需要對外請求的 Worker 啟用 egress allowlist。
驗收 KPI:
所有後端服務的 API 呼叫成功率 ≥ 99.9%。
GCP 核心計算的 30 秒 SLA 達成率 ≥ 95%。
階段 2.5：安全風險掃描 (新增里程碑)
目標: 在大規模部署前，完成全面的自動化安全審計。
主要工作: 在 CI/CD 流程中整合 SCA (Software Composition Analysis) 工具 (如 Snyk) 與 SAST (Static Application Security Testing) 工具。
驗收 KPI:
產出 SBOM (Software Bill of Materials) 報告。
無 CVSS > 7.0 的未修復漏洞。
階段 3：中樞神經建立與可觀測性
目標: 部署 Cloudflare Worker 作為系統總管，並建立統一的監控體系。
主要工作:
Worker 邏輯開發: 實現 API 路由、SSE 推送、JWT 簽發與驗證、以及對 Vercel/GCP 的呼叫邏輯。
Feature Flag 實現: 將功能旗標存放在 Workers KV 中，以便實現毫秒級的全球快速回滾。
可觀測性整合: 導入 OpenTelemetry，讓最前端的 Worker 負責產生 traceparent 並向下傳遞。
風險控管與強化措施:
[即時通知配額]: 優先採用 SSE 處理單向通知。
[API 契約]: 使用 OpenAPI/JSON Schema 驗證 Worker 的 API 規格。
驗收 KPI:
在全量切流後，具備 5 分鐘內透過修改 KV 中的 Feature Flag，將計算請求回滾至舊 Firebase 服務的能力。
階段 3.5：觀測性驗收 (新增里程碑)
目標: 驗證端到端的監控能力。
主要工作: 設定 Grafana Cloud 或類似平台的儀表板。
驗收 KPI:
在單一儀表板上，能清晰地追蹤一次交易請求的端到端延遲 (end-to-end latency)。
儀表板需包含 RED (Rate, Errors, Duration) / USE (Utilization, Saturation, Errors) 等關鍵指標。
實現 Metrics⇄Logs 連結 (例如，透過 Grafana Exemplars)。
階段 4：前端改造與灰度上線
目標: 讓前端應用程式與全新的後端架構安全、無縫地對接，並正式上線。
主要工作:
前端程式碼改造 (移除 Firebase SDK, 整合 JWT/SSE)。
部署至 Pages。
灰度發布: 採用金絲雀路由 (cookie % 100 < weight)，實現 1%→5%→20% 線性擴張。
風險控管與強化措施:
[測試環境隔離]: 設定 Pages 的 Deploy Preview 規則，讓每次 PR 自動連接到對應的 D1 fork。
[即時回退]: 保留 /v1/ (新) 與 /v0/ (舊) 兩條 API 前綴，任何異常可透過 Page Rule 或修改前端 localStorage 中的版本設定，無須重新部署即可回退。
驗收 KPI:
在 500 位活躍用戶的模擬測試下無嚴重錯誤。
前端核心頁面的 FCP (First Contentful Paint) ≤ 2 秒。
灰度發布期間，新架構的錯誤率不高於舊架構。
7. 最終結論與建議
本計畫書提供了一條從 Firebase 遷移至現代化混合雲的清晰、穩健且具備專業風險控管的執行路徑。

接受小額投資: 在專案初期就考慮升級至 Workers Paid Plan ($5/月)，將其視為專案的「基礎保險」。這能一次性解除 D1、Workers 的多項硬性限制，是風險效益比最高的策略。
以架構應對擴展: 將 冷熱資料分離、水平分片、SSE 作為核心的擴展與優化策略，從根本上控制配額消耗。
以工程實踐保證品質: 嚴格執行 IaC、可觀測性、自動化測試、安全掃描，並透過 Feature Flag 灰度發布 與 自動化配額警報，確保任何階段皆能秒級回滾與成本預警。
如依此路徑執行，系統可在百萬級讀寫/月、千級活躍用戶/日的規模內，將雲端固定支出穩定壓在個位數美元；同時保留隨流量線性付費、無平台鎖定的擴展彈性，為專案的長期成功奠定堅實基礎。
