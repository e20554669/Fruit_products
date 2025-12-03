氣果相連｜水果價格預測與天氣整合系統
本專案整合 農業部農糧署水果交易資料與中央氣象署逐時氣象資料，建置自動化 ETL Pipeline、雲端資料庫，
並運用 XGBoost 進行水果價格趨勢預測。

專案目標
整合異質資料（水果 × 天氣 × 面積 × 產量）
建立自動化 ETL 資料處理流程（Airflow）
建置 MySQL 雲端資料庫（GCP）
發展可解釋的天氣加權因子
建立 7 天水果價格預測模型
視覺化展示（Tableau Dashboard）

專案架構
Data Source (API)                                                                         
│- 農業部農糧署（水果交易量與價格資料）                 
│- 中央氣象署  （逐時氣象資料）    

Local Development
 ├─ VS Code + Docker Devcontainer
 ├─ Python + Poetry
 └─ XGBoost Model Training
        │
        ▼
Version Control (Git / GitHub)
        │  Push code & model artifacts
        ▼
Cloud Service (GCP)
 ├─ Compute Engine (ETL Node)
 │   ├─ Docker Runtime
 │   ├─ Airflow DAG (ETL Orchestration)
 │   ├─ Pandas (In-Memory Processing)
 │   └─ Startup Script (Auto git pull)
 │
 └─ MySQL Data Warehouse
       ├─ fruit_city_price
       ├─ weather_hourly
       ├─ area_production
       ├─ volume
       └─ model_predictions
        │
        ▼
Google Sheets (Data Sync)
        │
        ▼
Tableau Dashboard (Visualization)

技術包含：
● Python（Pandas、Requests、PyMySQL）
● Airflow DAG 自動化排程
● GCP Compute Engine + Startup Script
● MySQL schema 設計
● XGBoost Model
● Tableau 互動式視覺化

資料欄位標準化
資料來自三大來源（水果、天氣、種植面積與產量），欄位名稱格式完全不同：
有作物名稱、品項名稱、品種名稱混雜
因此，利用人工篩選並建立名稱對照表，統一所有欄位名稱，使所有資料成功整合。

資料庫設計（MySQL）
複合主鍵確保資料唯一性
外鍵設計（crop_id / city_id）確保跨資料一致性
正規化降低儲存冗餘、提升查詢效率

資料表包含：
volume
weather
area_production
crop
city
prediction_view

機器學習模型（XGBoost）
採用：時間序列滑動視窗
天氣加權因子（以種植面積/產量為權重）
統一建模 vs 個別建模比較
最佳模型 MAPE 錯誤率約 9%

Tableau 視覺化儀表板
水果價格趨勢
面積/產量分布
互動式地圖
天氣與價格關聯
未來 7 日價格預測折線圖

成果總結
成功整合多表資料（水果 × 天氣 × 面積 × 產量）
建立自動化 ETL Pipeline
完成雲端部署與 CI/CD
建置準確度 85% 的價格預測模型
完整視覺化儀表板提供分析與決策

聯絡方式
如需面試 Demo，歡迎聯繫我！
