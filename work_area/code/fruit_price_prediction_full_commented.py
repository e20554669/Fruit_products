#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
全水果合併分析（含氣象特徵，預測 7 天後價格，MySQL 版本）- 改進版 v6-GLOBAL + MySQL Output + Full Comments
=========================================================================================================
此腳本的功能總覽：
1. 從 MySQL 的 `crop_weather_data` 表讀取「作物 + 氣象」整併好的日資料。
2. 對每一個水果分別建立一個「單作物 XGBoost 模型」(Single Model)：
   - 僅使用該作物的歷史資料訓練與預測。
3. 建立一個「全水果統一 XGBoost 模型」(Global Model)：
   - 將所有作物的資料合併，並加入 `crop_index` 這個數值型作物編碼，訓練單一模型。
4. 使用「去年同期價格 (lag-365)」作為 Baseline 模型，與上述兩種 XGBoost 模型比較。
5. 對每個作物計算：
   - 測試期 (test window) 的 MAPE、MAE、RMSE、R² 等指標。
   - 相對於 Baseline 的改善百分比。
6. 使用遞迴預測 (recursive forecast) 對每個作物產生「未來 7 天」價格預測。
7. 保留原本所有本地端輸出：
   - 單作物模型與全水果統一模型的 CSV、JSON 指標與可視化圖檔。
8. 將「Single + Global + Baseline」合併後的結果上傳至 MySQL 表 `price_prediction_7days`：
   - 欄位：id, date, crop_id, price_actual,
           price_prediction_single, price_prediction_global, price_baseline

重要設計：
- 嚴格避免資料洩漏：
  - 先切 train/test，再在各自資料集上做 lag / rolling 等特徵工程。
  - Baseline 透過 shift(365) 來取得「去年同期價格」，不會偷看未來。
- 適用於：季節性明顯、價格高波動的農產品價格預測。
"""

import os
import warnings
import time
import numpy as np
import pandas as pd
from datetime import timedelta
import json
import mysql.connector
from mysql.connector import Error
from contextlib import contextmanager

# 機器學習相關套件
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# 設定中文字型與負號顯示，避免繪圖時亂碼
plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "SimHei", "Microsoft JhengHei"]
plt.rcParams["axes.unicode_minus"] = False
warnings.filterwarnings("ignore")

# =============================================================================
# MySQL 資料庫連線設定
# =============================================================================
DB_CONFIG = {
    "host": "104.199.220.12",
    "port": 3306,
    "database": "tjr103-team02",
    "user": "tjr103-team02",
    "password": "password",
}

@contextmanager
def get_mysql_connection():
    """
    建立 MySQL 連線的 context manager。
    - 使用 with 區塊時自動建立連線，結束時自動關閉連線。
    - 若連線過程出錯會印出錯誤訊息並拋出例外。
    """
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        yield conn
    except Error as e:
        print(f"❌ MySQL 連線錯誤: {e}")
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()

def load_data_from_mysql():
    """
    從 MySQL 讀取 `crop_weather_data` 表，作為整個價格預測流程的「輸入資料」。

    回傳：
        df (DataFrame): 內含 date, crop_id, crop_price_per_kg, 以及多個氣象欄位。
    """
    print("=" * 80)
    print("📊 從 MySQL 資料庫讀取資料...")
    print("=" * 80)

    with get_mysql_connection() as conn:
        query = "SELECT * FROM crop_weather_data"
        df = pd.read_sql(query, conn)

    print(f"✅ 成功讀取 {len(df)} 筆資料")
    print(f"📅 日期範圍: {df['date'].min()} 到 {df['date'].max()}")
    print(f"🌾 作物數量: {df['crop_id'].nunique()} 種")

    # 確保 date 欄位為 datetime 型態
    df['date'] = pd.to_datetime(df['date'])

    return df

# === 新版：將 Single + Global 預測結果寫入 MySQL =============================

def save_predictions_to_mysql_v2(predictions_merged_df, table_name='price_prediction_7days'):
    """
    將「單作物模型 + 全水果統一模型 + Baseline」合併後的預測結果寫入 MySQL。

    輸入：
        predictions_merged_df (DataFrame):
            需包含欄位：
                - date
                - crop_id
                - price_actual                (測試期有值，未來 7 天為 NaN)
                - price_prediction_single     (單作物模型預測)
                - price_prediction_global     (全水果統一模型預測)
                - price_baseline              (去年同期價格)
        table_name (str): 要寫入的資料表名稱（預設為 price_prediction_7days）

    MySQL 表格 schema：
        - id INT AUTO_INCREMENT PRIMARY KEY
        - date DATE NOT NULL
        - crop_id VARCHAR(20) NOT NULL
        - price_actual DECIMAL(10, 2)
        - price_prediction_single DECIMAL(10, 2)
        - price_prediction_global DECIMAL(10, 2)
        - price_baseline DECIMAL(10, 2)
    """
    print("\n" + "=" * 80)
    print(f"💾 將預測結果寫入 MySQL 資料庫 (表: {table_name})...")
    print("=" * 80)

    # 確保必要欄位存在，缺少即丟錯
    required_cols = [
        "date", "crop_id", "price_actual",
        "price_prediction_single", "price_prediction_global", "price_baseline"
    ]
    for col in required_cols:
        if col not in predictions_merged_df.columns:
            raise ValueError(f"缺少必要欄位: {col}")

    # 轉換日期型態為 date（去掉時間資訊）
    df_to_insert = predictions_merged_df.copy()
    if not np.issubdtype(df_to_insert["date"].dtype, np.datetime64):
        df_to_insert["date"] = pd.to_datetime(df_to_insert["date"])
    df_to_insert["date"] = df_to_insert["date"].dt.date

    with get_mysql_connection() as conn:
        cursor = conn.cursor()

        # 安全起見：先刪除舊表（如果存在），再重建
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # 建立新表（若結構要改，直接改這段 SQL）
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL,
            crop_id VARCHAR(20) NOT NULL,
            price_actual DECIMAL(10, 2),
            price_prediction_single DECIMAL(10, 2),
            price_prediction_global DECIMAL(10, 2),
            price_baseline DECIMAL(10, 2),
            INDEX idx_date_crop (date, crop_id),
            INDEX idx_crop (crop_id),
            INDEX idx_date (date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        cursor.execute(create_table_sql)
        print(f"✅ 成功創建表 {table_name}")

        # 準備 INSERT SQL
        insert_sql = f"""
        INSERT INTO {table_name}
        (date, crop_id, price_actual, price_prediction_single, price_prediction_global, price_baseline)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        # 將 DataFrame 轉成 list of tuples，交給 cursor.executemany
        data_to_insert = []
        for _, row in df_to_insert.iterrows():
            data_to_insert.append((
                row["date"],
                str(row["crop_id"]),
                float(row["price_actual"]) if pd.notna(row["price_actual"]) else None,
                float(row["price_prediction_single"]) if pd.notna(row["price_prediction_single"]) else None,
                float(row["price_prediction_global"]) if pd.notna(row["price_prediction_global"]) else None,
                float(row["price_baseline"]) if pd.notna(row["price_baseline"]) else None,
            ))

        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()

        print(f"✅ 成功插入 {len(data_to_insert)} 筆預測資料")
        print("📊 資料統計：")
        print(f"   - 有實際價格的筆數: {df_to_insert['price_actual'].notna().sum()}")
        print(f"   - 未來預測筆數: {df_to_insert['price_actual'].isna().sum()}")

        cursor.close()

# =============================================================================
# 資料前處理與特徵工程（改進版：分離計算）
# =============================================================================

def add_time_features(df):
    """
    添加時間相關特徵：
    - year: 年份
    - month: 月份
    - dayofyear: 當年的第幾天（1~365/366）
    - dayofweek: 星期幾 (0=Monday, 6=Sunday)
    """
    df = df.copy()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["dayofyear"] = df["date"].dt.dayofyear
    df["dayofweek"] = df["date"].dt.dayofweek
    return df

def compute_price_features(df, target_col="crop_price_per_kg", lookback=7):
    """
    計算價格相關的滯後與統計特徵（針對單一作物）。

    產生：
    - price_lag1 ~ price_lag7：過去 1~7 天的價格
    - price_rolling_mean7：過去 7 天 (以昨天為止) 的平均價格
    - price_rolling_std7：過去 7 天價格的標準差
    - price_diff1：前一日與前前一日的價差 (再向後 shift 1，避免使用當日資訊)
    """
    df = df.sort_values("date").copy()

    # 建立價格滯後特徵
    for lag in range(1, lookback + 1):
        df[f"price_lag{lag}"] = df[target_col].shift(lag)

    # 建立 7 日滾動平均與標準差（使用 shift(1) 確保只用到昨天以前的資料）
    df["price_rolling_mean7"] = df[target_col].shift(1).rolling(7, min_periods=1).mean()
    df["price_rolling_std7"] = df[target_col].shift(1).rolling(7, min_periods=1).std()

    # 價格差分（今天 - 昨天），再 shift(1)，避免洩漏當日資訊到特徵中
    df["price_diff1"] = df[target_col].diff(1).shift(1)

    return df

def compute_weather_features(df, weather_cols):
    """
    計算氣象相關的滯後特徵（針對單一作物）。

    對每個氣象欄位 col：
    - col_lag1, col_lag3, col_lag7：分別代表 1/3/7 天前的氣象值
    - col_rolling_mean7：過去 7 天 (以昨天為止) 的平均氣象值
    """
    df = df.sort_values("date").copy()

    for col in weather_cols:
        for lag in [1, 3, 7]:
            df[f"{col}_lag{lag}"] = df[col].shift(lag)
        df[f"{col}_rolling_mean7"] = df[col].shift(1).rolling(7, min_periods=1).mean()

    return df

def prepare_features(df, price_col="crop_price_per_kg"):
    """
    完整的特徵準備流程（針對單一作物的資料表）：
    1. 加入時間特徵
    2. 加入價格滯後與滾動統計特徵
    3. 加入氣象滯後與滾動特徵
    4. 清理颱風欄位：將 NaN 視為 0，並轉為 int
    """
    weather_cols = [
        "station_pressure", "air_temperature", "relative_humidity",
        "wind_speed", "precipitation"
    ]

    # 時間特徵
    df = add_time_features(df)

    # 價格特徵
    df = compute_price_features(df, target_col=price_col, lookback=7)

    # 氣象特徵
    df = compute_weather_features(df, weather_cols)

    # 颱風特徵（確保沒有 NaN）
    df["is_typhoon"] = df["is_typhoon"].fillna(0).astype(int)

    return df

# =============================================================================
# Baseline 模型：去年同期價格
# =============================================================================

def calculate_baseline_predictions(df, price_col="crop_price_per_kg"):
    """
    計算 Baseline 預測：使用去年同期價格 (lag-365) 作為當日預測值。
    適用於季節性強的農產品。

    回傳：
        df (DataFrame): 多一個欄位 price_baseline。
    """
    df = df.sort_values("date").copy()
    # 365 天前的價格作為今天的 baseline 預測
    df["price_baseline"] = df[price_col].shift(365)
    return df

# =============================================================================
# 評估指標計算
# =============================================================================

def calculate_metrics(y_true, y_pred, prefix=""):
    """
    計算多個評估指標並以字典回傳：
    - MAE
    - RMSE
    - MAPE
    - R²

    prefix 用來在 key 前加上模型名稱前綴，例如：
        prefix="xgb_test_" -> xgb_test_mae, xgb_test_rmse, ...
    """
    # 先移除 NaN（例如 baseline 在前 365 天會是 NaN）
    mask = ~(np.isnan(y_true) | np.isnan(y_pred))
    y_true = y_true[mask]
    y_pred = y_pred[mask]

    if len(y_true) == 0:
        return {}

    # MAE：平均絕對誤差
    mae = mean_absolute_error(y_true, y_pred)
    # RMSE：均方根誤差
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    # MAPE：平均絕對百分比誤差（乘以 100 為百分比）
    mape = np.mean(np.abs((y_true - y_pred) / (y_true + 1e-6))) * 100
    # R²：決定係數
    r2 = r2_score(y_true, y_pred)

    return {
        f"{prefix}mae": mae,
        f"{prefix}rmse": rmse,
        f"{prefix}mape": mape,
        f"{prefix}r2": r2
    }

# =============================================================================
# 遞迴預測函數（改進版：優化效率）
# =============================================================================

def predict_future_recursive_optimized(
    historical_data,
    best_model,
    test_end_date,
    feature_cols,
    price_col="crop_price_per_kg",
    forecast_horizon=7
):
    """
    使用「遞迴預測」方式，預測 test_end_date 之後的未來 N 天價格。

    核心想法：
    - Day 1：用 test_end_date 當天的特徵，預測第 1 天的價格。
    - Day 2：把 Day 1 的預測結果塞回 DataFrame，更新滯後特徵，再預測第 2 天。
    - 如此重複到預測 horizon（預設 7 天）。

    注意：
    - 未來 7 天沒有實際價格與 baseline（price_actual = NaN, price_baseline = NaN）
    - 僅生成模型預測 price_prediction。
    """
    weather_cols = [
        "station_pressure", "air_temperature", "relative_humidity",
        "wind_speed", "precipitation"
    ]

    # 確保資料按日期排序
    historical_data = historical_data.sort_values("date").reset_index(drop=True)

    # current_data 會持續 append 新的「未來預測行」
    current_data = historical_data.copy()

    future_predictions = []

    for day_offset in range(1, forecast_horizon + 1):
        future_date = test_end_date + timedelta(days=day_offset)

        # 取最新一列的特徵（這一列代表目前已知/預測到的最後一天）
        latest_row = current_data.iloc[-1:].copy()
        X_future = latest_row[feature_cols].fillna(0)

        # 預測未來一天價格
        predicted_price = best_model.predict(X_future)[0]

        # 暫存當天的預測結果（未來期沒有實際價格）
        future_predictions.append({
            "date": future_date,
            "crop_id": historical_data["crop_id"].iloc[0],
            "price_actual": np.nan,
            "price_prediction": predicted_price,
            "price_baseline": np.nan  # 未來預測不提供 baseline
        })

        # 建立一筆新的 row（包含新的價格 + 對應的滯後與氣象特徵）
        new_row = create_new_row_with_features(
            current_data,
            future_date,
            predicted_price,
            price_col,
            weather_cols
        )

        # 將新行加入 current_data，供下一日預測使用
        current_data = pd.concat([current_data, pd.DataFrame([new_row])], ignore_index=True)

    return pd.DataFrame(future_predictions)

def create_new_row_with_features(current_data, future_date, predicted_price, price_col, weather_cols):
    """
    在遞迴預測中，為「未來一天」建立一筆新的資料列，並手動計算其特徵。

    重要概念：
    - 時間特徵：用 future_date 直接計算 (year, month, ...)
    - 價格特徵：從 current_data 最後一列推導（滯後 & 滾動統計）
    - 氣象特徵：假設未來幾天與最後一日氣象持平（可視情況替換為真正氣象預報）
    - 颱風：預設未來 7 天沒有颱風 (is_typhoon=0)
    """
    latest_row = current_data.iloc[-1].copy()

    # 基礎欄位
    new_row = {
        "date": future_date,
        "crop_id": latest_row["crop_id"],
        price_col: predicted_price,
    }

    # 若全水果模型有用到 crop_index，就一併帶上
    if "crop_index" in latest_row.index:
        new_row["crop_index"] = latest_row["crop_index"]

    # 時間特徵
    new_row["year"] = future_date.year
    new_row["month"] = future_date.month
    new_row["dayofyear"] = future_date.dayofyear
    new_row["dayofweek"] = future_date.dayofweek

    # 氣象資料（假設未來氣象值與最後一日相同）
    for col in weather_cols:
        new_row[col] = latest_row[col]

    # 颱風相關欄位（此處簡化為未來 7 天無颱風）
    new_row["is_typhoon"] = 0
    new_row["typhoon_name"] = None

    # 價格滯後特徵：將目前的 predicted_price 往後推一格
    for lag in range(1, 8):
        if lag == 1:
            # lag1 = 前一日價格 = latest_row 的 price
            new_row[f"price_lag{lag}"] = latest_row[price_col]
        else:
            # lagk = 前一日的 lag(k-1)，若不存在則 fallback 最新價格
            new_row[f"price_lag{lag}"] = latest_row.get(f"price_lag{lag-1}", latest_row[price_col])

    # 價格滾動統計：從最新 row 的滯後價格估計
    recent_prices = []
    for lag in range(1, 8):
        key = f"price_lag{lag}"
        if key in latest_row and pd.notna(latest_row[key]):
            recent_prices.append(latest_row[key])

    if len(recent_prices) > 0:
        new_row["price_rolling_mean7"] = np.mean(recent_prices)
        new_row["price_rolling_std7"] = np.std(recent_prices) if len(recent_prices) > 1 else 0
    else:
        # 若沒有歷史價格可用，fallback 使用最新價格
        new_row["price_rolling_mean7"] = latest_row[price_col]
        new_row["price_rolling_std7"] = 0

    # 價格差分：預測價 - 最新實際價
    new_row["price_diff1"] = predicted_price - latest_row[price_col]

    # 氣象滯後特徵：同樣以最新 row 的資訊往後推一格
    for col in weather_cols:
        for lag in [1, 3, 7]:
            lag_col = f"{col}_lag{lag}"
            if lag == 1:
                new_row[lag_col] = latest_row[col]
            elif f"{col}_lag{lag-1}" in latest_row:
                new_row[lag_col] = latest_row[f"{col}_lag{lag-1}"]
            else:
                new_row[lag_col] = latest_row[col]

        # 氣象 7 日滾動平均：延續最新一列的值
        mean_col = f"{col}_rolling_mean7"
        if mean_col in latest_row:
            new_row[mean_col] = latest_row[mean_col]
        else:
            new_row[mean_col] = latest_row[col]

    return new_row

# =============================================================================
# 模型訓練與預測（改進版）- 單作物模型 & 全水果統一模型
# =============================================================================

# 全域預設參數（可視情況調整）
DEFAULTS = {
    "train_days": 365,          # 最少訓練天數
    "max_train_days": 730,      # 最多訓練天數（避免太早的資料干擾）
    "test_days": 90,            # 測試期天數
    "validation_windows": 15,   # 時間序列交叉驗證的切割數
    "forecast_horizon": 7,      # 未來預測天數
}

def prepare_single_crop_datasets(
    crop_df,
    crop_id,
    price_col="crop_price_per_kg",
    train_days=365,
    max_train_days=730,
    test_days=90,
    forecast_horizon=7
):
    """
    給定「單一作物」的完整歷史資料，負責：
    1. 挑出有價格資料的日期。
    2. 按時間切出訓練區間與測試區間。
    3. 對 train + test 合併後計算 baseline (lag-365)。
    4. 再分別對 train / test 各自做特徵工程，確保不洩漏未來資訊。
    5. 準備完整歷史資料 full_historical（訓練 + 測試）作為未來遞迴預測的基礎。

    回傳：
        dict 包含：
            - train_df: 單一作物的訓練資料（含特徵）
            - test_df: 測試資料（含特徵）
            - full_historical: train + test（含特徵，供遞迴預測使用）
            - feature_cols: 模型要用的特徵欄位名稱列表
            - meta: 關於 train/test 分割的一些資訊（train/test 長度、被丟棄的天數等）
    """
    crop_df = crop_df.sort_values("date").reset_index(drop=True)
    # 僅保留有價格資料的日期，避免 NaN 影響訓練
    df_with_price = crop_df[crop_df[price_col].notna()].copy()

    # 資料天數不足時直接跳過，避免模型訓練不穩定
    if len(df_with_price) < (train_days + test_days + forecast_horizon):
        print(f"⚠️  {crop_id}: 資料不足（需要至少 {train_days + test_days + forecast_horizon} 天），跳過")
        return None

    # Step 1: 以最後 test_days + forecast_horizon 為預測相關區間，往前取訓練期
    split_idx = len(df_with_price) - test_days - forecast_horizon
    # 實際使用的訓練天數 = min(最大訓練天數, split_idx)
    actual_train_days = min(split_idx, max_train_days)
    train_start_idx = split_idx - actual_train_days

    # 取出訓練與測試原始資料（尚未做特徵工程）
    train_df_raw = df_with_price.iloc[train_start_idx:split_idx].copy()
    test_df_raw = df_with_price.iloc[split_idx:split_idx + test_days].copy()

    print(f"   📊 總資料: {len(df_with_price)} 天")
    print(f"   📊 訓練期: {actual_train_days} 天 ({train_df_raw['date'].min()} ~ {train_df_raw['date'].max()})")
    print(f"   📊 測試期: {test_days} 天 ({test_df_raw['date'].min()} ~ {test_df_raw['date'].max()})")
    if actual_train_days < split_idx:
        discarded_days = split_idx - actual_train_days
        print(f"   ⚠️  丟棄較舊的資料: {discarded_days} 天（避免太早期資料干擾模型）")
    else:
        discarded_days = 0

    # Step 2: 對 train + test 合併後計算 baseline (lag-365)，避免 baseline 斷裂
    combined_df = pd.concat([train_df_raw, test_df_raw], ignore_index=True)
    combined_df = combined_df.sort_values("date").reset_index(drop=True)
    combined_df = calculate_baseline_predictions(combined_df, price_col=price_col)

    # 根據日期再切回 train/test
    train_dates = train_df_raw["date"].values
    test_dates = test_df_raw["date"].values
    train_df_with_baseline = combined_df[combined_df["date"].isin(train_dates)].copy()
    test_df_with_baseline = combined_df[combined_df["date"].isin(test_dates)].copy()

    # Step 3: 在 train/test 上各自做特徵工程（避免洩漏未來資訊）
    train_df = prepare_features(train_df_with_baseline, price_col=price_col)
    test_df = prepare_features(test_df_with_baseline, price_col=price_col)

    # 決定要送進模型的 feature 欄位，不包含日期 / 作物代碼 / 目標欄位 / 文字型欄位 / baseline
    exclude_cols = ["date", "crop_id", price_col, "typhoon_name", "price_baseline"]
    feature_cols = [c for c in train_df.columns if c not in exclude_cols]

    # 合併 train + test 作為完整歷史，用於未來遞迴預測
    full_historical = pd.concat([train_df, test_df], ignore_index=True)

    meta = {
        "crop_id": crop_id,
        "train_size": len(train_df),
        "test_size": len(test_df),
        "actual_train_days": actual_train_days,
        "discarded_days": discarded_days,
    }

    return {
        "train_df": train_df,
        "test_df": test_df,
        "full_historical": full_historical,
        "feature_cols": feature_cols,
        "meta": meta,
    }

def train_and_predict_single_crop(
    crop_df,
    crop_id,
    price_col="crop_price_per_kg",
    train_days=365,
    max_train_days=730,
    test_days=90,
    n_splits=15,
    forecast_horizon=7
):
    """
    單作物模型 (Single Model)：
    - 針對單一水果建立一個專屬 XGBoost 模型。
    - 使用時間序列交叉驗證挑選最佳模型（以 Validation MAPE 最小為準）。
    - 在測試期上輸出預測與 Baseline 比較。
    - 使用遞迴預測產生未來 7 天價格。

    回傳：
        all_pred_df (DataFrame): 該作物的測試期 + 未來 7 天預測結果
        metrics (dict): 該作物的各種評估指標與改善率
    """
    # 先準備 train/test 資料與特徵
    prep = prepare_single_crop_datasets(
        crop_df,
        crop_id,
        price_col=price_col,
        train_days=train_days,
        max_train_days=max_train_days,
        test_days=test_days,
        forecast_horizon=forecast_horizon
    )

    # 若資料不足或其他原因導致無法準備資料，直接略過
    if prep is None:
        return None, None

    train_df = prep["train_df"]
    test_df = prep["test_df"]
    full_historical = prep["full_historical"]
    feature_cols = prep["feature_cols"]
    meta = prep["meta"]

    # 將特徵與目標分開
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df[price_col]
    X_test = test_df[feature_cols].fillna(0)
    y_test = test_df[price_col]

    # Step 4: 使用 TimeSeriesSplit 作時間序列交叉驗證以選最佳模型
    tscv = TimeSeriesSplit(n_splits=n_splits)
    best_model = None
    best_score = float("inf")

    for train_idx, val_idx in tscv.split(X_train):
        X_tr, X_val = X_train.iloc[train_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_train.iloc[train_idx], y_train.iloc[val_idx]

        # 可以視需求調整 XGBoost 超參數
        model = XGBRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42,
            n_jobs=-1
        )
        model.fit(X_tr, y_tr)

        # 以 Validation MAPE 作為選模標準
        val_pred = model.predict(X_val)
        val_mape = np.mean(np.abs((y_val - val_pred) / (y_val + 1e-6))) * 100

        if val_mape < best_score:
            best_score = val_mape
            best_model = model

    # Step 5: 在測試集上做預測
    test_pred = best_model.predict(X_test)

    # 整理測試期結果：包含實際價格、預測價格、baseline
    test_pred_df = test_df[["date", "crop_id", price_col, "price_baseline"]].copy()
    test_pred_df.rename(columns={price_col: "price_actual"}, inplace=True)
    test_pred_df["price_prediction"] = test_pred

    # 單作物模型的測試期指標
    xgb_metrics = calculate_metrics(
        test_pred_df["price_actual"].values,
        test_pred_df["price_prediction"].values,
        prefix="xgb_test_"
    )

    # Baseline 模型測試期指標
    baseline_metrics = calculate_metrics(
        test_pred_df["price_actual"].values,
        test_pred_df["price_baseline"].values,
        prefix="baseline_test_"
    )

    # Step 6: 使用遞迴預測，產生未來 7 天預測
    future_pred_df = predict_future_recursive_optimized(
        historical_data=full_historical,
        best_model=best_model,
        test_end_date=test_df["date"].max(),
        feature_cols=feature_cols,
        price_col=price_col,
        forecast_horizon=forecast_horizon
    )

    # Step 7: 合併測試期 + 未來期
    all_pred_df = pd.concat([test_pred_df, future_pred_df], ignore_index=True)

    # 計算相對 Baseline 的改善率（以百分比表示）
    improvement = {}
    for metric in ["mae", "rmse", "mape"]:
        xgb_val = xgb_metrics.get(f"xgb_test_{metric}", np.nan)
        baseline_val = baseline_metrics.get(f"baseline_test_{metric}", np.nan)
        if not np.isnan(xgb_val) and not np.isnan(baseline_val) and baseline_val > 0:
            improvement[f"{metric}_improvement_pct"] = ((baseline_val - xgb_val) / baseline_val) * 100

    metrics = {
        **meta,
        **xgb_metrics,
        **baseline_metrics,
        **improvement,
        "best_val_mape": best_score,
    }

    return all_pred_df, metrics

# =============================================================================
# 全水果統一模型（單一 XGBoost 模型）
# =============================================================================

def train_and_predict_global_model(
    df,
    price_col="crop_price_per_kg",
    train_days=365,
    max_train_days=730,
    test_days=90,
    n_splits=15,
    forecast_horizon=7
):
    """
    全水果統一模型 (Global Model)：
    - 將所有水果的訓練資料合併成一個大的訓練集。
    - 透過數值特徵 `crop_index` 表示作物類別，讓單一 XGBoost 模型學到跨作物通用模式。
    - 對每個作物產生測試期預測與未來 7 天預測。
    - 與 Baseline (lag-365) 比較，計算各種指標與改善率。

    回傳：
        final_predictions_global (DataFrame): 各作物的測試期 + 未來 7 天，包含 price_prediction_global
        all_metrics_global (list[dict]): 各作物的評估指標列表
    """
    all_crops = df["crop_id"].unique()
    global_train_list = []
    global_test_list = []
    # per_crop_train_test 用來保存每個作物的 train/test/full_historical 等資料（供日後使用）
    per_crop_train_test = {}

    print("\n" + "=" * 80)
    print("🤝 建立全水果統一模型所需的訓練/測試資料集...")
    print("=" * 80)

    # 逐作物準備 train/test（重用與 Single Model 相同的 prepare_single_crop_datasets）
    for idx, crop_id in enumerate(all_crops, 1):
        print(f"\n[Global] 準備資料 {idx}/{len(all_crops)}: {crop_id}")
        crop_df = df[df["crop_id"] == crop_id].copy()

        prep = prepare_single_crop_datasets(
            crop_df,
            crop_id,
            price_col=price_col,
            train_days=train_days,
            max_train_days=max_train_days,
            test_days=test_days,
            forecast_horizon=forecast_horizon
        )

        if prep is None:
            # 該作物資料不足，略過
            continue

        train_df = prep["train_df"]
        test_df = prep["test_df"]
        full_historical = prep["full_historical"]
        feature_cols = prep["feature_cols"]
        meta = prep["meta"]

        global_train_list.append(train_df)
        global_test_list.append(test_df)
        per_crop_train_test[crop_id] = {
            "train_df": train_df,
            "test_df": test_df,
            "full_historical": full_historical,
            "feature_cols": feature_cols,
            "meta": meta,
        }

    if len(global_train_list) == 0:
        print("⚠️ 全水果統一模型：沒有足夠資料的作物可用於訓練，跳過。")
        return None, None

    # 將所有作物的 train/test 合併
    global_train = pd.concat(global_train_list, ignore_index=True)
    global_test = pd.concat(global_test_list, ignore_index=True)

    # 建立作物的整數編碼 crop_index（特徵之一）
    crop_ids_sorted = sorted(global_train["crop_id"].unique())
    crop_to_index = {cid: i for i, cid in enumerate(crop_ids_sorted)}

    global_train["crop_index"] = global_train["crop_id"].map(crop_to_index).astype(int)
    global_test["crop_index"] = global_test["crop_id"].map(crop_to_index).astype(int)

    # Global Model 的特徵欄位：排除日期、作物 id、目標欄位、文字欄位與 baseline
    exclude_cols = ["date", "crop_id", price_col, "typhoon_name", "price_baseline"]
    feature_cols_global = [c for c in global_train.columns if c not in exclude_cols]

    global_train = global_train.sort_values("date").reset_index(drop=True)
    X_train = global_train[feature_cols_global].fillna(0)
    y_train = global_train[price_col]

    # 使用 TimeSeriesSplit 對 Global Training Set 做交叉驗證
    print("\n🤝 開始訓練全水果統一模型（XGBoost）...")
    tscv = TimeSeriesSplit(n_splits=n_splits)
    best_global_model = None
    best_global_score = float("inf")

    for fold_idx, (tr_idx, val_idx) in enumerate(tscv.split(X_train), 1):
        X_tr, X_val = X_train.iloc[tr_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_train.iloc[tr_idx], y_train.iloc[val_idx]

        model = XGBRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42,
            n_jobs=-1
        )
        model.fit(X_tr, y_tr)

        val_pred = model.predict(X_val)
        val_mape = np.mean(np.abs((y_val - val_pred) / (y_val + 1e-6))) * 100
        print(f"   Fold {fold_idx}: Global Validation MAPE = {val_mape:.2f}%")

        if val_mape < best_global_score:
            best_global_score = val_mape
            best_global_model = model

    print(f"\n✅ 全水果統一模型最佳 Validation MAPE: {best_global_score:.2f}%")

    # 先在合併後的 global_test 上產生預測（之後會再分回各作物）
    global_test = global_test.sort_values("date").reset_index(drop=True)
    X_test_global = global_test[feature_cols_global].fillna(0)
    global_test["price_prediction_global"] = best_global_model.predict(X_test_global)

    # 逐作物整理測試期 + 未來 7 天預測
    all_predictions_global = []
    all_metrics_global = []

    for crop_id, info in per_crop_train_test.items():
        train_df = info["train_df"]
        test_df = info["test_df"]
        full_historical = info["full_historical"]
        meta = info["meta"]

        # 取出該作物在 global_test 中的資料
        gtest = global_test[global_test["crop_id"] == crop_id].copy()
        if len(gtest) == 0:
            continue

        # 測試期預測結果（含實際價格、baseline 與 global 預測）
        test_pred_df = gtest[["date", "crop_id", price_col, "price_baseline", "price_prediction_global"]].copy()
        test_pred_df.rename(columns={price_col: "price_actual"}, inplace=True)

        # Global 模型測試期指標
        global_metrics = calculate_metrics(
            test_pred_df["price_actual"].values,
            test_pred_df["price_prediction_global"].values,
            prefix="global_test_"
        )
        # Baseline 測試期指標
        baseline_metrics = calculate_metrics(
            test_pred_df["price_actual"].values,
            test_pred_df["price_baseline"].values,
            prefix="baseline_test_"
        )

        # 計算相對 Baseline 的改善率
        improvement = {}
        for metric in ["mae", "rmse", "mape"]:
            global_val = global_metrics.get(f"global_test_{metric}", np.nan)
            baseline_val = baseline_metrics.get(f"baseline_test_{metric}", np.nan)
            if not np.isnan(global_val) and not np.isnan(baseline_val) and baseline_val > 0:
                improvement[f"{metric}_improvement_pct"] = ((baseline_val - global_val) / baseline_val) * 100

        # 為該作物組出完整歷史資料（train + test），並加上 crop_index，供遞迴預測使用
        full_hist_global = pd.concat([train_df, test_df], ignore_index=True)
        full_hist_global["crop_index"] = full_hist_global["crop_id"].map(crop_to_index).astype(int)

        # 使用 global model 對該作物做未來 7 天遞迴預測
        future_pred_df = predict_future_recursive_optimized(
            historical_data=full_hist_global,
            best_model=best_global_model,
            test_end_date=test_df["date"].max(),
            feature_cols=feature_cols_global,
            price_col=price_col,
            forecast_horizon=forecast_horizon
        )

        # 調整欄位名稱，讓未來預測的欄位是 price_prediction_global
        future_pred_df = future_pred_df[["date", "crop_id", "price_actual", "price_prediction", "price_baseline"]].copy()
        future_pred_df.rename(columns={"price_prediction": "price_prediction_global"}, inplace=True)

        # 合併測試期 + 未來期
        all_pred_crop = pd.concat([test_pred_df, future_pred_df], ignore_index=True)
        all_predictions_global.append(all_pred_crop)

        metrics = {
            **meta,
            **global_metrics,
            **baseline_metrics,
            **improvement,
            "best_global_val_mape": best_global_score,
        }
        all_metrics_global.append(metrics)

        print(f"   🌐 {crop_id}: Global Test MAPE = {global_metrics.get('global_test_mape', np.nan):.2f}% | "
              f"Baseline MAPE = {baseline_metrics.get('baseline_test_mape', np.nan):.2f}%")

    if len(all_predictions_global) == 0:
        print("⚠️ 全水果統一模型：沒有任何作物產生預測結果")
        return None, None

    final_predictions_global = pd.concat(all_predictions_global, ignore_index=True)

    return final_predictions_global, all_metrics_global

# =============================================================================
# 繪圖函數（增強版）：單作物模型 & Single vs Global 比較
# =============================================================================

def save_per_fruit_test_plots(test_pred_df, out_dir):
    """
    繪製單作物模型 (Single Model) 的「測試期 + 未來 7 天預測圖」。

    圖中包含：
    - 實際價格 (price_actual)
    - 單作物 XGBoost 預測 (price_prediction)
    - Baseline 去年同期價格 (price_baseline)
    - 未來 7 天預測曲線
    """
    if test_pred_df is None or len(test_pred_df) == 0:
        print("⚠️ 無測試預測資料可繪圖")
        return

    os.makedirs(out_dir, exist_ok=True)
    test_pred_df = test_pred_df.sort_values(["crop_id", "date"]).copy()
    total = test_pred_df["crop_id"].nunique()

    print(f"\n🖼️  輸出各水果測試期 + 未來 7 天預測圖（單作物模型），共 {total} 種水果")

    for cid, g in test_pred_df.groupby("crop_id"):
        g = g.sort_values("date")

        # 分成測試期（有實際價格）與未來期（沒有實際價格）
        test_data = g[g["price_actual"].notna()]
        future_data = g[g["price_actual"].isna()]

        fig, ax = plt.subplots(figsize=(14, 6))

        # 測試期部分：畫實際價格與單作物 XGBoost 預測與 Baseline
        if len(test_data) > 0:
            ax.plot(test_data["date"], test_data["price_actual"], 
                   'o-', label="實際價格", linewidth=2.5, markersize=6, color='#2E86AB')
            ax.plot(test_data["date"], test_data["price_prediction"], 
                   's--', label="XGBoost 預測（單作物模型）", linewidth=2, markersize=5, color='#F77F00')

            # Baseline：去年同期價格（可能部分日期為 NaN）
            if "price_baseline" in test_data.columns:
                valid_baseline = test_data[test_data["price_baseline"].notna()]
                if len(valid_baseline) > 0:
                    ax.plot(valid_baseline["date"], valid_baseline["price_baseline"], 
                           'x:', label="Baseline（去年同期）", linewidth=1.5, markersize=6, color='#A4A8AB', alpha=0.7)

        # 未來 7 天預測：只有模型預測線
        if len(future_data) > 0:
            ax.plot(future_data["date"], future_data["price_prediction"], 
                   '^--', label="未來 7 天預測", linewidth=2.5, markersize=7, color='#D62828')

        # 測試期與未來期之間畫一條垂直分隔線
        if len(test_data) > 0 and len(future_data) > 0:
            split_date = test_data["date"].iloc[-1]
            ax.axvline(x=split_date, color='gray', linestyle=':', linewidth=1.5, alpha=0.7)
            ax.text(split_date, ax.get_ylim()[1] * 0.95, '測試期結束', 
                   ha='right', va='top', fontsize=9, color='gray')

        ax.set_title(f"{cid}：測試期 + 未來 7 天預測（單作物 XGBoost vs Baseline）", fontsize=14, fontweight="bold")
        ax.set_xlabel("日期", fontsize=11)
        ax.set_ylabel("價格（元/kg）", fontsize=11)
        ax.grid(alpha=0.3, linestyle='--')
        ax.legend(loc='best', fontsize=10)

        # 日期軸格式化
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45, ha='right')

        plt.tight_layout()
        save_path = os.path.join(out_dir, f"{cid}.png")
        plt.savefig(save_path, dpi=200, bbox_inches="tight")
        plt.close()

    print(f"✅ 各水果比較圖（單作物模型）已輸出至：{out_dir}")

def save_per_fruit_global_comparison_plots(single_df, global_df, out_dir):
    """
    繪製「單作物模型 vs 全水果統一模型 vs Baseline」的比較圖。

    圖中包含：
    - 測試期：
        - 實際價格
        - Single Model 預測 (price_prediction_single)
        - Global Model 預測 (price_prediction_global)
        - Baseline 去年同期價格
    - 未來 7 天：
        - Single Model 未來預測
        - Global Model 未來預測
    """
    if single_df is None or len(single_df) == 0:
        print("⚠️ SINGLE_MODEL 無資料，無法畫 GLOBAL 比較圖")
        return
    if global_df is None or len(global_df) == 0:
        print("⚠️ GLOBAL_MODEL 無資料，無法畫 GLOBAL 比較圖")
        return

    os.makedirs(out_dir, exist_ok=True)

    single_df = single_df.sort_values(["crop_id", "date"]).copy()
    global_df = global_df.sort_values(["crop_id", "date"]).copy()

    crops_single = set(single_df["crop_id"].unique())
    crops_global = set(global_df["crop_id"].unique())
    # 僅對兩種模型都有預測的作物畫比較圖
    common_crops = sorted(list(crops_single & crops_global))

    if len(common_crops) == 0:
        print("⚠️ SINGLE_MODEL 與 GLOBAL_MODEL 沒有共同作物，無法畫比較圖")
        return

    print(f"\n🖼️  輸出各水果『Single vs Global vs Baseline』比較圖，共 {len(common_crops)} 種水果")

    for cid in common_crops:
        # 單作物模型資料：包含實際價格、單作物預測與 baseline
        s = single_df[single_df["crop_id"] == cid][["date", "price_actual", "price_prediction", "price_baseline"]].copy()
        # 全水果統一模型資料：包含實際價格、global 預測與 baseline
        g = global_df[global_df["crop_id"] == cid][["date", "price_actual", "price_prediction_global", "price_baseline"]].copy()

        s.rename(columns={
            "price_actual": "price_actual_single",
            "price_prediction": "price_prediction_single",
            "price_baseline": "price_baseline_single"
        }, inplace=True)

        g.rename(columns={
            "price_actual": "price_actual_global",
            "price_baseline": "price_baseline_global"
        }, inplace=True)

        # 以日期為 key 做 outer merge，確保未來 7 天都在
        merged = pd.merge(s, g, on="date", how="outer")
        merged["crop_id"] = cid
        merged = merged.sort_values("date")

        # 實際值：若 single/global 任一邊有，就使用
        merged["price_actual"] = merged["price_actual_single"].combine_first(merged["price_actual_global"])

        # Baseline: 先用 single 的，如果沒有就用 global 的
        merged["price_baseline"] = merged["price_baseline_single"].combine_first(merged["price_baseline_global"])

        # 測試期：有實際價格的部分；未來期：沒有實際價格的部分
        test_mask = merged["price_actual"].notna()
        future_mask = merged["price_actual"].isna()

        fig, ax = plt.subplots(figsize=(14, 6))

        # 1) 測試期部分
        if test_mask.any():
            # 實際價格
            ax.plot(merged.loc[test_mask, "date"], merged.loc[test_mask, "price_actual"],
                    'o-', label="實際價格", linewidth=2.5, markersize=6, color='#2E86AB')

            # 單作物模型預測
            if "price_prediction_single" in merged.columns:
                ax.plot(merged.loc[test_mask, "date"], merged.loc[test_mask, "price_prediction_single"],
                        's--', label="Single XGBoost", linewidth=2, markersize=5, color='#F77F00')

            # 全水果統一模型預測
            if "price_prediction_global" in merged.columns:
                ax.plot(merged.loc[test_mask, "date"], merged.loc[test_mask, "price_prediction_global"],
                        'd--', label="Global XGBoost", linewidth=2, markersize=5, color='#4CAF50')

            # Baseline（去年同期）
            valid_baseline = merged[test_mask & merged["price_baseline"].notna()]
            if len(valid_baseline) > 0:
                ax.plot(valid_baseline["date"], valid_baseline["price_baseline"],
                        'x:', label="Baseline（去年同期）", linewidth=1.5, markersize=6, color='#A4A8AB', alpha=0.7)

        # 2) 未來 7 天預測（沒有實際價格）
        if future_mask.any():
            # Single 模型未來預測
            if "price_prediction_single" in merged.columns:
                ax.plot(merged.loc[future_mask, "date"], merged.loc[future_mask, "price_prediction_single"],
                        '^--', label="未來 7 天（Single）", linewidth=2.5, markersize=7, color='#D62828')
            # Global 模型未來預測
            if "price_prediction_global" in merged.columns:
                ax.plot(merged.loc[future_mask, "date"], merged.loc[future_mask, "price_prediction_global"],
                        'v--', label="未來 7 天（Global）", linewidth=2.5, markersize=7, color='#6A4C93')

        # 測試期與未來期分隔線
        if test_mask.any() and future_mask.any():
            split_date = merged.loc[test_mask, "date"].max()
            ax.axvline(x=split_date, color='gray', linestyle=':', linewidth=1.5, alpha=0.7)
            ax.text(split_date, ax.get_ylim()[1] * 0.95, '測試期結束',
                    ha='right', va='top', fontsize=9, color='gray')

        ax.set_title(f"{cid}：Single vs Global 模型比較（測試期 + 未來 7 天）", fontsize=14, fontweight="bold")
        ax.set_xlabel("日期", fontsize=11)
        ax.set_ylabel("價格（元/kg）", fontsize=11)
        ax.grid(alpha=0.3, linestyle='--')
        ax.legend(loc='best', fontsize=10)

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45, ha='right')

        plt.tight_layout()
        save_path = os.path.join(out_dir, f"{cid}.png")
        plt.savefig(save_path, dpi=200, bbox_inches="tight")
        plt.close()

    print(f"✅ 各水果比較圖（Single vs Global）已輸出至：{out_dir}")

# =============================================================================
# 主程式：整個價格預測流程的 orchestrator
# =============================================================================

def main():
    """
    主程式執行流程：
    1. 從 MySQL 讀取整併好的果物 + 氣象資料。
    2. 對每個作物分別訓練「單作物模型」，並輸出預測結果與指標。
    3. 使用所有作物共同訓練「全水果統一模型」，並輸出預測結果與指標。
    4. 將兩種模型的結果輸出成 CSV / JSON / 圖檔。
    5. 合併 Single & Global & Baseline 的預測結果，寫入 MySQL 表 `price_prediction_7days`。
    6. 在終端列印整體統計摘要（平均 MAPE、R² 等）。
    """
    start_time = time.time()

    print("\n" + "=" * 80)
    print("🚀 全水果合併分析（MySQL 版本）- 預測 7 天後價格【改進版 v6-GLOBAL + MySQL Output】")
    print("=" * 80)
    print("✨ 改進內容：")
    print("   1. ✅ 先分割訓練/測試，再計算特徵（徹底避免資料洩漏）")
    print("   2. ✅ 優化遞迴預測效率（避免重複計算歷史特徵）")
    print("   3. ✅ Baseline 模型使用去年同期價格（lag-365）")
    print("   4. ✅ 增加評估指標：R², MAE, RMSE")
    print("   5. ✅ 增加與 Baseline 的比較分析")
    print("   6. ✅ 未來7天預測不包含 baseline（避免混淆）")
    print("   7. ✅ 修正 Baseline 計算 bug（避免全部為 0）")
    print("   8. 🆕 新增『全水果統一模型』（單一 XGBoost 模型）")
    print("   9. 🆕 保留『每個水果各自建模』，並比較兩種方法性能")
    print("  10. 🆕 保留所有本地分析輸出 + 新增將預測結果寫入 MySQL 表 price_prediction_7days")
    print("=" * 80)

    # 1. 從 MySQL 讀取原始資料（果物 + 氣象）
    df = load_data_from_mysql()

    # 2. 單作物模型訓練與預測
    print("\n🤖 開始訓練『每個水果各自建模』的 XGBoost 模型...")
    all_crops = df["crop_id"].unique()
    print(f"   共有 {len(all_crops)} 種作物需要處理（單作物模型）")

    all_predictions_single = []
    all_metrics_single = []

    for idx, crop_id in enumerate(all_crops, 1):
        print(f"\n[Single] 處理 {idx}/{len(all_crops)}: {crop_id}")
        crop_df = df[df["crop_id"] == crop_id].copy()

        pred_df, metrics = train_and_predict_single_crop(
            crop_df,
            crop_id,
            price_col="crop_price_per_kg",
            train_days=DEFAULTS["train_days"],
            max_train_days=DEFAULTS["max_train_days"],
            test_days=DEFAULTS["test_days"],
            n_splits=DEFAULTS["validation_windows"],
            forecast_horizon=DEFAULTS["forecast_horizon"]
        )

        if pred_df is not None:
            all_predictions_single.append(pred_df)
            all_metrics_single.append(metrics)
            print(f"   ✅ Single XGBoost MAPE: {metrics.get('xgb_test_mape', 0):.2f}% | "
                  f"Baseline MAPE: {metrics.get('baseline_test_mape', 0):.2f}%")
            print(f"   📈 改善率 (MAPE): {metrics.get('mape_improvement_pct', 0):.2f}%")

    if len(all_predictions_single) == 0:
        print("\n❌ 沒有任何作物在單作物模型中產生預測結果，程式結束。")
        return

    # 將所有單作物預測結果合併
    final_predictions_single = pd.concat(all_predictions_single, ignore_index=True)
    metrics_df_single = pd.DataFrame(all_metrics_single)

    # 3. 全水果統一模型訓練與預測
    print("\n" + "=" * 80)
    print("🤝 開始訓練『全水果統一模型』（單一 XGBoost）...")
    print("=" * 80)

    final_predictions_global, all_metrics_global = train_and_predict_global_model(
        df,
        price_col="crop_price_per_kg",
        train_days=DEFAULTS["train_days"],
        max_train_days=DEFAULTS["max_train_days"],
        test_days=DEFAULTS["test_days"],
        n_splits=DEFAULTS["validation_windows"],
        forecast_horizon=DEFAULTS["forecast_horizon"]
    )

    if final_predictions_global is not None:
        metrics_df_global = pd.DataFrame(all_metrics_global)
    else:
        metrics_df_global = None

    # 4. 本地輸出：CSV / JSON / 圖表
    output_dir = "merged_fruits_results_IMPROVED_v5_GLOBAL"
    os.makedirs(output_dir, exist_ok=True)

    # (A) 單作物模型輸出
    csv_single_path = os.path.join(output_dir, "predictions_SINGLE_MODEL_with_baseline.csv")
    final_predictions_single.to_csv(csv_single_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ 單作物模型預測結果已儲存至: {csv_single_path}")

    metrics_single_path = os.path.join(output_dir, "metrics_comparison_SINGLE_MODEL.json")
    with open(metrics_single_path, "w", encoding="utf-8") as f:
        json.dump(all_metrics_single, f, ensure_ascii=False, indent=2)
    print(f"✅ 單作物模型評估指標已儲存至: {metrics_single_path}")

    # 單作物模型圖表
    plot_dir_single = os.path.join(output_dir, "comparison_plots_SINGLE_MODEL")
    save_per_fruit_test_plots(final_predictions_single, plot_dir_single)

    # (B) 全水果統一模型輸出
    if final_predictions_global is not None:
        csv_global_path = os.path.join(output_dir, "predictions_GLOBAL_MODEL_with_baseline.csv")
        final_predictions_global.to_csv(csv_global_path, index=False, encoding="utf-8-sig")
        print(f"\n✅ 全水果統一模型預測結果已儲存至: {csv_global_path}")

        metrics_global_path = os.path.join(output_dir, "metrics_comparison_GLOBAL_MODEL.json")
        with open(metrics_global_path, "w", encoding="utf-8") as f:
            json.dump(all_metrics_global, f, ensure_ascii=False, indent=2)
        print(f"✅ 全水果統一模型評估指標已儲存至: {metrics_global_path}")

        # Single vs Global 比較圖
        plot_dir_global = os.path.join(output_dir, "comparison_plots_GLOBAL_MODEL")
        save_per_fruit_global_comparison_plots(final_predictions_single, final_predictions_global, plot_dir_global)

    # 5. 合併 Single & Global & Baseline 的結果，並上傳到 MySQL
    if final_predictions_global is not None:
        print("\n" + "=" * 80)
        print("🔗 合併 Single / Global / Baseline 預測結果，準備上傳 MySQL...")
        print("=" * 80)

        # 調整欄位名稱以符合要寫入的 schema
        df_single = final_predictions_single.rename(columns={
            "price_prediction": "price_prediction_single"
        })[["date", "crop_id", "price_actual", "price_prediction_single", "price_baseline"]]

        df_global = final_predictions_global[["date", "crop_id", "price_prediction_global"]]

        # 外連接以保留所有日期 + 作物的紀錄
        merged = pd.merge(
            df_single,
            df_global,
            on=["date", "crop_id"],
            how="outer"
        )

        # 依作物 + 日期排序，方便查看
        merged = merged.sort_values(["crop_id", "date"]).reset_index(drop=True)

        # 寫入 MySQL 表 price_prediction_7days
        save_predictions_to_mysql_v2(merged, table_name="price_prediction_7days")

    # 6. 在終端輸出整體統計摘要（便於快速檢查模型表現）
    print("\n" + "=" * 80)
    print("📈 整體統計摘要（單作物模型 vs 全水果統一模型）")
    print("=" * 80)

    print(f"成功處理作物數（單作物模型）: {len(all_metrics_single)}")
    if metrics_df_global is not None:
        print(f"成功處理作物數（全水果統一模型）: {len(all_metrics_global)}")

    # 單作物模型平均表現
    print("\n🍎 單作物模型（每個水果一個 XGBoost）：")
    print(f"   平均 MAPE: {metrics_df_single['xgb_test_mape'].mean():.2f}%")
    print(f"   平均 MAE: {metrics_df_single['xgb_test_mae'].mean():.2f}")
    print(f"   平均 RMSE: {metrics_df_single['xgb_test_rmse'].mean():.2f}")
    print(f"   平均 R²: {metrics_df_single['xgb_test_r2'].mean():.4f}")

    print("\n   📊 Baseline 模型（單作物模型使用的同一 Baseline）：")
    print(f"   平均 MAPE: {metrics_df_single['baseline_test_mape'].mean():.2f}%")
    print(f"   平均 MAE: {metrics_df_single['baseline_test_mae'].mean():.2f}")
    print(f"   平均 RMSE: {metrics_df_single['baseline_test_rmse'].mean():.2f}")
    print(f"   平均 R²: {metrics_df_single['baseline_test_r2'].mean():.4f}")

    print("\n   🎯 相對於 Baseline 的改善（單作物模型）：")
    print(f"   MAPE 改善: {metrics_df_single['mape_improvement_pct'].mean():.2f}%")
    print(f"   MAE 改善: {metrics_df_single['mae_improvement_pct'].mean():.2f}%")
    print(f"   RMSE 改善: {metrics_df_single['rmse_improvement_pct'].mean():.2f}%")

    print(f"\n   🏆 最佳作物（單作物 XGBoost MAPE）: "
          f"{metrics_df_single['xgb_test_mape'].min():.2f}% "
          f"({metrics_df_single.loc[metrics_df_single['xgb_test_mape'].idxmin(), 'crop_id']})")
    print(f"   ⚠️ 最差作物（單作物 XGBoost MAPE）: "
          f"{metrics_df_single['xgb_test_mape'].max():.2f}% "
          f"({metrics_df_single.loc[metrics_df_single['xgb_test_mape'].idxmax(), 'crop_id']})")

    # 全水果統一模型平均表現（若有成功訓練）
    if metrics_df_global is not None and len(metrics_df_global) > 0:
        print("\n🍇 全水果統一模型（單一 XGBoost + crop_index）：")
        print(f"   平均 MAPE: {metrics_df_global['global_test_mape'].mean():.2f}%")
        print(f"   平均 MAE: {metrics_df_global['global_test_mae'].mean():.2f}")
        print(f"   平均 RMSE: {metrics_df_global['global_test_rmse'].mean():.2f}")
        print(f"   平均 R²: {metrics_df_global['global_test_r2'].mean():.4f}")

        print("\n   📊 Baseline 模型（同樣使用 lag-365）：")
        print(f"   平均 MAPE: {metrics_df_global['baseline_test_mape'].mean():.2f}%")
        print(f"   平均 MAE: {metrics_df_global['baseline_test_mae'].mean():.2f}")
        print(f"   平均 RMSE: {metrics_df_global['baseline_test_rmse'].mean():.2f}")
        print(f"   平均 R²: {metrics_df_global['baseline_test_r2'].mean():.4f}")

        print("\n   🎯 相對於 Baseline 的改善（全水果統一模型）：")
        print(f"   MAPE 改善: {metrics_df_global['mape_improvement_pct'].mean():.2f}%")
        print(f"   MAE 改善: {metrics_df_global['mae_improvement_pct'].mean():.2f}%")
        print(f"   RMSE 改善: {metrics_df_global['rmse_improvement_pct'].mean():.2f}%")

        print(f"\n   🏆 最佳作物（Global XGBoost MAPE）: "
              f"{metrics_df_global['global_test_mape'].min():.2f}% "
              f"({metrics_df_global.loc[metrics_df_global['global_test_mape'].idxmin(), 'crop_id']})")
        print(f"   ⚠️ 最差作物（Global XGBoost MAPE）: "
              f"{metrics_df_global['global_test_mape'].max():.2f}% "
              f"({metrics_df_global.loc[metrics_df_global['global_test_mape'].idxmax(), 'crop_id']})")

    elapsed = time.time() - start_time
    print(f"\n⏱️  總執行時間: {elapsed:.1f} 秒")
    print("=" * 80)
    print("✅ 程式執行完成！")
    print("=" * 80)
    print("\n💡 提醒：")
    print("   - Baseline 一律使用去年同期價格（lag-365），適合季節性農產品")
    print("   - 單作物模型與全水果統一模型皆與同一 Baseline 比較，方便觀察優劣")
    print("   - 未來7天預測不包含 baseline，專注於模型預測結果")
    print("   - 前365天可能無 baseline 數據（需要去年同期資料）")
    print("   - 本版會將 Single + Global + Baseline 合併後寫入 MySQL 表 price_prediction_7days")

if __name__ == "__main__":
    main()
