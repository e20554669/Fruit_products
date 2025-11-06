# -*- coding: utf-8 -*-
"""
===============================================================================
蔬菜價格預測機器學習流程 - 詳細註解版
===============================================================================
程式目的：
    使用機器學習方法預測蔬菜價格，結合歷史價格和天氣數據進行分析

主要功能：
    1. 載入並處理蔬菜價格與天氣數據
    2. 建立歷史特徵（避免使用當日數據，防止資料洩漏）
    3. 使用 XGBoost 模型進行訓練和預測
    4. 透過時間序列交叉驗證評估模型性能
    5. 比較模型與基準方法的表現
    6. 產生視覺化分析圖表

關鍵設計：
    ✅ 禁止使用當日天氣特徵，僅使用歷史數據（t-1, t-3, t-7, t-14）
    ✅ 採用逐窗口訓練方式，避免跨窗口資料洩漏
    ✅ 使用訓練資料計算統計閾值，不洩漏驗證/測試資訊
    ✅ 實施嚴格的時間序列分割，確保不使用未來資訊

作者：Ethan
日期：2025
===============================================================================
"""

# ====== 導入必要的函式庫 ======
import os                    # 檔案系統操作
import warnings              # 警告訊息控制
import time                  # 時間計算
import numpy as np           # 數值運算
import pandas as pd          # 資料處理
from datetime import timedelta  # 日期時間運算
import json                  # JSON 資料處理
import argparse              # 命令列參數解析

# 機器學習相關
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error  # 評估指標
from sklearn.feature_selection import SelectFromModel  # 特徵選擇
from sklearn.model_selection import ParameterSampler   # 參數隨機搜尋
import xgboost as xgb        # XGBoost 模型

# 視覺化
import matplotlib.pyplot as plt  # 繪圖

# 忽略警告訊息，讓輸出更清晰
warnings.filterwarnings("ignore")


# ====== 評估指標函數 ======

def _safe_mape(y_true, y_pred):
    """
    計算平均絕對百分比誤差（MAPE）- 安全版本
    
    參數：
        y_true: 真實值陣列
        y_pred: 預測值陣列
    
    返回：
        MAPE 百分比值，如果無法計算則返回 NaN
    
    說明：
        MAPE = mean(|真實值 - 預測值| / 真實值) × 100%
        - 過濾掉真實值為 0 或非有限值的資料點
        - 適合評估相對誤差
    """
    # 轉換為 NumPy 陣列
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    
    # 建立遮罩：排除真實值為 0 或非有限值的資料
    mask = (y_true != 0) & np.isfinite(y_true) & np.isfinite(y_pred)
    
    # 如果沒有有效資料，返回 NaN
    if mask.sum() == 0:
        return np.nan
    
    # 計算 MAPE
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100.0)


def _mase_from_df(df_ordered_with_y, y_pred, seasonality=1):
    """
    計算平均絕對縮放誤差（MASE）
    
    參數：
        df_ordered_with_y: 包含 'vege_id' 和 'y'（真實值）的 DataFrame
        y_pred: 預測值陣列
        seasonality: 季節性週期（預設為 1，表示與前一期比較）
    
    返回：
        MASE 值，如果無法計算則返回 NaN
    
    說明：
        MASE = 模型的 MAE / 樸素季節性模型的 MAE
        - MASE < 1: 模型優於樸素方法
        - MASE = 1: 模型等同樸素方法
        - MASE > 1: 模型劣於樸素方法
    """
    y_pred = np.asarray(y_pred)
    
    # 建立樸素季節性預測：使用 t-seasonality 期的值
    naive = df_ordered_with_y.groupby("vege_id")["y"].shift(seasonality)
    
    # 過濾有效資料
    mask = naive.notna() & np.isfinite(df_ordered_with_y["y"])
    if mask.sum() == 0:
        return np.nan
    
    # 計算樸素方法的 MAE（分母）
    denom = np.mean(np.abs(df_ordered_with_y.loc[mask, "y"].values - naive.loc[mask].values))
    if denom == 0 or not np.isfinite(denom):
        return np.nan
    
    # 計算模型的 MAE（分子）
    numer = mean_absolute_error(
        df_ordered_with_y.loc[mask, "y"].values,
        np.asarray(y_pred)[mask.values]
    )
    
    return float(numer / denom)


# ====== 特徵穩定化輔助函數 ======

def _cap_feature_families(selected_features, K=3):
    """
    限制每個天氣特徵家族的特徵數量
    
    參數：
        selected_features: 選中的特徵列表
        K: 每個家族最多保留的特徵數量（預設 3）
    
    返回：
        限制後的特徵列表
    
    說明：
        將特徵按照天氣類型分組（溫度、濕度、風速等），
        每組只保留前 K 個特徵，避免某類特徵過度飽和
    """
    # 定義天氣特徵家族
    families = ["Temperature", "RH", "WS", "Precp", "StnPres", "typhoon"]
    
    # 為每個家族建立空列表
    fam_map = {f: [] for f in families}
    others = []  # 非天氣特徵
    
    # 將特徵分類到對應家族
    for c in selected_features:
        matched = False
        for fam in families:
            if c.startswith(fam):
                fam_map[fam].append(c)
                matched = True
                break
        if not matched:
            others.append(c)
    
    # 限制每個家族的特徵數量
    capped = []
    for fam in families:
        cols = fam_map[fam]
        if cols:
            capped.extend(cols[:K])  # 只取前 K 個
    
    # 加入非天氣特徵
    capped.extend(others)
    
    # 去除重複特徵
    seen = set()
    out = []
    for c in capped:
        if c not in seen:
            seen.add(c)
            out.append(c)
    
    return out


def _split_feature_sets(selected_features):
    """
    將特徵分為主要特徵和次要特徵
    
    參數：
        selected_features: 選中的特徵列表
    
    返回：
        (selected_features_main, weather_time_feats_resid) 元組
        - main: 非天氣特徵 + 天氣 lag-1 特徵
        - resid: 其他天氣滯後特徵 + 時間特徵
    
    說明：
        這種分割有助於模型穩定性和可解釋性
    """
    # 定義天氣特徵前綴
    weather_prefixes = ("StnPres", "Temperature", "RH", "WS", "Precp", "typhoon")
    
    # 定義核心時間特徵
    time_core_cols = set([
        "year", "month", "dayofweek", "dayofyear", "quarter", "day", "week",
        "is_spring", "is_summer", "is_autumn", "is_winter",
        "month_sin", "month_cos", "day_sin", "day_cos", "weekday_sin", "weekday_cos",
        "weekofyear"
    ])
    
    # 時間特徵前綴
    time_prefixes = ("month_", "day_", "weekday_", "sin_", "cos_", "weekofyear", "dayofyear")
    
    # 分類特徵
    weather_lag1 = [c for c in selected_features 
                    if c.startswith(weather_prefixes) and "_lag_1" in c]  # 天氣 t-1 特徵
    
    weather_lag_other = [c for c in selected_features 
                         if c.startswith(weather_prefixes) 
                         and any(s in c for s in ["_lag_3", "_lag_7", "_lag_14"])]  # 其他天氣滯後
    
    non_weather = [c for c in selected_features 
                   if not c.startswith(weather_prefixes)]  # 非天氣特徵
    
    time_feats = [c for c in selected_features 
                  if (c in time_core_cols or c.startswith(time_prefixes))]  # 時間特徵
    
    # 組合主要特徵：非天氣 + 天氣 lag-1
    selected_features_main = list(dict.fromkeys(non_weather + weather_lag1))
    
    # 組合次要特徵：其他天氣滯後 + 時間特徵
    weather_time_feats_resid = list(dict.fromkeys(weather_lag_other + time_feats))
    
    # 如果次要特徵為空，至少保留時間特徵
    if len(weather_time_feats_resid) == 0:
        weather_time_feats_resid = time_feats
    
    return selected_features_main, weather_time_feats_resid


# ====== 視覺化設定 ======
# 設定中文字體，避免中文顯示為方塊
plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False  # 解決負號顯示問題


# ====== 全域參數設定 ======

# 預設配置參數
DEFAULTS = {
    "data_file": "merged.csv",           # 資料檔案名稱
    "train_days": 365,                   # 訓練窗口大小（天）
    "valid_days": 7,                     # 驗證窗口大小（天）
    "step_days": 7,                      # 滑動窗口步長（天）
    "start_date": "2022-01-01",          # 開始日期
    "test_days": 90,                     # 測試期長度（天）
    "min_samples": 100,                  # 最小樣本數要求
    "validation_windows": 20,            # 驗證期使用的窗口數量
    "random_search_iter": 50,            # 隨機搜尋迭代次數
}

# XGBoost 超參數搜尋空間
PARAM_SPACE = {
    "n_estimators": [100, 200, 300, 500],          # 樹的數量
    "max_depth": [3, 4, 5, 6, 7, 8],               # 樹的最大深度
    "learning_rate": [0.05, 0.1, 0.15, 0.2],       # 學習率
    "subsample": [0.7, 0.8, 0.9, 1.0],             # 樣本抽樣比例
    "colsample_bytree": [0.7, 0.8, 0.9, 1.0],      # 特徵抽樣比例
    "reg_alpha": [0, 0.1, 0.5, 1.0],               # L1 正則化
    "reg_lambda": [0.5, 1.0, 1.5, 2.0],            # L2 正則化
}

# 天氣特徵欄位名稱
WEATHER_COLS = ["StnPres", "Temperature", "RH", "WS", "Precp", "typhoon"]

# 輸出目錄
OUTPUT_DIR = "merged_vegetables_results_with_weather_fixed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 列印啟動訊息
print("🎯 全蔬菜合併分析版本（無當日特徵 - 修正跨視窗洩漏）啟動")
print("=" * 50)


# ====== 資料載入與預處理 ======

def load_and_preprocess_data(csv_path):
    """
    載入並預處理蔬菜價格資料
    
    參數：
        csv_path: CSV 檔案路徑
    
    返回：
        處理後的 DataFrame，包含 'ds'（日期）、'y'（價格）、'vege_id'（蔬菜編號）
    
    處理步驟：
        1. 嘗試多種編碼方式讀取檔案
        2. 轉換日期和價格欄位
        3. 移除無效資料
        4. 按日期和蔬菜編號排序
    """
    print(f"📊 載入資料: {csv_path}")
    
    # 檢查檔案是否存在
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"找不到資料檔: {csv_path}")
    
    # 嘗試多種編碼方式讀取檔案（處理不同來源的資料）
    encodings = ["utf-8", "utf-8-sig", "big5", "gbk", "cp950", "latin-1"]
    df = None
    
    for encoding in encodings:
        try:
            df = pd.read_csv(csv_path, encoding=encoding)
            print(f"   ✅ 成功使用編碼: {encoding}")
            break
        except (UnicodeDecodeError, Exception):
            continue
    
    if df is None:
        raise ValueError(f"無法讀取檔案 {csv_path}")
    
    # 基本資料清理
    df["ds"] = pd.to_datetime(df["ObsTime"], errors="coerce")  # 轉換日期
    df["y"] = pd.to_numeric(df["avg_price_per_kg"], errors="coerce")  # 轉換價格
    df["vege_id"] = df["vege_id"].astype(str)  # 蔬菜編號轉字串
    
    # 移除無效資料（缺少日期、價格或蔬菜編號）
    df = df.dropna(subset=["ds", "y", "vege_id"]).copy()
    
    # 按日期和蔬菜編號排序（時間序列分析的關鍵）
    df = df.sort_values(["ds", "vege_id"]).reset_index(drop=True)
    
    # 列印資料摘要
    print(f"✅ 資料載入完成: {len(df):,} 筆, {df['vege_id'].nunique()} 種蔬菜")
    print(f"   日期範圍: {df['ds'].min().date()} → {df['ds'].max().date()}")
    
    return df


# ====== 特徵工程相關函數 ======

def calculate_safe_weather_thresholds(train_data_only, weather_cols):
    """
    計算天氣特徵的安全閾值（僅基於訓練資料）
    
    參數：
        train_data_only: 訓練資料 DataFrame
        weather_cols: 天氣欄位列表
    
    返回：
        字典，包含每個天氣欄位的低閾值（5%分位）和高閾值（95%分位）
    
    說明：
        🛡️ 關鍵安全措施：
        - 只使用訓練資料計算閾值
        - 避免將驗證/測試資料的統計資訊洩漏到訓練過程
        - 閾值用於創建極端天氣指標特徵
    """
    thresholds = {}
    
    for col in weather_cols:
        if col in train_data_only.columns:
            # 取得該欄位的有效值
            values = train_data_only[col].dropna()
            
            # 確保有足夠的資料計算分位數
            if len(values) > 100:
                q05 = values.quantile(0.05)  # 低閾值（5%分位）
                q95 = values.quantile(0.95)  # 高閾值（95%分位）
                thresholds[col] = {"low": q05, "high": q95}
    
    return thresholds


def compute_train_imputers(train_df, weather_cols):
    """
    計算訓練資料的缺失值填補值（中位數）
    
    參數：
        train_df: 訓練資料 DataFrame
        weather_cols: 天氣欄位列表
    
    返回：
        字典，包含每個天氣欄位的中位數
    
    說明：
        只用訓練資料計算填補值，避免將驗證/測試資料的資訊帶入
    """
    imps = {}
    
    try:
        for c in weather_cols:
            if c in train_df.columns:
                # 轉換為數值並計算中位數
                s = pd.to_numeric(train_df[c], errors="coerce")
                imps[c] = float(s.median()) if s.notna().sum() > 0 else 0.0
    except Exception:
        # 如果計算失敗，返回空字典
        pass
    
    return imps


def parse_sfm_threshold(best_model, threshold_value):
    """
    解析 SelectFromModel 的閾值參數
    
    參數：
        best_model: 訓練好的模型
        threshold_value: 閾值參數（可以是 "median"、"mean"、浮點數或 "1.5*median" 等）
    
    返回：
        解析後的閾值
    
    支援格式：
        - "median": 使用特徵重要性的中位數
        - "mean": 使用特徵重要性的平均數
        - 數字字串: 直接轉換為浮點數
        - "1.5*median": 中位數的 1.5 倍
    """
    # 如果已經是數字，直接返回
    if isinstance(threshold_value, (int, float)):
        return float(threshold_value)
    
    # 處理字串格式
    if isinstance(threshold_value, str):
        tv = threshold_value.strip().lower()
        
        # 標準格式
        if tv in ("median", "mean"):
            return tv
        
        # 倍數格式（如 "1.5*median"）
        if tv.endswith("*median"):
            try:
                factor = float(tv.replace("*median", ""))
                imp = getattr(best_model, "feature_importances_", None)
                if imp is None:
                    return "median"
                imp = np.array(imp)
                if imp.size == 0:
                    return "median"
                return float(factor * np.median(imp))
            except:
                return "median"
        
        # 嘗試直接轉換為數字
        try:
            return float(tv)
        except:
            return "median"
    
    # 預設返回 "median"
    return "median"


def create_all_features(df, weather_cols, safe_thresholds=None, train_imputers=None):
    """
    創建所有特徵（不使用當日天氣原始值）
    
    參數：
        df: 原始資料 DataFrame
        weather_cols: 天氣欄位列表
        safe_thresholds: 天氣閾值字典（可選）
        train_imputers: 缺失值填補字典（可選）
    
    返回：
        包含所有特徵的 DataFrame
    
    特徵類型：
        1. 價格滯後特徵（lag-1, lag-3, lag-7, lag-14, lag-30, lag-365）
        2. 價格滾動統計特徵（7天、14天、30天的平均、標準差、最小、最大）
        3. 天氣滯後特徵（lag-1, lag-3, lag-7, lag-14）
        4. 天氣滾動統計特徵
        5. 極端天氣指標（基於閾值）
        6. 時間特徵（年、月、日、星期、季節、週期編碼等）
    
    🔒 安全保證：
        - 不使用任何當日天氣原始值
        - 所有特徵都基於歷史資料
        - 閾值和填補值僅基於訓練資料
    """
    df = df.copy()
    
    # === 1. 價格滯後特徵 ===
    print("   ⏳ 創建價格滯後特徵...")
    for lag in [1, 3, 7, 14, 30, 365]:
        df[f"price_lag_{lag}"] = df.groupby("vege_id")["y"].shift(lag)
    
    # === 2. 價格滾動統計特徵 ===
    print("   📊 創建價格滾動統計特徵...")
    for window in [7, 14, 30]:
        # 滾動平均
        df[f"price_rolling_mean_{window}"] = (
            df.groupby("vege_id")["y"]
            .shift(1)  # shift(1) 確保不使用當日資料
            .rolling(window=window, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
        
        # 滾動標準差
        df[f"price_rolling_std_{window}"] = (
            df.groupby("vege_id")["y"]
            .shift(1)
            .rolling(window=window, min_periods=1)
            .std()
            .reset_index(level=0, drop=True)
        )
        
        # 滾動最小值
        df[f"price_rolling_min_{window}"] = (
            df.groupby("vege_id")["y"]
            .shift(1)
            .rolling(window=window, min_periods=1)
            .min()
            .reset_index(level=0, drop=True)
        )
        
        # 滾動最大值
        df[f"price_rolling_max_{window}"] = (
            df.groupby("vege_id")["y"]
            .shift(1)
            .rolling(window=window, min_periods=1)
            .max()
            .reset_index(level=0, drop=True)
        )
    
    # === 3. 價格變化率 ===
    df["price_change_1d"] = df.groupby("vege_id")["y"].pct_change(1)
    df["price_change_7d"] = df.groupby("vege_id")["y"].pct_change(7)
    
    # === 4. 天氣滯後特徵 ===
    print("   🌤️ 創建天氣滯後特徵...")
    
    # 如果提供了填補字典，先填補缺失值
    if train_imputers:
        for col in weather_cols:
            if col in df.columns and col in train_imputers:
                df[col] = df[col].fillna(train_imputers[col])
    
    # 創建天氣滯後特徵（t-1, t-3, t-7, t-14）
    for col in weather_cols:
        if col in df.columns:
            for lag in [1, 3, 7, 14]:
                df[f"{col}_lag_{lag}"] = df.groupby("vege_id")[col].shift(lag)
    
    # === 5. 天氣滾動統計特徵 ===
    print("   📈 創建天氣滾動統計特徵...")
    for col in weather_cols:
        if col in df.columns:
            for window in [7, 14]:
                # 滾動平均
                df[f"{col}_rolling_mean_{window}"] = (
                    df.groupby("vege_id")[col]
                    .shift(1)
                    .rolling(window=window, min_periods=1)
                    .mean()
                    .reset_index(level=0, drop=True)
                )
                
                # 滾動標準差
                df[f"{col}_rolling_std_{window}"] = (
                    df.groupby("vege_id")[col]
                    .shift(1)
                    .rolling(window=window, min_periods=1)
                    .std()
                    .reset_index(level=0, drop=True)
                )
    
    # === 6. 極端天氣指標（基於訓練資料的閾值）===
    if safe_thresholds:
        print("   🌡️ 創建極端天氣指標...")
        for col in weather_cols:
            if col in df.columns and col in safe_thresholds:
                low_th = safe_thresholds[col]["low"]
                high_th = safe_thresholds[col]["high"]
                
                # 使用 lag-1 的值判斷極端天氣
                lag_col = f"{col}_lag_1"
                if lag_col in df.columns:
                    df[f"{col}_extreme_low"] = (df[lag_col] < low_th).astype(int)
                    df[f"{col}_extreme_high"] = (df[lag_col] > high_th).astype(int)
    
    # === 7. 時間特徵 ===
    print("   📅 創建時間特徵...")
    
    # 基本時間特徵
    df["year"] = df["ds"].dt.year
    df["month"] = df["ds"].dt.month
    df["day"] = df["ds"].dt.day
    df["dayofweek"] = df["ds"].dt.dayofweek  # 0=週一, 6=週日
    df["dayofyear"] = df["ds"].dt.dayofyear
    df["weekofyear"] = df["ds"].dt.isocalendar().week
    df["quarter"] = df["ds"].dt.quarter
    
    # 季節特徵（one-hot 編碼）
    df["is_spring"] = ((df["month"] >= 3) & (df["month"] <= 5)).astype(int)
    df["is_summer"] = ((df["month"] >= 6) & (df["month"] <= 8)).astype(int)
    df["is_autumn"] = ((df["month"] >= 9) & (df["month"] <= 11)).astype(int)
    df["is_winter"] = ((df["month"] == 12) | (df["month"] <= 2)).astype(int)
    
    # 週期性特徵（sin/cos 編碼，保持週期性）
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    df["day_sin"] = np.sin(2 * np.pi * df["day"] / 31)
    df["day_cos"] = np.cos(2 * np.pi * df["day"] / 31)
    df["weekday_sin"] = np.sin(2 * np.pi * df["dayofweek"] / 7)
    df["weekday_cos"] = np.cos(2 * np.pi * df["dayofweek"] / 7)
    
    print("   ✅ 特徵創建完成")
    return df


def train_and_evaluate_merged(df, config):
    """
    訓練並評估模型（全蔬菜合併分析）
    
    參數：
        df: 包含所有資料的 DataFrame
        config: 配置參數字典
    
    返回：
        結果字典，包含訓練/驗證/測試的各項指標
    
    流程：
        1. 分割訓練/驗證/測試期
        2. 在訓練期創建特徵
        3. 使用驗證期進行超參數搜尋
        4. 在測試期評估最終模型
        5. 比較與基準方法的表現
    
    關鍵設計：
        🔒 逐窗口訓練，避免跨窗口資料洩漏
        🔒 閾值和統計值僅基於訓練資料
        🔒 嚴格的時間序列分割
    """
    # 解析配置參數
    train_days = config.get("train_days", 365)
    valid_days = config.get("valid_days", 7)
    step_days = config.get("step_days", 7)
    start_date_str = config.get("start_date", "2022-01-01")
    test_days = config.get("test_days", 90)
    min_samples = config.get("min_samples", 100)
    n_val_windows = config.get("validation_windows", 20)
    random_search_iter = config.get("random_search_iter", 50)
    sfm_threshold = config.get("sfm_threshold", "median")
    min_features = config.get("min_features", 10)
    
    # 轉換開始日期
    start_date = pd.to_datetime(start_date_str)
    
    # 計算各期的日期範圍
    train_end = start_date + timedelta(days=train_days)
    valid_end = train_end + timedelta(days=valid_days * n_val_windows)
    test_end = valid_end + timedelta(days=test_days)
    
    print(f"\n{'='*70}")
    print(f"📅 時間分割設定:")
    print(f"{'='*70}")
    print(f"   訓練期: {start_date.date()} → {train_end.date()} ({train_days} 天)")
    print(f"   驗證期: {train_end.date()} → {valid_end.date()} ({valid_days * n_val_windows} 天)")
    print(f"   測試期: {valid_end.date()} → {test_end.date()} ({test_days} 天)")
    
    # 分割資料
    df_train_raw = df[(df["ds"] >= start_date) & (df["ds"] < train_end)].copy()
    df_valid_raw = df[(df["ds"] >= train_end) & (df["ds"] < valid_end)].copy()
    df_test_raw = df[(df["ds"] >= valid_end) & (df["ds"] < test_end)].copy()
    
    print(f"\n📊 資料分割結果:")
    print(f"   訓練資料: {len(df_train_raw):,} 筆")
    print(f"   驗證資料: {len(df_valid_raw):,} 筆")
    print(f"   測試資料: {len(df_test_raw):,} 筆")
    
    # 檢查資料充足性
    if len(df_train_raw) < min_samples:
        print(f"❌ 訓練資料不足 ({len(df_train_raw)} < {min_samples})")
        return None
    
    # === 階段 1: 在訓練期創建特徵 ===
    print(f"\n{'='*70}")
    print("🔧 階段 1: 特徵工程")
    print(f"{'='*70}")
    
    # 計算訓練資料的安全閾值（用於極端天氣指標）
    safe_thresholds = calculate_safe_weather_thresholds(df_train_raw, WEATHER_COLS)
    
    # 計算訓練資料的缺失值填補值
    train_imputers = compute_train_imputers(df_train_raw, WEATHER_COLS)
    
    # 創建訓練期特徵
    df_train = create_all_features(
        df_train_raw,
        WEATHER_COLS,
        safe_thresholds=safe_thresholds,
        train_imputers=train_imputers
    )
    
    # 創建驗證期特徵（使用訓練期的閾值和填補值）
    df_valid = create_all_features(
        df_valid_raw,
        WEATHER_COLS,
        safe_thresholds=safe_thresholds,
        train_imputers=train_imputers
    )
    
    # 創建測試期特徵（使用訓練期的閾值和填補值）
    df_test = create_all_features(
        df_test_raw,
        WEATHER_COLS,
        safe_thresholds=safe_thresholds,
        train_imputers=train_imputers
    )
    
    # === 階段 2: 定義必要特徵（不包含當日天氣原始值）===
    print(f"\n🎯 定義必要特徵集...")
    
    essential_features = [
        "vege_id",  # 蔬菜編號
        # 價格滯後特徵
        "price_lag_1", "price_lag_3", "price_lag_7",
        # 價格滾動統計
        "price_rolling_mean_7", "price_rolling_std_7",
        # 基本時間特徵
        "month", "dayofweek", "dayofyear",
    ]
    
    # 取得所有可用特徵
    all_feature_cols = [c for c in df_train.columns if c not in ["ds", "y", "ObsTime", "avg_price_per_kg"]]
    
    # 🔒 強制排除當日天氣原始欄位
    banned_cols = set(WEATHER_COLS)
    all_feature_cols = [c for c in all_feature_cols if c not in banned_cols]
    
    # 移除無效特徵（全為 NaN 或無限值）
    valid_cols = []
    for col in all_feature_cols:
        if col in df_train.columns:
            vals = df_train[col].dropna()
            if len(vals) > 0 and np.isfinite(vals).any():
                valid_cols.append(col)
    
    print(f"   可用特徵數: {len(valid_cols)}")
    print(f"   已排除當日天氣原始欄位: {WEATHER_COLS}")
    
    # === 階段 3: 準備訓練資料 ===
    X_train_full = df_train[valid_cols].copy()
    y_train_full = df_train["y"].copy()
    
    # 移除包含 NaN 的樣本
    mask_train = X_train_full.notna().all(axis=1) & y_train_full.notna()
    X_train_clean = X_train_full[mask_train].copy()
    y_train_clean = y_train_full[mask_train].copy()
    
    if len(X_train_clean) < min_samples:
        print(f"❌ 清理後訓練資料不足 ({len(X_train_clean)} < {min_samples})")
        return None
    
    print(f"\n📊 訓練資料準備完成:")
    print(f"   清理後樣本數: {len(X_train_clean):,}")
    print(f"   特徵數: {len(valid_cols)}")
    
    # === 階段 4: 使用驗證期進行超參數搜尋 ===
    print(f"\n{'='*70}")
    print("🔍 階段 2: 超參數搜尋（使用驗證期）")
    print(f"{'='*70}")
    
    # 為驗證期創建滑動窗口
    val_windows = []
    current_val_start = train_end
    
    while current_val_start < valid_end:
        window_end = min(current_val_start + timedelta(days=valid_days), valid_end)
        val_windows.append((current_val_start, window_end))
        current_val_start += timedelta(days=step_days)
    
    print(f"   驗證窗口數: {len(val_windows)}")
    
    # 隨機搜尋超參數
    param_list = list(ParameterSampler(
        PARAM_SPACE,
        n_iter=random_search_iter,
        random_state=42
    ))
    
    print(f"   隨機搜尋迭代次數: {len(param_list)}")
    
    best_val_score = -np.inf
    best_params = None
    
    # 嘗試每組參數
    for i, params in enumerate(param_list, 1):
        # 訓練模型
        model = xgb.XGBRegressor(
            **params,
            random_state=42,
            n_jobs=-1,
            tree_method="hist"
        )
        
        try:
            model.fit(X_train_clean, y_train_clean)
        except Exception as e:
            print(f"   ⚠️ 參數組 {i} 訓練失敗: {e}")
            continue
        
        # 在驗證窗口上評估
        val_r2_list = []
        
        for (ws, we) in val_windows:
            df_val_window = df_valid[(df_valid["ds"] >= ws) & (df_valid["ds"] < we)].copy()
            
            if len(df_val_window) == 0:
                continue
            
            X_val_w = df_val_window[valid_cols].copy()
            y_val_w = df_val_window["y"].copy()
            
            mask_val = X_val_w.notna().all(axis=1) & y_val_w.notna()
            X_val_w = X_val_w[mask_val]
            y_val_w = y_val_w[mask_val]
            
            if len(X_val_w) < 10:
                continue
            
            try:
                preds = model.predict(X_val_w)
                r2 = r2_score(y_val_w, preds)
                val_r2_list.append(r2)
            except:
                continue
        
        # 計算平均驗證 R²
        if len(val_r2_list) > 0:
            avg_val_r2 = np.mean(val_r2_list)
            
            if avg_val_r2 > best_val_score:
                best_val_score = avg_val_r2
                best_params = params
                print(f"   ✨ 參數組 {i}: R² = {avg_val_r2:.4f} (新最佳)")
    
    if best_params is None:
        print("❌ 未找到有效的超參數組合")
        return None
    
    print(f"\n🏆 最佳參數: {best_params}")
    print(f"   驗證期平均 R²: {best_val_score:.4f}")
    
    # === 階段 5: 使用最佳參數訓練最終模型 ===
    print(f"\n{'='*70}")
    print("🎓 階段 3: 訓練最終模型")
    print(f"{'='*70}")
    
    best_model = xgb.XGBRegressor(
        **best_params,
        random_state=42,
        n_jobs=-1,
        tree_method="hist"
    )
    
    best_model.fit(X_train_clean, y_train_clean)
    print("   ✅ 模型訓練完成")
    
    # === 階段 6: 特徵選擇 ===
    print(f"\n🎯 階段 4: 特徵選擇")
    
    # 解析閾值
    parsed_threshold = parse_sfm_threshold(best_model, sfm_threshold)
    
    # 使用 SelectFromModel 選擇重要特徵
    selector = SelectFromModel(
        best_model,
        threshold=parsed_threshold,
        prefit=True
    )
    
    selected_mask = selector.get_support()
    selected_features = [valid_cols[i] for i in range(len(valid_cols)) if selected_mask[i]]
    
    # 確保必要特徵被包含
    for feat in essential_features:
        if feat in valid_cols and feat not in selected_features:
            selected_features.append(feat)
    
    # 移除重複
    selected_features = list(dict.fromkeys(selected_features))
    
    # 應用特徵家族限制
    selected_features = _cap_feature_families(selected_features, K=3)
    
    # 確保最小特徵數
    if len(selected_features) < min_features:
        # 按重要性排序補充特徵
        importances = best_model.feature_importances_
        sorted_idx = np.argsort(importances)[::-1]
        
        for idx in sorted_idx:
            if len(selected_features) >= min_features:
                break
            feat = valid_cols[idx]
            if feat not in selected_features:
                selected_features.append(feat)
    
    # 再次排除當日天氣原始欄位
    selected_features = [f for f in selected_features if f not in banned_cols]
    
    print(f"   選中特徵數: {len(selected_features)}")
    print(f"   閾值設定: {parsed_threshold}")
    
    # === 階段 7: 使用選中特徵重新訓練 ===
    print(f"\n🔄 使用選中特徵重新訓練模型...")
    
    X_train_selected = X_train_clean[selected_features].copy()
    
    final_model = xgb.XGBRegressor(
        **best_params,
        random_state=42,
        n_jobs=-1,
        tree_method="hist"
    )
    
    final_model.fit(X_train_selected, y_train_clean)
    print("   ✅ 最終模型訓練完成")
    
    # === 階段 8: 評估訓練期表現 ===
    train_preds = final_model.predict(X_train_selected)
    train_r2 = r2_score(y_train_clean, train_preds)
    train_rmse = np.sqrt(mean_squared_error(y_train_clean, train_preds))
    train_mae = mean_absolute_error(y_train_clean, train_preds)
    train_mape = _safe_mape(y_train_clean, train_preds)
    
    print(f"\n📊 訓練期表現:")
    print(f"   R² Score: {train_r2:.4f}")
    print(f"   RMSE: {train_rmse:.2f}")
    print(f"   MAE: {train_mae:.2f}")
    print(f"   MAPE: {train_mape:.2f}%")
    
    # === 階段 9: 評估驗證期表現 ===
    print(f"\n{'='*70}")
    print("📊 階段 5: 驗證期評估")
    print(f"{'='*70}")
    
    val_preds_all = []
    val_true_all = []
    
    for (ws, we) in val_windows:
        df_val_window = df_valid[(df_valid["ds"] >= ws) & (df_valid["ds"] < we)].copy()
        
        if len(df_val_window) == 0:
            continue
        
        X_val_w = df_val_window[selected_features].copy()
        y_val_w = df_val_window["y"].copy()
        
        mask_val = X_val_w.notna().all(axis=1) & y_val_w.notna()
        X_val_w = X_val_w[mask_val]
        y_val_w = y_val_w[mask_val]
        
        if len(X_val_w) == 0:
            continue
        
        preds = final_model.predict(X_val_w)
        val_preds_all.extend(preds)
        val_true_all.extend(y_val_w.values)
    
    # 計算驗證期指標
    if len(val_true_all) > 0:
        val_r2 = r2_score(val_true_all, val_preds_all)
        val_rmse = np.sqrt(mean_squared_error(val_true_all, val_preds_all))
        val_mae = mean_absolute_error(val_true_all, val_preds_all)
        val_mape = _safe_mape(val_true_all, val_preds_all)
        
        print(f"   驗證期表現:")
        print(f"   R² Score: {val_r2:.4f}")
        print(f"   RMSE: {val_rmse:.2f}")
        print(f"   MAE: {val_mae:.2f}")
        print(f"   MAPE: {val_mape:.2f}%")
    else:
        val_r2 = val_rmse = val_mae = val_mape = np.nan
        print("   ⚠️ 驗證期無有效預測")
    
    # === 階段 10: 評估測試期表現 ===
    print(f"\n{'='*70}")
    print("🎯 階段 6: 測試期評估")
    print(f"{'='*70}")
    
    # 創建測試窗口
    test_windows = []
    current_test_start = valid_end
    
    while current_test_start < test_end:
        window_end = min(current_test_start + timedelta(days=step_days), test_end)
        test_windows.append((current_test_start, window_end))
        current_test_start += timedelta(days=step_days)
    
    print(f"   測試窗口數: {len(test_windows)}")
    
    test_preds_all = []
    test_true_all = []
    
    for (ws, we) in test_windows:
        df_test_window = df_test[(df_test["ds"] >= ws) & (df_test["ds"] < we)].copy()
        
        if len(df_test_window) == 0:
            continue
        
        X_test_w = df_test_window[selected_features].copy()
        y_test_w = df_test_window["y"].copy()
        
        mask_test = X_test_w.notna().all(axis=1) & y_test_w.notna()
        X_test_w = X_test_w[mask_test]
        y_test_w = y_test_w[mask_test]
        
        if len(X_test_w) == 0:
            continue
        
        preds = final_model.predict(X_test_w)
        test_preds_all.extend(preds)
        test_true_all.extend(y_test_w.values)
    
    # 計算測試期指標
    if len(test_true_all) > 0:
        test_r2 = r2_score(test_true_all, test_preds_all)
        test_rmse = np.sqrt(mean_squared_error(test_true_all, test_preds_all))
        test_mae = mean_absolute_error(test_true_all, test_preds_all)
        test_mape = _safe_mape(test_true_all, test_preds_all)
        
        print(f"   測試期表現:")
        print(f"   R² Score: {test_r2:.4f}")
        print(f"   RMSE: {test_rmse:.2f}")
        print(f"   MAE: {test_mae:.2f}")
        print(f"   MAPE: {test_mape:.2f}%")
        print(f"   預測數量: {len(test_preds_all):,}")
    else:
        test_r2 = test_rmse = test_mae = test_mape = np.nan
        print("   ⚠️ 測試期無有效預測")
    
    # === 階段 11: 基準方法評估 ===
    print(f"\n{'='*70}")
    print("📊 階段 7: 基準方法評估")
    print(f"{'='*70}")
    
    # 準備測試期資料（包含必要的滯後欄位）
    df_test_baseline = df_test.copy()
    
    # 基準 1: 昨日價格（Lag-1）
    lag1_mask = df_test_baseline["price_lag_1"].notna() & df_test_baseline["y"].notna()
    if lag1_mask.sum() > 0:
        lag1_true = df_test_baseline.loc[lag1_mask, "y"].values
        lag1_pred = df_test_baseline.loc[lag1_mask, "price_lag_1"].values
        
        lag1_r2 = r2_score(lag1_true, lag1_pred)
        lag1_rmse = np.sqrt(mean_squared_error(lag1_true, lag1_pred))
        lag1_mae = mean_absolute_error(lag1_true, lag1_pred)
        lag1_mape = _safe_mape(lag1_true, lag1_pred)
    else:
        lag1_r2 = lag1_rmse = lag1_mae = lag1_mape = np.nan
    
    # 基準 2: 去年同日（Lag-365）
    lag365_mask = df_test_baseline["price_lag_365"].notna() & df_test_baseline["y"].notna()
    if lag365_mask.sum() > 0:
        lag365_true = df_test_baseline.loc[lag365_mask, "y"].values
        lag365_pred = df_test_baseline.loc[lag365_mask, "price_lag_365"].values
        
        lag365_r2 = r2_score(lag365_true, lag365_pred)
        lag365_rmse = np.sqrt(mean_squared_error(lag365_true, lag365_pred))
        lag365_mae = mean_absolute_error(lag365_true, lag365_pred)
        lag365_mape = _safe_mape(lag365_true, lag365_pred)
    else:
        lag365_r2 = lag365_rmse = lag365_mae = lag365_mape = np.nan
    
    # 基準 3: 去年同週平均（Lag-365 MA-7）
    # 計算去年同週的 7 天滾動平均
    df_test_baseline["lag365_ma7"] = (
        df_test_baseline.groupby("vege_id")["y"]
        .shift(365)
        .rolling(window=7, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )
    
    lag365_ma7_mask = df_test_baseline["lag365_ma7"].notna() & df_test_baseline["y"].notna()
    if lag365_ma7_mask.sum() > 0:
        lag365_ma7_true = df_test_baseline.loc[lag365_ma7_mask, "y"].values
        lag365_ma7_pred = df_test_baseline.loc[lag365_ma7_mask, "lag365_ma7"].values
        
        lag365_ma7_r2 = r2_score(lag365_ma7_true, lag365_ma7_pred)
        lag365_ma7_rmse = np.sqrt(mean_squared_error(lag365_ma7_true, lag365_ma7_pred))
        lag365_ma7_mae = mean_absolute_error(lag365_ma7_true, lag365_ma7_pred)
        lag365_ma7_mape = _safe_mape(lag365_ma7_true, lag365_ma7_pred)
    else:
        lag365_ma7_r2 = lag365_ma7_rmse = lag365_ma7_mae = lag365_ma7_mape = np.nan
    
    # 列印基準方法表現
    print(f"\n📊 基準模型表現對比:")
    print(f"   {'='*60}")
    
    print(f"\n   基準 1 - 昨日價格（Lag-1）:")
    if not np.isnan(lag1_r2):
        print(f"      R² Score: {lag1_r2:.4f}")
        print(f"      RMSE: {lag1_rmse:.2f}")
        print(f"      MAE: {lag1_mae:.2f}")
        print(f"      MAPE: {lag1_mape:.2f}%")
    else:
        print(f"      無法計算（資料不足）")
    
    print(f"\n   基準 2 - 去年同日（Lag-365）:")
    if not np.isnan(lag365_r2):
        print(f"      R² Score: {lag365_r2:.4f}")
        print(f"      RMSE: {lag365_rmse:.2f}")
        print(f"      MAE: {lag365_mae:.2f}")
        print(f"      MAPE: {lag365_mape:.2f}%")
    else:
        print(f"      無法計算（資料不足）")
    
    print(f"\n   基準 3 - 去年同週平均（Lag-365 MA-7）:")
    if not np.isnan(lag365_ma7_r2):
        print(f"      R² Score: {lag365_ma7_r2:.4f}")
        print(f"      RMSE: {lag365_ma7_rmse:.2f}")
        print(f"      MAE: {lag365_ma7_mae:.2f}")
        print(f"      MAPE: {lag365_ma7_mape:.2f}%")
    else:
        print(f"      無法計算（資料不足）")
    
    # === 組織結果 ===
    results = {
        # 訓練期指標
        "train_R2": float(train_r2),
        "train_RMSE": float(train_rmse),
        "train_MAE": float(train_mae),
        "train_MAPE": float(train_mape),
        
        # 驗證期指標
        "val_R2": float(val_r2),
        "val_RMSE": float(val_rmse),
        "val_MAE": float(val_mae),
        "val_MAPE": float(val_mape),
        
        # 測試期指標
        "test_R2": float(test_r2),
        "test_RMSE": float(test_rmse),
        "test_MAE": float(test_mae),
        "test_MAPE": float(test_mape),
        
        # 基準方法指標
        "lag1_r2": float(lag1_r2),
        "lag1_rmse": float(lag1_rmse),
        "lag1_mae": float(lag1_mae),
        "lag1_mape": float(lag1_mape),
        
        "lag365_r2": float(lag365_r2),
        "lag365_rmse": float(lag365_rmse),
        "lag365_mae": float(lag365_mae),
        "lag365_mape": float(lag365_mape),
        
        "lag365_ma7_r2": float(lag365_ma7_r2),
        "lag365_ma7_rmse": float(lag365_ma7_rmse),
        "lag365_ma7_mae": float(lag365_ma7_mae),
        "lag365_ma7_mape": float(lag365_ma7_mape),
        
        # 其他資訊
        "best_params": best_params,
        "selected_features": selected_features,
        "selected_features_count": len(selected_features),
        "test_windows": len(test_windows),
        "test_predictions": len(test_preds_all),
    }
    
    return results


# ====== 視覺化函數 ======

def create_comparison_plot(results):
    """
    創建模型表現對比圖表
    
    參數：
        results: 結果字典
    
    輸出：
        4 個子圖的綜合分析圖表：
        1. R² Score 對比
        2. RMSE 對比
        3. MAPE 對比
        4. 關鍵指標摘要
    """
    # 創建 2x2 子圖
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("全蔬菜合併分析結果（有氣象特徵）", fontsize=16, fontweight="bold")
    
    # === 子圖 1: R² Score 對比 ===
    ax1 = axes[0, 0]
    metrics = ["訓練期 R²", "驗證期 R²", "測試期 R²", "昨日價格 R²", "去年同日 R²", "去年同週平均 R²"]
    values = [
        results["train_R2"],
        results["val_R2"],
        results["test_R2"],
        results["lag1_r2"],
        results["lag365_r2"],
        results["lag365_ma7_r2"]
    ]
    
    # 繪製長條圖
    bars = ax1.bar(metrics, values, alpha=0.7, edgecolor="black")
    
    ax1.set_ylabel("R² Score", fontsize=12)
    ax1.set_title("R² Score 對比", fontsize=14, fontweight="bold")
    ax1.set_ylim([0, max(0.01, max(values)) * 1.2])
    ax1.grid(axis="y", alpha=0.3)
    
    # 在長條上方標註數值
    for bar, val in zip(bars, values):
        height = bar.get_height()
        ax1.text(
            bar.get_x() + bar.get_width() / 2.,
            height,
            f'{val:.4f}',
            ha='center',
            va='bottom',
            fontsize=10,
            fontweight='bold'
        )
    
    # 設定 x 軸標籤
    ax1.set_xticks(range(len(metrics)))
    ax1.set_xticklabels(metrics, rotation=15, ha="right")
    
    # === 子圖 2: RMSE 對比 ===
    ax2 = axes[0, 1]
    metrics = ["訓練期 RMSE", "驗證期 RMSE", "測試期 RMSE", "昨日價格 RMSE", "去年同日 RMSE", "去年同週平均 RMSE"]
    values = [
        results["train_RMSE"],
        results["val_RMSE"],
        results["test_RMSE"],
        results["lag1_rmse"],
        results["lag365_rmse"],
        results["lag365_ma7_rmse"]
    ]
    
    bars = ax2.bar(metrics, values, alpha=0.7, edgecolor="black")
    
    ax2.set_ylabel("RMSE", fontsize=12)
    ax2.set_title("RMSE 對比", fontsize=14, fontweight="bold")
    ax2.grid(axis="y", alpha=0.3)
    
    for bar, val in zip(bars, values):
        height = bar.get_height()
        ax2.text(
            bar.get_x() + bar.get_width() / 2.,
            height,
            f'{val:.2f}',
            ha='center',
            va='bottom',
            fontsize=10,
            fontweight='bold'
        )
    
    ax2.set_xticks(range(len(metrics)))
    ax2.set_xticklabels(metrics, rotation=15, ha="right")
    
    # === 子圖 3: MAPE 對比 ===
    ax3 = axes[1, 0]
    metrics = ["訓練期 MAPE", "驗證期 MAPE", "測試期 MAPE", "昨日價格 MAPE", "去年同日 MAPE", "去年同週平均 MAPE"]
    values = [
        results["train_MAPE"],
        results["val_MAPE"],
        results["test_MAPE"],
        results["lag1_mape"],
        results["lag365_mape"],
        results["lag365_ma7_mape"]
    ]
    
    bars = ax3.bar(metrics, values, alpha=0.7, edgecolor="black")
    
    ax3.set_ylabel("MAPE (%)", fontsize=12)
    ax3.set_title("MAPE 對比", fontsize=14, fontweight="bold")
    ax3.grid(axis="y", alpha=0.3)
    
    for bar, val in zip(bars, values):
        height = bar.get_height()
        ax3.text(
            bar.get_x() + bar.get_width() / 2.,
            height,
            f'{val:.2f}%',
            ha='center',
            va='bottom',
            fontsize=10,
            fontweight='bold'
        )
    
    ax3.set_xticks(range(len(metrics)))
    ax3.set_xticklabels(metrics, rotation=15, ha="right")
    
    # === 子圖 4: 關鍵指標摘要 ===
    ax4 = axes[1, 1]
    ax4.axis('off')  # 關閉座標軸
    
    # 準備摘要文字
    summary_text = f"""
    關鍵指標摘要
    {'='*40}

    測試期表現:
      • R² Score: {results['test_R2']:.4f}
      • RMSE: {results['test_RMSE']:.2f}
      • MAE: {results['test_MAE']:.2f}
      • MAPE: {results['test_MAPE']:.2f}%

    與基準比較:
      • R² 改進: {results['test_R2'] - results['lag1_r2']:.4f}
      • RMSE 改進: {results['lag1_rmse'] - results['test_RMSE']:.2f}

    窗口統計:
      • 測試窗口數: {results['test_windows']}
      • 預測數量: {results['test_predictions']}
      • 選中特徵數: {results['selected_features_count']}

    驗證-測試一致性:
      • R² 差異: {results['test_R2'] - results['val_R2']:.4f}
    """
    
    # 在圖上顯示文字
    ax4.text(
        0.1, 0.9,
        summary_text,
        transform=ax4.transAxes,
        fontsize=10,
        verticalalignment='top',
        fontfamily='monospace',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3)
    )
    
    # 調整子圖布局
    plt.tight_layout()
    
    # 儲存圖表
    plot_path = os.path.join(OUTPUT_DIR, "merged_analysis_with_weather_fixed.png")
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"\n📊 圖表已儲存: {plot_path}")
    plt.close()


# ====== 主程式 ======

def main(sfm_threshold="median", min_features=10):
    """
    主程式入口
    
    參數：
        sfm_threshold: SelectFromModel 的閾值設定
        min_features: 最少保留的特徵數量
    
    流程：
        1. 載入資料
        2. 訓練並評估模型
        3. 儲存結果
        4. 產生視覺化圖表
        5. 列印最終報告
    """
    start_time = time.time()
    
    # === 1. 載入資料 ===
    csv_path = os.path.join("/mnt/user-data/uploads", DEFAULTS["data_file"])
    if not os.path.exists(csv_path):
        csv_path = DEFAULTS["data_file"]
    
    df = load_and_preprocess_data(csv_path)
    
    # === 2. 配置參數 ===
    config = {
        **DEFAULTS,
        "sfm_threshold": sfm_threshold,
        "min_features": min_features,
    }
    
    # === 3. 訓練並評估模型 ===
    results = train_and_evaluate_merged(df, config)
    
    if results is not None:
        # === 4. 儲存結果 ===
        results_path = os.path.join(OUTPUT_DIR, "merged_results_with_weather_fixed.json")
        
        # 轉換 NumPy 類型為 Python 原生類型
        results_to_save = {}
        for key, value in results.items():
            if isinstance(value, (np.integer, np.floating)):
                results_to_save[key] = float(value)
            elif isinstance(value, list):
                results_to_save[key] = value
            elif isinstance(value, dict):
                results_to_save[key] = value
            else:
                results_to_save[key] = value
        
        # 寫入 JSON 檔案
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(results_to_save, f, indent=2, ensure_ascii=False)
        print(f"\n💾 結果已儲存: {results_path}")
        
        # === 5. 產生視覺化圖表 ===
        create_comparison_plot(results)
        
        # === 6. 列印最終報告 ===
        print("\n" + "=" * 70)
        print("🎯 全蔬菜合併分析最終報告（無當日特徵 - 修正跨視窗洩漏）")
        print("=" * 70)
        
        print(f"\n📊 整體表現:")
        print(f"   測試期 R²: {results['test_R2']:.4f}")
        print(f"   測試期 RMSE: {results['test_RMSE']:.2f}")
        print(f"   測試期 MAPE: {results['test_MAPE']:.2f}%")
        
        print(f"\n📊 與基準比較:")
        print(f"   基準 R²: {results['lag1_r2']:.4f}")
        print(f"   R² 改進: {results['test_R2'] - results['lag1_r2']:.4f}")
        
        consistency = 1 - abs(results['test_R2'] - results['val_R2'])
        print(f"\n🔍 驗證-測試一致性:")
        print(f"   驗證期 R²: {results['val_R2']:.4f}")
        print(f"   測試期 R²: {results['test_R2']:.4f}")
        print(f"   一致性分數: {consistency:.4f}")
        
        # 判斷模型是否優於基準
        if results['test_R2'] > results['lag1_r2']:
            print(f"\n✅ 模型優於基準方法！")
        else:
            print(f"\n⚠️ 模型未能超越基準方法")
        
        print("\n" + "=" * 70)
    else:
        print("❌ 分析失敗")
    
    # === 7. 計算執行時間 ===
    elapsed = time.time() - start_time
    print(f"\n⏱️ 總執行時間: {elapsed:.1f} 秒")
    print("✅ 全蔬菜合併分析完成！（無當日特徵 - 修正跨視窗洩漏）")
    
    return results


# ====== 程式入口點 ======

if __name__ == "__main__":
    # 解析命令列參數
    parser = argparse.ArgumentParser(
        description="全蔬菜合併分析的機器學習流程（無當日特徵 - 修正跨視窗洩漏）"
    )
    
    parser.add_argument(
        "--sfm-threshold",
        type=str,
        default="median",
        help="SelectFromModel threshold: median, mean, or a float e.g. 0.01",
    )
    
    parser.add_argument(
        "--min-features",
        type=int,
        default=10,
        help="Minimum features to keep if threshold is too strict",
    )
    
    args = parser.parse_args()
    
    # 執行主程式
    results = main(
        sfm_threshold=args.sfm_threshold,
        min_features=args.min_features
    )
    
    # 列印最終狀態
    if results is not None:
        print("\n🎉 程式執行成功！")
        print(f"📊 最終測試期 R² Score: {results['test_R2']:.4f}")
    else:
        print("\n❌ 程式執行失敗")
