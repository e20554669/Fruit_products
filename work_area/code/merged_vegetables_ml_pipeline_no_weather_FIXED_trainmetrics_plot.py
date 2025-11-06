# -*- coding: utf-8 -*-
"""
全蔬菜合併分析版本 - 完全移除氣象特徵（修正資料洩漏版）
================================================
主要變更：
1. ✅ 完全移除所有氣象相關特徵（Temperature、RH、Precp、StnPres、WS、typhoon）
2. ✅ 僅使用價格滯後特徵、時間特徵、蔬菜 ID 特徵
3. ✅ 保留所有分析方式：Random Search、特徵選擇、多基準對比、殘差模型
4. ✅ 保留完整的驗證/測試流程與可視化
5. ✅ 【重要修正】改為逐視窗訓練+評分，杜絕跨視窗資料洩漏
"""

import os
import warnings
import time
import numpy as np
import pandas as pd
from datetime import timedelta
import json
import argparse

from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from sklearn.feature_selection import SelectFromModel

def _safe_mape(y_true, y_pred):
    y_true = np.asarray(y_true); y_pred = np.asarray(y_pred)
    mask = (y_true != 0) & np.isfinite(y_true) & np.isfinite(y_pred)
    if mask.sum() == 0:
        return np.nan
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100.0)

def _mase_from_df(df_ordered_with_y, y_pred, seasonality=1):
    """
    df_ordered_with_y: DataFrame 包含 'vege_id','y'，與 y_pred 對齊
    y_pred: 與 df_ordered_with_y 等長的預測值
    定義：MASE = MAE(model) / MAE(naive seasonal)
    """
    y_pred = np.asarray(y_pred)
    naive = df_ordered_with_y.groupby("vege_id")["y"].shift(seasonality)
    mask = naive.notna() & np.isfinite(df_ordered_with_y["y"])
    if mask.sum() == 0:
        return np.nan
    denom = np.mean(np.abs(df_ordered_with_y.loc[mask, "y"].values - naive.loc[mask].values))
    if denom == 0 or not np.isfinite(denom):
        return np.nan
    numer = mean_absolute_error(df_ordered_with_y.loc[mask, "y"].values, np.asarray(y_pred)[mask.values])
    return float(numer / denom)

from sklearn.model_selection import ParameterSampler

import xgboost as xgb
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

# 修復中文字體問題
plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

# ====== 參數設定 ======
DEFAULTS = {
    "data_file": "merged.csv",
    "train_days": 365,
    "valid_days": 7,
    "step_days": 7,
    "start_date": "2022-01-01",
    "test_days": 90,
    "min_samples": 100,
    "validation_windows": 20,
    "random_search_iter": 50,
}

# 🎯 Random Search 參數空間
PARAM_SPACE = {
    "n_estimators": [100, 200, 300, 500],
    "max_depth": [3, 4, 5, 6, 7, 8],
    "learning_rate": [0.05, 0.1, 0.15, 0.2],
    "subsample": [0.7, 0.8, 0.9, 1.0],
    "colsample_bytree": [0.7, 0.8, 0.9, 1.0],
    "reg_alpha": [0, 0.1, 0.5, 1.0],
    "reg_lambda": [0.5, 1.0, 1.5, 2.0],
}

OUTPUT_DIR = "merged_vegetables_results_no_weather_fixed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("🎯 全蔬菜合併分析版本（完全無氣象特徵 - 修正資料洩漏）啟動")
print("=" * 50)


# ====== HELPER FUNCTIONS ======
def _split_feature_sets(selected_features):
    """分離時間特徵用於殘差模型（無氣象特徵版本）"""
    time_core_cols = set(["year","month","dayofweek","dayofyear","quarter","day","week",
                          "is_spring","is_summer","is_autumn","is_winter",
                          "month_sin","month_cos","day_sin","day_cos","weekday_sin","weekday_cos",
                          "weekofyear"])
    time_prefixes = ("month_","day_","weekday_","sin_","cos_","weekofyear","dayofyear")

    # 主模型使用非時間特徵（主要是價格滯後特徵）
    non_time = [c for c in selected_features if c not in time_core_cols and not c.startswith(time_prefixes)]
    # 殘差模型使用時間特徵
    time_feats = [c for c in selected_features if (c in time_core_cols or c.startswith(time_prefixes))]

    selected_features_main = list(dict.fromkeys(non_time))
    time_feats_resid = list(dict.fromkeys(time_feats))
    if len(time_feats_resid) == 0:
        time_feats_resid = time_feats if time_feats else selected_features[:min(5, len(selected_features))]

    return selected_features_main, time_feats_resid


def parse_sfm_threshold(best_model, threshold_value):
    """解析 SelectFromModel 的 threshold"""
    if isinstance(threshold_value, (int, float)):
        return float(threshold_value)
    if isinstance(threshold_value, str):
        tv = threshold_value.strip().lower()
        if tv in ("median", "mean"):
            return tv
        if tv.endswith("*median"):
            try:
                factor = float(tv.replace("*median", ""))
                imp = getattr(best_model, "feature_importances_", None)
                if imp is None:
                    return "median"
                imp = np.array(imp)
                if imp.size == 0:
                    return "median"
                return float(np.median(imp) * factor)
            except Exception:
                return "median"
        try:
            return float(tv)
        except Exception:
            return "median"
    return "median"


# ====== 資料載入 ======
def load_and_preprocess_data(csv_path):
    print(f"📊 載入資料: {csv_path}")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"找不到資料檔: {csv_path}")

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
    df["ds"] = pd.to_datetime(df["ObsTime"], errors="coerce")
    df["y"] = pd.to_numeric(df["avg_price_per_kg"], errors="coerce")
    df["vege_id"] = df["vege_id"].astype(str)

    # 移除無效資料
    df = df.dropna(subset=["ds", "y", "vege_id"]).copy()
    df = df.sort_values(["ds", "vege_id"]).reset_index(drop=True)

    print(f"✅ 資料載入完成: {len(df):,} 筆, {df['vege_id'].nunique()} 種蔬菜")
    print(f"   日期範圍: {df['ds'].min().date()} → {df['ds'].max().date()}")
    return df


# ====== 特徵工程 ======
def add_time_features(df):
    """添加時間特徵"""
    df = df.copy()
    ds = df["ds"]

    df["year"] = ds.dt.year
    df["month"] = ds.dt.month
    df["dayofweek"] = ds.dt.dayofweek
    df["dayofyear"] = ds.dt.dayofyear
    df["quarter"] = ds.dt.quarter
    df["day"] = ds.dt.day
    df["week"] = ds.dt.isocalendar().week.astype(int)

    # 季節標記
    df["is_spring"] = ((df["month"] >= 3) & (df["month"] <= 5)).astype(int)
    df["is_summer"] = ((df["month"] >= 6) & (df["month"] <= 8)).astype(int)
    df["is_autumn"] = ((df["month"] >= 9) & (df["month"] <= 11)).astype(int)
    df["is_winter"] = ((df["month"] == 12) | (df["month"] <= 2)).astype(int)

    # 週期性編碼
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    df["day_sin"] = np.sin(2 * np.pi * df["dayofyear"] / 365)
    df["day_cos"] = np.cos(2 * np.pi * df["dayofyear"] / 365)
    df["weekday_sin"] = np.sin(2 * np.pi * df["dayofweek"] / 7)
    df["weekday_cos"] = np.cos(2 * np.pi * df["dayofweek"] / 7)
    return df


def add_price_lags(df):
    """添加價格滯後特徵 - 核心預測特徵"""
    df = df.copy().sort_values(["vege_id", "ds"]).reset_index(drop=True)

    # 基本滯後特徵
    for lag in [1, 3, 7, 14, 30]:
        df[f"y_lag_{lag}"] = df.groupby("vege_id")["y"].shift(lag)

    # 移動平均
    for w in [7, 14, 30]:
        df[f"y_ma_{w}"] = df.groupby("vege_id")["y"].shift(1).transform(
            lambda x: x.rolling(w, min_periods=1).mean()
        )

    # 變化量
    df["y_change_1"] = df.groupby("vege_id")["y"].shift(1) - df.groupby("vege_id")["y"].shift(2)
    df["y_change_7"] = df.groupby("vege_id")["y"].shift(7) - df.groupby("vege_id")["y"].shift(8)
    df["y_change_30"] = df.groupby("vege_id")["y"].shift(30) - df.groupby("vege_id")["y"].shift(31)

    # 百分比變化
    df["y_pct_change_1"] = df.groupby("vege_id")["y"].shift(1).pct_change(1)
    df["y_pct_change_7"] = df.groupby("vege_id")["y"].shift(7).pct_change(1)

    # 波動性
    df["y_volatility_7"] = df.groupby("vege_id")["y"].shift(1).transform(
        lambda x: x.rolling(7, min_periods=1).std()
    )
    df["y_volatility_14"] = df.groupby("vege_id")["y"].shift(1).transform(
        lambda x: x.rolling(14, min_periods=1).std()
    )

    # 相對位置
    ma30 = df.groupby("vege_id")["y"].shift(1).transform(
        lambda x: x.rolling(30, min_periods=1).mean()
    )
    df["y_relative_position"] = (df.groupby("vege_id")["y"].shift(1) - ma30) / ma30.replace(0, np.nan)
    df["y_relative_position"] = df["y_relative_position"].fillna(0)
    return df


def add_vege_id_features(df):
    """蔬菜 ID one-hot 編碼"""
    df = df.copy()
    vege_dummies = pd.get_dummies(df["vege_id"], prefix="vege")
    df = pd.concat([df, vege_dummies], axis=1)
    return df


def build_features(df):
    """
    特徵工程 - 僅使用價格、時間和蔬菜ID特徵
    注意：需要傳入包含足夠歷史資料的 df，以確保滯後特徵能正確計算
    """
    df = add_time_features(df)
    df = add_price_lags(df)
    df = add_vege_id_features(df)
    return df


def safe_select_features(df, required_features):
    """安全選擇特徵，缺失的補0"""
    missing_features = set(required_features) - set(df.columns)
    for feature in missing_features:
        df[feature] = 0
    return df[required_features]


# ====== 基準模型 ======
def calculate_baseline_metrics(test_data, test_only=False):
    """
    計算多種基準模型指標
    - 基準 1：昨日價格（lag-1）
    - 基準 2：去年同日價格（lag-365）
    - 基準 3：去年同週平均（lag-365 MA-7）
    """
    dfb = test_data.copy().sort_values(["vege_id", "ds"])

    # 基準 1：lag-1
    dfb["y_pred_lag1"] = dfb.groupby("vege_id")["y"].shift(1)

    # 基準 2：lag-365
    dfb["y_pred_lag365"] = dfb.groupby("vege_id")["y"].shift(365)

    # 基準 3：lag-365 ±3 天移動平均
    dfb["y_pred_lag365_ma7"] = (
        dfb.groupby("vege_id")["y"]
           .shift(365)
           .rolling(window=7, center=False, min_periods=1)
           .mean()
    )

    eval_df = dfb[dfb["is_test"]] if (test_only and "is_test" in dfb.columns) else dfb
    results = {}

    # lag-1
    m = eval_df["y_pred_lag1"].notna()
    if m.sum() > 0:
        y_true, y_pred = eval_df.loc[m, "y"], eval_df.loc[m, "y_pred_lag1"]
        results["lag1_r2"]   = r2_score(y_true, y_pred)
        results["lag1_rmse"] = np.sqrt(mean_squared_error(y_true, y_pred))
        results["lag1_mae"]  = mean_absolute_error(y_true, y_pred)
        results["lag1_mape"] = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
    else:
        results.update({"lag1_r2": np.nan, "lag1_rmse": np.nan, "lag1_mae": np.nan, "lag1_mape": np.nan})

    # lag-365
    m = eval_df["y_pred_lag365"].notna()
    if m.sum() >= 10:
        y_true, y_pred = eval_df.loc[m, "y"], eval_df.loc[m, "y_pred_lag365"]
        results["lag365_r2"]   = r2_score(y_true, y_pred)
        results["lag365_rmse"] = np.sqrt(mean_squared_error(y_true, y_pred))
        results["lag365_mae"]  = mean_absolute_error(y_true, y_pred)
        results["lag365_mape"] = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
    else:
        results.update({"lag365_r2": np.nan, "lag365_rmse": np.nan, "lag365_mae": np.nan, "lag365_mape": np.nan})

    # lag-365 MA7
    m = eval_df["y_pred_lag365_ma7"].notna()
    if m.sum() >= 10:
        y_true, y_pred = eval_df.loc[m, "y"], eval_df.loc[m, "y_pred_lag365_ma7"]
        results["lag365_ma7_r2"]   = r2_score(y_true, y_pred)
        results["lag365_ma7_rmse"] = np.sqrt(mean_squared_error(y_true, y_pred))
        results["lag365_ma7_mae"]  = mean_absolute_error(y_true, y_pred)
        results["lag365_ma7_mape"] = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
    else:
        results.update({"lag365_ma7_r2": np.nan, "lag365_ma7_rmse": np.nan, "lag365_ma7_mae": np.nan, "lag365_ma7_mape": np.nan})

    return results


# ====== 時間窗口生成 ======
def generate_validation_windows(df, train_days, valid_days, step_days, num_windows):
    """生成驗證期窗口"""
    windows = []
    df = df.sort_values("ds")
    start_date = df["ds"].min()

    for i in range(num_windows):
        train_end = start_date + timedelta(days=train_days + i * step_days)
        valid_start = train_end + timedelta(days=1)
        valid_end = valid_start + timedelta(days=valid_days - 1)

        if valid_end > df["ds"].max():
            break

        # 訓練資料：只到 train_end
        train_data = df[df["ds"] <= train_end].copy()
        
        # 驗證資料：需要包含足夠的歷史資料來計算滯後特徵
        # 從 valid_start 往前推 60 天（足夠計算 lag_30）
        valid_history_start = valid_start - timedelta(days=60)
        valid_data_full = df[(df["ds"] >= valid_history_start) & (df["ds"] <= valid_end)].copy()

        if len(train_data) >= 100 and len(valid_data_full) >= 10:
            windows.append({
                "train_end": train_end,
                "valid_start": valid_start,
                "valid_end": valid_end,
                "train_data": train_data,
                "valid_data_full": valid_data_full,
                "valid_start_date": valid_start,
            })
    return windows


def generate_test_windows(df, train_days, valid_days, step_days, test_days):
    """生成測試期窗口"""
    windows = []
    df = df.sort_values("ds")

    test_end = df["ds"].max()
    test_start = test_end - timedelta(days=test_days - 1)

    print(f"\n🔧 測試期設定:")
    print(f"   測試期起點: {test_start.date()}")
    print(f"   測試期終點: {test_end.date()}")
    print(f"   測試期天數: {test_days}")

    current_train_end = test_start - timedelta(days=1)
    window_count = 0

    while True:
        valid_start = current_train_end + timedelta(days=1)
        valid_end = valid_start + timedelta(days=valid_days - 1)
        if valid_end > test_end:
            break

        train_start = current_train_end - timedelta(days=train_days - 1)
        train_data = df[(df["ds"] >= train_start) & (df["ds"] <= current_train_end)].copy()
        
        # 驗證資料：包含歷史資料
        valid_history_start = valid_start - timedelta(days=60)
        valid_data_full = df[(df["ds"] >= valid_history_start) & (df["ds"] <= valid_end)].copy()

        if len(train_data) >= 100 and len(valid_data_full) >= 10:
            windows.append({
                "train_end": current_train_end,
                "valid_start": valid_start,
                "valid_end": valid_end,
                "train_data": train_data,
                "valid_data_full": valid_data_full,
                "valid_start_date": valid_start,
            })
            window_count += 1

        current_train_end = current_train_end + timedelta(days=step_days)

    print(f"   生成窗口數: {window_count}")
    expected_windows = int(np.ceil(test_days / step_days))
    print(f"   理論窗口數: {expected_windows}")
    return windows


# ====== 模型訓練與評估 ======
def random_search_optimize(train_X, train_y, valid_X, valid_y, n_iter=50):
    """Random Search 超參數優化"""
    print(f"   🔍 開始 Random Search ({n_iter} 次迭代)...")
    best_score = -np.inf
    best_params = None

    param_list = list(ParameterSampler(PARAM_SPACE, n_iter=n_iter, random_state=42))
    for i, params in enumerate(param_list):
        model = xgb.XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            n_jobs=-1,
            **params
        )
        model.fit(train_X, train_y, verbose=False)
        y_pred = model.predict(valid_X)
        score = r2_score(valid_y, y_pred)
        if score > best_score:
            best_score = score
            best_params = params
        if (i + 1) % 10 == 0:
            print(f"      進度: {i+1}/{n_iter}, 當前最佳 R²: {best_score:.4f}")
    print(f"   ✅ Random Search 完成，最佳 R²: {best_score:.4f}")
    return best_params, best_score


def train_and_evaluate_merged(df, config):
    """
    主訓練與評估流程（修正版 - 逐視窗訓練+評分）
    
    修正重點：
    1. 在第一個視窗上進行 Random Search 和特徵選擇
    2. 對每個驗證視窗獨立訓練模型
    3. 在該視窗的驗證期評估
    4. 取所有視窗指標的平均值
    """
    print("\n" + "=" * 70)
    print("🚀 開始全蔬菜合併分析（完全無氣象特徵 - 修正版）")
    print("=" * 70)

    # ====== 階段 1: 驗證期逐視窗優化 ======
    print("\n📊 階段 1: 驗證期逐視窗參數優化與特徵選擇")
    print("-" * 70)

    validation_windows = generate_validation_windows(
        df,
        config["train_days"],
        config["valid_days"],
        config["step_days"],
        config["validation_windows"]
    )

    print(f"✅ 生成驗證窗口數: {len(validation_windows)}")
    if len(validation_windows) == 0:
        print("❌ 無法生成驗證窗口")
        return None
    
    # 🟢 修正：先在第一個視窗上做 Random Search 和特徵選擇
    print(f"\n   🔍 在第一個視窗上進行 Random Search 和特徵選擇...")
    first_window = validation_windows[0]
    
    train_with_features = build_features(first_window["train_data"])
    valid_with_features = build_features(first_window["valid_data_full"])
    valid_mask = valid_with_features["ds"] >= first_window["valid_start_date"]
    valid_eval = valid_with_features[valid_mask].copy()
    
    # 準備特徵
    exclude_cols = ["ds", "y", "vege_id", "ObsTime", "avg_price_per_kg"]
    feature_cols = [col for col in train_with_features.columns if col not in exclude_cols]
    
    train_X = train_with_features[feature_cols].fillna(0)
    train_y = train_with_features["y"]
    valid_X = safe_select_features(valid_eval, feature_cols).fillna(0)
    valid_y = valid_eval["y"]
    
    print(f"   特徵數量: {len(feature_cols)}")
    
    # Random Search 優化
    best_params, best_val_score = random_search_optimize(
        train_X, train_y, valid_X, valid_y,
        n_iter=config["random_search_iter"]
    )
    
    # 使用最佳參數訓練模型（用於特徵選擇）
    print(f"\n   🎯 使用最佳參數訓練模型...")
    best_model = xgb.XGBRegressor(
        objective="reg:squarederror",
        random_state=42,
        n_jobs=-1,
        **best_params
    )
    best_model.fit(train_X, train_y, verbose=False)
    
    # 特徵選擇
    print(f"   🔍 進行特徵選擇...")
    threshold_value = config.get("sfm_threshold", "median")
    selector = SelectFromModel(
        estimator=best_model,
        threshold=parse_sfm_threshold(best_model, threshold_value),
        prefit=True
    )
    
    selected_features = np.array(feature_cols)[selector.get_support()].tolist()
    
    # 重要性排序
    try:
        importance = best_model.feature_importances_
        imp_map = {feature_cols[i]: float(importance[i]) for i in range(len(feature_cols))}
        selected_features = sorted(selected_features, key=lambda c: imp_map.get(c, 0.0), reverse=True)
    except Exception:
        pass
    
    # 確保保留基礎特徵
    essential_features = [
        'y_lag_1',
        'y_lag_7',
        'y_ma_7',
        'month',
        'dayofweek',
    ]
    
    for feat in essential_features:
        if feat in feature_cols and feat not in selected_features:
            selected_features.append(feat)
    
    # 至少保留最少特徵數
    min_features = config.get("min_features", 10)
    if len(selected_features) < min_features:
        importance = best_model.feature_importances_
        top_indices = np.argsort(importance)[-min_features:]
        selected_features = [feature_cols[i] for i in top_indices]
    
    selected_features = list(set(selected_features))
    
    # 分離主模型和殘差模型特徵
    selected_features_main, time_feats_resid = _split_feature_sets(selected_features)
    
    print(f"   ✅ 選中特徵數: {len(selected_features)} / {len(feature_cols)}")
    print(f"      主模型特徵數: {len(selected_features_main)}")
    print(f"      殘差模型特徵數: {len(time_feats_resid)}")
    
    # 🟢 修正：逐視窗訓練和評估
    print(f"\n   📊 開始逐視窗訓練和評估...")
    window_metrics = []
    window_predictions = []
    window_actuals = []
    
    for i, window in enumerate(validation_windows):
        print(f"\n   視窗 {i+1}/{len(validation_windows)}: "
              f"訓練至 {window['train_end'].date()}, 驗證 {window['valid_start'].date()}")
        
        # 準備當前視窗的資料
        train_with_features = build_features(window["train_data"])
        valid_with_features = build_features(window["valid_data_full"])
        valid_mask = valid_with_features["ds"] >= window["valid_start_date"]
        valid_eval = valid_with_features[valid_mask].copy()
        
        train_X = safe_select_features(train_with_features, selected_features_main).fillna(0)
        train_y = train_with_features["y"]
        valid_X = safe_select_features(valid_eval, selected_features_main).fillna(0)
        valid_y = valid_eval["y"]
        
        # 🟢 在當前視窗訓練主模型
        window_model = xgb.XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            n_jobs=-1,
            **best_params
        )
        window_model.fit(train_X, train_y, verbose=False)
        
        # 🟢 訓練殘差模型
        train_base_pred = window_model.predict(train_X)
        train_residual = train_y - train_base_pred
        
        residual_model = xgb.XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            n_jobs=-1,
            n_estimators=300,
            learning_rate=0.05,
            max_depth=4
        )
        train_X_resid = safe_select_features(train_with_features, time_feats_resid).fillna(0)
        residual_model.fit(train_X_resid, train_residual, verbose=False)

        # === 訓練集完整預測（主模型 + 殘差模型）===
        train_resid_pred = residual_model.predict(train_X_resid)
        train_y_pred = train_base_pred + train_resid_pred

        # 訓練集指標：R² / RMSE / MAE / MAPE / MASE（季節性=1；如需週或年週期可改 7 或 365）
        window_train_r2 = r2_score(train_y, train_y_pred)
        window_train_rmse = np.sqrt(mean_squared_error(train_y, train_y_pred))
        window_train_mae = mean_absolute_error(train_y, train_y_pred)
        window_train_mape = _safe_mape(train_y, train_y_pred)
        window_train_mase = _mase_from_df(
            train_with_features[["vege_id", "y"]].reset_index(drop=True),
            train_y_pred, seasonality=1
        )
        # 🟢 預測當前視窗
        val_base_pred = window_model.predict(valid_X)
        valid_X_resid = safe_select_features(valid_eval, time_feats_resid).fillna(0)
        val_resid_pred = residual_model.predict(valid_X_resid)
        val_y_pred = val_base_pred + val_resid_pred
        
        # 🟢 計算當前視窗的指標
        window_r2 = r2_score(valid_y, val_y_pred)
        window_rmse = np.sqrt(mean_squared_error(valid_y, val_y_pred))
        window_mae = mean_absolute_error(valid_y, val_y_pred)
        window_mape = np.mean(np.abs((valid_y - val_y_pred) / valid_y)) * 100
        
        window_metrics.append({
            "train_R2": window_train_r2,
            "train_RMSE": window_train_rmse,
            "train_MAE": window_train_mae,
            "train_MAPE": window_train_mape,
            "train_MASE": window_train_mase,
            "R2": window_r2,
            "RMSE": window_rmse,
            "MAE": window_mae,
            "MAPE": window_mape,
            "samples": len(valid_y)
        })
        
        window_predictions.extend(val_y_pred)
        window_actuals.extend(valid_y)
        
        print(f"      R²: {window_r2:.4f}, RMSE: {window_rmse:.2f}, "
              f"MAE: {window_mae:.2f}, 樣本數: {len(valid_y)}")
    
    # 🟢 計算平均指標
    # 方式 1：各視窗指標的簡單平均
    avg_metrics_simple = {
        # 訓練（跨視窗平均）
        "train_R2":  np.mean([m["train_R2"]  for m in window_metrics]),
        "train_RMSE": np.mean([m["train_RMSE"] for m in window_metrics]),
        "train_MAE":  np.mean([m["train_MAE"]  for m in window_metrics]),
        "train_MAPE": np.mean([m["train_MAPE"] for m in window_metrics]),
        "train_MASE": np.mean([m["train_MASE"] for m in window_metrics]),
        # 驗證（跨視窗平均）
        "val_R2":  np.mean([m["R2"]  for m in window_metrics]),
        "val_RMSE": np.mean([m["RMSE"] for m in window_metrics]),
        "val_MAE":  np.mean([m["MAE"]  for m in window_metrics]),
        "val_MAPE": np.mean([m["MAPE"] for m in window_metrics]),
    }
    
    # 方式 2：所有預測的整體指標
    all_predictions = np.array(window_predictions)
    all_actuals = np.array(window_actuals)
    overall_metrics = {
        "val_R2_overall": r2_score(all_actuals, all_predictions),
        "val_RMSE_overall": np.sqrt(mean_squared_error(all_actuals, all_predictions)),
        "val_MAE_overall": mean_absolute_error(all_actuals, all_predictions),
        "val_MAPE_overall": np.mean(np.abs((all_actuals - all_predictions) / all_actuals)) * 100,
    }
    
    val_metrics = {
        **avg_metrics_simple,
        **overall_metrics,
        "validation_windows": len(validation_windows),
        "validation_samples": len(all_actuals),
    }
    
    print(f"\n   📊 驗證期整體表現:")
    print(f"      平均 R²: {val_metrics['val_R2']:.4f}")
    print(f"      平均 RMSE: {val_metrics['val_RMSE']:.2f}")
    print(f"      平均 (Train) R²: {val_metrics['train_R2']:.4f}, RMSE: {val_metrics['train_RMSE']:.2f}, MAE: {val_metrics['train_MAE']:.2f}, MAPE: {val_metrics['train_MAPE']:.2f}%, MASE: {val_metrics['train_MASE']:.3f}")
    print(f"      整體 R² (所有預測): {val_metrics['val_R2_overall']:.4f}")
    print(f"      整體 RMSE (所有預測): {val_metrics['val_RMSE_overall']:.2f}")

    # ====== 階段 2: 測試期評估（保持不變，已經是逐視窗） ======
    print("\n" + "=" * 70)
    print("📊 階段 2: 測試期評估（逐視窗訓練）")
    print("-" * 70)

    test_windows = generate_test_windows(
        df,
        config["train_days"],
        config["valid_days"],
        config["step_days"],
        config["test_days"]
    )

    print(f"✅ 生成測試窗口數: {len(test_windows)}")
    if len(test_windows) == 0:
        print("❌ 無法生成測試窗口")
        return None

    all_test_predictions = []
    all_test_actuals = []

    for window in test_windows:
        # 對完整資料進行特徵工程
        test_train_data = build_features(window["train_data"])
        test_valid_data_full = build_features(window["valid_data_full"])
        
        # 只保留真正的驗證期資料
        valid_mask = test_valid_data_full["ds"] >= window["valid_start_date"]
        test_valid_data = test_valid_data_full[valid_mask].copy()

        test_train_X = safe_select_features(test_train_data, selected_features_main).fillna(0)
        test_train_y = test_train_data["y"]
        test_valid_X = safe_select_features(test_valid_data, selected_features_main).fillna(0)
        test_valid_y = test_valid_data["y"]

        # 訓練主模型
        test_model = xgb.XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            n_jobs=-1,
            **best_params
        )
        test_model.fit(test_train_X, test_train_y, verbose=False)
        
        # 訓練殘差模型
        base_pred_train = test_model.predict(test_train_X)
        resid_train = (test_train_y - base_pred_train)

        test_resid_model = xgb.XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            n_jobs=-1,
            n_estimators=300,
            learning_rate=0.05,
            max_depth=4
        )
        test_train_X_resid = safe_select_features(test_train_data, time_feats_resid).fillna(0)
        test_resid_model.fit(test_train_X_resid, resid_train, verbose=False)

        # 測試集預測
        y_base_pred = test_model.predict(test_valid_X)
        test_valid_X_resid = safe_select_features(test_valid_data, time_feats_resid).fillna(0)
        y_resid_pred = test_resid_model.predict(test_valid_X_resid)
        y_pred = y_base_pred + y_resid_pred

        all_test_predictions.extend(y_pred)
        all_test_actuals.extend(test_valid_y)

    all_test_predictions = np.array(all_test_predictions)
    all_test_actuals = np.array(all_test_actuals)

    test_metrics = {
        "test_R2": r2_score(all_test_actuals, all_test_predictions),
        "test_RMSE": np.sqrt(mean_squared_error(all_test_actuals, all_test_predictions)),
        "test_MAE": mean_absolute_error(all_test_actuals, all_test_predictions),
        "test_MAPE": np.mean(np.abs((all_test_actuals - all_test_predictions) / all_test_actuals)) * 100,
        "test_windows": len(test_windows),
        "test_predictions": len(all_test_predictions),
        "selected_features_count": len(selected_features),
    }

    print(f"\n   📊 測試期整體表現:")
    print(f"      R² Score: {test_metrics['test_R2']:.4f}")
    print(f"      RMSE: {test_metrics['test_RMSE']:.2f}")
    print(f"      MAE: {test_metrics['test_MAE']:.2f}")
    print(f"      MAPE: {test_metrics['test_MAPE']:.2f}%")
    print(f"      預測數量: {test_metrics['test_predictions']}")

    # ====== 階段 3: 基準測試 ======
    print("\n" + "=" * 70)
    print("📊 階段 3: 基準測試（Lag-1 / Lag-365 / Lag-365 MA-7）")
    print("-" * 70)

    # 收集所有測試期資料
    merged_test_data = []
    for window in test_windows:
        test_valid_data_full = build_features(window["valid_data_full"])
        valid_mask = test_valid_data_full["ds"] >= window["valid_start_date"]
        merged_test_data.append(test_valid_data_full[valid_mask])
    
    merged_test_data = pd.concat(merged_test_data, ignore_index=True)

    # 為計算 lag-365，需要包含歷史資料
    test_start_date = merged_test_data["ds"].min()
    history_start_date = test_start_date - timedelta(days=400)

    full_data_for_baseline = df[df["ds"] >= history_start_date].copy()
    full_data_for_baseline["is_test"] = full_data_for_baseline["ds"].isin(merged_test_data["ds"])

    baseline_metrics = calculate_baseline_metrics(full_data_for_baseline, test_only=True)

    print(f"📊 基準模型表現對比:")
    print(f"   " + "=" * 60)

    # 基準 1
    print(f"   基準 1 - 昨日價格（Lag-1）:")
    if not np.isnan(baseline_metrics.get('lag1_r2', np.nan)):
        print(f"      R² Score: {baseline_metrics['lag1_r2']:.4f}")
        print(f"      RMSE: {baseline_metrics['lag1_rmse']:.2f}")
        print(f"      MAE: {baseline_metrics['lag1_mae']:.2f}")
        print(f"      MAPE: {baseline_metrics['lag1_mape']:.2f}%")
    else:
        print(f"      無法計算（資料不足）")

    # 基準 2
    print(f"   基準 2 - 去年同日（Lag-365）:")
    if not np.isnan(baseline_metrics.get('lag365_r2', np.nan)):
        print(f"      R² Score: {baseline_metrics['lag365_r2']:.4f}")
        print(f"      RMSE: {baseline_metrics['lag365_rmse']:.2f}")
        print(f"      MAE: {baseline_metrics['lag365_mae']:.2f}")
        print(f"      MAPE: {baseline_metrics['lag365_mape']:.2f}%")
    else:
        print(f"      無法計算（資料不足）")

    # 基準 3
    print(f"   基準 3 - 去年同週平均（Lag-365 MA-7）:")
    if not np.isnan(baseline_metrics.get('lag365_ma7_r2', np.nan)):
        print(f"      R² Score: {baseline_metrics['lag365_ma7_r2']:.4f}")
        print(f"      RMSE: {baseline_metrics['lag365_ma7_rmse']:.2f}")
        print(f"      MAE: {baseline_metrics['lag365_ma7_mae']:.2f}")
        print(f"      MAPE: {baseline_metrics['lag365_ma7_mape']:.2f}%")
    else:
        print(f"      無法計算（資料不足）")

    # 合併結果
    results = {
        **val_metrics,
        **test_metrics,
        **baseline_metrics,
        "best_params": best_params,
        "selected_features": selected_features,
    }

    return results


# ====== 可視化 ======
def create_comparison_plot(results):
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("全蔬菜合併分析結果（完全無氣象特徵｜修正版）", fontsize=16, fontweight="bold")

    # R² 對比
    ax1 = axes[0, 0]
    metrics = ["訓練期 R²", "驗證期 R²", "測試期 R²", "昨日價格 R²", "去年同日 R²", "去年同週平均 R²"]
    values = [results["train_R2"], results["val_R2"], results["test_R2"], results["lag1_r2"], results["lag365_r2"], results["lag365_ma7_r2"]]
    bars = ax1.bar(metrics, values, alpha=0.7, edgecolor="black")

    ax1.set_ylabel("R² Score", fontsize=12)
    ax1.set_title("R² Score 對比", fontsize=14, fontweight="bold")
    ax1.set_ylim([0, max(0.01, max(values)) * 1.2])
    ax1.grid(axis="y", alpha=0.3)

    for bar, val in zip(bars, values):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height, f'{val:.4f}', ha='center', va='bottom', fontsize=10, fontweight='bold')

    # ✅ 關鍵：要明確設定 xticks 對應 metrics
    ax1.set_xticks(range(len(metrics)))
    ax1.set_xticklabels(metrics, rotation=15, ha="right")

    # RMSE 對比
    ax2 = axes[0, 1]
    metrics = ["訓練期 RMSE", "驗證期 RMSE", "測試期 RMSE", "昨日價格 RMSE", "去年同日 RMSE", "去年同週平均 RMSE"]
    values = [results["train_RMSE"],results["val_RMSE"], results["test_RMSE"], results["lag1_rmse"], results["lag365_rmse"], results["lag365_ma7_rmse"]]
    bars = ax2.bar(metrics, values, alpha=0.7, edgecolor="black")

    ax2.set_ylabel("RMSE", fontsize=12)
    ax2.set_title("RMSE 對比", fontsize=14, fontweight="bold")
    ax2.grid(axis="y", alpha=0.3)

    for bar, val in zip(bars, values):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height, f'{val:.2f}', ha='center', va='bottom', fontsize=10, fontweight='bold')

   # ✅ 關鍵：要明確設定 xticks 對應 metrics
    ax2.set_xticks(range(len(metrics)))
    ax2.set_xticklabels(metrics, rotation=15, ha="right")

    # MAPE 對比
    ax3 = axes[1, 0]
    metrics = ["訓練期 MAPE", "驗證期 MAPE", "測試期 MAPE", "昨日價格 MAPE", "去年同日 MAPE", "去年同週平均 MAPE"]
    values = [results["train_MAPE"], results["val_MAPE"], results["test_MAPE"], results["lag1_mape"], results["lag365_mape"], results["lag365_ma7_mape"]]
    bars = ax3.bar(metrics, values, alpha=0.7, edgecolor="black")

    ax3.set_ylabel("MAPE (%)", fontsize=12)
    ax3.set_title("MAPE 對比", fontsize=14, fontweight="bold")
    ax3.grid(axis="y", alpha=0.3)

    for bar, val in zip(bars, values):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height, f'{val:.2f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')

   # ✅ 關鍵：要明確設定 xticks 對應 metrics
    ax3.set_xticks(range(len(metrics)))
    ax3.set_xticklabels(metrics, rotation=15, ha="right")

    # 關鍵指標摘要
    ax4 = axes[1, 1]
    ax4.axis('off')
    summary_text = f"""
    關鍵指標摘要（修正版）
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
      • 驗證窗口數: {results['validation_windows']}
      • 測試窗口數: {results['test_windows']}
      • 選中特徵數: {results['selected_features_count']}

    驗證-測試一致性:
      • R² 差異: {results['test_R2'] - results['val_R2']:.4f}
    
    ✅ 已修正跨視窗資料洩漏問題
    """
    ax4.text(0.1, 0.9, summary_text, transform=ax4.transAxes,
              fontsize=9, verticalalignment='top', fontfamily='monospace',
              bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

    plt.tight_layout()
    plot_path = os.path.join(OUTPUT_DIR, "merged_analysis_fixed.png")
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"\n📊 圖表已儲存: {plot_path}")
    plt.close()


# ====== 主程式 ======
def main(sfm_threshold="median", min_features=10):
    start_time = time.time()

    # 載入資料
    csv_path = os.path.join("/mnt/user-data/uploads", DEFAULTS["data_file"])
    if not os.path.exists(csv_path):
        csv_path = DEFAULTS["data_file"]

    df = load_and_preprocess_data(csv_path)

    config = {
        **DEFAULTS,
        "sfm_threshold": sfm_threshold,
        "min_features": min_features,
    }

    results = train_and_evaluate_merged(df, config)

    if results is not None:
        results_path = os.path.join(OUTPUT_DIR, "merged_results_fixed.json")
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
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(results_to_save, f, indent=2, ensure_ascii=False)
        print(f"\n💾 結果已儲存: {results_path}")

        create_comparison_plot(results)

        print("\n" + "=" * 70)
        print("🎯 全蔬菜合併分析最終報告（完全無氣象特徵 - 修正版）")
        print("=" * 70)
        print(f"\n📊 整體表現:")
        print(f"   驗證期平均 R²: {results['val_R2']:.4f}")
        print(f"   驗證期平均 RMSE: {results['val_RMSE']:.2f}")
        print(f"   驗證期整體 R² (所有預測): {results['val_R2_overall']:.4f}")
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
        if results['test_R2'] > results['lag1_r2']:
            print(f"\n✅ 模型優於基準方法！")
        else:
            print(f"\n⚠️ 模型未能超越基準方法")
        
        print(f"\n✅ 已修正跨視窗資料洩漏問題")
        print(f"   - 在第一個視窗上進行參數優化和特徵選擇")
        print(f"   - 對每個驗證視窗獨立訓練模型")
        print(f"   - 取所有視窗指標的平均值")
        
        print("\n" + "=" * 70)
    else:
        print("❌ 分析失敗")

    elapsed = time.time() - start_time
    print(f"\n⏱️ 總執行時間: {elapsed:.1f} 秒")
    print("✅ 全蔬菜合併分析完成！（完全無氣象特徵 - 修正版）")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="全蔬菜合併分析的機器學習流程（完全無氣象特徵 - 修正版）")
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

    results = main(sfm_threshold=args.sfm_threshold, min_features=args.min_features)
    if results is not None:
        print("\n🎉 程式執行成功！")
        print(f"📊 最終測試期 R² Score: {results['test_R2']:.4f}")
    else:
        print("\n❌ 程式執行失敗")
