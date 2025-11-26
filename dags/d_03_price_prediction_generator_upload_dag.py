# 導入必要套件
import pymysql
import pandas as pd
import numpy as np
from pathlib import Path
import joblib  # 用以保存/載入模型
import json
from datetime import datetime, timedelta

# 載入airflow所需套件
import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ===設定MySQL資訊、模型檔案存放位置

# 模型儲存路徑設定
model_dir = "/app/models"
model_path = f"{model_dir}/fruit_price_model.pkl"
metadata_path = f"{model_dir}/model_metadata.json"
feature_cols_path = f"{model_dir}/feature_columns.pkl"
crop_index_path = f"{model_dir}/crop_index_mapping.pkl"

# 模型參數設定
defaults = {
    "train_days": 365,  # 最大嘗試抓的訓練天數
    "max_train_days": 730,  # 訓練最多不超過 730 天
    "test_days": 90,  # 測試集天數（從 90 降到 30，更容易滿足）
    "validation_windows": 15,  # TimeSeriesSplit 的切法（從 15 降到 10）
    "forecast_horizon": 8,  # 未來預測天數
    "min_data_points": 60,  # 最小資料點數（測試集 30 + 訓練集至少 30）
}


# 設定資料庫連線資訊
db_config = {
    "host": "104.199.220.12",
    "port": 3306,
    "user": "tjr103-team02",
    "passwd": "password",
    "db": "tjr103-team02",
    "charset": "utf8mb4",
}

# 設定資料庫查詢模板
sql_template = """
    SELECT
        `date`
        ,crop_id
        ,crop_price_per_kg
        ,is_typhoon
        ,typhoon_name
        ,station_pressure_area AS station_pressure
        ,air_temperature_area AS air_temperature
        ,relative_humidity_area AS relative_humidity
        ,wind_speed_area AS wind_speed
        ,precipitation_area AS precipitation
    FROM v_crop_daily
    WHERE `date` >= CURDATE() - INTERVAL 365*2 DAY
"""

# ===================================設定資訊的結尾===================================


# ===定義從MySQL Server取得資料的函數
def query_data_from_mysql(sql_template: str):
    """從 MySQL 讀取 v_crop_daily view"""

    try:
        with pymysql.connect(**db_config) as connection:

            # 準備 SQL 查詢
            print("開始執行 select...")

            # 執行查詢
            df = pd.read_sql(sql_template, connection)

            print(f"成功取得 {(df.shape[0])} 筆資料。")

    except Exception as e:
        print(f"資料庫操作發生錯誤：{e}")
        raise

    # 檢查 DataFrame 是否為空
    if df.empty:
        print("查無資料")
        return None

    # 執行日期轉換
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # JSON 只認得字串，不認得 Timestamp 物件
    df["date"] = df["date"].astype(str)

    return df


# ===定義特徵工程的函數


def add_time_features(df):
    """添加時間相關特徵"""
    df = df.copy()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["dayofyear"] = df["date"].dt.dayofyear
    df["dayofweek"] = df["date"].dt.dayofweek
    return df


def compute_price_features(df, target_col="crop_price_per_kg", lookback=7):
    """計算價格相關的滯後與統計特徵"""
    df = df.sort_values("date").copy()

    for lag in range(1, lookback + 1):
        df[f"price_lag{lag}"] = df[target_col].shift(lag)

    df["price_rolling_mean7"] = df[target_col].shift(1).rolling(7, min_periods=1).mean()
    df["price_rolling_std7"] = df[target_col].shift(1).rolling(7, min_periods=1).std()
    df["price_diff1"] = df[target_col].diff(1).shift(1)

    return df


def compute_weather_features(df, weather_cols):
    """計算氣象相關的滯後特徵"""
    df = df.sort_values("date").copy()

    for col in weather_cols:
        for lag in [1, 3, 7]:
            df[f"{col}_lag{lag}"] = df[col].shift(lag)
        df[f"{col}_rolling_mean7"] = df[col].shift(1).rolling(7, min_periods=1).mean()

    return df


def prepare_features(df, price_col="crop_price_per_kg"):
    """完整的特徵準備流程"""
    weather_cols = [
        "station_pressure",
        "air_temperature",
        "relative_humidity",
        "wind_speed",
        "precipitation",
    ]

    df = add_time_features(df)
    df = compute_price_features(df, target_col=price_col, lookback=7)
    df = compute_weather_features(df, weather_cols=weather_cols)

    return df


# 定義預測未來價格的函數(使用遞迴)


def create_new_row_with_features(
    current_data,
    future_date,
    predicted_price,
    price_col="crop_price_per_kg",
    weather_cols=None,
):
    """建立包含所有特徵的新資料列，用於遞迴預測"""
    if weather_cols is None:
        weather_cols = [
            "station_pressure",
            "air_temperature",
            "relative_humidity",
            "wind_speed",
            "precipitation",
        ]

    latest_row = current_data.iloc[-1]
    new_row = {}

    # 基本資訊
    new_row["date"] = future_date
    new_row["crop_id"] = latest_row["crop_id"]
    if "crop_index" in latest_row:
        new_row["crop_index"] = latest_row["crop_index"]
    new_row[price_col] = predicted_price

    # 時間特徵
    new_row["year"] = future_date.year
    new_row["month"] = future_date.month
    new_row["dayofyear"] = future_date.dayofyear
    new_row["dayofweek"] = future_date.dayofweek

    # 氣象特徵（使用最新值）
    for col in weather_cols:
        new_row[col] = latest_row[col]

    new_row["is_typhoon"] = 0
    new_row["typhoon_name"] = None

    # 更新價格滯後特徵
    for lag in range(1, 8):
        if lag == 1:
            new_row[f"price_lag{lag}"] = latest_row[price_col]
        else:
            new_row[f"price_lag{lag}"] = latest_row.get(
                f"price_lag{lag-1}", latest_row[price_col]
            )

    # 更新價格滾動統計特徵
    recent_prices = []
    for lag in range(1, 8):
        key = f"price_lag{lag}"
        if key in latest_row and pd.notna(latest_row[key]):
            recent_prices.append(latest_row[key])

    if len(recent_prices) > 0:
        new_row["price_rolling_mean7"] = float(np.mean(recent_prices))
        new_row["price_rolling_std7"] = float(
            np.std(recent_prices) if len(recent_prices) > 1 else 0
        )
    else:
        new_row["price_rolling_mean7"] = latest_row[price_col]
        new_row["price_rolling_std7"] = 0.0

    new_row["price_diff1"] = predicted_price - latest_row[price_col]

    # 更新氣象滯後特徵
    for col in weather_cols:
        for lag in [1, 3, 7]:
            lag_col = f"{col}_lag{lag}"
            if lag == 1:
                new_row[lag_col] = latest_row[col]
            else:
                prev_lag_col = f"{col}_lag{lag-1}"
                new_row[lag_col] = latest_row.get(prev_lag_col, latest_row[col])

        # 更新滾動平均
        recent_weather = []
        for lag in range(1, 8):
            lag_col = f"{col}_lag{lag}"
            if lag_col in latest_row and pd.notna(latest_row[lag_col]):
                recent_weather.append(latest_row[lag_col])
            elif lag == 1:
                recent_weather.append(latest_row[col])

        if len(recent_weather) > 0:
            new_row[f"{col}_rolling_mean7"] = float(np.mean(recent_weather))
        else:
            new_row[f"{col}_rolling_mean7"] = latest_row[col]

    return new_row


def predict_future_recursive_optimized(
    historical_data,
    best_model,
    test_end_date,
    feature_cols,
    price_col="crop_price_per_kg",
    forecast_horizon=7,
):
    """遞迴預測未來價格"""
    current_data = historical_data.copy()
    future_predictions = []

    for day_offset in range(1, forecast_horizon + 1):
        future_date = test_end_date + timedelta(days=day_offset)

        # 準備特徵
        X_future = current_data.iloc[[-1]][feature_cols].fillna(0)
        predicted_price = best_model.predict(X_future)[0]

        # 建立新資料列
        new_row = create_new_row_with_features(
            current_data,
            future_date,
            predicted_price,
            price_col=price_col,
        )

        future_predictions.append(
            {
                "date": future_date,
                "crop_id": new_row["crop_id"],
                "price_prediction": predicted_price,
            }
        )

        # 加入歷史資料
        new_row_df = pd.DataFrame([new_row])
        current_data = pd.concat([current_data, new_row_df], ignore_index=True)

    return pd.DataFrame(future_predictions)


# ===定義使用模型的參數


# 使用模型進行預測
def predict_with_loaded_model(
    df, model, feature_cols, crop_to_index, price_col, test_days, forecast_horizon
):
    """使用已載入的模型進行預測"""

    # 資料清理與特徵準備
    df_clean = df.dropna(subset=[price_col]).copy()
    df_clean = prepare_features(df_clean, price_col=price_col)

    # 只移除關鍵欄位的 NaN
    df_clean = df_clean.dropna(subset=[price_col, "price_lag1"])

    # 加入 crop_index
    df_clean["crop_index"] = df_clean["crop_id"].map(crop_to_index)
    df_clean = df_clean[df_clean["crop_index"].notna()]

    per_crop_data = {}
    for crop_id in sorted(df_clean["crop_id"].unique()):
        crop_data = (
            df_clean[df_clean["crop_id"] == crop_id]
            .sort_values("date")
            .reset_index(drop=True)
        )

        if len(crop_data) < test_days + 30:
            print(f"{crop_id}: 資料不足 ({len(crop_data)} 筆 < {test_days + 30})")
            continue

        test_start_idx = len(crop_data) - test_days
        test_df = crop_data.iloc[test_start_idx:].copy()

        per_crop_data[crop_id] = {
            "test_df": test_df,
            "full_df": crop_data,
        }

    # 開始預測
    all_predictions = []

    for crop_id, data_dict in per_crop_data.items():
        test_df = data_dict["test_df"]
        full_df = data_dict["full_df"]

        # 測試集預測
        X_test = test_df[feature_cols].fillna(0)
        test_pred = model.predict(X_test)

        test_results = test_df[["date", "crop_id", price_col]].copy()
        test_results["price_prediction_global"] = test_pred
        test_results = test_results.rename(columns={price_col: "price_actual"})

        # 未來預測
        test_end_date = test_df["date"].max()
        future_pred = predict_future_recursive_optimized(
            historical_data=full_df,
            best_model=model,
            test_end_date=test_end_date,
            feature_cols=feature_cols,
            price_col=price_col,
            forecast_horizon=forecast_horizon,
        )

        future_pred = future_pred.rename(
            columns={"price_prediction": "price_prediction_global"}
        )

        all_pred_crop = pd.concat([test_results, future_pred], ignore_index=True)
        all_predictions.append(all_pred_crop)

        print(f"{crop_id}: 預測完成")

    if len(all_predictions) == 0:
        return None

    final_predictions = pd.concat(all_predictions, ignore_index=True)
    return final_predictions


# 將預測價格轉換為price_prediction格式的函數


def transform_to_price_prediction_format(df):
    """將預測結果格式轉換為 price_prediction 格式"""
    print("🔄 轉換為 price_prediction 格式...")

    actual_df = df[["crop_id", "date"]].copy()
    actual_df["mode"] = "actual"
    actual_df["price"] = df["price_actual"]

    prediction_df = df[["crop_id", "date"]].copy()
    prediction_df["mode"] = "prediction"
    prediction_df["price"] = df["price_prediction_global"]

    result_df = pd.concat([actual_df, prediction_df], ignore_index=True)
    result_df = result_df.sort_values(
        by=["crop_id", "date", "mode"], ascending=[True, True, True]
    ).reset_index(drop=True)

    print(f"✅ 轉換完成：{len(result_df)} 筆資料")
    return result_df


# 正式載入模型的函數
def load_model_artifacts():
    """載入模型和相關檔案"""

    # 檢查檔案是否存在
    required_files = [
        model_dir,
        model_path,
        metadata_path,
        feature_cols_path,
        crop_index_path,
    ]
    for file_path in required_files:
        if not Path(file_path).exists():
            raise FileNotFoundError(
                f"找不到模型檔案: {file_path}\n" f"查無模型，請先訓練模型！"
            )

    # 載入模型
    model = joblib.load(model_path)

    # 載入特徵欄位
    feature_cols = joblib.load(feature_cols_path)

    # 載入作物對應表
    crop_to_index = joblib.load(crop_index_path)

    # 載入元數據
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    return model, feature_cols, crop_to_index, metadata


#  開始處理 Airflow DAG
@dag(
    dag_id="d_03_price_prediction_generator_upload_dag",
    description="每日從MySQL執行view取出歷史價格，並透過模型預測資料後回傳至MySQL",
    schedule=None,
    start_date=pendulum.datetime(2020, 1, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["view", "etl", "mysql", "pymysql", "price", "prediction", "ML"],
    is_paused_upon_creation=False,
)
def price_prediction_generator_etl_dag_pymysql():

    @task
    def extract_price_data():
        """extract"""
        print("--- [Task 1] Extract: 開始抓取近90天價格資料 ---")
        df = query_data_from_mysql(sql_template)

        # 將DataFrame轉為 dict 傳給下一個 task
        return df.to_dict(orient="records")

    @task
    def transform_price_data(raw_data: list):
        """transform"""
        print("--- [Task 2] Transform: 開始載入模型並預測價格 ---")
        if not raw_data:
            print("無資料可處理。")
            return []

        # 從 XCom (List of Dicts) 還原回 DataFrame
        df = pd.DataFrame(raw_data)

        # 再將時間轉回timestamp才能做特徵工程
        df["date"] = pd.to_datetime(df["date"])

        try:
            # 載入模型
            model, feature_cols, crop_to_index, metadata = load_model_artifacts()

            # 正式開始執行預測
            final_predictions = predict_with_loaded_model(
                df,
                model,
                feature_cols,
                crop_to_index,
                price_col="crop_price_per_kg",
                test_days=defaults["test_days"],
                forecast_horizon=defaults["forecast_horizon"],
            )

            if final_predictions is None:
                print("\n預測失敗，程式結束")

        except FileNotFoundError as e:
            print(f"\n{e}")
            print(f"查無模型，請先訓練模型！")

        # 篩選出需要欄位
        df = final_predictions[
            ["date", "crop_id", "price_actual", "price_prediction_global"]
        ].copy()

        # 去重，保留最後一筆
        df = df.drop_duplicates(subset=["date", "crop_id"], keep="last")

        # 重新命名欄位
        df = df.rename(
            columns={
                "price_actual": "actual",
                "price_prediction_global": "prediction",
            }
        )

        # 合併欄位(寬轉長)
        df = df.melt(id_vars=["date", "crop_id"], var_name="mode", value_name="price")

        # 依照crop_id及date重新排序
        df = df.sort_values(["crop_id", "date"]).reset_index(drop=True)

        # JSON 只認得字串，不認得 Timestamp 物件
        df["date"] = df["date"].astype(str)

        # 再次轉為 Dict List 回傳給 Load Task
        return df.to_dict(orient="records")

    @task
    def load_price_data(transformed_data: list):
        """load"""
        print("--- [Task 3] Load: 開始寫入資料庫 ---")
        if not transformed_data:
            print("無資料可寫入。")
            return

        # 從 XCom (List of Dicts) 還原回 DataFrame
        df = pd.DataFrame(transformed_data)

        # MySQL不接受Nan，故要先轉為None
        df = df.replace({np.nan: None})

        # 指定要匯入的表格
        db_table = "price_prediction"

        # 正式開始匯入
        try:
            # A. 將 DataFrame 轉換為(tuple)
            data_tuples = [tuple(row) for row in df.to_numpy()]

            # B. 準備 SQL 插入模板
            sql_template = f"""
                INSERT INTO {db_table} ({", ".join(list(df.columns))})
                VALUES ({", ".join(["%s"] * len(df.columns))})
                ON DUPLICATE KEY UPDATE
                    `price` = VALUES(`price`);
            """
            conn = pymysql.connect(**db_config)
            cursor = conn.cursor()

            # C. 執行「一次性」批次插入
            print("開始執行 executemany...")
            cursor.executemany(sql_template, data_tuples)

            # D. 獲取插入成功的筆數
            cursor.execute("SELECT ROW_COUNT()")
            successful_inserts = cursor.fetchone()[0]

            # E. 提交交易
            conn.commit()
            print(f"成功將 {successful_inserts} 筆資料寫入 '{db_table}' 資料表。")

        except Exception as e:
            conn.rollback()
            print(f"寫入資料庫時發生錯誤: {e}")

        finally:
            cursor.close()
            conn.close()

    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_dag_04",
        trigger_dag_id="d_04_insert_to_gsheet_dag",
        wait_for_completion=False,  # 不等確認下一個dag執行就結束任務
    )

    # 設定相依性
    raw_data = extract_price_data()
    clean_data = transform_price_data(raw_data)
    load_price_data(clean_data) >> trigger_next_dag


price_prediction_generator_etl_dag_pymysql()
