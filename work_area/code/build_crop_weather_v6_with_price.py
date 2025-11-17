#!/usr/bin/env python
# -*- coding: utf-8 -*-

# build_crop_weather_v6_with_price.py
#
# 改版說明：
# - 基於 v5 版本
# - 新增 crop_price_per_kg 欄位：計算每日每個 crop_id 在所有 city_id 的 avg_price 平均值
# - 欄位順序：date, crop_id, crop_price_per_kg, station_pressure, ...
#
# 其他功能與 v5 相同：
# - 從 MySQL 讀取 volume / area_production / weather 三張表
# - 計算 crop_weather 並寫回 MySQL
# - 每次執行都重建 crop_weather_data 表和 crop_weather VIEW

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# === 資料庫連線設定（請依實際環境調整） ===
DB_CONFIG = {
    "host": "104.199.220.12",
    "port": 3306,
    "database": "tjr103-team02",
    "user": "tjr103-team02",
    "password": "password",
}

# 建立 SQLAlchemy 連線字串
def get_engine():
    """建立 SQLAlchemy engine"""
    connection_string = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string)

WEATHER_VARS = [
    "station_pressure",
    "air_temperature",
    "relative_humidity",
    "wind_speed",
    "precipitation",
]


def load_tables_from_mysql(engine):
    # 從 MySQL 讀取 volume, area_production, weather 三張表為 DataFrame
    print("Reading tables from MySQL ...")

    volume = pd.read_sql("SELECT * FROM volume", engine)
    area_prod = pd.read_sql("SELECT * FROM area_production", engine)
    weather = pd.read_sql("SELECT * FROM weather", engine)

    print(f"  volume: {len(volume)} rows")
    print(f"  area_production: {len(area_prod)} rows")
    print(f"  weather: {len(weather)} rows")
    return volume, area_prod, weather


def compute_crop_price(volume):
    """
    計算每日每個 crop_id 的平均價格
    對所有 city_id 的 avg_price 取平均
    """
    print("Computing crop_price_per_kg (average across all cities)...")
    
    # 只保留需要的欄位
    price_data = volume[['date', 'crop_id', 'avg_price']].copy()
    
    # 轉換日期格式
    price_data['date'] = pd.to_datetime(price_data['date'])
    
    # 按 (date, crop_id) 分組,計算 avg_price 的平均值
    crop_price = (
        price_data.groupby(['date', 'crop_id'])['avg_price']
        .mean()
        .reset_index()
        .rename(columns={'avg_price': 'crop_price_per_kg'})
    )
    
    print(f"  Computed {len(crop_price)} (date, crop_id) price records")
    return crop_price


def compute_crop_weather(volume, area_prod, weather):
    # 沿用 build_crop_weather_v4.py 的邏輯計算 crop_weather DataFrame

    print("Converting date columns...")
    volume = volume.copy()
    area_prod = area_prod.copy()
    weather = weather.copy()

    volume["date"] = pd.to_datetime(volume["date"])
    weather["date"] = pd.to_datetime(weather["date"])

    # === Phase 2: 決定每個作物代表海拔 ===
    print("Computing crop altitude preference (High/Low)...")
    crop_altitude_pref = (
        area_prod.groupby(["crop_id", "altitude"])["production"]
        .sum()
        .reset_index()
    )
    idx = crop_altitude_pref.groupby("crop_id")["production"].idxmax()
    crop_alt_map = (
        crop_altitude_pref.loc[idx]
        .set_index("crop_id")["altitude"]
    )  # Series: crop_id -> "High" or "Low"

    # === Phase 3: city 權重 (含 2025=2024) ===
    print("Building production-based city weights...")
    weights_raw = (
        area_prod.groupby(["year", "crop_id", "city_id"])["production"]
        .sum()
        .reset_index()
    )
    totals = (
        weights_raw.groupby(["year", "crop_id"])["production"]
        .sum()
        .reset_index()
        .rename(columns={"production": "total_production"})
    )
    weights = weights_raw.merge(totals, on=["year", "crop_id"], how="left")
    weights["weight"] = weights["production"] / weights["total_production"]

    # 2025 年使用 2024 的權重
    weights_2024 = weights[weights["year"] == 2024].copy()
    weights_2025 = weights_2024.copy()
    weights_2025["year"] = 2025
    weights_ext = pd.concat([weights, weights_2025], ignore_index=True)

    # === Phase 4: 整理 weather 成單一 pivot，包含 High/Low 兩套欄位 ===
    print("Preparing weather pivot (High & Low columns)...")
    pivot = weather.pivot_table(
        index=["date", "city_id"],
        columns="altitude",
        values=WEATHER_VARS,
    )
    pivot.columns = [f"{var}_{alt}" for (var, alt) in pivot.columns]
    weather_pivot = pivot.reset_index()

    # === Phase 5: 每日颱風資訊 ===
    print("Aggregating typhoon info per day...")
    typhoon_daily = (
        weather.groupby("date")
        .agg(
            is_typhoon=("is_typhoon", "max"),
            typhoon_name=(
                "typhoon_name",
                lambda x: next(
                    (v for v in x if isinstance(v, str) and v.strip() != ""),
                    None,
                ),
            ),
        )
        .reset_index()
    )

    # === Phase 6: (date, crop_id) + 權重 ===
    print("Preparing (date, crop_id) table and merging weights...")
    pairs = volume[["date", "crop_id"]].drop_duplicates().copy()
    pairs["year"] = pairs["date"].dt.year
    # 2025 用 2024 的權重
    pairs["year_for_weight"] = pairs["year"].clip(upper=2024)

    # 加入作物海拔屬性
    pairs = pairs.merge(
        crop_alt_map.rename("crop_altitude"),
        left_on="crop_id",
        right_index=True,
        how="left",
    )

    # 和 city 權重表 merge，讓每個 (date, crop_id) 拆成多個 city 行
    pairs_weights = pairs.merge(
        weights_ext,
        left_on=["year_for_weight", "crop_id"],
        right_on=["year", "crop_id"],
        how="left",
        suffixes=("", "_w"),
    )

    # === Phase 7: 併入 weather_pivot + 依 altitude 選欄位 ===
    print("Merging weather pivot and selecting High/Low by crop altitude...")

    merged = pairs_weights.merge(
        weather_pivot,
        on=["date", "city_id"],
        how="left",
    )

    # 依作物 altitude 選擇 High 或 Low，如缺值會 fallback
    def choose_weather_value(row, var):
        high_col = f"{var}_High"
        low_col = f"{var}_Low"
        v_high = row.get(high_col, pd.NA)
        v_low = row.get(low_col, pd.NA)
        alt = row.get("crop_altitude", None)

        if alt == "High":
            if pd.notna(v_high):
                return v_high
            elif pd.notna(v_low):
                return v_low
            else:
                return pd.NA
        else:
            if pd.notna(v_low):
                return v_low
            elif pd.notna(v_high):
                return v_high
            else:
                return pd.NA

    print("Applying altitude-based weather selection...")
    for col in WEATHER_VARS:
        merged[col] = merged.apply(lambda r, c=col: choose_weather_value(r, c), axis=1)

    # === Phase 8: 加權平均 ===
    print("Computing weighted weather per (date, crop_id)...")

    def weighted_agg(group):
        w = group["weight"]
        res = {}
        w_sum = w.sum()
        if w_sum == 0 or pd.isna(w_sum):
            for c in WEATHER_VARS:
                res[c] = pd.NA
            return pd.Series(res)

        for c in WEATHER_VARS:
            vals = group[c]
            mask = vals.notna()
            if not mask.any():
                res[c] = pd.NA
            else:
                ww = w[mask]
                vv = vals[mask]
                res[c] = (ww * vv).sum() / ww.sum()
        return pd.Series(res)

    agg = (
        merged.groupby(["date", "crop_id"], as_index=False)
        .apply(weighted_agg, include_groups=False)
    )

    # === Phase 9: 合併颱風資訊 ===
    print("Merging typhoon info ...")
    final = pairs.merge(agg, on=["date", "crop_id"], how="left")
    final = final.merge(typhoon_daily, on="date", how="left")

    final["is_typhoon"] = final["is_typhoon"].fillna(0).astype(int)

    final_out = final[
        [
            "date",
            "crop_id",
            "station_pressure",
            "air_temperature",
            "relative_humidity",
            "wind_speed",
            "precipitation",
            "is_typhoon",
            "typhoon_name",
        ]
    ]

    return final_out


def write_table_and_view(engine, df, table_name="crop_weather_data", view_name="crop_weather"):
    # 將 DataFrame 寫入 MySQL，重建暫存表 + VIEW
    print(f"Writing data to MySQL table `{table_name}` and creating view `{view_name}` ...")
    
    with engine.connect() as conn:
        # 1. 先刪掉舊的 VIEW（避免還引用舊 table）
        conn.execute(text(f"DROP VIEW IF EXISTS `{view_name}`"))
        conn.commit()

        # 2. 刪除舊表
        conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
        conn.commit()

        # 3. 建立新表 (新增 crop_price_per_kg 欄位)
        create_table_sql = f"""
        CREATE TABLE `{table_name}` (
            `date` DATE NOT NULL,
            `crop_id` VARCHAR(20) NOT NULL,
            `crop_price_per_kg` DOUBLE NULL,
            `station_pressure` DOUBLE NULL,
            `air_temperature` DOUBLE NULL,
            `relative_humidity` DOUBLE NULL,
            `wind_speed` DOUBLE NULL,
            `precipitation` DOUBLE NULL,
            `is_typhoon` TINYINT(1) NOT NULL,
            `typhoon_name` VARCHAR(255) NULL,
            PRIMARY KEY (`date`, `crop_id`)
        ) ENGINE=InnoDB
        """
        conn.execute(text(create_table_sql))
        conn.commit()

        # 4. 插入資料 - 使用 pandas to_sql (更高效)
        if not df.empty:
            # 準備資料
            df_to_insert = df.copy()
            
            # 處理日期格式
            df_to_insert['date'] = pd.to_datetime(df_to_insert['date']).dt.date
            
            # 確保 crop_id 是字串型別
            df_to_insert['crop_id'] = df_to_insert['crop_id'].astype(str)
            
            # 四捨五入數值欄位並確保是 float 型別
            numeric_cols = ['crop_price_per_kg', 'station_pressure', 'air_temperature', 
                          'relative_humidity', 'wind_speed', 'precipitation']
            for col in numeric_cols:
                # 先轉換為 numeric (處理可能的非數值)
                df_to_insert[col] = pd.to_numeric(df_to_insert[col], errors='coerce')
                # 再四捨五入
                df_to_insert[col] = df_to_insert[col].round(2)
            
            # 處理 is_typhoon - 確保是整數
            df_to_insert['is_typhoon'] = df_to_insert['is_typhoon'].fillna(0).astype(int)
            
            # 處理 typhoon_name - 確保是字串或 None
            df_to_insert['typhoon_name'] = df_to_insert['typhoon_name'].astype(object)
            df_to_insert['typhoon_name'] = df_to_insert['typhoon_name'].where(
                df_to_insert['typhoon_name'].notna(), None
            )
            
            # 使用 to_sql 插入資料
            df_to_insert.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )
            print(f"  Inserted {len(df_to_insert)} rows into {table_name}")
        else:
            print("  Warning: DataFrame 為空，未插入任何資料。")

        # 5. 建立/重建 VIEW
        conn.execute(text(f"CREATE VIEW `{view_name}` AS SELECT * FROM `{table_name}`"))
        conn.commit()

    print("MySQL table & view committed.")


def main():
    try:
        print("Connecting to MySQL ...")
        engine = get_engine()
        
        # 測試連線
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("MySQL connection OK.\n")

        # 1. 從 MySQL 讀取三張表
        volume, area_prod, weather = load_tables_from_mysql(engine)

        # 2. 計算作物價格 (新增)
        crop_price = compute_crop_price(volume)

        # 3. 計算 crop_weather DataFrame
        crop_weather_df = compute_crop_weather(volume, area_prod, weather)
        print(f"\nComputed crop_weather_df: {len(crop_weather_df)} rows")

        # 4. 合併價格資訊 (新增)
        print("Merging crop price into final DataFrame...")
        crop_weather_df = crop_weather_df.merge(
            crop_price,
            on=['date', 'crop_id'],
            how='left'
        )
        
        # 5. 調整欄位順序
        crop_weather_df = crop_weather_df[
            [
                "date",
                "crop_id",
                "crop_price_per_kg",
                "station_pressure",
                "air_temperature",
                "relative_humidity",
                "wind_speed",
                "precipitation",
                "is_typhoon",
                "typhoon_name",
            ]
        ]

        # 6. 將結果寫回 MySQL，建立暫存表 + VIEW
        write_table_and_view(engine, crop_weather_df)

    except SQLAlchemyError as e:
        print(f"Database Error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        try:
            if 'engine' in locals():
                engine.dispose()
                print("MySQL connection closed.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
