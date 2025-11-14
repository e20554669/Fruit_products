# -*- coding: utf-8 -*-
"""
台灣 2020–2025 年全縣市氣象資料擷取程式
來源：Meteostat (免費無金鑰)
輸出：
1. taiwan_weather_hourly.csv（逐時）
2. taiwan_weather_daily.csv（每日平均、最高、最低、總雨量）
"""

import pandas as pd
from datetime import datetime
from meteostat import Stations, Hourly
from tqdm import tqdm
import pytz
from pathlib import Path

# =============================
# 1️⃣ 參數設定
# =============================
START = datetime(2020, 1, 1, 0, 0)
END = datetime(2025, 12, 31, 23, 0)
TZ = pytz.timezone("Asia/Taipei")

OUT_HOURLY = "taiwan_weather_hourly.csv"
OUT_DAILY = "taiwan_weather_daily.csv"
TYPHOON_CSV = "taiwan_typhoons_2020_2025.csv"  # 可留空

# =============================
# 2️⃣ 全台 22 縣市座標
# =============================
CITIES = [
    {"city_id": "TPE", "city_name": "台北市", "lat": 25.0375, "lon": 121.5637},
    {"city_id": "NTP", "city_name": "新北市", "lat": 25.0169, "lon": 121.4628},
    {"city_id": "TYN", "city_name": "桃園市", "lat": 24.9931, "lon": 121.2969},
    {"city_id": "HSC", "city_name": "新竹市", "lat": 24.8066, "lon": 120.9686},
    {"city_id": "HSQ", "city_name": "新竹縣", "lat": 24.8381, "lon": 121.0173},
    {"city_id": "MLI", "city_name": "苗栗縣", "lat": 24.5602, "lon": 120.8214},
    {"city_id": "TXG", "city_name": "台中市", "lat": 24.1477, "lon": 120.6736},
    {"city_id": "CHA", "city_name": "彰化縣", "lat": 24.0818, "lon": 120.5383},
    {"city_id": "NTO", "city_name": "南投縣", "lat": 23.9609, "lon": 120.9719},
    {"city_id": "YUN", "city_name": "雲林縣", "lat": 23.7092, "lon": 120.4313},
    {"city_id": "CYI", "city_name": "嘉義市", "lat": 23.4801, "lon": 120.4491},
    {"city_id": "CYQ", "city_name": "嘉義縣", "lat": 23.4518, "lon": 120.2555},
    {"city_id": "TNN", "city_name": "台南市", "lat": 22.9997, "lon": 120.2270},
    {"city_id": "KHH", "city_name": "高雄市", "lat": 22.6273, "lon": 120.3014},
    {"city_id": "PIF", "city_name": "屏東縣", "lat": 22.5519, "lon": 120.5485},
    {"city_id": "ILA", "city_name": "宜蘭縣", "lat": 24.7021, "lon": 121.7378},
    {"city_id": "HUA", "city_name": "花蓮縣", "lat": 23.9872, "lon": 121.6016},
    {"city_id": "TTT", "city_name": "台東縣", "lat": 22.7975, "lon": 121.0715},
    {"city_id": "PEN", "city_name": "澎湖縣", "lat": 23.5711, "lon": 119.5797},
    {"city_id": "KIN", "city_name": "金門縣", "lat": 24.4496, "lon": 118.3587},
    {"city_id": "LIE", "city_name": "連江縣", "lat": 26.1612, "lon": 119.9499},
    {"city_id": "KEE", "city_name": "基隆市", "lat": 25.1283, "lon": 121.7419},
]

# =============================
# 3️⃣ 找最近測站
# =============================
def find_nearest_station(lat, lon):
    st = Stations().nearby(lat, lon).fetch(3)
    return st.iloc[0] if len(st) else None


# =============================
# 4️⃣ 抓逐時資料
# =============================
def fetch_hourly_by_station(station_id, start, end):
    data = Hourly(station_id, start, end)
    df = data.fetch()
    if df.empty:
        return df

    # 🩵 修正：若時間無時區，先 localize 再 convert
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    df = df.tz_convert(TZ)

    df = df.reset_index().rename(columns={"time": "datetime"})
    return df


# =============================
# 5️⃣ 主流程
# =============================
city_station = []
for c in CITIES:
    st = find_nearest_station(c["lat"], c["lon"])
    if st is None:
        print(f"[WARN] 找不到測站: {c['city_name']}")
        continue
    city_station.append({
        "city_id": c["city_id"],
        "city_name": c["city_name"],
        "lat": c["lat"],
        "lon": c["lon"],
        "station_id": st.name,
        "station_name": st.get("name", None)
    })

city_station_df = pd.DataFrame(city_station)
print("\n✅ 城市對應測站：")
print(city_station_df[["city_id", "city_name", "station_id", "station_name"]])

# =============================
# 6️⃣ 下載資料
# =============================
all_list = []
for row in tqdm(city_station, desc="Downloading hourly weather"):
    sid = row["station_id"]
    df = fetch_hourly_by_station(sid, START, END)
    if df.empty:
        continue

    out = pd.DataFrame({
        "ObsDate": df["datetime"].dt.date.astype(str),
        "city_id": row["city_id"],
        "ObsTime": df["datetime"].dt.strftime("%H:%M"),
        "StnPres": df.get("pres"),
        "Temperature": df.get("temp"),
        "RH": df.get("rhum"),
        "WS": df.get("wspd"),
        "Precp": df.get("prcp"),
    })
    out["typhoon"] = 0
    out["typhoon_name"] = ""
    all_list.append(out)

# 合併全部城市
result = pd.concat(all_list, ignore_index=True) if all_list else pd.DataFrame()

# =============================
# 7️⃣ 匯入颱風表（選擇性）
# =============================
typhoon_path = Path(TYPHOON_CSV)
if typhoon_path.exists() and not result.empty:
    ty = pd.read_csv(typhoon_path, dtype={"typhoon": int, "typhoon_name": str})
    ty["date"] = pd.to_datetime(ty["date"]).dt.date.astype(str)
    result = result.merge(
        ty.rename(columns={"date": "ObsDate"}),
        on="ObsDate", how="left", suffixes=("", "_y")
    )
    result["typhoon"] = result["typhoon_y"].fillna(result["typhoon"]).fillna(0).astype(int)
    result["typhoon_name"] = result["typhoon_name_y"].fillna(result["typhoon_name"]).fillna("")
    result.drop(columns=[c for c in result.columns if c.endswith("_y")], inplace=True)

# =============================
# 8️⃣ 輸出逐時資料
# =============================
if not result.empty:
    result.to_csv(OUT_HOURLY, index=False, encoding="utf-8-sig")
    print(f"\n✅ 已輸出逐時氣象資料：{OUT_HOURLY}（共 {len(result):,} 筆）")
else:
    print("\n⚠️ 沒有抓到任何資料，請檢查測站或時間設定")

# =============================
# 9️⃣ 產出每日聚合資料
# =============================
if not result.empty:
    daily = result.copy()
    daily["ObsDate"] = pd.to_datetime(daily["ObsDate"])
    daily = daily.groupby(["city_id", "ObsDate"], as_index=False).agg({
        "Temperature": ["mean", "max", "min"],
        "RH": "mean",
        "WS": "mean",
        "Precp": "sum"
    })
    daily.columns = ["city_id", "ObsDate", "Temp_Avg", "Temp_Max", "Temp_Min", "RH_Avg", "WS_Avg", "Precp_Sum"]
    daily["ObsDate"] = daily["ObsDate"].dt.date.astype(str)
    daily.to_csv(OUT_DAILY, index=False, encoding="utf-8-sig")
    print(f"✅ 已輸出每日氣象彙總：{OUT_DAILY}（共 {len(daily):,} 筆）")
