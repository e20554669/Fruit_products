import pandas as pd
import requests
from datetime import datetime, timedelta

# === 1️ API 網址 ===
url = "https://data.moa.gov.tw/Service/OpenData/FromM/FarmTransData.aspx"

# === 2️ 中英欄位對照表 ===
column_map = {
    "交易日期": "TransDate",
    "市場代號": "MarketCode",
    "市場名稱": "MarketName",
    "作物代號": "CropCode",
    "作物名稱": "CropName",
    "上價": "UpperPrice",
    "中價": "MiddlePrice",
    "下價": "LowerPrice",
    "平均價": "AveragePrice",
    "交易量": "TransVolume",
    "種類代碼": "TypeCode"
}

# === 3️ 作物代號對應 FinalName ===
code_to_name = {
    "72": "番茄", "I1": "木瓜", "51": "百香果", "T1": "西瓜", "N3": "李",
    "R1": "芒果", "L1": "枇杷", "H1": "文旦柚", "H2": "白柚", "Z4": "柿",
    "W1": "洋香瓜", "A1": "香蕉", "Y1": "桃", "45": "草莓", "J1": "荔枝",
    "D1": "楊桃", "41": "梅", "O10": "梨", "V1": "香瓜", "E1": "柳橙",
    "22": "蓮霧", "C1": "椪柑", "P1": "番石榴", "11": "可可椰子", "M3": "楊桃",
    "C5": "溫州蜜柑", "S1": "葡萄", "H4": "葡萄柚", "B2": "鳳梨",
    "Q1": "蓮霧", "G7": "龍眼", "K3": "棗", "F1": "蘋果",
    "X69": "釋迦", "31": "番茄枝"
}

# === 4️ 抓取資料函式 ===
def fetch_data(start, end, page_top=2000):
    """抓取 N05 水果資料（只保留指定作物代號）"""
    all_data = []
    valid_codes = set(code_to_name.keys())

    params = {
        "StartDate": f"{start.year - 1911:03d}.{start.month:02d}.{start.day:02d}",
        "EndDate": f"{end.year - 1911:03d}.{end.month:02d}.{end.day:02d}",
        "TcType": "N05",  # 水果
        "$top": page_top,
        "$skip": 0
    }

    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break

        # 篩選出指定作物代號
        filtered = [item for item in data if item.get("作物代號") in valid_codes]
        all_data.extend(filtered)

        if len(data) < page_top:
            break

        params["$skip"] += page_top  # 下一頁

    return all_data


# === 5️ 設定抓取期間 ===
start_date = datetime(2020, 1, 1)
end_date = datetime(2025, 10, 29)

# === 6️ 一天一天抓取 ===
all_records = []
cursor = start_date

while cursor <= end_date:
    print(f"抓取日期：{cursor.date()}")
    data = fetch_data(cursor, cursor)  # 每天抓一天
    if data:
        all_records.extend(data)
    cursor += timedelta(days=1)

print(f"\n 資料抓取完成，共 {len(all_records)} 筆")

# === 7️ 建立 DataFrame ===
df = pd.DataFrame(all_records)

# === 8️ 欄位轉英文 ===
df = df.rename(columns={col: column_map.get(col, col) for col in df.columns})

# === 9️ 民國 → 西元日期轉換 ===
def roc_to_ad(date_str):
    """將民國日期轉西元 (例：1090101 → 2020-01-01)"""
    if pd.isna(date_str):
        return None
    date_str = str(date_str).replace(".", "").replace("/", "")
    if len(date_str) != 7:
        return None
    try:
        y = int(date_str[:3]) + 1911
        m = int(date_str[3:5])
        d = int(date_str[5:7])
        return f"{y:04d}-{m:02d}-{d:02d}"
    except:
        return None

if "TransDate" in df.columns:
    df["TransDate"] = df["TransDate"].apply(roc_to_ad)
    df["TransDate"] = pd.to_datetime(df["TransDate"], errors="coerce")

# === 10 替換作物名稱 ===
df["CropName"] = df["CropCode"].map(code_to_name)

# === 11 輸出結果 ===
output_file = "moa_N05_35fruit.csv"
df.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"\n🎉 全部完成！共 {len(df)} 筆資料（35 種水果）")
print(f"💾 已輸出檔案：{output_file}")
