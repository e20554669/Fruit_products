import requests
import time
import datetime as dt
from dateutil.relativedelta import relativedelta
import csv
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# -----------------------------
# 設定
# -----------------------------
FARM_TRANS_URL = "https://data.moa.gov.tw/Service/OpenData/FromM/FarmTransData.aspx"
CROP_DICT_URL  = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=LC7YWlenhLuP"
OUTPUT_CSV     = "moa_vege_fruit_N05_20200101_20251019.csv"

USE_ROC = True        # 查詢時使用民國年格式
MAX_WORKERS = 10      # 多執行緒數量
SEGMENT_DAYS = 10     # 每段區間天數
RETRY_DELAY_BASE = 1  # 失敗重試延遲基礎秒數

# ✅ 設定時間範圍：2020/1/1 ~ 2025/10/19
start_date = dt.date(2020, 1, 1)
end_date   = dt.date(2025, 10, 19)

# -----------------------------
# SSL 完整繞過設定
# -----------------------------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
session = requests.Session()
session.verify = False

# -----------------------------
# 民國 ↔ 西元日期處理
# -----------------------------
def to_roc(date_obj: dt.date, use_roc=True) -> str:
    if use_roc:
        year = date_obj.year - 1911
    else:
        year = date_obj.year
    return f"{year:03d}.{date_obj.month:02d}.{date_obj.day:02d}"

def roc_to_western(date_str: str) -> str:
    if not date_str:
        return ""
    try:
        parts = date_str.split(".")
        if len(parts) != 3:
            return date_str
        roc_year = int(parts[0])
        western_year = roc_year + 1911
        return f"{western_year:04d}.{int(parts[1]):02d}.{int(parts[2]):02d}"
    except Exception:
        return date_str

print("📅 抓取期間：", start_date, "~", end_date)
print("目前使用查詢格式：", "民國" if USE_ROC else "西元")

# -----------------------------
# 分段時間產生器（10天為一批）
# -----------------------------
def period_windows(start_d: dt.date, end_d: dt.date, days=SEGMENT_DAYS):
    cursor = start_d
    while cursor <= end_d:
        period_start = cursor
        period_end = min(end_d, cursor + dt.timedelta(days=days - 1))
        yield period_start, period_end
        cursor = period_end + dt.timedelta(days=1)

# -----------------------------
# JSON 請求函式（含重試與延遲）
# -----------------------------
def get_json(url, params=None, max_retry=5, timeout=40):
    for i in range(max_retry):
        try:
            r = session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"⚠️ 第 {i+1} 次連線失敗：{e}")
            time.sleep(RETRY_DELAY_BASE * (i + 1))
    print("❌ 超過最大重試次數，放棄此次請求。")
    return []

# -----------------------------
# 抓作物代碼表
# -----------------------------
print("⏳ 嘗試抓取作物代碼表...")
crop_dict = get_json(CROP_DICT_URL)

if not crop_dict:
    print("❌ 無法取得作物代碼表，請稍後再試。")

def is_vege_or_fruit(rec: dict) -> bool:
    text = f"{rec.get('PLV1_NAME', '')}{rec.get('PLV2_NAME', '')}{rec.get('PLV3_NAME', '')}"
    return any(kw in text for kw in ["蔬菜", "果菜", "水果"])

def get_crop_code(rec: dict) -> str:
    return str(rec.get("CROP_UID", "")).strip()

def get_crop_name(rec: dict) -> str:
    return str(rec.get("CNAME", "")).strip()

vege_fruit_codes = set()
code_to_name = {}

for row in crop_dict:
    if is_vege_or_fruit(row):
        code = get_crop_code(row)
        name = get_crop_name(row)
        if code:
            vege_fruit_codes.add(code)
            if name:
                code_to_name[code] = name

print(f"✅ 蔬果品項數量：{len(vege_fruit_codes)}")
print(f"前幾個蔬果名稱預覽：{list(code_to_name.values())[:10]}")

# -----------------------------
# 資料正規化
# -----------------------------
OUTPUT_FIELDS = [
    "TransDate", "MarketCode", "MarketName", "CropCode", "CropName",
    "CategoryCode", "UpperPrice", "MiddlePrice", "LowerPrice",
    "AveragePrice", "TransVolume", "TransAmount", "Unit",
    "County", "Township", "UpdateTime"
]

def normalize_record(rec: dict) -> dict:
    def pick(*candidates):
        for c in candidates:
            if c in rec and rec[c] not in (None, ""):
                return rec[c]
        return ""
    d = {
        "TransDate": pick("TransDate", "交易日期"),
        "MarketCode": pick("MarketCode", "市場代號"),
        "MarketName": pick("MarketName", "市場名稱"),
        "CropCode": pick("CropCode", "作物代號"),
        "CropName": pick("CropName", "作物名稱"),
        "CategoryCode": pick("CategoryCode", "種類代碼", "TypeCode"),
        "UpperPrice": pick("UpperPrice", "上價"),
        "MiddlePrice": pick("MiddlePrice", "中價", "平均價"),
        "LowerPrice": pick("LowerPrice", "下價"),
        "AveragePrice": pick("AveragePrice", "平均價", "中價"),
        "TransVolume": pick("TransVolume", "交易量"),
        "TransAmount": pick("TransAmount", "交易金額"),
        "Unit": pick("Unit", "單位"),
        "County": pick("County", "縣市"),
        "Township": pick("Township", "鄉鎮"),
        "UpdateTime": pick("UpdateTime", "更新時間")
    }
    d["TransDate"] = roc_to_western(d["TransDate"])
    return d

# -----------------------------
# 單段抓取函式（僅 N05）
# -----------------------------
def fetch_period_block(period_start: dt.date, period_end: dt.date, page_top=2000):
    all_rows = []
    skip = 0
    print(f"🔍 抓取期間：{period_start} ~ {period_end} ({to_roc(period_start)} ~ {to_roc(period_end)})")

    while True:
        params = {
            "StartDate": to_roc(period_start, USE_ROC),
            "EndDate": to_roc(period_end, USE_ROC),
            "$top": page_top,
            "$skip": skip
        }
        data = get_json(FARM_TRANS_URL, params=params)
        if not isinstance(data, list) or not data:
            break

        matched = 0
        for rec in data:
            category_code = str(rec.get("CategoryCode", rec.get("種類代碼", ""))).strip()
            if category_code == "N05":  # ✅ 只抓 N05
                all_rows.append(normalize_record(rec))
                matched += 1

        print(f"✅ {period_start}~{period_end} 共 {len(data)} 筆，其中符合 {matched} 筆。")
        if len(data) < page_top:
            break
        skip += page_top

    return all_rows

# -----------------------------
# 多執行緒處理 + 進度顯示
# -----------------------------
progress_lock = Lock()
completed_tasks = 0

def process_period(p_start, p_end, total_tasks):
    global completed_tasks
    rows = fetch_period_block(p_start, p_end)
    if rows:
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
            for r in rows:
                writer.writerow(r)
        print(f"[{p_start} ~ {p_end}] ✅ 寫入 {len(rows)} 筆")
    else:
        print(f"[{p_start} ~ {p_end}] ⚠️ 無資料")

    # 更新進度
    with progress_lock:
        completed_tasks += 1
        percent = completed_tasks / total_tasks * 100
        print(f"📊 進度：{completed_tasks}/{total_tasks} ({percent:.1f}%)")

    return len(rows)

# -----------------------------
# 主執行區塊
# -----------------------------
period_list = list(period_windows(start_date, end_date, days=SEGMENT_DAYS))
total_tasks = len(period_list)

# 寫入 CSV 標頭
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
    writer.writeheader()

total_cnt = 0
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(process_period, p_start, p_end, total_tasks): (p_start, p_end)
               for p_start, p_end in period_list}
    for future in as_completed(futures):
        try:
            total_cnt += future.result()
        except Exception as e:
            print(f"❌ 子執行緒錯誤：{e}")

print(f"\n🎉 完成！總筆數：{total_cnt}，輸出檔：{OUTPUT_CSV}")
