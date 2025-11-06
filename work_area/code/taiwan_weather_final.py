# -*- coding: utf-8 -*-
"""
台灣氣象資料擷取程式 - API 版本（推薦）
使用 CODiS API 直接獲取資料，不需要 Selenium
整合颱風警報資料
輸出逐時 (hourly) 和每日 (daily) 兩種格式
"""

import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import warnings
from urllib3.exceptions import InsecureRequestWarning
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
import os

warnings.filterwarnings("ignore", category=InsecureRequestWarning)

# =============================
# 參數設定
# =============================
START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2025, 10, 22)
OUTPUT_DIR = "codis_data"
OUTPUT_CSV_HOURLY = "taiwan_weather_hourly.csv"  # 逐時資料
OUTPUT_CSV_DAILY = "taiwan_weather_daily.csv"    # 每日資料

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 全台 22 縣市測站資料
STATIONS = [
    {"city_id": "TPE", "city_name": "台北市", "station_id": "466920"},
    {"city_id": "NTP", "city_name": "新北市", "station_id": "466880"},
    {"city_id": "TYN", "city_name": "桃園市", "station_id": "467050"},
    {"city_id": "HSC", "city_name": "新竹市", "station_id": "467080"},
    {"city_id": "HSQ", "city_name": "新竹縣", "station_id": "467571"},
    {"city_id": "MLI", "city_name": "苗栗縣", "station_id": "467300"},
    {"city_id": "TXG", "city_name": "台中市", "station_id": "467490"},
    {"city_id": "CHA", "city_name": "彰化縣", "station_id": "467530"},
    {"city_id": "NTO", "city_name": "南投縣", "station_id": "467650"},
    {"city_id": "YUN", "city_name": "雲林縣", "station_id": "467550"},
    {"city_id": "CYI", "city_name": "嘉義市", "station_id": "467410"},
    {"city_id": "CYQ", "city_name": "嘉義縣", "station_id": "467480"},
    {"city_id": "TNN", "city_name": "台南市", "station_id": "467420"},
    {"city_id": "KHH", "city_name": "高雄市", "station_id": "467440"},
    {"city_id": "PIF", "city_name": "屏東縣", "station_id": "467590"},
    {"city_id": "ILA", "city_name": "宜蘭縣", "station_id": "467060"},
    {"city_id": "HUA", "city_name": "花蓮縣", "station_id": "466990"},
    {"city_id": "TTT", "city_name": "台東縣", "station_id": "467540"},
    {"city_id": "PEN", "city_name": "澎湖縣", "station_id": "467350"},
    {"city_id": "KIN", "city_name": "金門縣", "station_id": "467110"},
    {"city_id": "LIE", "city_name": "連江縣", "station_id": "467990"},
    {"city_id": "KEE", "city_name": "基隆市", "station_id": "466940"},
]

# =============================
# 颱風警報處理
# =============================
class TyphoonWarningFetcher:
    """中央氣象署颱風警報資料擷取器"""
    
    def __init__(self):
        self.typhoon_api_url = "https://rdc28.cwa.gov.tw/TDB/public/warning_typhoon_list/get_warning_typhoon"
        self.typhoon_main_url = "https://rdc28.cwa.gov.tw/TDB/public/warning_typhoon_list/"
    
    def create_scraper_session(self):
        session = requests.Session()
        retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        return session
    
    def fetch_typhoon_warnings_for_year(self, year):
        """擷取指定年份的颱風警報資料"""
        print(f"[颱風警報] 查詢 {year} 年資料...")
        session = self.create_scraper_session()
        try:
            response = session.get(self.typhoon_main_url, timeout=30, verify=False)
            if response.status_code != 200:
                return []
            
            post_data = {"year": str(year)}
            session.headers.update({
                "Referer": self.typhoon_main_url,
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            })
            
            response = session.post(self.typhoon_api_url, data=post_data, timeout=30, verify=False)
            if response.status_code == 200:
                response_text = response.text.strip()
                if response_text.startswith("\ufeff"):
                    response_text = response_text[1:]
                try:
                    data = json.loads(response_text)
                    if isinstance(data, list) and data:
                        print(f"[颱風警報] {year} 年找到 {len(data)} 個颱風警報")
                        return data
                except:
                    return []
        except:
            return []
        return []
    
    def parse_typhoon_data_to_dates(self, typhoon_warnings):
        """將颱風警報資料解析為日期對應表"""
        date_warnings = {}
        for warning in typhoon_warnings:
            typhoon_name = warning.get("cht_name", "")
            sea_start = warning.get("sea_start_datetime", "")
            sea_end = warning.get("sea_end_datetime", "")
            
            if sea_start and sea_end:
                try:
                    start_dt = datetime.strptime(sea_start, "%Y-%m-%d %H:%M:%S")
                    end_dt = datetime.strptime(sea_end, "%Y-%m-%d %H:%M:%S")
                    current_date = start_dt.date()
                    end_date = end_dt.date()
                    
                    while current_date <= end_date:
                        date_str = current_date.strftime("%Y-%m-%d")
                        if date_str not in date_warnings:
                            date_warnings[date_str] = []
                        if typhoon_name not in date_warnings[date_str]:
                            date_warnings[date_str].append(typhoon_name)
                        current_date += timedelta(days=1)
                except:
                    continue
        return date_warnings
    
    def fetch_all_warnings(self, start_year, end_year):
        """擷取指定年份範圍內的所有颱風警報"""
        all_warnings = []
        for year in range(start_year, end_year + 1):
            warnings = self.fetch_typhoon_warnings_for_year(year)
            all_warnings.extend(warnings)
        date_warnings = self.parse_typhoon_data_to_dates(all_warnings)
        print(f"[颱風警報] 總計找到 {len(date_warnings)} 個有颱風警報的日期\n")
        return date_warnings

# =============================
# CODiS API 爬蟲（不需要 Selenium）
# =============================
class CODiSAPICrawler:
    """使用 CODiS API 直接獲取資料"""
    
    def __init__(self):
        self.api_url = "https://codis.cwa.gov.tw/api/station"
        self.session = requests.Session()
        
        # 設定重試策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # 設定 Headers
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Referer": "https://codis.cwa.gov.tw/StationData",
            "Origin": "https://codis.cwa.gov.tw"
        })
    
    def get_station_type(self, station_id):
        """根據測站代號判斷類型"""
        prefix = station_id[:2]
        if prefix == "46":
            return "cwb"  # 署屬有人站
        elif prefix == "C1":
            return "auto_C1"  # 自動雨量站
        elif prefix == "C0":
            return "auto_C0"  # 自動氣象站
        else:
            return "agr"  # 農業站
    
    def parse_hourly_data(self, data_dict):
        """解析逐時資料"""
        parsed = {}
        for key, value in data_dict.items():
            if key == 'DataTime':
                parsed['DataTime'] = value
                continue
            
            # 處理巢狀結構 (例如: {'Instantaneous': 999.8})
            if isinstance(value, dict):
                # 取第一個值（通常是 Instantaneous）
                if value:
                    first_key = list(value.keys())[0]
                    raw_value = value[first_key]
                    
                    # 處理特殊代碼：負值通常代表無效資料
                    if raw_value is not None and isinstance(raw_value, (int, float)):
                        if raw_value < 0:
                            parsed[key] = None  # 將負值轉為 None (空值)
                        else:
                            parsed[key] = raw_value
                    else:
                        parsed[key] = raw_value
                else:
                    parsed[key] = None
            else:
                # 處理非巢狀結構的特殊代碼
                if value is not None and isinstance(value, (int, float)):
                    if value < 0:
                        parsed[key] = None
                    else:
                        parsed[key] = value
                else:
                    parsed[key] = value
        
        return parsed
    
    def fetch_weather_data(self, station_id, target_date):
        """獲取指定測站的指定日期資料"""
        
        # 準備 API 參數
        date_str = target_date.strftime("%Y-%m-%d")
        date = f"{date_str}T00:00:00.000+08:00"
        start = f"{date_str}T00:00:00"
        end = f"{date_str}T23:59:59"
        
        stn_type = self.get_station_type(station_id)
        
        data = {
            "type": "report_date",
            "more": "",
            "item": "",
            "stn_type": stn_type,
            "date": date,
            "start": start,
            "end": end,
            "stn_ID": station_id,
        }
        
        try:
            # 第一次請求（建立 session）- 關閉 SSL 驗證
            self.session.get(self.api_url, verify=False, timeout=10)
            
            # POST 請求獲取資料 - 關閉 SSL 驗證
            response = self.session.post(self.api_url, data=data, verify=False, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                
                # 檢查回應結構
                if 'code' in result and result['code'] != 200:
                    # API 回傳錯誤
                    return None
                
                if 'data' in result and len(result['data']) > 0:
                    # 使用正確的 key: 'dts'
                    hourly_data = result['data'][0].get('dts', [])
                    
                    if not hourly_data:
                        return None
                    
                    # 解析每小時資料
                    parsed_records = []
                    for hour_dict in hourly_data:
                        parsed_record = self.parse_hourly_data(hour_dict)
                        parsed_records.append(parsed_record)
                    
                    # 轉換為 DataFrame
                    df = pd.DataFrame(parsed_records)
                    
                    return df
                else:
                    return None
            else:
                return None
                
        except Exception as e:
            return None

# =============================
# 資料處理函數
# =============================
def create_daily_summary(hourly_df):
    """將逐時資料彙總為每日資料"""
    
    # 確保 DataTime 是 datetime 格式
    hourly_df['DataTime'] = pd.to_datetime(hourly_df['DataTime'])
    
    # 提取日期
    hourly_df['Date'] = hourly_df['DataTime'].dt.date
    
    # 定義不同類型的欄位
    # 1. 需要計算平均值的欄位（瞬時狀態值）
    mean_cols = [
        'StationPressure', 'SeaLevelPressure', 'AirTemperature', 
        'DewPointTemperature', 'RelativeHumidity', 'WindSpeed',
        'Visibility', 'UVIndex', 'TotalCloudAmount',
        'SoilTemperatureAt0cm', 'SoilTemperatureAt5cm', 'SoilTemperatureAt10cm',
        'SoilTemperatureAt20cm', 'SoilTemperatureAt30cm', 'SoilTemperatureAt50cm',
        'SoilTemperatureAt100cm', 'GlobalSolarRadiation'
    ]
    
    # 2. 需要累積加總的欄位（累積量）
    sum_cols = [
        'Precipitation',           # 降水量：累積一天的總雨量
        'PrecipitationDuration',   # 降水延時：累積一天下雨的總時數
        'SunshineDuration'         # 日照時數：累積一天的日照總時數
    ]
    
    # 3. 需要取最大值的欄位
    max_cols = [
        'PeakGust',  # 最大陣風：當天最強的陣風
        'typhoon'    # 颱風警報：當天是否有颱風（1=有，0=無）
    ]
    
    # 建立分組欄位
    group_cols = ['Date', 'city_id', 'city_name', 'station_id']
    
    # 建立彙總規則
    agg_dict = {}
    
    # 平均值欄位
    for col in mean_cols:
        if col in hourly_df.columns:
            agg_dict[col] = 'mean'
    
    # 累積值欄位
    for col in sum_cols:
        if col in hourly_df.columns:
            agg_dict[col] = 'sum'
    
    # 最大值欄位
    for col in max_cols:
        if col in hourly_df.columns:
            agg_dict[col] = 'max'
    
    # 風向需要特殊處理（向量平均）
    if 'WindDirection' in hourly_df.columns:
        # 取眾數（最常出現的風向）
        agg_dict['WindDirection'] = lambda x: x.mode()[0] if len(x.mode()) > 0 else x.mean()
    
    # 颱風名稱取第一個非空值
    if 'typhoon_name' in hourly_df.columns:
        agg_dict['typhoon_name'] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ''
    
    # 執行分組彙總
    daily_df = hourly_df.groupby(group_cols).agg(agg_dict).reset_index()
    
    # 將 Date 轉回字串格式
    daily_df['Date'] = daily_df['Date'].astype(str)
    
    # 四捨五入到小數點後 1 位
    numeric_cols = daily_df.select_dtypes(include=['float64']).columns
    for col in numeric_cols:
        if col not in ['typhoon']:
            daily_df[col] = daily_df[col].round(1)
    
    return daily_df

# =============================
# 主流程
# =============================
def main():
    print("\n" + "="*70)
    print("🌤️  台灣氣象資料擷取程式（CODiS API + 颱風警報）")
    print("="*70 + "\n")
    
    # 步驟1：擷取颱風警報
    print("[步驟 1] 擷取颱風警報資料")
    print("-"*70)
    typhoon_fetcher = TyphoonWarningFetcher()
    typhoon_date_dict = typhoon_fetcher.fetch_all_warnings(START_DATE.year, END_DATE.year)
    
    # 步驟2：初始化 API 爬蟲
    print("[步驟 2] 初始化 API 爬蟲")
    print("-"*70)
    crawler = CODiSAPICrawler()
    print("✅ API 爬蟲初始化完成\n")
    
    # 步驟3：爬取資料
    print("[步驟 3] 爬取氣象資料")
    print("-"*70)
    
    all_data = []
    success_count = 0
    fail_count = 0
    
    # 生成日期範圍
    current_date = START_DATE
    dates = []
    while current_date <= END_DATE:
        dates.append(current_date)
        current_date += timedelta(days=1)
    
    # 爬取所有測站和日期
    total_tasks = len(STATIONS) * len(dates)
    
    with tqdm(total=total_tasks, desc="下載進度") as pbar:
        for station in STATIONS:
            for date in dates:
                pbar.set_description(f"{station['city_name']} {date.strftime('%Y-%m-%d')}")
                
                # 獲取資料
                df = crawler.fetch_weather_data(station['station_id'], date)
                
                if df is not None and not df.empty:
                    # 加入城市資訊
                    df['city_id'] = station['city_id']
                    df['city_name'] = station['city_name']
                    df['station_id'] = station['station_id']
                    
                    # 加入颱風資訊
                    date_str = date.strftime("%Y-%m-%d")
                    if date_str in typhoon_date_dict:
                        df['typhoon'] = 1
                        df['typhoon_name'] = ', '.join(typhoon_date_dict[date_str])
                    else:
                        df['typhoon'] = 0
                        df['typhoon_name'] = ''
                    
                    all_data.append(df)
                    success_count += 1
                else:
                    fail_count += 1
                
                pbar.update(1)
    
    # 步驟4：資料彙總與輸出
    print("\n[步驟 4] 資料彙總與輸出")
    print("-"*70)
    
    if all_data:
        # 合併所有逐時資料
        hourly_df = pd.concat(all_data, ignore_index=True)
        
        # 資料品質統計
        total_records = len(hourly_df)
        
        print(f"📊 逐時資料品質檢查:")
        numeric_cols = hourly_df.select_dtypes(include=['float64', 'int64']).columns
        null_summary = []
        for col in numeric_cols:
            if col not in ['typhoon']:  # 排除 typhoon 欄位
                null_count = hourly_df[col].isna().sum()
                null_pct = (null_count / total_records) * 100
                if null_count > 0:
                    null_summary.append(f"   - {col}: {null_count} 筆空值 ({null_pct:.1f}%)")
        
        if null_summary:
            for line in null_summary[:5]:  # 只顯示前5個
                print(line)
            if len(null_summary) > 5:
                print(f"   ... 還有 {len(null_summary) - 5} 個欄位有空值")
        else:
            print("   ✅ 所有欄位都無空值")
        
        # 輸出逐時資料
        hourly_output_path = os.path.join(OUTPUT_DIR, OUTPUT_CSV_HOURLY)
        hourly_df.to_csv(hourly_output_path, index=False, encoding='utf-8-sig')
        print(f"\n✅ 已輸出逐時資料：{hourly_output_path}")
        print(f"   總筆數：{len(hourly_df):,} 筆（每小時一筆）")
        print(f"   時間範圍：{hourly_df['DataTime'].min()} ~ {hourly_df['DataTime'].max()}")
        
        # 產生每日彙總資料
        print(f"\n📊 產生每日彙總資料...")
        daily_df = create_daily_summary(hourly_df)
        
        # 輸出每日資料
        daily_output_path = os.path.join(OUTPUT_DIR, OUTPUT_CSV_DAILY)
        daily_df.to_csv(daily_output_path, index=False, encoding='utf-8-sig')
        print(f"✅ 已輸出每日資料：{daily_output_path}")
        print(f"   總筆數：{len(daily_df):,} 筆（每日彙總）")
        print(f"   日期範圍：{daily_df['Date'].min()} ~ {daily_df['Date'].max()}")
        
        print(f"\n📋 資料欄位:")
        print(f"   逐時資料: {len(hourly_df.columns)} 個欄位")
        print(f"   每日資料: {len(daily_df.columns)} 個欄位")
        
        print(f"\n💡 每日資料彙總方式:")
        print(f"   📊 平均值 (Mean): 氣溫、氣壓、濕度、風速、能見度、土壤溫度等")
        print(f"   ➕ 累積值 (Sum): 降水量、降水延時、日照時數")
        print(f"   📈 最大值 (Max): 最大陣風、颱風警報")
        print(f"\n💡 其他說明:")
        print(f"   - 逐時資料: 原始資料，每小時一筆記錄")
        print(f"   - 空值 (NaN): 該時段測站未提供資料或儀器故障")
        print(f"   - 原始資料中的負值已自動轉換為空值")
    else:
        print("⚠️ 沒有成功下載任何資料")
    
    # 統計
    print("\n" + "="*70)
    print("📊 執行結果")
    print("="*70)
    print(f"✅ 成功: {success_count} 筆")
    print(f"❌ 失敗: {fail_count} 筆")
    print(f"📁 輸出目錄: {OUTPUT_DIR}")
    print(f"📄 逐時資料: {OUTPUT_CSV_HOURLY} (每小時一筆，共 24 筆/天)")
    print(f"📄 每日資料: {OUTPUT_CSV_DAILY} (每日彙總，共 1 筆/天)")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
