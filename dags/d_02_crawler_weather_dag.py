# 套件載入及抓取日期、因子的設定
import pandas as pd
import numpy as np
import requests
import json
import warnings
import re
from datetime import datetime, timedelta
from urllib3.exceptions import InsecureRequestWarning
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pymysql

# Airflow 專用模組
import pendulum
from airflow.decorators import dag, task

# 建立測站資料對照表
stations = [
    # 彰化縣
    {
        "city_id": "CHA",
        "city_name": "彰化縣",
        "station_id": "C0G720",
        "Altitude": "Low",
    },
    # 新竹市
    {
        "city_id": "HSC",
        "city_name": "新竹市",
        "station_id": "C0D660",
        "Altitude": "Low",
    },
    # # 新竹縣
    {
        "city_id": "HSQ",
        "city_name": "新竹縣",
        "station_id": "C0D650",
        "Altitude": "Low",
    },
    {
        "city_id": "HSQ",
        "city_name": "新竹縣",
        "station_id": "C0D360",
        "Altitude": "High",
    },
    {
        "city_id": "HSQ",
        "city_name": "新竹縣",
        "station_id": "C0D550",
        "Altitude": "High",
    },
    {
        "city_id": "HSQ",
        "city_name": "新竹縣",
        "station_id": "C0D750",
        "Altitude": "High",
    },
    {
        "city_id": "HSQ",
        "city_name": "新竹縣",
        "station_id": "C0D760",
        "Altitude": "High",
    },
    # 嘉義市
    {
        "city_id": "CYI",
        "city_name": "嘉義市",
        "station_id": "C0M730",
        "Altitude": "Low",
    },
    # 嘉義縣
    {
        "city_id": "CYQ",
        "city_name": "嘉義縣",
        "station_id": "C0M760",
        "Altitude": "Low",
    },
    {
        "city_id": "CYQ",
        "city_name": "嘉義縣",
        "station_id": "C0M530",
        "Altitude": "High",
    },
    {
        "city_id": "CYQ",
        "city_name": "嘉義縣",
        "station_id": "C0M810",
        "Altitude": "High",
    },
    {
        "city_id": "CYQ",
        "city_name": "嘉義縣",
        "station_id": "C0M820",
        "Altitude": "High",
    },
    {
        "city_id": "CYQ",
        "city_name": "嘉義縣",
        "station_id": "C0M850",
        "Altitude": "High",
    },
    {
        "city_id": "CYQ",
        "city_name": "嘉義縣",
        "station_id": "C0M860",
        "Altitude": "High",
    },
    # 花蓮縣
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0Z061",
        "Altitude": "Low",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0T820",
        "Altitude": "High",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0T9B0",
        "Altitude": "High",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0T9G0",
        "Altitude": "High",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0T9H0",
        "Altitude": "High",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0TA40",
        "Altitude": "High",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0TA80",
        "Altitude": "High",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0Z050",
        "Altitude": "High",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0Z220",
        "Altitude": "High",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0Z250",
        "Altitude": "High",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0Z290",
        "Altitude": "High",
    },
    {
        "city_id": "HUA",
        "city_name": "花蓮縣",
        "station_id": "C0Z320",
        "Altitude": "High",
    },
    # 宜蘭縣
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0U940",
        "Altitude": "Low",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0U520",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0U710",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0U720",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0U950",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0U960",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0U980",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UA10",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UA20",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UA30",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UA40",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UA50",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UA60",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UA70",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UB60",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UB70",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UB80",
        "Altitude": "High",
    },
    {
        "city_id": "ILA",
        "city_name": "宜蘭縣",
        "station_id": "C0UB90",
        "Altitude": "High",
    },
    # 基隆市
    {
        "city_id": "KEE",
        "city_name": "基隆市",
        "station_id": "C0B010",
        "Altitude": "Low",
    },
    # 金門縣
    {
        "city_id": "KIN",
        "city_name": "金門縣",
        "station_id": "C0W150",
        "Altitude": "Low",
    },
    # 高雄市
    {
        "city_id": "KHH",
        "city_name": "高雄市",
        "station_id": "C0V740",
        "Altitude": "Low",
    },
    {
        "city_id": "KHH",
        "city_name": "高雄市",
        "station_id": "C0V210",
        "Altitude": "High",
    },
    # 連江縣
    {
        "city_id": "LIE",
        "city_name": "連江縣",
        "station_id": "C0W110",
        "Altitude": "Low",
    },
    # 苗栗縣
    {
        "city_id": "MLI",
        "city_name": "苗栗縣",
        "station_id": "C0E750",
        "Altitude": "Low",
    },
    {
        "city_id": "MLI",
        "city_name": "苗栗縣",
        "station_id": "C0E610",
        "Altitude": "High",
    },
    {
        "city_id": "MLI",
        "city_name": "苗栗縣",
        "station_id": "C0E940",
        "Altitude": "High",
    },
    {
        "city_id": "MLI",
        "city_name": "苗栗縣",
        "station_id": "C0E950",
        "Altitude": "High",
    },
    {
        "city_id": "MLI",
        "city_name": "苗栗縣",
        "station_id": "C0E960",
        "Altitude": "High",
    },
    # 南投縣
    {
        "city_id": "NTO",
        "city_name": "南投縣",
        "station_id": "C0H890",
        "Altitude": "Low",
    },
    {
        "city_id": "NTO",
        "city_name": "南投縣",
        "station_id": "C0H9A0",
        "Altitude": "High",
    },
    {
        "city_id": "NTO",
        "city_name": "南投縣",
        "station_id": "C0I010",
        "Altitude": "High",
    },
    {
        "city_id": "NTO",
        "city_name": "南投縣",
        "station_id": "C0I080",
        "Altitude": "High",
    },
    {
        "city_id": "NTO",
        "city_name": "南投縣",
        "station_id": "C0I370",
        "Altitude": "High",
    },
    {
        "city_id": "NTO",
        "city_name": "南投縣",
        "station_id": "C0I390",
        "Altitude": "High",
    },
    {
        "city_id": "NTO",
        "city_name": "南投縣",
        "station_id": "C0I480",
        "Altitude": "High",
    },
    {
        "city_id": "NTO",
        "city_name": "南投縣",
        "station_id": "C0I490",
        "Altitude": "High",
    },
    # 新北市
    {
        "city_id": "NTP",
        "city_name": "新北市",
        "station_id": "C0AC60",
        "Altitude": "Low",
    },
    {
        "city_id": "NTP",
        "city_name": "新北市",
        "station_id": "C0A870",
        "Altitude": "High",
    },
    {
        "city_id": "NTP",
        "city_name": "新北市",
        "station_id": "C0AH30",
        "Altitude": "High",
    },
    {
        "city_id": "NTP",
        "city_name": "新北市",
        "station_id": "C0AH90",
        "Altitude": "High",
    },
    {
        "city_id": "NTP",
        "city_name": "新北市",
        "station_id": "C0AK30",
        "Altitude": "High",
    },
    # 澎湖縣
    {
        "city_id": "PEN",
        "city_name": "澎湖縣",
        "station_id": "C0W130",
        "Altitude": "Low",
    },
    # 屏東縣
    {
        "city_id": "PIF",
        "city_name": "屏東縣",
        "station_id": "C0R590",
        "Altitude": "Low",
    },
    {
        "city_id": "PIF",
        "city_name": "屏東縣",
        "station_id": "C0R100",
        "Altitude": "High",
    },
    {
        "city_id": "PIF",
        "city_name": "屏東縣",
        "station_id": "C0R130",
        "Altitude": "High",
    },
    {
        "city_id": "PIF",
        "city_name": "屏東縣",
        "station_id": "C0R140",
        "Altitude": "High",
    },
    {
        "city_id": "PIF",
        "city_name": "屏東縣",
        "station_id": "C0R440",
        "Altitude": "High",
    },
    {
        "city_id": "PIF",
        "city_name": "屏東縣",
        "station_id": "C0R600",
        "Altitude": "High",
    },
    {
        "city_id": "PIF",
        "city_name": "屏東縣",
        "station_id": "C0R750",
        "Altitude": "High",
    },
    {
        "city_id": "PIF",
        "city_name": "屏東縣",
        "station_id": "C0R820",
        "Altitude": "High",
    },
    {
        "city_id": "PIF",
        "city_name": "屏東縣",
        "station_id": "C0R840",
        "Altitude": "High",
    },
    # 臺中市
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0F850",
        "Altitude": "Low",
    },
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0F0C0",
        "Altitude": "High",
    },
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0F9V0",
        "Altitude": "High",
    },
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0FA60",
        "Altitude": "High",
    },
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0FA70",
        "Altitude": "High",
    },
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0FA80",
        "Altitude": "High",
    },
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0FB00",
        "Altitude": "High",
    },
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0FB10",
        "Altitude": "High",
    },
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0FB20",
        "Altitude": "High",
    },
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0FB30",
        "Altitude": "High",
    },
    {
        "city_id": "TXG",
        "city_name": "臺中市",
        "station_id": "C0FB40",
        "Altitude": "High",
    },
    # 臺北市
    {
        "city_id": "TPE",
        "city_name": "臺北市",
        "station_id": "C0A980",
        "Altitude": "Low",
    },
    {
        "city_id": "TPE",
        "city_name": "臺北市",
        "station_id": "C0AC40",
        "Altitude": "High",
    },
    # 臺南市
    {
        "city_id": "TNN",
        "city_name": "臺南市",
        "station_id": "C0O900",
        "Altitude": "Low",
    },
    # 臺東縣
    {
        "city_id": "TTT",
        "city_name": "臺東縣",
        "station_id": "C0S890",
        "Altitude": "Low",
    },
    {
        "city_id": "TTT",
        "city_name": "臺東縣",
        "station_id": "C0S660",
        "Altitude": "High",
    },
    {
        "city_id": "TTT",
        "city_name": "臺東縣",
        "station_id": "C0S690",
        "Altitude": "High",
    },
    {
        "city_id": "TTT",
        "city_name": "臺東縣",
        "station_id": "C0S700",
        "Altitude": "High",
    },
    {
        "city_id": "TTT",
        "city_name": "臺東縣",
        "station_id": "C0S750",
        "Altitude": "High",
    },
    {
        "city_id": "TTT",
        "city_name": "臺東縣",
        "station_id": "C0S760",
        "Altitude": "High",
    },
    {
        "city_id": "TTT",
        "city_name": "臺東縣",
        "station_id": "C0S980",
        "Altitude": "High",
    },
    {
        "city_id": "TTT",
        "city_name": "臺東縣",
        "station_id": "C0SA20",
        "Altitude": "High",
    },
    # 桃園市
    {
        "city_id": "TYN",
        "city_name": "桃園市",
        "station_id": "C0C630",
        "Altitude": "Low",
    },
    {
        "city_id": "TYN",
        "city_name": "桃園市",
        "station_id": "C0C460",
        "Altitude": "High",
    },
    {
        "city_id": "TYN",
        "city_name": "桃園市",
        "station_id": "C0C790",
        "Altitude": "High",
    },
    {
        "city_id": "TYN",
        "city_name": "桃園市",
        "station_id": "C0C800",
        "Altitude": "High",
    },
    # 雲林縣
    {
        "city_id": "YUN",
        "city_name": "雲林縣",
        "station_id": "C0K330",
        "Altitude": "Low",
    },
]

# 設定過濾規則，不顯示SSL認證憑證
warnings.filterwarnings("ignore", category=InsecureRequestWarning)


# ===颱風警報處理
class TyphoonWarningFetcher:
    """中央氣象署颱風警報資料擷取器"""

    def __init__(self):
        self.typhoon_api_url = "https://rdc28.cwa.gov.tw/TDB/public/warning_typhoon_list/get_warning_typhoon"
        self.typhoon_main_url = (
            "https://rdc28.cwa.gov.tw/TDB/public/warning_typhoon_list/"
        )

    def create_scraper_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )
        return session

    def fetch_typhoon_warnings_for_year(self, year):
        session = self.create_scraper_session()
        try:
            session.get(self.typhoon_main_url, timeout=30, verify=False)
            post_data = {"year": str(year)}
            session.headers.update(
                {
                    "Referer": self.typhoon_main_url,
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                }
            )
            response = session.post(
                self.typhoon_api_url, data=post_data, timeout=30, verify=False
            )
            if response.status_code == 200:
                text = response.text.strip().lstrip("\ufeff")
                try:
                    data = json.loads(text)
                    if isinstance(data, list) and data:
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
                    current = start_dt.date()
                    while current <= end_dt.date():
                        d_str = current.strftime("%Y-%m-%d")
                        if d_str not in date_warnings:
                            date_warnings[d_str] = []
                        if typhoon_name not in date_warnings[d_str]:
                            date_warnings[d_str].append(typhoon_name)
                        current += timedelta(days=1)
                except:
                    continue
        return date_warnings

    def fetch_all_warnings(self, start_year, end_year):
        """擷取指定年份範圍內的所有颱風警報"""
        all_warnings = []
        for year in range(start_year, end_year + 1):
            all_warnings.extend(self.fetch_typhoon_warnings_for_year(year))
        return self.parse_typhoon_data_to_dates(all_warnings)


# ===建立爬蟲程式的物件
class CODiSAPICrawler:
    """使用 CODiS API 直接獲取資料"""

    def __init__(self):
        self.api_url = "https://codis.cwa.gov.tw/api/station"
        self.session = requests.Session()
        retry = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
                "Referer": "https://codis.cwa.gov.tw/StationData",
                "Origin": "https://codis.cwa.gov.tw",
            }
        )

    def get_station_type(self, station_id):
        """根據測站代號判斷類型"""
        if station_id.startswith("46"):
            return "cwb"
        elif station_id.startswith("C1"):
            return "auto_C1"
        elif station_id.startswith("C0"):
            return "auto_C0"
        return "agr"

    def parse_hourly_data(self, data_dict):
        """解析逐時資料"""
        parsed = {}
        for key, value in data_dict.items():
            if key == "DataTime":
                parsed["DataTime"] = value
                continue
            val_to_store = None
            if isinstance(value, dict) and value:
                first_key = list(value.keys())[0]
                val_to_store = value[first_key]
            else:
                val_to_store = value
            if isinstance(val_to_store, (int, float)) and val_to_store < 0:
                parsed[key] = None
            else:
                parsed[key] = val_to_store
        return parsed

    def fetch_weather_data(self, station_id, target_date):
        """獲取指定測站的指定日期資料"""
        date_str = target_date.strftime("%Y-%m-%d")
        stn_type = self.get_station_type(station_id)
        data = {
            "type": "report_date",
            "stn_type": stn_type,
            "date": f"{date_str}T00:00:00.000+08:00",
            "start": f"{date_str}T00:00:00",
            "end": f"{date_str}T23:59:59",
            "stn_ID": station_id,
        }
        try:
            self.session.get(self.api_url, verify=False, timeout=10)
            resp = self.session.post(self.api_url, data=data, verify=False, timeout=30)
            if resp.status_code == 200:
                res = resp.json()
                if res.get("code") == 200 and res.get("data"):
                    dts = res["data"][0].get("dts", [])
                    if not dts:
                        return None
                    return pd.DataFrame([self.parse_hourly_data(x) for x in dts])
        except:
            pass
        return None


# ===資料處理函數
def create_daily_summary(hourly_df):
    """每日資料彙總邏輯"""
    hourly_df["DataTime"] = pd.to_datetime(hourly_df["DataTime"])
    hourly_df["Date"] = hourly_df["DataTime"].dt.date

    mean_cols = [
        "StationPressure",
        "SeaLevelPressure",
        "AirTemperature",
        "DewPointTemperature",
        "RelativeHumidity",
        "WindSpeed",
        "Visibility",
        "UVIndex",
        "TotalCloudAmount",
        "SoilTemperatureAt0cm",
        "SoilTemperatureAt5cm",
        "SoilTemperatureAt10cm",
        "SoilTemperatureAt20cm",
        "SoilTemperatureAt30cm",
        "SoilTemperatureAt50cm",
        "SoilTemperatureAt100cm",
        "GlobalSolarRadiation",
    ]
    sum_cols = ["Precipitation", "PrecipitationDuration", "SunshineDuration"]
    max_cols = ["PeakGust", "is_typhoon"]

    # 建立分組欄位
    group_cols = ["Date", "city_id", "city_name", "station_id", "Altitude"]

    agg_dict = {}
    all_targets = mean_cols + sum_cols + max_cols
    if "WindDirection" in hourly_df.columns:
        agg_dict["WindDirection"] = lambda x: (
            x.mode()[0] if len(x.mode()) > 0 else x.mean()
        )
    if "typhoon_name" in hourly_df.columns:
        agg_dict["typhoon_name"] = lambda x: (
            x.dropna().iloc[0] if len(x.dropna()) > 0 else ""
        )

    # 將每個欄位賦予聚合函數
    for col in all_targets:
        if col in hourly_df.columns:
            if col in mean_cols: agg_dict[col] = "mean"
            elif col in sum_cols: agg_dict[col] = "sum"
            elif col in max_cols: agg_dict[col] = "max"

    # 第一次分組
    daily_df = hourly_df.groupby(group_cols).agg(agg_dict).reset_index()

    # 移除 station_id，準備進行第二次合併
    daily_df = daily_df.drop(columns=["station_id"])
    group_cols.remove("station_id")

    agg_dict = {}  # 重新初始化聚合用字典 
    numeric_cols = mean_cols + sum_cols + max_cols  # 建立一個清單用以存放數值欄位
    for col in numeric_cols:
        if col in daily_df.columns and col not in group_cols:
            # 第二階段全部用平均 (Mean)
            agg_dict[col] = "mean"

    # 風向和颱風名稱依舊特殊處理 (取眾數或第一個)
    if "WindDirection" in daily_df.columns:
         agg_dict["WindDirection"] = lambda x: x.mode()[0] if len(x.mode()) > 0 else x.mean()
    if "typhoon_name" in daily_df.columns:
         agg_dict["typhoon_name"] = lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else ""

    # 第二次分組
    daily_df = daily_df.groupby(group_cols).agg(agg_dict).reset_index()

    daily_df["Date"] = daily_df["Date"].astype(str)

    for col in daily_df.select_dtypes(include=["float64"]).columns:
        if col != "is_typhoon":
            daily_df[col] = daily_df[col].round(1)

    if "city_name" in daily_df.columns:
        daily_df = daily_df.drop(columns=["city_name"])

    return daily_df

def camel_to_snake(name):
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

#  開始處理 Airflow DAG

@dag(
    dag_id="d_02_crawler_weather_dag",
    description="每日抓取台灣各縣市天氣資料",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2020, 1, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["weather", "etl", "mysql", "pymysql"],
    is_paused_upon_creation=False,
)
def weather_etl_dag_pymysql():

    @task
    def extract_weather_data():
        """extract"""
        print("--- [Task 1] Extract: 開始抓取氣象資料 ---")
        st = datetime.today().date() - timedelta(days=3)
        et = datetime.today().date() - timedelta(days=1)
        print(f"抓取範圍: {st}~{et}")

        # 產生日期列表
        dates = []
        current = st
        while current <= et:
            dates.append(current)
            current += timedelta(days=1)

        # 呼叫測站列表
        global station

        # 初始化颱風警報爬蟲程式
        typhoon_fetcher = TyphoonWarningFetcher()
        typhoon_date_dict = typhoon_fetcher.fetch_all_warnings(
            st.year, et.year
        )

        # 初始化天氣資料爬蟲程式
        crawler = CODiSAPICrawler()
        all_data_list = []

        for date in dates:
            print(f"正在抓取日期: {date}")
            for station in stations:
                df = crawler.fetch_weather_data(station["station_id"], date)
                if df is not None and not df.empty:
                    df["city_id"] = station["city_id"]
                    df["city_name"] = station["city_name"]
                    df["station_id"] = station["station_id"]
                    df["Altitude"] = station["Altitude"]

                    date_str = date.strftime("%Y-%m-%d")
                    if date_str in typhoon_date_dict:
                        df["is_typhoon"] = 1
                        df["typhoon_name"] = ", ".join(typhoon_date_dict[date_str])
                    else:
                        df["is_typhoon"] = 0
                        df["typhoon_name"] = None

                    # 轉換為 dict (JSON serializable) 以便透過 XCom 傳遞
                    all_data_list.extend(df.to_dict(orient='records'))

        if not all_data_list:
            print("今日無資料可抓取。")
            return []

        print(f"抓取完成，共 {len(all_data_list)} 筆逐時資料。")
        return all_data_list # 回傳給下一個 Task

    @task
    def transform_weather_data(raw_data: list):
        """transform"""
        print("--- [Task 2] Transform: 開始處理資料 ---")
        if not raw_data:
            print("無資料可處理。")
            return []

        # 從 XCom (List of Dicts) 還原回 DataFrame
        hourly_df = pd.DataFrame(raw_data)
        
        # 執行彙總邏輯
        daily_df = create_daily_summary(hourly_df)
        
        # 欄位轉 snake_case
        daily_df.columns = daily_df.columns.map(camel_to_snake)

        # 只擷取db裡有的欄位
        db_columns = [
            "date", 
            "city_id", 
            "altitude", 
            "station_pressure", 
            "air_temperature", 
            "relative_humidity", 
            "wind_speed", 
            "precipitation", 
            "is_typhoon", 
            "typhoon_name"
        ]
        
        daily_df = daily_df[db_columns]

        # MySQL看不懂Nan，故要將其轉為None
        daily_df = daily_df.replace({np.nan: None})
        
        print(f"彙總完成，共 {len(daily_df)} 筆每日資料。")
        
        # 再次轉為 Dict List 回傳給 Load Task
        return daily_df.to_dict(orient='records')

    @task
    def load_weather_data(transformed_data: list):
        """load"""
        print("--- [Task 3] Load: 開始寫入資料庫 ---")
        if not transformed_data:
            print("無資料可寫入。")
            return
        
        # mysql server
        host = "104.199.220.12"
        port = 3306
        user = "tjr103-team02"
        passwd = "password"
        db = "tjr103-team02"
        charset = "utf8mb4"
        table = "weather"

        conn = None
        cursor = None
        try:
            print("連接資料庫中...")
            conn = pymysql.connect(
                host=host,
                port=port,
                user=user,
                passwd=passwd,
                db=db,
                charset=charset,
                cursorclass=pymysql.cursors.DictCursor,
            )
            cursor = conn.cursor()
            print("成功連線！")

            # 從 List of Dicts 準備寫入資料
            first_record = transformed_data[0]  # 第一筆資料為欄位名稱
            cols = list(first_record.keys()) # 取出欄位名稱
            cols_str = ", ".join([f"`{c}`" for c in cols])  # 加入反引號避免關鍵字衝突
            placeholders = ", ".join(["%s"] * len(cols))  # 依照欄位數目製作(%s, %s...) 

            # 使用 INSERT IGNORE 來避免重複主鍵錯誤
            sql = f"INSERT IGNORE INTO `{table}` ({cols_str}) VALUES ({placeholders})"

            # 將dict轉換為tuple，才能被excutemany使用
            values = []
            for record in transformed_data:
                values.append(tuple(record[c] for c in cols))

            print(f"開始批次寫入 {len(values)} 筆資料...")
            cursor.executemany(sql, values)
            conn.commit()

            print(f"資料庫寫入成功！(影響行數: {cursor.rowcount})")

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"資料庫寫入失敗: {e}")
            raise  # 讓 Airflow 知道任務失敗
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # 設定相依性
    raw_data = extract_weather_data()
    clean_data = transform_weather_data(raw_data)
    load_weather_data(clean_data)

weather_etl_dag_pymysql()
