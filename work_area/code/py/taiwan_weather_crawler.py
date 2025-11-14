# 套件載入及抓取日期、因子的設定
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import warnings
from urllib3.exceptions import InsecureRequestWarning
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
import re
from pathlib import Path

# 設定過濾規則，不顯示SSL認證憑證
warnings.filterwarnings("ignore", category=InsecureRequestWarning)

# 設定抓取日期
# st = datetime(2023, 1, 1)
# et = datetime(2023, 1, 6)
st = datetime.today().date() - timedelta(days=1)
et = datetime.today().date() - timedelta(days=1)
# 生成資料夾的名稱
output_dir = "weather_data"


def find_project_root(anchor_file=".gitignore"):
    """從當前目錄開始向上尋找，直到找到 .gitignore 所在的目錄，即為專案根目錄"""
    current_path = Path.cwd()  # 從當前目錄開始

    # 迴圈向上尋找，直到檔案系統的根目錄
    while current_path != current_path.parent:
        if (current_path / anchor_file).exists():
            return current_path
        current_path = current_path.parent

    # (最後再檢查一次根目錄)
    if (current_path / anchor_file).exists():
        return current_path

    # 如果一路找到了檔案系統的根目錄都沒找到...
    raise FileNotFoundError(
        f"找不到錨點檔案 '{anchor_file}'。 "
        f"請確認您是在專案內部執行，或 'anchor_file' 名稱正確。"
    )


try:
    # 執行 'find_project_root' (現在預設會尋找 .gitignore)
    project_root = find_project_root()
except FileNotFoundError:
    print("警告: 找不到 '.gitignore' 錨點檔案。")
    print("將 fallback (退回) 使用當前工作目錄作為根目錄。")
    project_root = Path.cwd()
output_dir_path = Path(project_root, "work_area/data", output_dir)
Path(output_dir_path).mkdir(parents=True, exist_ok=True)

# 建立日期的字串，直接加到檔名中
data_period = f"{st.strftime("%Y%m%d")}-{et.strftime("%Y%m%d")}"
# 逐時資料的檔名
output_hour = f"taiwan_weather_hourly_{data_period}.csv"
# 每日資料的檔名
output_daily = f"taiwan_weather_daily_{data_period}.csv"
# 建立一個欲抓取的天氣因子的列表
weather_factor = [
    "StationPressure",
    "AirTemperature",
    "RelativeHumidity",
    "WindSpeed",
    "Precipitation",
    "is_typhoon",
    "typhoon_name",
]

# %%
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

# %%
# 颱風警報處理


class TyphoonWarningFetcher:
    """中央氣象署颱風警報資料擷取器"""

    def __init__(self):
        self.typhoon_api_url = (
            "https://rdc28.cwa.gov.tw/TDB/public/warning_typhoon_list/"
            "get_warning_typhoon"
        )
        self.typhoon_main_url = (
            "https://rdc28.cwa.gov.tw/TDB/public/warning_typhoon_list/"
        )

    def create_scraper_session(self):
        """建立爬蟲請求"""
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
        """擷取指定年份的颱風警報資料"""
        print(f"[颱風警報] 查詢 {year} 年資料...")
        session = self.create_scraper_session()
        try:
            response = session.get(self.typhoon_main_url, timeout=30, verify=False)
            if response.status_code != 200:
                return []

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


# %%
# 建立爬蟲程式的物件


class CODiSAPICrawler:
    """使用 CODiS API 直接獲取資料"""

    def __init__(self):
        self.api_url = "https://codis.cwa.gov.tw/api/station"
        self.session = requests.Session()

        # 設定重試策略
        retry_strategy = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # 設定 Headers
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
        prefix = station_id[:2]  # 比對station_id的前兩碼
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
            """以下為json的部分檔案結構
            {
            "code": 200,
            "message": "",
            "data": [
                {
                "StationID": "C0G720",
                "dts": [
                    {
                    "DataTime": "2025-11-01T01:00:00",
                    "StationPressure": {
                        "Instantaneous": 1009.3
                    },
                    "AirTemperature": {
                        "Instantaneous": 21.9
                    },
                    "RelativeHumidity": {
                        "Instantaneous": 79
                    },
                    "WindSpeed": {
                        "Mean": 1.6
                    },
                    "WindDirection": {
                        "Mean": 5
                    },
            ...
                "count": 24
            }
            }
            """
            if key == "DataTime":
                parsed["DataTime"] = value  # Datetime的value是string，所以直接複製
                continue

            # 處理巢狀結構 (例如: {'Instantaneous': 999.8})
            if isinstance(value, dict):  # 處理value是dict的結構
                # 如果是dict，則取第一個值（通常是 Instantaneous）
                if value:
                    first_key = list(value.keys())[0]  # 取得key值
                    raw_value = value[first_key]  # 使用得到的key取value值

                    # 處理特殊代碼：負值通常代表無效資料
                    if raw_value is not None and isinstance(raw_value, (int, float)):
                        if raw_value < 0:
                            parsed[key] = None  # 將負值轉為 None (空值)
                        else:
                            parsed[key] = raw_value  # 如果是正值就直接複製
                    else:
                        parsed[key] = raw_value
                # 如果是空dict，也要變成None
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
        date_str = target_date.strftime("%Y-%m-%d")  # 格式化日期
        date = f"{date_str}T00:00:00.000+08:00"  # 產生 API 需要的第一種日期格式（包含時區）
        start = f"{date_str}T00:00:00"  # 產生 API 需要的第二種日期格式（開始時間）
        end = f"{date_str}T23:59:59"  # 產生 API 需要的第三種日期格式（結束時間）

        stn_type = self.get_station_type(station_id)  # 取得站別的種類

        # 設定post請求的Payload
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
            response = self.session.post(
                self.api_url, data=data, verify=False, timeout=30
            )

            if response.status_code == 200:
                result = response.json()

                # 檢查回應結構
                if "code" in result and result["code"] != 200:
                    # API 回傳錯誤
                    return None

                if "data" in result and len(result["data"]) > 0:
                    # 使用正確的 key: 'dts' -> dts的value存放各項天氣資料
                    hourly_data = result["data"][0].get("dts", [])
                    # 如果hourly_data 為空值則回傳None
                    if not hourly_data:
                        return None

                    # 解析每小時資料
                    parsed_records = []  # 建立一個list來存放逐時資料
                    for hour_dict in hourly_data:
                        parsed_record = self.parse_hourly_data(
                            hour_dict
                        )  # 呼叫函式解析資料
                        parsed_records.append(parsed_record)  # 將資料放入list

                    # 轉換為 DataFrame
                    df = pd.DataFrame(parsed_records)

                    return df
                else:
                    return None
            else:
                return None

        except Exception as e:
            return None


# %%
# 資料處理函數


def create_daily_summary(hourly_df):
    """將逐時資料彙總為每日資料"""

    # 確保 DataTime 是 datetime 格式
    hourly_df["DataTime"] = pd.to_datetime(hourly_df["DataTime"])

    # 提取日期
    hourly_df["Date"] = hourly_df["DataTime"].dt.date

    # 定義不同類型的欄位
    # 需要計算平均值的欄位（瞬時狀態值）
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

    # 需要累積加總的欄位（累積量）
    sum_cols = [
        "Precipitation",  # 降水量：累積一天的總雨量
        "PrecipitationDuration",  # 降水延時：累積一天下雨的總時數
        "SunshineDuration",  # 日照時數：累積一天的日照總時數
    ]

    # 需要取最大值的欄位
    max_cols = [
        "PeakGust",  # 最大陣風：當天最強的陣風
        "is_typhoon",  # 颱風警報：當天是否有颱風（1=有，0=無）
    ]

    # 建立分組欄位
    group_cols = ["Date", "city_id", "city_name", "station_id", "Altitude"]

    # 將必要欄位加入清單
    global weather_factor
    weather_factor = weather_factor + group_cols

    # 建立一個清單用以存放需要的欄位
    cols_to_keep = [col for col in hourly_df.columns if col in weather_factor]

    # 篩選需要的欄位
    hourly_df = hourly_df[cols_to_keep]

    # 建立彙總規則
    agg_dict = {}

    # 平均值欄位
    for col in mean_cols:
        if col in hourly_df.columns:
            agg_dict[col] = "mean"

    # 累積值欄位
    for col in sum_cols:
        if col in hourly_df.columns:
            agg_dict[col] = "sum"

    # 最大值欄位
    for col in max_cols:
        if col in hourly_df.columns:
            agg_dict[col] = "max"

    # 風向需要特殊處理（向量平均）
    if "WindDirection" in hourly_df.columns:
        # 取眾數（最常出現的風向）
        agg_dict["WindDirection"] = lambda x: (
            x.mode()[0] if len(x.mode()) > 0 else x.mean()
        )

    # 颱風名稱取第一個非空值
    if "typhoon_name" in hourly_df.columns:
        agg_dict["typhoon_name"] = lambda x: (
            x.dropna().iloc[0] if len(x.dropna()) > 0 else ""
        )

    # 執行分組彙總(第一次)
    daily_df = hourly_df.groupby(group_cols).agg(agg_dict).reset_index()

    # 進行第二次分組，將高低海拔的測站各自進行平均
    daily_df = daily_df.drop(columns=["station_id"])  # 把station_id移除(平均後已不重要)
    group_cols = ["Date", "city_id", "city_name", "Altitude"]  # 建立分組欄位
    agg_dict = {}  # 建立一個存放聚合條件的dict
    numeric_cols = mean_cols + sum_cols + max_cols  # 建立一個清單用以存放數值欄位

    # 如果有抓風向，也要加入清單
    if "WindDirection" in daily_df.columns:
        numeric_cols.append("WindDirection")

    # 將在daily_df中的數值欄位加上聚合條件
    for col in numeric_cols:
        if (col in daily_df.columns) and (col not in group_cols):
            agg_dict[col] = "mean"

    # 額外處理不在分組條件的typhon name
    if "typhoon_name" in daily_df.columns and "typhoon_name" not in group_cols:
        agg_dict["typhoon_name"] = "first"  # 取該日該城的第一個颱風名稱

    # 執行分組彙總(第二次)
    daily_df = daily_df.groupby(group_cols).agg(agg_dict).reset_index()

    # 將 Date 轉回字串格式
    daily_df["Date"] = daily_df["Date"].astype(str)

    # 四捨五入到小數點後 1 位
    numeric_cols = daily_df.select_dtypes(include=["float64"]).columns
    for col in numeric_cols:
        if col not in ["is_typhoon"]:
            daily_df[col] = daily_df[col].round(1)

    # 為了符合資料庫設計，故移除city_name
    if "city_name" in daily_df.columns:
        daily_df = daily_df.drop(columns=["city_name"])

    return daily_df


# %%
# 欄位名稱處理函數


def camel_to_snake(name):
    """將駝峰命名轉為蛇形命名，確保資料庫的建立無虞"""
    # 使用re裡面的反向引用來分組
    # 在大寫字母後面跟著小寫字母的邊界插入底線 (處理 ABCDef -> AB_CDef)
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    # 在小寫字母/數字後面跟著大寫字母的邊界插入底線 (處理 aBc -> a_Bc)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    # 全部轉為小寫
    return s2.lower()


# %%
# 主程式
def main():
    print("=" * 70)
    print("台灣氣象資料擷取程式")
    print("=" * 70)

    # 抓取颱風資料
    typhoon_fetcher = TyphoonWarningFetcher()
    typhoon_date_dict = typhoon_fetcher.fetch_all_warnings(st.year, et.year)

    # 抓取天氣資料
    crawler = CODiSAPICrawler()
    print("\n" + "-" * 70)
    print("開始爬取氣象資料")
    print("-" * 70)

    all_data = []  # 建立一個列表存放所有資料
    # 建立計數器
    success_count = 0
    fail_count = 0

    # 生成日期範圍
    current_date = st  # 設定一個變數用來迭迨日期
    dates = []  # 建立一個列表來存放所有日期
    while current_date <= et:
        dates.append(current_date)  # 把撈取過的日期加入日期列表
        current_date += timedelta(days=1)  # 逐一更新日期

    # 爬取所有測站和日期
    total_tasks = len(stations) * len(dates)

    # 使用tqdm製作進度條
    with tqdm(total=total_tasks, desc="下載進度") as pbar:
        for station in stations:
            for date in dates:
                pbar.set_description(
                    f"{station['city_name']} {station['station_id']} {date.strftime('%Y-%m-%d')}"
                )

                # 獲取資料
                df = crawler.fetch_weather_data(station["station_id"], date)

                if df is not None and not df.empty:
                    # 加入城市資訊
                    df["city_id"] = station["city_id"]
                    df["city_name"] = station["city_name"]
                    df["station_id"] = station["station_id"]
                    df["Altitude"] = station["Altitude"]

                    # 加入颱風資訊
                    date_str = date.strftime("%Y-%m-%d")
                    if date_str in typhoon_date_dict:
                        df["is_typhoon"] = 1
                        df["typhoon_name"] = ", ".join(typhoon_date_dict[date_str])
                    else:
                        df["is_typhoon"] = 0
                        df["typhoon_name"] = ""

                    all_data.append(df)
                    success_count += 1
                else:
                    fail_count += 1

                pbar.update(1)

    print("\n資料彙總與輸出")
    print("-" * 70)

    if all_data:
        # 合併所有逐時資料
        hourly_df = pd.concat(all_data, ignore_index=True)

        # 資料品質統計
        total_records = len(hourly_df)

        print(f" 逐時資料品質檢查:")
        numeric_cols = hourly_df.select_dtypes(include=["float64", "int64"]).columns
        null_summary = []
        for col in numeric_cols:
            if col not in ["typhoon"]:  # 排除 typhoon 欄位
                null_count = hourly_df[col].isna().sum()
                null_pct = (null_count / total_records) * 100
                if null_count > 0:
                    null_summary.append(
                        f"   - {col}: {null_count} 筆空值 ({null_pct:.1f}%)"
                    )

        if null_summary:
            for line in null_summary[:5]:  # 只顯示前5個
                print(line)
            if len(null_summary) > 5:
                print(f"   ... 還有 {len(null_summary) - 5} 個欄位有空值")
        else:
            print("所有欄位都無空值")

        # 輸出逐時資料
        hourly_output_path = Path(output_dir_path, output_hour)
        hourly_df.to_csv(hourly_output_path, index=False, encoding="utf-8-sig")
        print(f"\n已輸出逐時資料：{hourly_output_path}")
        print(f"   總筆數：{len(hourly_df):,} 筆（每小時一筆）")
        print(
            f"   時間範圍：{hourly_df['DataTime'].min()} ~ {hourly_df['DataTime'].max()}"
        )

        # 產生每日彙總資料
        print(f"\n產生每日彙總資料...")
        daily_df = create_daily_summary(hourly_df)
        daily_df.columns = daily_df.columns.map(camel_to_snake)  # 重新命名欄位

        # 輸出每日資料
        daily_output_path = Path(output_dir_path, output_daily)
        daily_df.to_csv(daily_output_path, index=False, encoding="utf-8-sig")
        print(f"   已輸出每日資料：{daily_output_path}")
        print(f"   總筆數：{len(daily_df):,} 筆（每日彙總）")
        print(f"   日期範圍：{daily_df['date'].min()} ~ {daily_df['date'].max()}")

        print(f"\n 資料欄位:")
        print(f"   逐時資料: {len(hourly_df.columns)} 個欄位")
        print(f"   每日資料: {len(daily_df.columns)} 個欄位")

        print(f"\n   每日資料彙總方式:")
        print(f"     平均值 (Mean): 氣溫、氣壓、濕度、風速、能見度、土壤溫度等")
        print(f"     累積值 (Sum): 降水量、降水延時、日照時數")
        print(f"     最大值 (Max): 最大陣風、颱風警報")
        print(f"\n   其他說明:")
        print(f"   - 逐時資料: 原始資料，每小時一筆記錄")
        print(f"   - 空值 (NaN): 該時段測站未提供資料或儀器故障")
        print(f"   - 原始資料中的負值已自動轉換為空值")
    else:
        print("沒有成功下載任何資料")

    # 統計
    print("\n" + "=" * 70)
    print(" 執行結果")
    print("=" * 70)
    print(f" 成功: {success_count} 筆")
    print(f" 失敗: {fail_count} 筆")
    print(f" 輸出目錄: {output_dir}")
    print(f" 逐時資料: {output_hour} (每小時一筆，共 24 筆/天)")
    print(f" 每日資料: {output_daily} (每日彙總，共 1 筆/天)")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
