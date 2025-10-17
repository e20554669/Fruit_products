#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
農委會自動氣象站資料處理程式（合併輸出版）
====================================

1. 取得指定日期範圍自動氣象站資料
2. 城市平均、颱風標記
3. 將所有日期結果「合併在同一個 CSV」
"""

import sys
import io
import os

# 處理Windows系統的編碼問題
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
    os.environ["PYTHONIOENCODING"] = "utf-8"

# 隱藏 SSL 警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Union
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed


class WeatherDataProcessor:
    """氣象資料處理器類別"""

    def __init__(self):
        """初始化氣象資料處理器"""
        # 農委會自動氣象站資料API端點
        self.api_url = "https://data.moa.gov.tw/api/v1/AutoWeatherStationType/"

        # 設定輸出目錄
        self.output_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data", "weather"
        )

        # 颱風警報相關API端點
        self.typhoon_api_url = "https://rdc28.cwa.gov.tw/TDB/public/warning_typhoon_list/get_warning_typhoon"
        self.typhoon_main_url = "https://rdc28.cwa.gov.tw/TDB/public/warning_typhoon_list/"

        # 多線程處理設定
        self.use_multithreading = True
        self.max_workers = 4

        # 城市名稱到城市代碼的對應表
        self.city_mapping = {
            "南投縣": "NTO",
            "臺中市": "TXG",
            "台中市": "TXG",
            "臺北市": "TPE",
            "台北市": "TPE",
            "臺南市": "TNN",
            "台南市": "TNN",
            "臺東縣": "TTT",
            "台東縣": "TTT",
            "嘉義市": "CYI",
            "嘉義縣": "CYQ",
            "基隆市": "KEE",
            "宜蘭縣": "ILN",
            "屏東縣": "PIF",
            "彰化縣": "CHA",
            "新北市": "NTP",
            "新竹市": "HSZ",
            "新竹縣": "HSC",
            "桃園市": "TAO",
            "澎湖縣": "PEN",
            "花蓮縣": "HUA",
            "苗栗縣": "MIA",
            "金門縣": "KIN",
            "雲林縣": "YUN",
            "高雄市": "KHH",
            "連江縣": "LCC",
        }

    def create_scraper_session(self):
        """建立模擬瀏覽器的HTTP Session"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        return session

    def check_yesterday_typhoon_warning(self, target_date=None):
        """檢查指定日期是否有颱風警報"""
        if target_date is None:
            target_date = datetime.now() - timedelta(days=1)
        elif isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d")
        print(f"[檢查] {target_date.strftime('%Y-%m-%d')} 是否有颱風警報")
        return self.scrape_typhoon_warning_history(target_date, days_range=0)

    def scrape_typhoon_warning_history(self, target_date=None, days_range=1):
        """爬取指定日期前N天的颱風警報歷史資料"""
        if target_date is None:
            target_date = datetime.now()
        elif isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d")

        query_date = target_date - timedelta(days=days_range)
        print(f"[查詢] {query_date.strftime('%Y-%m-%d')} 的颱風警報資料")

        session = self.create_scraper_session()

        try:
            print("[步驟1] 訪問主頁面建立session...")
            response = session.get(self.typhoon_main_url, timeout=30, verify=False)
            print(f"   主頁面狀態碼: {response.status_code}")
            if response.status_code != 200:
                print("[錯誤] 無法訪問主頁面")
                return None

            all_warnings = []
            target_year = target_date.year
            post_data = {"year": str(target_year)}

            print(f"[步驟2] 嘗試POST請求...")
            print(f"   參數: {post_data}")

            session.headers.update(
                {
                    "Referer": self.typhoon_main_url,
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                }
            )

            try:
                response = session.post(
                    self.typhoon_api_url, data=post_data, timeout=30, verify=False
                )
                print(f"   狀態碼: {response.status_code}")

                if response.status_code == 200:
                    response_text = response.text.strip()
                    if "No direct script access allowed" in response_text:
                        print("   [錯誤] 防護機制阻擋")
                        return None

                    try:
                        clean_text = response_text
                        if clean_text.startswith("\ufeff"):
                            clean_text = clean_text[1:]
                        data = json.loads(clean_text)
                        print("   [成功] 取得JSON資料！")
                        print(
                            f"   [資料] 筆數: {len(data) if isinstance(data, list) else '1 (dict)'}"
                        )
                        if isinstance(data, list) and data:
                            all_warnings.extend(data)
                            print(f"   [成功] 加入 {len(data)} 筆警報資料")
                    except json.JSONDecodeError as e:
                        print(f"   [錯誤] JSON解析錯誤: {e}")
                        return None

            except requests.exceptions.RequestException as e:
                print(f"   [錯誤] 請求錯誤: {e}")
                return None

            if all_warnings:
                print(f"\n[成功] 取得 {len(all_warnings)} 筆颱風警報資料")
                filtered_warnings = self.filter_warnings_by_date(
                    all_warnings, target_date, days_range
                )
                if filtered_warnings:
                    print(f"\n🚨 找到 {len(filtered_warnings)} 個颱風警報！")
                    for warning in filtered_warnings:
                        typhoon_name = warning.get("cht_name", "N/A")
                        eng_name = warning.get("eng_name", "N/A")
                        sea_start = warning.get("sea_start_datetime", "N/A")
                        sea_end = warning.get("sea_end_datetime", "N/A")
                        print(f"[警報] 颱風: {typhoon_name} ({eng_name})")
                        print(f"   海警期間: {sea_start} ~ {sea_end}")

                    return {
                        "has_warning": True,
                        "warnings": filtered_warnings,
                        "summary": f"颱風警報: {', '.join([w.get('cht_name', 'N/A') for w in filtered_warnings])}",
                    }
                else:
                    query_date = target_date - timedelta(days=days_range)
                    print(f"\n[正常] {query_date.strftime('%Y-%m-%d')} 沒有颱風警報")
                    return {
                        "has_warning": False,
                        "warnings": [],
                        "summary": f"{query_date.strftime('%Y-%m-%d')} 無颱風警報",
                    }
            else:
                print("[錯誤] 無法取得颱風警報資料")
                return {
                    "has_warning": False,
                    "warnings": [],
                    "summary": "無法查詢颱風警報資料",
                }

        except Exception as e:
            print(f"[錯誤] 爬蟲錯誤: {e}")
        finally:
            session.close()
        return None

    def filter_warnings_by_date(self, warnings, target_date, days_range):
        """篩選指定日期範圍內的警報"""
        if days_range == 0:
            query_date = target_date
        else:
            query_date = target_date - timedelta(days=days_range)

        filtered = []
        for warning in warnings:
            sea_start = warning.get("sea_start_datetime")
            sea_end = warning.get("sea_end_datetime")

            if sea_start:
                try:
                    start_dt = datetime.strptime(sea_start[:19], "%Y-%m-%d %H:%M:%S")
                    if sea_end and sea_end.strip():
                        end_dt = datetime.strptime(sea_end[:19], "%Y-%m-%d %H:%M:%S")
                    else:
                        end_dt = datetime.now() + timedelta(days=30)

                    query_start = query_date.replace(hour=0, minute=0, second=0)
                    query_end = query_date.replace(hour=23, minute=59, second=59)

                    if not (end_dt < query_start or start_dt > query_end):
                        filtered.append(warning)
                except Exception:
                    continue
        return filtered

    def get_weather_data(
        self, start_date: str, end_date: str = None
    ) -> List[Dict[str, Any]]:
        """從農委會API獲取指定日期範圍的氣象資料"""
        if end_date is None:
            end_date = start_date
        print(f"[獲取] 正在獲取 {start_date} 到 {end_date} 的氣象資料...")

        params = {"Start_time": start_date, "End_time": end_date}
        try:
            response = requests.get(
                self.api_url, params=params, timeout=60, verify=False
            )
            print(f"[除錯] 完整 URL: {response.url}")
            print(f"[除錯] HTTP 狀態碼: {response.status_code}")
            response.raise_for_status()

            data = response.json()
            print(f"[除錯] API 回應的鍵值: {list(data.keys())}")

            if "Data" in data:
                weather_data = data["Data"]
                print(f"[成功] 獲取 {len(weather_data)} 筆氣象資料")
                if len(weather_data) > 0:
                    print(f"[範例] 第一筆鍵值: {list(weather_data[0].keys())}")
                    print(f"[範例] 第一筆城市: {weather_data[0].get('CITY', 'N/A')}")
                return weather_data
            else:
                print("[錯誤] API回應中沒有Data欄位")
                print(f"[除錯] 回應: {json.dumps(data, ensure_ascii=False, indent=2)}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"[錯誤] API請求失敗: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"[錯誤] JSON解析失敗: {e}")
            print(f"[除錯] 回應內容前 500 字元: {response.text[:500]}")
            return []

    def filter_valid_stations(
        self, weather_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """過濾出有效的氣象站資料並排除指定城市"""
        valid_data = []
        excluded_city_names = {
            "嘉義市",
            "基隆市",
            "新竹市",
            "澎湖縣",
            "金門縣",
            "連江縣",
        }

        for record in weather_data:
            city = record.get("CITY", "")
            temp = record.get("TEMP")
            pres = record.get("PRES")
            if city and temp is not None and pres is not None and city not in excluded_city_names:
                valid_data.append(record)

        print(f"[過濾] 有效氣象站資料: {len(valid_data)} 筆（已排除 {len(excluded_city_names)} 個指定城市）")
        return valid_data

    def calculate_city_averages(
        self, weather_data: List[Dict[str, Any]], target_date: str = None
    ) -> pd.DataFrame:
        """計算各縣市的氣象資料平均值"""
        print("[計算] 正在計算各縣市平均值...")

        df = pd.DataFrame(weather_data)

        numeric_fields = ["TEMP", "HUMD", "PRES", "WDSD", "H_24R"]
        for field in numeric_fields:
            df[field] = pd.to_numeric(df[field], errors="coerce")

        city_averages = (
            df.groupby("CITY")
            .agg({"PRES": "mean", "TEMP": "mean", "HUMD": "mean", "WDSD": "mean", "H_24R": "mean"})
            .round(2)
        )

        city_averages.columns = ["StnPres", "Temperature", "RH", "WS", "Precp"]
        city_averages.reset_index(inplace=True)
        city_averages["city_id"] = city_averages["CITY"].map(self.city_mapping)
        city_averages = city_averages.dropna(subset=["city_id"])

        # 日期欄位（兩種：顯示用 ObsTime = yyyy/MM/dd；機器友善 ObsDate = yyyy-MM-dd）
        if target_date is None:
            obs_date_dash = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            obs_date_dash = target_date
        city_averages["ObsDate"] = obs_date_dash
        city_averages["ObsTime"] = obs_date_dash.replace("-", "/")

        # 檢查颱風狀況
        typhoon_result = self.check_yesterday_typhoon_warning(obs_date_dash)
        if typhoon_result and typhoon_result.get("has_warning"):
            city_averages["typhoon"] = "1"
            first_typhoon = typhoon_result["warnings"][0]
            city_averages["typhoon_name"] = first_typhoon.get("eng_name", "")
        else:
            city_averages["typhoon"] = "0"
            city_averages["typhoon_name"] = ""

        city_averages = city_averages[
            ["ObsDate", "city_id", "ObsTime", "StnPres", "Temperature", "RH", "WS", "Precp", "typhoon", "typhoon_name"]
        ]

        print(f"[完成] {len(city_averages)} 個縣市的平均值計算")
        return city_averages

    # ================== 新增：合併存檔 ==================
    def save_merged_csv(self, merged_df: pd.DataFrame, start_str: str, end_str: str) -> str:
        """將所有日期的結果合併後一次存檔"""
        os.makedirs(self.output_dir, exist_ok=True)
        filename = f"weather_{start_str.replace('-', '')}_{end_str.replace('-', '')}.csv"
        filepath = os.path.join(self.output_dir, filename)
        try:
            merged_df.to_csv(filepath, index=False, encoding="utf-8-sig")
            print(f"[儲存] 合併資料已儲存至: {filepath}")
            return filepath
        except Exception as e:
            print(f"[錯誤] 儲存合併檔案失敗: {e}")
            raise

    # ---- 工具：將 str/datetime 統一為 yyyy-mm-dd 字串 ----
    @staticmethod
    def _to_datestr(d: Union[str, datetime]) -> str:
        if isinstance(d, datetime):
            return d.strftime("%Y-%m-%d")
        elif isinstance(d, str):
            return d
        else:
            raise TypeError(f"日期型別不支援：{type(d)}，請用 str 或 datetime")

    # ================== 這裡開始變更流程 ==================
    def process_single_date(self, date_str: str) -> Union[pd.DataFrame, None]:
        """處理單一日期：回傳本日城市平均的 DataFrame（不落地寫檔）"""
        print(f"\n[處理] {date_str}")
        try:
            weather_data = self.get_weather_data(date_str)
            if not weather_data:
                print(f"[警告] 無法獲取 {date_str} 的氣象資料，跳過此日期")
                return None

            valid_data = self.filter_valid_stations(weather_data)
            if not valid_data:
                print(f"[警告] {date_str} 沒有有效氣象站資料，跳過此日期")
                return None

            city_averages = self.calculate_city_averages(valid_data, date_str)
            if city_averages.empty:
                print(f"[警告] {date_str} 無法計算縣市平均值，跳過此日期")
                return None

            # 顯示摘要（不寫檔）
            print(f"[摘要] {date_str}：縣市數 {len(city_averages)}，"
                  f"溫度 {city_averages['Temperature'].min():.1f}~{city_averages['Temperature'].max():.1f} °C，"
                  f"濕度 {city_averages['RH'].min():.1f}~{city_averages['RH'].max():.1f} %")
            return city_averages

        except Exception as e:
            print(f"[錯誤] 處理 {date_str} 發生錯誤: {e}")
            return None

    def process_weather_data(self, start_date: Union[str, datetime] = None, end_date: Union[str, datetime] = None):
        """主要處理流程：收集所有日期結果合併為一個 CSV"""
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = start_date

        start_str = self._to_datestr(start_date)
        end_str = self._to_datestr(end_date)

        print(f"[開始] 處理氣象資料 ({start_str} 到 {end_str})...")
        print("=" * 50)

        # 生成日期清單
        start_dt = datetime.strptime(start_str, "%Y-%m-%d")
        end_dt = datetime.strptime(end_str, "%Y-%m-%d")
        date_list = []
        current_dt = start_dt
        while current_dt <= end_dt:
            date_list.append(current_dt.strftime("%Y-%m-%d"))
            current_dt += timedelta(days=1)
        print(f"[資訊] 共需處理 {len(date_list)} 個日期")

        # 收集每日結果（DataFrame）
        dfs: List[pd.DataFrame] = []

        if self.use_multithreading and len(date_list) > 1:
            print(f"[多線程] 使用 {self.max_workers} 線程並行處理")
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_date = {executor.submit(self.process_single_date, d): d for d in date_list}
                for future in as_completed(future_to_date):
                    d = future_to_date[future]
                    try:
                        df = future.result()
                        if df is not None and not df.empty:
                            dfs.append(df)
                    except Exception as e:
                        print(f"[錯誤] 多線程處理 {d} 失敗: {e}")
        else:
            print("[單線程] 順序處理")
            for d in date_list:
                df = self.process_single_date(d)
                if df is not None and not df.empty:
                    dfs.append(df)

        if not dfs:
            print("[完成] 無任何可用資料，未產出 CSV。")
            return None

        # 合併所有日期
        merged_df = pd.concat(dfs, axis=0, ignore_index=True)

        # 建議排序：先依日期、再依 city_id
        if "ObsDate" in merged_df.columns and "city_id" in merged_df.columns:
            merged_df = merged_df.sort_values(["ObsDate", "city_id"]).reset_index(drop=True)

        # 一次寫入同一個 CSV
        out_path = self.save_merged_csv(merged_df, start_str, end_str)

        print(f"\n[完成] 氣象資料處理完成！")
        print(f"輸出檔案：{out_path}")
        print("=" * 50)
        return out_path


def main():
    """主程式"""
    # ============== 執行參數設定區 ==============
    USE_MULTITHREADING = False  # 先關閉多線程方便除錯
    MAX_WORKERS = 10

    # ============== 日期設定區 ==============
    START_DATE = "2025-10-10"
    END_DATE = "2025-10-15"

    print(f"[測試] 日期範圍: {START_DATE} 到 {END_DATE}")
    # =======================================

    processor = WeatherDataProcessor()
    processor.use_multithreading = USE_MULTITHREADING
    processor.max_workers = MAX_WORKERS
    processor.process_weather_data(START_DATE, END_DATE)


if __name__ == "__main__":
    main()
