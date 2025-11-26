# 載入必要套件
import pandas as pd
import numpy as np
import pymysql
import pygsheets
from datetime import datetime, timedelta

# 載入airflow所需套件
import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ===設定MySQL資訊、gsheet_URL、gsheet_title、key存放位置

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

sql_template_dict = {}  # 先建立一個dict來存放模板對照表

sql_template_dict[
    "sql_template1"
] = """
    WITH 
    -- CTE1 處理水果價格: 將實際價格與預測價格合併
    price AS(
        SELECT * FROM price_prediction 
        WHERE `date` < CURDATE() AND `mode` = 'actual'
        UNION ALL
        SELECT * FROM price_prediction 
        WHERE `date` >= CURDATE() AND `mode` = 'prediction'
    ),
    -- CTE2 取得各作物每日成交量，並換算成公噸
    volume_sum AS(
        SELECT
            `date`
            ,crop_id
            ,SUM(trans_volume)/1000 AS `trans_volume(t)`
        FROM volume
        GROUP BY `date`, crop_id
    )
    -- 主查詢: 加入水果名稱，並將欄位名稱轉換為中文
    SELECT
        cr.crop_name AS `水果名稱`
        ,p.date AS `交易日期`
        ,CASE 
            WHEN p.mode = 'actual' THEN '實際價格'
            WHEN p.mode = 'prediction' THEN '預測價格'
        END AS `模式`
        ,p.price AS `平均價(元/公斤)`
        ,v.`trans_volume(t)` AS `交易量(公頓)`
    FROM
        price p
        JOIN crop cr
            ON p.crop_id = cr.crop_id
        LEFT JOIN volume_sum v
            ON p.crop_id = v.crop_id
            AND p.date = v.date
    ;
"""

sql_template_dict[
    "sql_template2"
] = """
    SELECT
        w.`date` AS `日期`
        ,c.city_name AS `縣市`
        ,w.altitude AS `海拔高度`
        ,w.station_pressure AS `氣壓`
        ,w.air_temperature AS `氣溫`
        ,w.relative_humidity AS `相對溼度`
        ,w.wind_speed AS `風速`
        ,w.precipitation AS `降雨量`
        ,CASE 
            WHEN w.is_typhoon = 1 THEN '有'
            WHEN w.is_typhoon = 0 THEN '無'
        END AS `有無颱風`
        ,typhoon_name AS `颱風名稱`
    FROM
        weather w
        JOIN city c
            ON w.city_id = c.city_id
    ;
"""

# 建立一個dict用來存放城市中英對照表
city_dict = {
    "彰化縣": "Changhua County",
    "嘉義市": "Chiayi City",
    "嘉義縣": "Chiayi County",
    "新竹市": "Hsinchu City",
    "新竹縣": "Hsinchu County",
    "花蓮縣": "Hualien County",
    "宜蘭縣": "Yilan County",
    "基隆市": "Keelung City",
    "高雄市": "Kaohsiung City",
    "金門縣": "Kinmen County",
    "連江縣": "Lienchiang County",
    "苗栗縣": "Miaoli County",
    "南投縣": "Nantou County",
    "新北市": "New Taipei City",
    "澎湖縣": "Penghu County",
    "屏東縣": "Pingtung County",
    "臺南市": "Tainan City",
    "臺北市": "Taipei City",
    "臺東縣": "Taitung County",
    "臺中市": "Taichung City",
    "桃園市": "Taoyuan City",
    "雲林縣": "Yunlin County",
}


# 設定google sheet的URL
gsheets_url = "https://docs.google.com/spreadsheets/d/1W_OelHOCt5EqatWQtzggBAi0VeYCInyiak_lDW-E57Q/edit?usp=sharing"

# 設定要存取的分頁

sheet_title_dict = {}  # 先建立一個dict來存放sheet對照表
sheet_title_dict["sheet_title1"] = "Price"
sheet_title_dict["sheet_title2"] = "Weather"

# 提供鑰匙存放路徑
bigquery_credentials_file_path = "/app/keys/bigquery-user.json"

# ===================================設定資訊的結尾===================================


# ===取得gsheet的函數
def get_google_sheet_client(bigquery_credentials_file_path):
    """Get Google Sheets client."""
    return pygsheets.authorize(service_account_file=bigquery_credentials_file_path)


def get_gsheet(client, gsheet_url: str, worksheet_title: str | None = None):
    """Return DataFrame from a specified Google Sheets worksheet."""
    sheet = client.open_by_url(gsheet_url)  # 選擇使用網址來開啟這個sheet
    if worksheet_title:
        return sheet.worksheet_by_title(worksheet_title)
    return sheet.sheet1


#  開始處理 Airflow DAG
@dag(
    dag_id="d_04_insert_to_gsheet_dag",
    description="每日從 MySQL 匯出需求資料到 Google Sheets (給Tableau用)",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["weather", "volume", "area_production", "gsheet", "tableau"],
    is_paused_upon_creation=False,
)
def export_data_to_gsheet():

    # === FIX: 明確引用全域字典，避免 Airflow 解析錯誤 ===
    # 在 DAG 函式內部創建本機參考，防止 Airflow 序列化時將外部字典誤判為整數。
    _sql_template_dict = sql_template_dict
    _sheet_title_dict = sheet_title_dict

    @task
    def query_data_from_sql(sql_template: str):
        """extract"""
        print("--- [Task 1] Query: 從 MySQL 撈取資料 ---")
        try:
            with pymysql.connect(**db_config) as connection:

                with connection.cursor() as cursor:

                    # 準備 SQL 查詢
                    print("開始執行 select...")

                    # 執行查詢
                    cursor.execute(sql_template)
                    data = cursor.fetchall()

                    if not data:
                        print("查無資料(可能是爬蟲還沒跑，或昨天沒資料)")
                        return None

                    # 將tuple轉為df，其中cursor.description[0]會存放欄位名稱
                    df = pd.DataFrame(
                        data, columns=[desc[0] for desc in cursor.description]
                    )
                    print(f"成功取得 {(df.shape[0])} 筆資料。")

        except Exception as e:
            print(f"資料庫操作發生錯誤：{e}")
            raise

        # 如果欄位中有"縣市"，則要將其轉為英文供tableau辨識
        if "縣市" in list(df.columns):
            df["縣市"] = df["縣市"].replace(city_dict)

        # 將DataFrame轉為 dict 傳給下一個 task
        return df.to_dict(orient="records")

    @task
    def upload_to_gsheet(data_list: list, sheet_title: str):
        """transform + load"""
        print("--- [Task 2] Upload: 上傳至 Google Sheets ---")

        if not data_list:
            print("無資料需要上傳，略過。")
            return

        # 1. 還原 DataFrame
        df = pd.DataFrame(data_list)

        try:
            # 2. 連線 Google Sheets
            print(f"連線至 Google Sheet: {sheet_title} ...")
            gc = get_google_sheet_client(bigquery_credentials_file_path)
            wks = get_gsheet(gc, gsheets_url, sheet_title)

            # 3. 寫入資料至Google sheet
            print("先將google sheet清空，確保資料正確性")
            wks.clear()
            print("開始寫入資料...")
            wks.set_dataframe(df, start="A1", copy_head=True, nan="")
            print(f"寫入完成，共{df.shape[0]-1}筆資料")
            return

        except Exception as e:
            print(f"寫入失敗: {e}")
            raise

    # 使用for loop來完成多個工作表的覆寫
    for suffix, sheet_title in sheet_title_dict.items():

        # 1. 根據 sheet_title 找出對應的 SQL key (例如 'sheet_title1' -> 'sql_template1')
        sql_key = suffix.replace("sheet_title", "sql_template")
        sql_template = sql_template_dict[sql_key]

        # 2. 執行查詢任務 (使用override確保 ID 唯一性)
        records = query_data_from_sql.override(task_id=f"query_data_for_{sheet_title}")(
            sql_template=sql_template
        )

        # 3. 執行上傳任務 (相依性自動建立)
        upload_to_gsheet.override(task_id=f"upload_to_{sheet_title}")(
            data_list=records, sheet_title=sheet_title
        )


dag = export_data_to_gsheet()
