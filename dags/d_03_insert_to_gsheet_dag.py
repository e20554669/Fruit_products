# 載入必要套件
import pandas as pd
import numpy as np
import pymysql
import pygsheets
from datetime import datetime, timedelta

# 載入airflow所需套件
import pendulum
from airflow.decorators import dag, task

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
sql_template = """
    WITH 
    -- CTE1 處理水果價格: 將實際價格與預測價格合併
    price AS(
        SELECT * FROM price_prediction 
        WHERE `date` <= CURDATE() AND `mode` = 'actual'
        UNION ALL
        SELECT * FROM price_prediction 
        WHERE `date` > CURDATE() AND `mode` = 'prediction'
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

# 設定google sheet的URL
gsheets_url = (
    "https://docs.google.com/spreadsheets/d/"
    "1uIWklrNfFHXt7lsMApmXNVP1VbjkcXQ0NcEPH9dj6G4/edit?usp=sharing"
)
# 設定要存取的分頁
sheet_title = "price_prediction"

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
    dag_id="d_03_insert_to_gsheet_dag",
    description="每日從 MySQL 匯出價格資料到 Google Sheets (給Tableau用)",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["weather", "gsheet", "tableau"],
    is_paused_upon_creation=False,
)
def export_data_to_gsheet():

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

        # 將DataFrame轉為 dict 傳給下一個 task
        return df.to_dict(orient="records")

    @task
    def upload_to_gsheet(data_list: list):
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

    records = query_data_from_sql(sql_template)
    upload_to_gsheet(records)


dag = export_data_to_gsheet()
