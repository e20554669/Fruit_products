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

# 設定資料庫查詢模板及URL設定
sql_template = "SELECT * FROM weather " "WHERE `date` = CURDATE() - INTERVAL 1 DAY"

# 設定google sheet的URL
gsheets_url = (
    "https://docs.google.com/spreadsheets/d/"
    "1uIWklrNfFHXt7lsMApmXNVP1VbjkcXQ0NcEPH9dj6G4/edit?usp=sharing"
)
# 設定要存取的分頁
sheet_title = "weather"

# 提供鑰匙存放路徑
bigquery_credentials_file_path = "/app/keys/bigquery-user.json"


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

            # 3. 讀取現有資料 (為了比對日期)
            current_data_df = wks.get_as_df(
                has_header=True, include_tailing_empty=False, numerize=False
            )

            # --- 狀況 A: 空表 ---
            if current_data_df.empty:
                print("Google Sheet 為空，直接寫入資料...")
                wks.set_dataframe(df, start="A1", copy_head=True, nan="")
                print(f"寫入完成，共{df.shape[0]-1}筆資料")
                return

            # --- 狀況 B: 追加 ---
            # 處理日期格式 (確保都是 date 物件)
            gs_dates = pd.to_datetime(current_data_df["date"], format="mixed").dt.date
            last_date_in_gs = gs_dates.max()

            # MySQL 新資料 (取第一筆即可)
            # 從 XCom 傳過來的 date 可能是字串，要轉 datetime
            new_data_date = pd.to_datetime(df["date"].iloc[0]).date()

            print(
                f"檢查日期：GSheet 最新為 {last_date_in_gs}，準備寫入 {new_data_date}"
            )

            if new_data_date > last_date_in_gs:
                print("檢測到新資料！正在追加到 Google Sheet...")
                last_row = len(current_data_df) + 1  # 跳過標題
                wks.set_dataframe(
                    df, start=(last_row + 1, 1), copy_head=False, nan="", extend=True
                )
                print("更新完成！")
            else:
                print(
                    f"資料已存在！({new_data_date} <= {last_date_in_gs})，跳過寫入以避免重複。"
                )

        except Exception as e:
            print(f"日期比對或寫入失敗: {e}")
            raise

    records = query_data_from_sql(sql_template)
    upload_to_gsheet(records)


dag = export_data_to_gsheet()
