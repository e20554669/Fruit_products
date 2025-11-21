# 載入必要套件
import pandas as pd
import numpy as np
import pymysql


# 建立SQL query的函數
def query_data_from_sql(sql_template: list, connection: pymysql.connections.Connection):
    cursor = connection.cursor()
    try:
        with pymysql.connect(
            host=host, port=port, user=user, passwd=passwd, db=db, charset=charset
        ) as connection:

            with connection.cursor() as cursor:

                # 準備 SQL 查詢
                print("開始執行 select...")

                # 執行查詢
                cursor.execute(sql_template)
                data = cursor.fetchall()

                # 將tuple轉為df，其中cursor.description[0]會存放欄位名稱
                df = pd.DataFrame(
                    data, columns=[desc[0] for desc in cursor.description]
                )
                print(f"成功取得 {(df.shape[0])} 筆資料。")
                return df

    except Exception as e:
        print(f"資料庫操作發生錯誤：{e}")


# 建立存檔所需的函數
from pathlib import Path


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


def restore_to_csv(table_name: str):
    """將以pymysql取得的資料額外存成csv"""

    # 生成資料夾的名稱
    output_dir = table_name

    try:
        # 執行 'find_project_root' (現在預設會尋找 .gitignore)
        project_root = find_project_root()
    except FileNotFoundError:
        print("警告: 找不到 '.gitignore' 錨點檔案。")
        print("將 fallback (退回) 使用當前工作目錄作為根目錄。")
        project_root = Path.cwd()
    output_dir_path = Path(project_root, "work_area/data", output_dir)
    Path(output_dir_path).mkdir(parents=True, exist_ok=True)
    output_dir_path = Path(output_dir_path, f"{table_name}_from_sql.csv")
    df.to_csv(output_dir_path, index=False, encoding="utf-8-sig")
    print(f"已將資料存在以下路徑: {output_dir_path}")


# 載入GCP API for gsheets，並設定取得工作表的函數
import pygsheets
from pygsheets.client import Client


def get_google_sheet_client(bigquery_credentials_file_path) -> Client:
    """Get Google Sheets client."""
    return pygsheets.authorize(service_account_file=bigquery_credentials_file_path)


def get_gsheet(gsheet_url: str, worksheet_title: str | None = None) -> pd.DataFrame:
    """Return DataFrame from a specified Google Sheets worksheet."""
    gc = get_google_sheet_client(bigquery_credentials_file_path)
    sheet = gc.open_by_url(gsheet_url)  # 選擇使用網址來開啟這個sheet
    if worksheet_title:
        wks = sheet.worksheet_by_title(worksheet_title)
        return wks
    wks = sheet.sheet1
    return wks


def upload_to_gsheet(df: pd.DataFrame, gsheets_url: str, sheet_title: str):
    """
    將 DataFrame 上傳到 Google Sheet
    """

    if df.empty:
        print("MySQL 查詢無資料，跳過更新。")
        return

    # 以指定url, sheet_name 取得gsheet
    wks = get_gsheet(gsheets_url, sheet_title)
    # 轉為DataFrame
    current_data_df = wks.get_as_df(
        has_header=True, include_tailing_empty=False, numerize=False
    )

    # ===若gsheet為空
    if current_data_df.empty:
        print("Google Sheet 為空，直接寫入資料...")
        wks.set_dataframe(df, start="A1", copy_head=True, nan="")
        print(f"寫入完成，共{df.shape[0]-1}筆資料")
        return

    # ===若gsheet不為空
    try:
        # 處理 GSheet 的日期 (轉成 date 物件)
        gs_dates = pd.to_datetime(current_data_df["date"], format="mixed").dt.date
        last_date_in_gs = gs_dates.max()

        # 處理 MySQL 新資料的日期
        new_data_date = pd.to_datetime(df["date"].iloc[0]).date()

        print(f"檢查日期：GSheet 最新為 {last_date_in_gs}，準備寫入 {new_data_date}")

        if new_data_date > last_date_in_gs:
            print("檢測到新資料！正在追加到 Google Sheet...")

            # 找出下一列的位置
            last_row = len(current_data_df) + 1  # 要跳過標題列

            # 執行追加 (不寫入標題)
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


# 設定資料庫查詢模板及URL設定
sql_table_name = "price_prediction"
sql_template = f"""
    SELECT *
    FROM {sql_table_name}
"""

# 設定google sheet的URL
gsheets_url = (
    "https://docs.google.com/spreadsheets/d/"
    "1uIWklrNfFHXt7lsMApmXNVP1VbjkcXQ0NcEPH9dj6G4/edit?usp=sharing"
)
# 設定要存取的分頁
sheet_title = "weather"

# 提供鑰匙存放路徑
bigquery_credentials_file_path = "/app/work_area/code/bigquery-user.json"

# 設定資料庫連線資訊
host = "104.199.220.12"
port = 3306
user = "tjr103-team02"
passwd = "password"
db = "tjr103-team02"
charset = "utf8mb4"
connection = pymysql.connect(
    host=host, port=port, user=user, passwd=passwd, db=db, charset=charset
)

# 執行主程式
if __name__ == "__main__":
    df = query_data_from_sql(sql_template, connection)  # 執行SQL query
    # upload_to_gsheet(df, gsheets_url, sheet_title)  # 上傳資料到google sheet
    restore_to_csv(sql_table_name)  # 將結果存到csv
