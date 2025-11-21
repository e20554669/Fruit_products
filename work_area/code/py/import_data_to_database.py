import pandas as pd
import numpy as np
import pymysql

# --- 1. 讀取 CSV ---
csv_path = "/app/work_area/data/price_prediction/price_prediction_from_sql.csv"  # <-- 修改為要讀取的csv
df = pd.read_csv(csv_path)
df = df.replace(np.nan, None)  # 把Nan替換成None，pymysql才有辦法處理
db_table = "price_prediction"  # <-- 修改為要匯入的table名稱

# --- 2. 建立連線 ---

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
cursor = connection.cursor()


# --- 3. 批次寫入 ---
try:
    # A. 將 DataFrame 轉換為(tuple)
    data_tuples = [tuple(row) for row in df.to_numpy()]

    # B. 準備 SQL 插入模板
    sql_template = f"""
        INSERT IGNORE INTO {db_table} ({", ".join(list(df.columns))})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """

    # C. 執行「一次性」批次插入
    print("開始執行 executemany...")
    cursor.executemany(sql_template, data_tuples)

    # D. 獲取插入成功的筆數
    cursor.execute("SELECT ROW_COUNT()")
    successful_inserts = cursor.fetchone()[0]

    # E. 提交交易
    connection.commit()
    print(f"成功將 {successful_inserts} 筆資料寫入 '{db_table}' 資料表。")

except Exception as e:
    connection.rollback()
    print(f"寫入資料庫時發生錯誤: {e}")

finally:
    cursor.close()
    connection.close()
