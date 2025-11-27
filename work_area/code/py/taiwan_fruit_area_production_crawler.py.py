# 套件載入及抓取年分、資料夾的設定
import pandas as pd
import numpy as np
import urllib3
import requests
import urllib.parse
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

unit_id_list = {113: "蔬菜", 135: "果品"}
year_cutoff = 2020  # 起始年份
# 以下填入需要查詢的水果(不支援模糊查詢)
vegetables = [
    "蘋果",
    "梨",
    "梅",
    "桃",
    "柿",
    "李",
    "龍眼",
    "鳳梨",
    "香蕉",
    "香瓜",
    "西瓜",
    "蓮霧",
    "葡萄柚",
    "葡萄",
    "荔枝",
    "草莓",
    "芒果",
    "百香果",
    "白柚",
    "番荔枝",
    "番茄",
    "番石榴",
    "溫州蜜柑",
    "洋香瓜",
    "檸檬",
    "橄欖",
    "楊桃",
    "椪柑",
    "棗",
    "桶柑",
    "柳橙",
    "枇杷",
    "木瓜",
    "文旦柚",
    "可可椰子",
]

# 生成資料夾的名稱
output_dir = "data for database"


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
print(f"共 {len(vegetables)} 種蔬果，準備下載...")


# === 建立城市、作物資料對照表

# 處理城市的部分
city_dict = {
    "city_name": [
        "高雄市",
        "雲林縣",
        "金門縣",
        "連江縣",
        "苗栗縣",
        "花蓮縣",
        "臺東縣",
        "臺南市",
        "臺北市",
        "臺中市",
        "澎湖縣",
        "桃園市",
        "新竹縣",
        "新竹市",
        "新北市",
        "彰化縣",
        "屏東縣",
        "宜蘭縣",
        "基隆市",
        "嘉義縣",
        "嘉義市",
        "南投縣",
    ],
    "city_id": [
        "KHH",
        "YUN",
        "KIN",
        "LIE",
        "MLI",
        "HUA",
        "TTT",
        "TNN",
        "TPE",
        "TXG",
        "PEN",
        "TYN",
        "HSQ",
        "HSC",
        "NTP",
        "CHA",
        "PIF",
        "ILA",
        "KEE",
        "CYQ",
        "CYI",
        "NTO",
    ],
}

city_df = pd.DataFrame(city_dict)

# 處理作物的部分
crop_dict = {
    "crop_name": [
        "番茄",
        "木瓜",
        "百香果",
        "西瓜",
        "李",
        "芒果",
        "枇杷",
        "文旦柚",
        "白柚",
        "柿",
        "洋香瓜",
        "香蕉",
        "桃",
        "草莓",
        "荔枝",
        "桶柑",
        "梅",
        "梨",
        "香瓜",
        "柳橙",
        "棗",
        "椪柑",
        "番石榴",
        "可可椰子",
        "楊桃",
        "溫州蜜柑",
        "葡萄",
        "葡萄柚",
        "鳳梨",
        "蓮霧",
        "橄欖",
        "龍眼",
        "檸檬",
        "蘋果",
        "番荔枝",
    ],
    "crop_id": [
        "72",
        "I1",
        "51",
        "T1",
        "N3",
        "R1",
        "L1",
        "H1",
        "H2",
        "Z4",
        "W1",
        "A1",
        "Y1",
        "45",
        "J1",
        "D1",
        "41",
        "O10",
        "V1",
        "E1",
        "22",
        "C1",
        "P1",
        "11",
        "M3",
        "C5",
        "S1",
        "H4",
        "B2",
        "Q1",
        "G7",
        "K3",
        "F1",
        "X69",
        "31",
    ],
}

crop_df = pd.DataFrame(crop_dict)

# 處理海拔的部分
altitude_dict = {
    "crop_name": [
        "蘋果",
        "梨",
        "梅",
        "桃",
        "柿",
        "李",
        "龍眼",
        "鳳梨",
        "香蕉",
        "香瓜",
        "西瓜",
        "蓮霧",
        "葡萄柚",
        "葡萄",
        "荔枝",
        "草莓",
        "芒果",
        "百香果",
        "白柚",
        "番荔枝",
        "番茄",
        "番石榴",
        "溫州蜜柑",
        "洋香瓜",
        "檸檬",
        "橄欖",
        "楊桃",
        "椪柑",
        "棗",
        "桶柑",
        "柳橙",
        "枇杷",
        "木瓜",
        "文旦柚",
        "可可椰子",
    ],
    "altitude": [
        "High",
        "High",
        "High",
        "High",
        "High",
        "High",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
        "Low",
    ],
}

altitude_df = pd.DataFrame(altitude_dict)


# === 正式開始撈取資料

# 停用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 建立一個空的Dataframe，來存放所有資料
df_sum = pd.DataFrame()

# 抓取並儲存每一種蔬果的資料
for name in vegetables:
    try:
        for id, kind in unit_id_list.items():
            filter_str = f"年度 gt {year_cutoff} and {kind}類別 like {name}"
            encoded = urllib.parse.quote(filter_str, safe="")

            url = f"https://data.moa.gov.tw/Service/OpenData/DataFileService.aspx?UnitId={id}&$top=1000&$skip=0&$filter={encoded}"

            # 加入 verify=False 停用 SSL 驗證
            r = requests.get(url, timeout=30, verify=False)
            data = r.json()

            if data:
                df = pd.DataFrame(data)
                fname = f"{output_dir_path}/{year_cutoff}-2024年{name}.csv"
                # 處理欄位名稱
                if kind == "蔬菜":
                    df = df.rename(
                        columns={
                            "種植面積": "種植面積_公頃",
                            "收穫面積": "收穫面積_公頃",
                            "產量": "產量_公噸",
                        }
                    )
                # 篩選需要的欄位
                df = df[
                    [
                        "年度",
                        "地區別",
                        f"{kind}類別",
                        "種植面積_公頃",
                        "收穫面積_公頃",
                        "產量_公噸",
                    ]
                ]
                df = df.rename(columns={f"{kind}類別": "蔬果類別"})  # 統一欄位名稱
                # 只篩選需要的名稱，避免不同水果被抓進來(ex:要抓葡萄，但葡萄柚也被抓進來)
                df = df[df["蔬果類別"] == name]
                df_sum = pd.concat([df_sum, df])  # 合併資料表進入df_sum
                print(f"{name}在{kind}類別已匯入總表")
            else:
                print(f"{name}在{kind}類別無資料")
    except Exception as e:
        print(f"發生錯誤：{name} → {e}")

if len(df_sum.index) > 0:
    df_sum = df_sum.sort_values(by=["年度", "蔬果類別", "地區別"], ascending=False)
    # 使用inner join匯入city_id, crop_id
    df_sum = df_sum.merge(city_df, left_on="地區別", right_on="city_name", how="inner")
    df_sum = df_sum.merge(
        crop_df, left_on="蔬果類別", right_on="crop_name", how="inner"
    )
    df_sum = df_sum.merge(
        altitude_df, left_on="蔬果類別", right_on="crop_name", how="inner"
    )
    # 將欄位名稱重新命名
    df_sum = df_sum.rename(
        columns={
            "年度": "year",
            "種植面積_公頃": "planted_area",
            "收穫面積_公頃": "harvested_area",
            "產量_公噸": "production",
        }
    )
    # 只篩選需要的欄位
    df_sum = df_sum[
        [
            "year",
            "city_id",
            "crop_id",
            "altitude",
            "planted_area",
            "harvested_area",
            "production",
        ]
    ]
    # - 代表空值，故改填入Nan
    df_sum = df_sum.replace("-", np.nan)
    # 存檔
    df_sum.to_csv(
        f"{output_dir_path}/import_data_area_production.csv",
        index=False,
        encoding="utf-8-sig",
    )
    print("已完成存檔!")
