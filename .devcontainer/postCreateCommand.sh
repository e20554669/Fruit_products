#!/bin/bash

# 設定腳本在任何命令失敗時立即退出
set -e

echo "--- 執行 Dev Container 建立後腳本 (postCreateCommand) ---"

# Poetry 的絕對路徑 (繞過 $PATH 載入問題)
POETRY_BIN="/root/.local/bin/poetry" 

# 🌟 關鍵：給予執行權限並確認 Poetry 可用 🌟
# 由於 Dockerfile 已經安裝了 Poetry，我們檢查並確保它是可執行的。
chmod +x "$POETRY_BIN" || true

# --- 1. 配置 Poetry 虛擬環境路徑 (使用絕對路徑) ---
echo "1. 配置 Poetry: 確保虛擬環境建立在專案目錄 (.venv) 內..."
# 確保 Poetry 在執行時有足夠的權限，並使用絕對路徑
"$POETRY_BIN" config virtualenvs.in-project true --local 

# --- 2. 安裝或同步 Poetry 依賴 (使用絕對路徑) ---
echo "2. 安裝或同步專案依賴 (根據 poetry.lock)..."
if [ -f "poetry.lock" ]; then
    "$POETRY_BIN" sync --no-root
else
    echo "警告: 找不到 poetry.lock 檔案。正在嘗試根據 pyproject.toml 進行安裝。"
    "$POETRY_BIN" install --no-root
fi

# --- 3. 新增：Airflow 自動初始化區塊
echo "3. 檢查並初始化 Airflow (Standalone 模式)..."

# 設定 Airflow 家目錄 (如果 Dockerfile 沒設，預設為 /app/airflow)
export AIRFLOW_HOME=${AIRFLOW_HOME:-/app/airflow}

# 1. 建立必要目錄
echo "  >> 建立 Airflow 目錄結構..."

mkdir -p "$AIRFLOW_HOME/dags"
mkdir -p "$AIRFLOW_HOME/logs"
mkdir -p "$AIRFLOW_HOME/utils"
mkdir -p "$AIRFLOW_HOME/tasks"

# 2. 檢查資料庫是否存在，避免重複初始化
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "  >> 初始化 SQLite 資料庫..."
    "$POETRY_BIN" run airflow db migrate

    echo "  >> 建立團隊專用帳號 (tjr103-team02)..."
    # 這裡改用您指定的團隊帳號，讓開發環境與 VM 一致
    "$POETRY_BIN" run airflow users create \
        --username tjr103-team02 \
        --firstname tjr103 \
        --lastname team02 \
        --role Admin \
        --email your_email@example.com \
        --password password
else
    echo "  >> Airflow 資料庫已存在，跳過初始化。"
fi

echo "--- Dev Container 環境準備完成！ ---"