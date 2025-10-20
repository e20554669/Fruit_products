#!/bin/bash

# 設定腳本在任何命令失敗時立即退出
set -e

echo "--- 執行 Dev Container 建立後腳本 (postCreateCommand) ---"

# 🌟 關鍵修正：確保 /root/.local/bin/poetry 的執行權限 🌟
# (如果檔案沒有執行權限，即使路徑正確也會 Exit 127)
chmod +x /root/.local/bin/poetry 

# Poetry 的絕對路徑 (繞過 $PATH 載入問題)
POETRY_BIN="/root/.local/bin/poetry"

# --- 1. 配置 Poetry 虛擬環境路徑 (使用絕對路徑) ---
echo "1. 配置 Poetry: 確保虛擬環境建立在專案目錄 (.venv) 內..."
# 確保 Poetry 在執行時有足夠的權限，並使用絕對路徑
"$POETRY_BIN" config virtualenvs.in-project true --local 

# --- 2. 安裝或同步 Poetry 依賴 (使用絕對路徑) ---
echo "2. 安裝或同步專案依賴 (根據 poetry.lock)..."
if [ -f "poetry.lock" ]; then
    "$POETRY_BIN" install --no-root --sync
else
    echo "警告: 找不到 poetry.lock 檔案。正在嘗試根據 pyproject.toml 進行安裝。"
    "$POETRY_BIN" install --no-root
fi

echo "--- Dev Container 環境準備完成！ ---"