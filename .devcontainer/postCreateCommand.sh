#!/bin/bash

# 設定腳本在任何命令失敗時立即退出
set -e

echo "--- 執行 Dev Container 建立後腳本 (postCreateCommand) ---"

# --- 1. 配置 Poetry 虛擬環境路徑 ---
# 設置 Poetry 將虛擬環境建立在專案目錄內（.venv/），而非全域緩存中。
# 這使 Dev Container 啟動時虛擬環境更易於定位和管理。
echo "1. 配置 Poetry: 確保虛擬環境建立在專案目錄 (.venv) 內..."
poetry config virtualenvs.in-project true --local 

# --- 2. 安裝或同步 Poetry 依賴 ---
# 使用 --no-root 避免在開發環境中安裝專案本身 (如果它不是一個庫)
# 使用 --sync 確保虛擬環境與 poetry.lock 完全同步
echo "2. 安裝或同步專案依賴 (根據 poetry.lock)..."
if [ -f "poetry.lock" ]; then
    poetry install --no-root --sync
else
    echo "警告: 找不到 poetry.lock 檔案。正在嘗試根據 pyproject.toml 進行安裝。"
    poetry install --no-root
fi

# --- 3. (可選) 執行其他環境設定或初始化 ---
# 如果您在 Dockerfile 中安裝了 Zsh/Oh My Zsh，可能需要進行一些設定，
# 但通常 Zsh 的安裝指令在 Dockerfile 運行後已經生效。

# 範例: 如果您需要設定 Git 身份 (若未從主機掛載 .gitconfig)
# echo "3. 檢查或設定 Git 身份..."
# git config --global user.name "Your Name"
# git config --global user.email "your.email@example.com"

# 範例: 如果專案需要運行資料庫遷移
# echo "3. 執行資料庫遷移 (如果適用)..."
# poetry run python your_project/manage.py migrate


echo "--- Dev Container 環境準備完成！ ---"