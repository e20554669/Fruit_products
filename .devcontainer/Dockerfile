FROM python:3.12-slim-bullseye

ENV TZ=Asia/Taipei
ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"

# --- 1. 安裝與配置 (單一高效 RUN 層) ---
RUN apt-get update && \
    apt-get install -y --no-install-recommends git zsh vim curl procps gcc python3-dev && \
    \
    # 時區設定
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    \
    # 安裝 Poetry
    pip install pipx && \
    pipx install poetry && \
    \
    # 執行物理禁用 pip 的操作
    if [ -f /usr/local/bin/pip ]; then \
        mv /usr/local/bin/pip /usr/local/bin/pip_disabled_by_docker ; \
    fi && \
    if [ -f /usr/local/bin/pip3 ]; then \
        mv /usr/local/bin/pip3 /usr/local/bin/pip3_disabled_by_docker ; \
    fi && \
    echo '#!/bin/bash\necho "錯誤：請使用 Poetry 管理依賴，而不是 pip！" > /usr/local/bin/pip && exit 1' > /usr/local/bin/pip && \
    chmod +x /usr/local/bin/pip && \
    \
    # 安裝 Oh My Zsh
    echo "Y" | sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" && \
    \
    # 🌟 關鍵修正：將 Poetry PATH 寫入 Zsh 設定檔 (.zshrc) 🌟
    echo 'export POETRY_HOME="/opt/poetry"' >> /root/.zshrc && \
    echo 'export PATH="$POETRY_HOME/bin:$PATH"' >> /root/.zshrc && \
    \
    # 清理以縮小映像檔大小
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- 2. 複製 Poetry 依賴文件（不需安裝，留給 postCreateCommand.sh）---
COPY pyproject.toml poetry.lock ./

# --- 3. 複製專案程式碼 ---
COPY . .