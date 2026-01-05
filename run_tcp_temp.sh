#!/bin/bash

# 取得腳本所在目錄的絕對路徑 (專案根目錄)
PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

# 檢查虛擬環境是否存在
if [ -f ".venv/bin/activate" ]; then
    # 使用虛擬環境的 Python 執行 workers
    echo "[INFO] Using virtual environment: .venv"
    "$PROJECT_ROOT/.venv/bin/python" "$PROJECT_ROOT/NexWorkers/tcp_temp.py"
else
    echo "[ERROR] Virtual environment not found!"
    echo "Please set up first:"
    echo "  python3 -m venv .venv"
    echo "  .venv/bin/pip install -r requirements.txt"
    exit 1
fi