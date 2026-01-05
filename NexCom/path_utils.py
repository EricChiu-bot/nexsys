from pathlib import Path
import sys

# 自動偵測專案根目錄
# 假設此檔案位置固定在 [PROJECT_ROOT]/NexCom/path_utils.py
# 所以 .parent = NexCom, .parent.parent = PROJECT_ROOT
PROJECT_ROOT = Path(__file__).resolve().parent.parent

def add_project_root_to_sys_path():
    """確保專案根目錄在 sys.path 中，解決 import 問題"""
    root_str = str(PROJECT_ROOT)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
