from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


def load_env(script_file: str) -> Path:
    """
    依序嘗試載入 .env：
    1) ENV_FILE（最優先）
    2) 與 script 同資料夾：<stem>.env
    3) 專案根（script 上一層）：<stem>.env
    4) 專案根：.env
    """
    script_path = Path(script_file).resolve()
    here = script_path.parent
    root = here.parent
    stem_env_here = here / f"{script_path.stem}.env"
    stem_env_root = root / f"{script_path.stem}.env"
    dot_env_root = root / ".env"

    candidates = [
        os.environ.get("ENV_FILE"),
        str(stem_env_here),
        str(stem_env_root),
        str(dot_env_root),
    ]

    for p in candidates:
        if p and Path(p).exists():
            load_dotenv(dotenv_path=p, override=True)
            return Path(p)

    raise RuntimeError("找不到 .env：請設定 ENV_FILE 或提供 <stem>.env / 專案根 .env")


def required(name: str) -> str:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"缺少必要環境變數：{name}")
    return str(v).strip()


def get_int(name: str, default: int | None = None) -> int:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        if default is None:
            raise RuntimeError(f"缺少必要環境變數：{name}")
        return default
    return int(str(v).strip())


def get_float(name: str, default: float | None = None) -> float:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        if default is None:
            raise RuntimeError(f"缺少必要環境變數：{name}")
        return default
    return float(str(v).strip())


def get_json(name: str) -> Any:
    return json.loads(required(name))


def get_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return default
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")
