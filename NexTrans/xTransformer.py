from __future__ import annotations

from typing import Any, Dict, List


def calc_read_count(mapping: List[Dict[str, Any]]) -> int:
    """
    依 mapping 自動算需要讀多少 registers（含 combine 的 high_index）
    """
    if not mapping:
        raise ValueError("MODBUS_REGISTERS_MAPPING is empty")

    max_index = max(m["index"] for m in mapping)
    if any("combine" in m for m in mapping):
        max_index = max(
            max_index,
            max(m["combine"]["high_index"] for m in mapping if "combine" in m),
        )
    return int(max_index) + 1


def transform_registers(
    mapping: List[Dict[str, Any]], regs: List[int]
) -> Dict[str, Any]:
    """
    raw registers -> payload metrics
    支援：
      - ignore: true
      - combine: {"high_index": X, "shift": 16}
      - unit/decimals
    """
    combined: Dict[str, int] = {}

    # 先做 32-bit combine
    for m in mapping:
        if "combine" in m:
            key = m["key"]
            low = regs[m["index"]]
            high = regs[m["combine"]["high_index"]]
            shift = int(m["combine"].get("shift", 16))
            combined[key] = (high << shift) | low

    out: Dict[str, Any] = {}

    for m in mapping:
        if m.get("ignore"):
            continue

        key = m["key"]
        if "combine" in m:
            raw = combined[key]
        else:
            raw = regs[m["index"]]

        unit = float(m.get("unit", 1.0))
        decimals = int(m.get("decimals", 0))
        out[key] = round(float(raw) * unit, decimals)

    return out


def transform_json(payload: dict, cfg: dict) -> Dict[str, Any]:
    """
    MQTT 專用：直接轉發 JSON payload，或者在此做欄位對應
    """
    # 這裡可以實作欄位過濾或重命名，目前先全部轉發
    return payload
