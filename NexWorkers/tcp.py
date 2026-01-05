from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


import logging
import signal
import time

# noqa: E402
from NexCom.env_loader import get_float, get_int, get_json, load_env, required
from NexCom.envelope_builder import build_telemetry_v1, utc_iso_now
from NexDrv.xModbusRtuTcp import ModbusRtuTcpDriver, RtuParams
from NexPub.xRabbitPublisher import RabbitPublisher
from NexTrans.xTransformer import calc_read_count, transform_registers

# 優雅停止
_STOP = False


def _graceful(*_):
    global _STOP
    _STOP = True


def main() -> int:
    env_path = load_env(__file__)
    print("[ENV] loaded:", env_path)

    # ---- logging ----
    log_level = (
        required("LOG_LEVEL").upper()
        if "LOG_LEVEL" in __import__("os").environ
        else "ERROR"
    )
    logging.basicConfig(
        level=getattr(logging, log_level, logging.ERROR),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    log = logging.getLogger("NexWorkers.tcp")

    # ---- basic env ----
    app_env = __import__("os").environ.get("APP_ENV", "dev")
    site_uid = required("SITE_UID")
    device_uid = required("DEVICE_UID")
    worker_name = __import__("os").environ.get("WORKER_NAME", "rtu_tcp_rmq")
    worker_version = __import__("os").environ.get("WORKER_VERSION", "1.0.0")
    read_interval_sec = get_float("READ_INTERVAL_SEC")

    # ---- modbus env ----
    host = required("MODBUS_HOST")
    port = get_int("MODBUS_PORT")
    slave_id = get_int("MODBUS_SLAVE_ID")
    fc = get_int("MODBUS_FUNCTION_CODE", 3)

    rtu = RtuParams(
        baudrate=get_int("RTU_BAUD"),
        bytesize=get_int("RTU_BYTESIZE"),
        parity=required("RTU_PARITY"),
        stopbits=get_int("RTU_STOPBITS"),
        timeout=get_float("RTU_TIMEOUT"),
    )

    mapping = get_json("MODBUS_REGISTERS_MAPPING")
    read_count = calc_read_count(mapping)

    # ---- publisher env ----
    # [Robust Design] 自動偵測路徑，若 env 沒設定或路徑不存在(跨平台)就用預設值
    default_lib_root = str(PROJECT_ROOT)
    mnx_lib_root = __import__("os").environ.get("MNX_LIB_ROOT")

    # 如果沒設定，或者設定的路徑不存在 (例如 .env 裡是 Windows 路徑但現在跑在 Mac)
    if not mnx_lib_root or not Path(mnx_lib_root).exists():
        mnx_lib_root = default_lib_root

    # 若沒設定 RMQ_CFG_PATH，或者路徑不存在，就預設在 NexCore 底下找
    rmq_cfg_path = __import__("os").environ.get("RMQ_CFG_PATH")
    if not rmq_cfg_path or not Path(rmq_cfg_path).exists():
        rmq_cfg_path = str(Path(mnx_lib_root) / "NexCore" / "rabbitmq_cfg.json")

    # ---- init components ----
    driver = ModbusRtuTcpDriver(host, port, rtu)
    publisher = RabbitPublisher(mnx_lib_root=mnx_lib_root, rmq_cfg_path=rmq_cfg_path)

    log.info("START env=%s site=%s device=%s", app_env, site_uid, device_uid)

    while not _STOP:
        try:
            regs = driver.read_registers(
                fc=fc, address=0, count=read_count, unit=slave_id
            )
            if regs:
                payload = transform_registers(mapping, regs)
                ts = utc_iso_now()
                doc = build_telemetry_v1(
                    ts=ts,
                    site_uid=site_uid,
                    device_uid=device_uid,
                    worker_name=worker_name,
                    worker_version=worker_version,
                    payload=payload,
                )
                publisher.publish(doc)
        except Exception as e:
            log.error("Loop error: %s", e)
        finally:
            time.sleep(read_interval_sec)

    log.info("STOP worker")
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)
    raise SystemExit(main())
