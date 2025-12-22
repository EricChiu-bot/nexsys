# rtu_tcp_p_rmq.py
from __future__ import annotations

import os
import sys
import time
import json
import logging
import signal
import datetime
from pathlib import Path

from pymodbus.client.sync import ModbusSerialClient
import pika
from dotenv import load_dotenv


##改讀哪個檔案<目的是我要走哪個MQ
MQ_CFG_FILE = "./NexCore/rabbitmq_cfg_p.json"

# ── 自動載入 .env（優先用 ENV_FILE 指定的檔） ──
def _load_env():
    candidates = [
        os.environ.get("ENV_FILE"),
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parents[1] / ".env",
        Path(__file__).resolve().parents[2] / ".env",
        Path.cwd() / ".env",
    ]
    for p in candidates:
        if p and Path(p).exists():
            load_dotenv(dotenv_path=p, override=False)
            break

_load_env()

# ── MNXNexlib Rabbit wrapper ──
_MNX_LIB_ROOT = os.environ.get("MNX_LIB_ROOT")
if not _MNX_LIB_ROOT:
    here = Path(__file__).resolve().parent
    candidates = [
        here / "MNXNexlib-main",
        here.parent / "MNXNexlib-main",
        Path.home() / "iot" / "MNXNexlib-main",
    ]
    for c in candidates:
        if c.exists():
            _MNX_LIB_ROOT = str(c)
            break

if not _MNX_LIB_ROOT:
    raise RuntimeError("找不到 MNXNexlib-main 路徑，請設定環境變數 MNX_LIB_ROOT 或把專案放在同層。")

if _MNX_LIB_ROOT not in sys.path:
    sys.path.append(_MNX_LIB_ROOT)

from NexSig.xRabbit import RabbitMq, ExChangeType, MsgAdapter  # type: ignore

# ── Logger ──
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("power_meter_to_rmq")

# ── 基本環境 ──
def _required(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"缺少必要環境變數：{name}")
    return v

APP_ENV     = os.environ.get("APP_ENV", "dev")
SITE_CODE   = _required("SITE_CODE")
DEVICE_CODE = _required("DEVICE_CODE")
GATEWAY_ID  = os.environ.get("GATEWAY_ID")  # 可空

# ── Modbus (Serial RTU，走 USB-RS485 COM3) ──
SERIAL_PORT = os.environ.get("SERIAL_PORT")
if not SERIAL_PORT:
    raise RuntimeError("缺少 SERIAL_PORT，請在 .env_power 設定，例如 SERIAL_PORT=COM3")

MODBUS_UNIT       = int(os.environ.get("MODBUS_UNIT", "1"))
READ_INTERVAL_SEC = float(os.environ.get("READ_INTERVAL_SEC", "1"))

# PZEM-003 建議：9600, 8N2
BAUDRATE = int(os.environ.get("MODBUS_BAUD", "9600"))
BYTESIZE = int(os.environ.get("MODBUS_BYTESIZE", "8"))
PARITY   = os.environ.get("MODBUS_PARITY", "N")
STOPBITS = int(os.environ.get("MODBUS_STOPBITS", "2"))
TIMEOUT  = float(os.environ.get("MODBUS_TIMEOUT", "1.0"))

# ── RabbitMQ config ──
_RMQ_CFG_PATH = os.environ.get("RMQ_CFG_PATH") or str(
    Path(_MNX_LIB_ROOT) / "NexCore" / "rabbitmq_cfg_p.json"
)

_STOP = False
_NEX_MQ: RabbitMq | None = None

def _graceful(*_):
    global _STOP
    _STOP = True

signal.signal(signal.SIGTERM, _graceful)
signal.signal(signal.SIGINT, _graceful)

# ── MNXNexlib: MqSend 發佈器 ──
class MqSend(RabbitMq):
    def __init__(self, _host, _port, _exchange, _exType: ExChangeType,
                 _queue, _virtualHost, _user, _pwd,
                 _routeKey, _msgAdapter, _procQueue, _mutex):
        self.queue = _procQueue
        self.lock = _mutex
        super().__init__(_host, _port, _exchange, _exType,
                         _queue, _virtualHost, _user, _pwd,
                         _routeKey, _msgAdapter)

    def publish_msg(self, _msg: str):
        headers = {'CMD': 'flush'}
        props = pika.BasicProperties(
            content_type='text/plain',
            delivery_mode=2,
            headers=headers
        )
        self._publish_msg(_msg, props)

def _init_nex_mq(cfg_path: str) -> MqSend:
    import orjson
    from queue import Queue
    from threading import Lock

    with open(cfg_path, "rb") as f:
        mqCfg = orjson.loads(f.read())

    ex_str = str(mqCfg[0].get('mqExType', 'direct')).lower()
    ex_map = {
        'fanout': ExChangeType.fanout,
        'direct': ExChangeType.direct,
        'topic': ExChangeType.topic,
        'headers': ExChangeType.headers,
    }
    exType = ex_map.get(ex_str, ExChangeType.direct)

    mq = MqSend(
        mqCfg[0]['mqHost'], mqCfg[0]['mqPort'], mqCfg[0]['mqExchange'], exType,
        mqCfg[0]['mqQueue'], mqCfg[0]['mqVirtualHost'],
        mqCfg[0]['mqUser'], mqCfg[0]['mqPwd'],
        mqCfg[0]['mqRouteKey'],
        MsgAdapter.blockingConn, Queue(), Lock()
    )
    mq.isReceive = False

    # 支援多節點
    connStr = []
    for node in mqCfg:
        if node['mqVirtualHost'] == '/':
            connStr.append(
                'amqp://{name}:{pwd}@{host}:{port}/'.format(
                    name=node['mqUser'], pwd=node['mqPwd'],
                    host=node['mqHost'], port=node['mqPort']
                )
            )
        else:
            connStr.append(
                'amqp://{name}:{pwd}@{host}:{port}/{viHost}'.format(
                    name=node['mqUser'], pwd=node['mqPwd'],
                    host=node['mqHost'], port=node['mqPort'],
                    viHost=node['mqVirtualHost']
                )
            )
    mq.start(connStr)
    return mq

# ── 解析 PZEM-003 量測 ──
def _parse_power_regs(regs: list[int]) -> dict:
    """
    regs[0..7]:
      0: voltage (0.01 V)
      1: current (0.01 A)
      2,3: power (0.1 W, 32-bit, low16 at reg2, high16 at reg3)
      4,5: energy (1 Wh, 32-bit, low16 at reg4, high16 at reg5)
      6: high voltage alarm
      7: low  voltage alarm
    """
    if len(regs) < 8:
        raise ValueError(f"expect 8 regs, got {len(regs)}")

    voltage_v = regs[0] / 100.0
    current_a = regs[1] / 100.0

    power_raw  = (regs[3] << 16) | regs[2]
    power_w    = power_raw / 10.0

    energy_raw = (regs[5] << 16) | regs[4]
    energy_Wh  = float(energy_raw)

    high_alarm = regs[6]
    low_alarm  = regs[7]

    return {
        "voltage_V": voltage_v,
        "current_A": current_a,
        "power_W": power_w,
        "energy_Wh": energy_Wh,
        "high_voltage_alarm": high_alarm,
        "low_voltage_alarm": low_alarm,
    }

# ── Modbus 讀取（Serial RTU，COM3） ──
def read_modbus_power() -> dict | None:
    """
    從 0x0000 讀 8 個 Input Registers，解析成 power meter 資料。
    回傳 dict（含 ts）或 None。
    """
    cli = ModbusSerialClient(
        method="rtu",
        port=SERIAL_PORT,
        baudrate=BAUDRATE,
        bytesize=BYTESIZE,
        parity=PARITY,
        stopbits=STOPBITS,
        timeout=TIMEOUT,
    )

    if not cli.connect():
        log.error("無法連線 Serial Port %s", SERIAL_PORT)
        return None

    try:
        rr = cli.read_input_registers(address=0, count=8, unit=MODBUS_UNIT)
        if rr is None:
            log.error("讀取失敗: 回傳 None")
            return None
        if rr.isError():
            log.error("讀取失敗: %s", rr)
            return None

        regs = rr.registers
        ts_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        m = _parse_power_regs(regs)
        m["ts"] = ts_iso

        # ★ 在 publish 之前，把讀到的數值印出來給你看
        print("[MEASURE power_meter] regs =", regs, "parsed =", m, flush=True)

        return m


    finally:
        try:
            cli.close()
        except Exception:
            pass

# ── 封裝要送出的 JSON ──
def build_transport_power(metrics: dict) -> dict:
    return {
        "envelope": {
            "schema_version": "telemetry.v1",
            "ts": metrics["ts"],
            "site_code": SITE_CODE,
            "gateway_id": GATEWAY_ID,
            "device_code": DEVICE_CODE,
            "worker_name": "power_meter_poller",
            "worker_version": "0.1.0",
            "status": "OK",
        },
        "payload": {
            "voltage_V": metrics["voltage_V"],
            "current_A": metrics["current_A"],
            "power_W": metrics["power_W"],
            "energy_Wh": metrics["energy_Wh"],
            # "high_voltage_alarm": metrics["high_voltage_alarm"],
            # "low_voltage_alarm": metrics["low_voltage_alarm"],
        },
    }

# ── 發佈到 RabbitMQ ──
def publish_rmq(doc: dict) -> None:
    global _NEX_MQ
    if _NEX_MQ is None:
        _NEX_MQ = _init_nex_mq(_RMQ_CFG_PATH)
        log.info("RabbitMQ init by MNXNexlib (%s)", _RMQ_CFG_PATH)

    msg_text = json.dumps(doc, ensure_ascii=False)
    _NEX_MQ.publish_msg(msg_text)

# ── 主流程 ──
def main() -> int:
    log.info("START power_meter env=%s site=%s device=%s", APP_ENV, SITE_CODE, DEVICE_CODE)
    while not _STOP:
        try:
            m = read_modbus_power()
            if m:
                doc = build_transport_power(m)
                publish_rmq(doc)
        except Exception as e:
            log.error("Loop error: %s", e)
        finally:
            time.sleep(READ_INTERVAL_SEC)

    log.info("STOP power_meter worker")
    return 0

if __name__ == "__main__":
    sys.exit(main())
