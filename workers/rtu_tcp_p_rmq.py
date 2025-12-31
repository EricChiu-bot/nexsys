# ~/iot/src/workers/rtu_tcp_rmq.py
from __future__ import annotations

import os, sys, time, json, logging, signal, datetime
from pathlib import Path

from pymodbus.client.sync import ModbusSerialClient
import pika
from dotenv import load_dotenv

# -------------------------------------------------------
# load .env
# -------------------------------------------------------
def _load_env():
    here = Path(__file__).resolve().parent
    candidates = [
        os.environ.get("ENV_FILE"),
        here / (Path(__file__).stem + ".env"),
        here.parent / ".env",
    ]
    for p in candidates:
        if p and Path(p).exists():
            load_dotenv(dotenv_path=p, override=True)
            print("[ENV] loaded:", p)
            return
    raise RuntimeError("找不到 .env")

_load_env()

# -------------------------------------------------------
# utils
# -------------------------------------------------------
def _required(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"缺少必要環境變數：{name}")
    return v

# -------------------------------------------------------
# MNXNexlib / RabbitMQ
# -------------------------------------------------------
MNX_LIB_ROOT = _required("MNX_LIB_ROOT")
if not Path(MNX_LIB_ROOT).exists():
    raise RuntimeError(f"MNX_LIB_ROOT 不存在：{MNX_LIB_ROOT}")

if MNX_LIB_ROOT not in sys.path:
    sys.path.append(MNX_LIB_ROOT)

from NexSig.xRabbit import RabbitMq, ExChangeType, MsgAdapter  # type: ignore

# -------------------------------------------------------
# Logger
# -------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "ERROR").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("rtu_tcp_rmq")

# -------------------------------------------------------
# Basic env
# -------------------------------------------------------
APP_ENV        = os.environ.get("APP_ENV", "dev")
SITE_UID       = _required("SITE_UID")
DEVICE_UID     = _required("DEVICE_UID")
WORKER_NAME    = os.environ.get("WORKER_NAME", "rtu_tcp_rmq")
WORKER_VERSION = os.environ.get("WORKER_VERSION", "1.0.0")

READ_INTERVAL_SEC = float(_required("READ_INTERVAL_SEC"))

# -------------------------------------------------------
# Modbus TCP / RTU-over-TCP
# -------------------------------------------------------
MODBUS_HOST     = _required("MODBUS_HOST")
MODBUS_PORT     = int(_required("MODBUS_PORT"))
MODBUS_SLAVE_ID = int(_required("MODBUS_SLAVE_ID"))
MODBUS_FC       = int(os.environ.get("MODBUS_FUNCTION_CODE", "3"))

# RTU params（完全照你的 .env 命名）
RTU_BAUD     = int(_required("RTU_BAUD"))
RTU_BYTESIZE = int(_required("RTU_BYTESIZE"))
RTU_PARITY   = _required("RTU_PARITY")
RTU_STOPBITS = int(_required("RTU_STOPBITS"))
RTU_TIMEOUT  = float(_required("RTU_TIMEOUT"))

# -------------------------------------------------------
# Register mapping
# -------------------------------------------------------
MODBUS_REGISTERS_MAPPING = json.loads(_required("MODBUS_REGISTERS_MAPPING"))

# 自動算 READ_COUNT（含 32-bit combine）
max_index = max(m["index"] for m in MODBUS_REGISTERS_MAPPING)
if any("combine" in m for m in MODBUS_REGISTERS_MAPPING):
    max_index = max(
        max_index,
        max(m["combine"]["high_index"] for m in MODBUS_REGISTERS_MAPPING if "combine" in m),
    )
READ_COUNT = max_index + 1

# -------------------------------------------------------
# RabbitMQ config
# -------------------------------------------------------
RMQ_CFG_PATH = _required("RMQ_CFG_PATH")

_STOP = False
_NEX_MQ: RabbitMq | None = None

def _graceful(*_):
    global _STOP
    _STOP = True

signal.signal(signal.SIGTERM, _graceful)
signal.signal(signal.SIGINT, _graceful)

# -------------------------------------------------------
# MNXNexlib sender
# -------------------------------------------------------
class MqSend(RabbitMq):
    def __init__(self, _host, _port, _exchange, _exType: ExChangeType,
                 _queue, _virtualHost, _user, _pwd,
                 _routeKey, _msgAdapter, _procQueue, _mutex):
        self.queue = _procQueue
        self.lock = _mutex
        super().__init__(
            _host, _port, _exchange, _exType,
            _queue, _virtualHost, _user, _pwd,
            _routeKey, _msgAdapter
        )

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
        mqCfg[0]['mqHost'], mqCfg[0]['mqPort'],
        mqCfg[0]['mqExchange'], exType,
        mqCfg[0]['mqQueue'], mqCfg[0]['mqVirtualHost'],
        mqCfg[0]['mqUser'], mqCfg[0]['mqPwd'],
        mqCfg[0]['mqRouteKey'],
        MsgAdapter.blockingConn, Queue(), Lock()
    )
    mq.isReceive = False

    connStr = []
    for node in mqCfg:
        if node['mqVirtualHost'] == '/':
            connStr.append(
                f"amqp://{node['mqUser']}:{node['mqPwd']}@{node['mqHost']}:{node['mqPort']}/"
            )
        else:
            connStr.append(
                f"amqp://{node['mqUser']}:{node['mqPwd']}@{node['mqHost']}:{node['mqPort']}/{node['mqVirtualHost']}"
            )

    mq.start(connStr)
    return mq

# -------------------------------------------------------
# Modbus read
# -------------------------------------------------------
def read_modbus() -> dict | None:
    cli = ModbusSerialClient(
        method="rtu",
        port=f"socket://{MODBUS_HOST}:{MODBUS_PORT}",
        baudrate=RTU_BAUD,
        bytesize=RTU_BYTESIZE,
        parity=RTU_PARITY,
        stopbits=RTU_STOPBITS,
        timeout=RTU_TIMEOUT,
    )

    if not cli.connect():
        log.error("無法連線 Modbus Gateway %s:%s", MODBUS_HOST, MODBUS_PORT)
        return None

    try:
        if MODBUS_FC == 3:
            rr = cli.read_holding_registers(0, READ_COUNT, unit=MODBUS_SLAVE_ID)
        elif MODBUS_FC == 4:
            rr = cli.read_input_registers(0, READ_COUNT, unit=MODBUS_SLAVE_ID)
        else:
            raise ValueError(f"Unsupported MODBUS_FUNCTION_CODE={MODBUS_FC}")

        if rr.isError():
            log.error("Modbus read error: %s", rr)
            return None

        regs = rr.registers
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

        metrics = {"ts": ts}

        # 32-bit combine
        combined = {}
        for m in MODBUS_REGISTERS_MAPPING:
            if "combine" in m:
                low = regs[m["index"]]
                high = regs[m["combine"]["high_index"]]
                combined[m["key"]] = (high << m["combine"].get("shift", 16)) | low

        for m in MODBUS_REGISTERS_MAPPING:
            if m.get("ignore"):
                continue

            raw = combined[m["key"]] if "combine" in m else regs[m["index"]]
            metrics[m["key"]] = round(float(raw) * m["unit"], m["decimals"])

        return metrics

    finally:
        try:
            cli.close()
        except Exception:
            pass

# -------------------------------------------------------
# Transport
# -------------------------------------------------------
def build_transport(metrics: dict) -> dict:
    return {
        "envelope": {
            "schema_version": "telemetry.v1",
            "ts": metrics["ts"],
            "site_uid": SITE_UID,
            "device_uid": DEVICE_UID,
            "worker_name": WORKER_NAME,
            "worker_version": WORKER_VERSION,
            "status": "OK",
        },
        "payload": {k: v for k, v in metrics.items() if k != "ts"},
    }

# -------------------------------------------------------
# Publish
# -------------------------------------------------------
def publish_rmq(doc: dict) -> None:
    global _NEX_MQ
    if _NEX_MQ is None:
        _NEX_MQ = _init_nex_mq(RMQ_CFG_PATH)
        log.info("RabbitMQ init (%s)", RMQ_CFG_PATH)

    _NEX_MQ.publish_msg(json.dumps(doc, ensure_ascii=False))

# -------------------------------------------------------
# Main
# -------------------------------------------------------
def main() -> int:
    log.info("START env=%s site=%s device=%s", APP_ENV, SITE_UID, DEVICE_UID)

    while not _STOP:
        try:
            m = read_modbus()
            if m:
                publish_rmq(build_transport(m))
        except Exception as e:
            log.error("Loop error: %s", e)
        finally:
            time.sleep(READ_INTERVAL_SEC)

    log.info("STOP worker")
    return 0

if __name__ == "__main__":
    sys.exit(main())
