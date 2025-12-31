# ~/iot/src/workers/rtu_rmp.py
from __future__ import annotations
import os, sys, time, json, logging, signal, datetime
from pathlib import Path

# ── Modbus (pymodbus 2.5.3 sync) ──
from pymodbus.client.sync import ModbusSerialClient
import pika

#debug
print("[ENV] RMQ_CFG_PATH exists =", Path(os.environ.get("RMQ_CFG_PATH","")).exists())

# --- load .env (自動尋找) ---
from dotenv import load_dotenv
from pathlib import Path

def _load_env():
    here = Path(__file__).resolve().parent

    candidates = [
        os.environ.get("ENV_FILE"),                 # 最高優先：env
        here / (Path(__file__).stem + ".env"),      # workers/rtu_rmq.env（同名）
        here.parent / ".env",                       # 專案根 iot/.env（全域預設）
    ]
    for p in candidates:
        if p and Path(p).exists():
            load_dotenv(dotenv_path=p, override=True)
            #debug
            print("[ENV] loaded:", p)
            print("[ENV] SITE_UID =", os.environ.get("SITE_UID"))
            return
    raise RuntimeError("找不到 .env：請設定 ENV_FILE 或提供 workers/<worker>.env 或專案根 .env")

_load_env()

# ── 基本環境 ──
def _required(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"缺少必要環境變數：{name}")
    return v

# ── MNXNexlib Rabbit wrapper ──
#   優先讀環境變數 MNX_LIB_ROOT，否則嘗試常見路徑
_MNX_LIB_ROOT = _required("MNX_LIB_ROOT")   # 只信 env

# 可選：順便檢查資料夾是否存在（比較好 debug）
if not Path(_MNX_LIB_ROOT).exists():
    raise RuntimeError(f"MNX_LIB_ROOT 路徑不存在：{_MNX_LIB_ROOT}")

if _MNX_LIB_ROOT not in sys.path:
    sys.path.append(_MNX_LIB_ROOT)


from NexSig.xRabbit import RabbitMq, ExChangeType, MsgAdapter  # type: ignore

# ── Logger ──
LOG_LEVEL = os.environ.get("LOG_LEVEL", "ERROR").upper() 
# LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper() #測試，後面改回來

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("modbus_to_rmq")



APP_ENV     = os.environ.get("APP_ENV", "dev")
SITE_UID   = _required("SITE_UID")
DEVICE_UID = _required("DEVICE_UID")
WORKER_NAME = os.environ.get("WORKER_NAME")
WORKER_VERSION = os.environ.get("WORKER_VERSION")




# ── Modbus (RTU over TCP) ──
MODBUS_HOST       = _required("MODBUS_HOST")
MODBUS_PORT       = int(_required("MODBUS_PORT"))
MODBUS_SLAVE_ID = int(_required("MODBUS_SLAVE_ID")) 
READ_INTERVAL_SEC = float(_required("READ_INTERVAL_SEC"))

#RTU
BAUDRATE = int(os.environ.get("RTU_BAUD"))
BYTESIZE = int(os.environ.get("RTU_BYTESIZE"))
PARITY   = os.environ.get("RTU_PARITY")
STOPBITS = int(os.environ.get("RTU_STOPBITS"))
TIMEOUT  = float(os.environ.get("RTU_TIMEOUT"))

# ── 新增：JSON mapping ──
MODBUS_REGISTERS_MAPPING = json.loads(_required("MODBUS_REGISTERS_MAPPING"))

# ── RabbitMQ（Use MNXNexlib config） ──
# 讀 MNXNexlib 的設定檔（與 test_send.py 同一份）
_RMQ_CFG_PATH = _required("RMQ_CFG_PATH")

# ── 內部狀態 ──
_STOP = False
_NEX_MQ: RabbitMq | None = None

def _graceful(*_):
    global _STOP
    _STOP = True

signal.signal(signal.SIGTERM, _graceful)
signal.signal(signal.SIGINT, _graceful)

# ── MNXNexlib: 建立 MqSend 發佈器（與 test_send 同行為） ──
class MqSend(RabbitMq):
    def __init__(self, _host, _port, _exchange, _exType: ExChangeType,
                 _queue, _virtualHost, _user, _pwd,
                 _routeKey, _msgAdapter, _procQueue, _mutex):
        # test_send 的寫法：保存 queue/mutex，呼叫父類完整建構子
        self.queue = _procQueue
        self.lock = _mutex
        super().__init__(_host, _port, _exchange, _exType,
                         _queue, _virtualHost, _user, _pwd,
                         _routeKey, _msgAdapter)

    def publish_msg(self, _msg: str):
        # 與 test_send 相同：content_type='text/plain', delivery_mode=2, headers={'CMD':'flush'}
        headers = {'CMD': 'flush'}
        props = pika.BasicProperties(content_type='text/plain', delivery_mode=2, headers=headers)
        self._publish_msg(_msg, props)

def _init_nex_mq(cfg_path: str) -> MqSend:
    import orjson
    from queue import Queue
    from threading import Lock

    with open(cfg_path, "rb") as f:
        mqCfg = orjson.loads(f.read())

    # 解析 exchange type
    ex_str = str(mqCfg[0].get('mqExType', 'direct')).lower()
    ex_map = {'fanout': ExChangeType.fanout, 'direct': ExChangeType.direct, 'topic': ExChangeType.topic, 'headers': ExChangeType.headers}
    exType = ex_map.get(ex_str, ExChangeType.direct)

    mq = MqSend(
        mqCfg[0]['mqHost'], mqCfg[0]['mqPort'], mqCfg[0]['mqExchange'], exType,
        mqCfg[0]['mqQueue'], mqCfg[0]['mqVirtualHost'],
        mqCfg[0]['mqUser'], mqCfg[0]['mqPwd'],
        mqCfg[0]['mqRouteKey'],
        MsgAdapter.blockingConn, Queue(), Lock()
    )
    mq.isReceive = False

    # 支援多節點連線
    connStr = []
    for node in mqCfg:
        if node['mqVirtualHost'] == '/':
            connStr.append('amqp://{name}:{pwd}@{host}:{port}/'.format(
                name=node['mqUser'], pwd=node['mqPwd'], host=node['mqHost'], port=node['mqPort']))
        else:
            connStr.append('amqp://{name}:{pwd}@{host}:{port}/{viHost}'.format(
                name=node['mqUser'], pwd=node['mqPwd'], host=node['mqHost'], port=node['mqPort'], viHost=node['mqVirtualHost']))
    mq.start(connStr)
    return mq

# ── Modbus 讀取 ──
def read_modbus() -> dict | None:

    cli = ModbusSerialClient(
        method="rtu",
        port=f"socket://{MODBUS_HOST}:{MODBUS_PORT}",
        baudrate=BAUDRATE,
        bytesize=BYTESIZE,
        parity=PARITY,
        stopbits=STOPBITS,
        timeout=TIMEOUT
    )
    if not cli.connect():
        log.error("無法連線 Modbus Gateway %s:%s", MODBUS_HOST, MODBUS_PORT)
        return None

    try:
        rr = cli.read_holding_registers(address= 0, count=2, unit=MODBUS_SLAVE_ID)
        if rr.isError():
            rr = cli.read_input_registers(address=0, count=2, unit=MODBUS_SLAVE_ID)

        if rr.isError():
            log.error("讀取失敗: %s", rr)
            return None

        registers = rr.registers
        device_ts_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        metrics = {"ts": device_ts_iso}
        for mapping in MODBUS_REGISTERS_MAPPING:
            idx = mapping["index"]
            key = mapping["key"]
            raw = registers[idx]
            value = round(float(raw) * mapping["unit"], mapping["decimals"])
            metrics[key] = value

        return metrics
    finally:
        try:
            cli.close()
        except Exception:
            pass

# ── 封裝要送出的 JSON（保留你原本格式） ──
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
        "payload": {k: v for k, v in metrics.items() if k != "ts"},  # 動態放所有 key
    }

# ── 發佈（改成呼叫 MNXNexlib 的 mq） ──
def publish_rmq(doc: dict) -> None:
    global _NEX_MQ
    if _NEX_MQ is None:
        _NEX_MQ = _init_nex_mq(_RMQ_CFG_PATH)
        log.info("RabbitMQ init by MNXNexlib (%s)", _RMQ_CFG_PATH)

    msg_text = json.dumps(doc, ensure_ascii=False)
    _NEX_MQ.publish_msg(msg_text)

# ── 主流程 ──
def main() -> int:
    log.info("START env=%s site=%s device=%s", APP_ENV, SITE_UID, DEVICE_UID)
    while not _STOP:
        try:
            m = read_modbus()
            if m:
                publish_rmq(build_transport(m))
                ### dubug 用 print這個節點的字串+數值
                # t = build_transport(m)  
                # log.info("[DEBUG] payload before MQ: %s", json.dumps(t["payload"], ensure_ascii=False))
                # publish_rmq(t)
                # log.info("[DEBUG] published bytes=%d", len(json.dumps(t, ensure_ascii=False).encode("utf-8")))
                
        except Exception as e:
            log.error("Loop error: %s", e)
        finally:
            time.sleep(READ_INTERVAL_SEC)

    log.info("STOP worker")
    return 0

if __name__ == "__main__":
    sys.exit(main())