# rtu_mqtt_worker.py
import os, time, json, socket
from datetime import datetime, timezone
from dotenv import load_dotenv

try:
    from loguru import logger
except Exception:
    class _L: 
        def info(self,*a,**k): print(*a)
        def warning(self,*a,**k): print(*a)
        def error(self,*a,**k): print(*a)
    logger = _L()

# ---- 讀環境 ----
load_dotenv()
SITE = os.getenv("SITE_CODE", "").strip()
DEVICE = os.getenv("DEVICE_CODE", "").strip()
if not SITE or not DEVICE:
    raise SystemExit("缺少 SITE_CODE / DEVICE_CODE")

MODBUS_TRANSPORT = os.getenv("MODBUS_TRANSPORT", "serial").lower()
TIMEOUT = float(os.getenv("TIMEOUT_SEC", "1.5"))
READ_FC = int(os.getenv("READ_FC", "3"))
START_ADDR = int(os.getenv("START_ADDR", "0"))
REG_COUNT  = int(os.getenv("REG_COUNT", "8"))
INTERVAL   = float(os.getenv("READ_INTERVAL_SEC", "1.0"))

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_BASE = os.getenv("MQTT_BASE_TOPIC", "telemetry/rtu").rstrip("/")
MQTT_QOS  = int(os.getenv("MQTT_QOS", "1"))
PUB_TOPIC = f"{MQTT_BASE}/{SITE}/{DEVICE}"

# ---- Modbus Client 準備（pymodbus 2.5.3 同步API）----
from pymodbus.client.sync import ModbusSerialClient

def make_client():
    if MODBUS_TRANSPORT == "serial":
        port = os.getenv("SERIAL_PORT", "COM3")
        baud = int(os.getenv("BAUDRATE", "9600"))
        parity = os.getenv("PARITY", "N")
        stopbits = int(os.getenv("STOPBITS", "1"))
        bytesize = int(os.getenv("BYTESIZE", "8"))
        logger.info(f"[RTU] {port} {baud}{parity}{bytesize}{stopbits}")
        return ModbusSerialClient(method="rtu", port=port, baudrate=baud,
                                  parity=parity, stopbits=stopbits,
                                  bytesize=bytesize, timeout=TIMEOUT)
    elif MODBUS_TRANSPORT in ("rtu_over_tcp", "rtu-over-tcp", "rtu_tcp"):
        host = os.getenv("MODBUS_HOST", "127.0.0.1")
        port = int(os.getenv("MODBUS_PORT", "3000"))
        logger.info(f"[RTU-over-TCP] socket://{host}:{port}")
        return ModbusSerialClient(method="rtu", port=f"socket://{host}:{port}",
                                  timeout=TIMEOUT)
    else:
        raise SystemExit("MODBUS_TRANSPORT 僅支援 serial 或 rtu_over_tcp")

# ---- MQTT ----
import paho.mqtt.client as mqtt
def make_mqtt():
    cli = mqtt.Client(client_id=f"rtu_mqtt_{SITE}_{DEVICE}_{os.getpid()}")
    def on_connect(c,u,f,rc):
        logger.info(f"[MQTT] connected rc={rc}")
    def on_disconnect(c,u,rc):
        logger.warning(f"[MQTT] disconnected rc={rc}")
    cli.on_connect = on_connect
    cli.on_disconnect = on_disconnect
    cli.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    cli.loop_start()
    return cli

def now_ts_ms():
    return int(time.time() * 1000)

def iso8601():
    return datetime.now(timezone.utc).isoformat()

def read_registers(client):
    if READ_FC == 3:
        rr = client.read_holding_registers(address=START_ADDR, count=REG_COUNT, unit=1)
    elif READ_FC == 4:
        rr = client.read_input_registers(address=START_ADDR, count=REG_COUNT, unit=1)
    else:
        raise ValueError("READ_FC 僅支援 3 或 4")
    if rr is None or rr.isError():
        raise RuntimeError(f"Modbus error: {getattr(rr, 'function_code', None)}")
    return list(rr.registers)

def main():
    logger.info(f"RTU over MQTT :: site={SITE} device={DEVICE} topic={PUB_TOPIC}")
    cli = make_mqtt()
    mclient = make_client()
    if not mclient.connect():
        raise SystemExit("Modbus 連線失敗")

    try:
        while True:
            t0 = time.time()
            try:
                regs = read_registers(mclient)
                payload = {
                    "site_code": SITE,
                    "device_code": DEVICE,
                    "ingest_ts": now_ts_ms(),
                    "metrics": {
                        "ts": iso8601(),
                        "fc": READ_FC,
                        "addr": START_ADDR,
                        "count": REG_COUNT,
                        "values": regs
                    },
                    "src": {"transport": MODBUS_TRANSPORT, "host": socket.gethostname()}
                }
                cli.publish(PUB_TOPIC, json.dumps(payload), qos=MQTT_QOS, retain=False)
                logger.info(f"PUB {PUB_TOPIC} {payload['metrics']['values']}")
            except Exception as e:
                logger.error(f"read/publish error: {e}")
            # 節流
            dt = time.time() - t0
            time.sleep(max(0.0, INTERVAL - dt))
    finally:
        try: cli.loop_stop(); cli.disconnect()
        except: pass
        try: mclient.close()
        except: pass

if __name__ == "__main__":
    main()
