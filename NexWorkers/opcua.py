# NexWorkers/opcua.py
from __future__ import annotations

import sys
from pathlib import Path

# [CRITICAL Hack] Fix for circular import:
# Because this file is named "opcua.py", running it puts its directory (NexWorkers)
# at the start of sys.path. When we later "import opcua" (the library),
# Python finds THIS file instead, causing a crash.
# We remove the script's directory from sys.path[0] to force Python to find the real library.
if sys.path[0] == str(Path(__file__).resolve().parent):
    sys.path.pop(0)

# [Bootstrapping]
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import signal
import time

# noqa: E402
# from NexTrans.xTransformer import transformad_env
from NexCom.env_loader import load_env
from NexCom.envelope_builder import build_telemetry_v1
from NexDrv.xOpcUaSubscriber import OpcUaSubscriber
from NexPub.xRabbitPublisher import RabbitPublisher
from NexTrans.xTransformer import transform_json


class WorkerState:
    def __init__(self):
        self.running = True
        self.last_msg_ts = None
        self.last_error = None


def install_signal_handlers(state: WorkerState):
    def _handler(sig, frame):
        state.running = False

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def main():
    env_path = load_env(__file__)
    print(f"[ENV] loaded: {env_path}")
    cfg = __import__("os").environ
    print("[ENV] loaded")
    state = WorkerState()

    install_signal_handlers(state)

    # [Robust Design] 自動偵測路徑
    default_lib_root = str(PROJECT_ROOT)
    mnx_lib_root = __import__("os").environ.get("MNX_LIB_ROOT")
    if not mnx_lib_root or not Path(mnx_lib_root).exists():
        mnx_lib_root = default_lib_root

    rmq_cfg_path = __import__("os").environ.get("RMQ_CFG_PATH")
    if not rmq_cfg_path or not Path(rmq_cfg_path).exists():
        rmq_cfg_path = str(Path(mnx_lib_root) / "NexCore" / "rabbitmq_cfg.json")

    rabbit = RabbitPublisher(mnx_lib_root=mnx_lib_root, rmq_cfg_path=rmq_cfg_path)
    opcua = OpcUaSubscriber(cfg)

    def on_data(raw_payload: dict):
        try:
            print(f"[OPCUA] Received: {raw_payload}")
            metrics = transform_json(raw_payload, cfg)
            doc = build_telemetry_v1(metrics, cfg)
            rabbit.publish(doc)
            state.last_msg_ts = time.time()
        except Exception as e:
            state.last_error = str(e)

    opcua.start(on_data)

    # ---- Worker 主生命週期（manager 控制）----
    while state.running:
        time.sleep(1)

    # ---- 收尾 ----
    opcua.stop()


if __name__ == "__main__":
    main()
