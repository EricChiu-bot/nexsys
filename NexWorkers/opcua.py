# NexWorkers/opcua.py
import time
import signal

from NexCom.env_loader import load_env
from NexDrv.xOpcUaSubscriber import OpcUaSubscriber
from NexTrans.xTransformer import transform
from NexCom.envelope_builder import build_telemetry
from NexPub.xRabbitPublisher import RabbitPublisher


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
    cfg = load_env()
    state = WorkerState()

    install_signal_handlers(state)

    rabbit = RabbitPublisher(cfg)
    opcua = OpcUaSubscriber(cfg)

    def on_data(raw_payload: dict):
        try:
            metrics = transform(raw_payload, cfg)
            doc = build_telemetry(metrics, cfg)
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
