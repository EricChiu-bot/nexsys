# NexDrv/xOpcUaSubscriber.py
import time
from opcua import Client, ua


class _SubHandler:
    def __init__(self, on_data_cb):
        self.on_data_cb = on_data_cb
        self.cache = {}

    def datachange_notification(self, node, val, data):
        nodeid = node.nodeid.to_string()
        self.cache[nodeid] = val
        # 每次變化就送一包 raw dict
        self.on_data_cb(self.cache.copy())


class OpcUaSubscriber:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.client = Client(cfg["OPCUA_ENDPOINT_URL"])
        self.sub = None
        self.handler = None
        self.running = False

    def start(self, on_data_cb):
        # --- Security（目前 None，對齊你的 env）---
        if self.cfg.get("OPCUA_USERNAME"):
            self.client.set_user(self.cfg.get("OPCUA_USERNAME"))
            self.client.set_password(self.cfg.get("OPCUA_PASSWORD"))

        self.client.connect()

        self.handler = _SubHandler(on_data_cb)
        interval = int(self.cfg.get("OPCUA_PUBLISH_INTERVAL_MS", 1000))
        self.sub = self.client.create_subscription(interval, self.handler)

        node_ids = self.cfg["OPCUA_SUBSCRIBE_NODE_IDS"].split(",")
        for nid in node_ids:
            node = self.client.get_node(nid)
            self.sub.subscribe_data_change(node)

        self.running = True

    def stop(self):
        if self.sub:
            self.sub.delete()
        self.client.disconnect()
        self.running = False
