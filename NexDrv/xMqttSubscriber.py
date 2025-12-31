# NexDrv/xMqttSubscriber.py
import json
import paho.mqtt.client as mqtt


class MqttSubscriber:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.client = mqtt.Client(
            client_id=cfg.get("MQTT_CLIENT_ID")
        )

        if cfg.get("MQTT_USERNAME"):
            self.client.username_pw_set(
                cfg.get("MQTT_USERNAME"),
                cfg.get("MQTT_PASSWORD"),
            )

        self._on_message_cb = None

    def start(self, on_message_cb):
        self._on_message_cb = on_message_cb

        self.client.on_message = self._on_message
        self.client.connect(
            self.cfg["MQTT_HOST"],
            int(self.cfg["MQTT_PORT"]),
            keepalive=60,
        )
        self.client.subscribe(self.cfg["MQTT_TOPIC"])
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

    def _on_message(self, client, userdata, msg):
        payload = json.loads(msg.payload.decode("utf-8"))
        if self._on_message_cb:
            self._on_message_cb(payload)
