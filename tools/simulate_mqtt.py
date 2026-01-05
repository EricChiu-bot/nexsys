import json
import random
import time

import paho.mqtt.client as mqtt

# 設定
BROKER = "127.0.0.1"
PORT = 1883
TOPIC = "sensors/s001/d0002"
INTERVAL_SEC = 1.0


def main():
    client = mqtt.Client()

    try:
        print(f"Connecting to {BROKER}:{PORT} ...")
        client.connect(BROKER, PORT, 60)
        client.loop_start()  # 背景執行網路迴圈

        print(f"Start publishing to {TOPIC}")
        while True:
            # 產生假資料
            payload = {
                "temperature": round(random.uniform(20.0, 30.0), 1),
                "humidity": round(random.uniform(40.0, 60.0), 1),
                "ts": time.time(),
            }

            payload_str = json.dumps(payload)
            client.publish(TOPIC, payload_str)
            print(f"Published: {payload_str}")

            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\nStopping...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
