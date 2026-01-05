import logging
import random
import time

from opcua import Server

# 設定
ENDPOINT = "opc.tcp://127.0.0.1:49320"
NAMESPACE_URI = "http://examples.freeopcua.github.io"
INTERVAL_SEC = 1.0


def main():
    # 1. 建立 Server
    server = Server()
    server.set_endpoint(ENDPOINT)

    # 2. 設定 Namespace
    idx = server.register_namespace(NAMESPACE_URI)
    print(f"[Sim] Registered Namespace index: {idx}")

    # 3. 建立節點
    objects = server.nodes.objects

    # Node: ns=2;s=CH_SIM.temp
    # 注意: opcua 舊版 add_variable(idx, "name", val) 預設會用 "name" 當 BrowseName
    # 但 NodeId 可能是 ns=2;i=... 或 ns=2;s=name 取決於實作
    # 我們明確指定 nodeid
    my_temp = objects.add_variable(f"ns={idx};s=CH_SIM.temp", "CH_SIM.temp", 25.0)
    my_temp.set_writable()

    # Node: ns=2;s=CH_SIM.rh
    my_rh = objects.add_variable(f"ns={idx};s=CH_SIM.rh", "CH_SIM.rh", 50.0)
    my_rh.set_writable()

    print(f"[Sim] Created Node: {my_temp}")
    print(f"[Sim] Created Node: {my_rh}")

    # 4. 啟動 Server
    server.start()
    print(f"[Sim] Server started at {ENDPOINT}")

    try:
        count = 0
        while True:
            time.sleep(INTERVAL_SEC)

            # 產生隨機數值
            t_val = round(20.0 + random.uniform(0, 10), 1)
            h_val = round(40.0 + random.uniform(0, 20), 1)

            # 更新節點數值 (同步版直接 write)
            my_temp.set_value(t_val)
            my_rh.set_value(h_val)

            count += 1
            print(f"[Sim] Update: Temp={t_val}, RH={h_val}")

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        server.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    main()
