from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import pika


class RabbitPublisher:
    """
    封裝 NexSig/xRabbit，不動原檔。
    負責：讀 cfg -> init -> publish(dict)
    """

    def __init__(self, *, mnx_lib_root: str, rmq_cfg_path: str):
        self.mnx_lib_root = Path(mnx_lib_root)
        self.rmq_cfg_path = Path(rmq_cfg_path)

        if not self.mnx_lib_root.exists():
            raise RuntimeError(f"MNX_LIB_ROOT 不存在：{self.mnx_lib_root}")
        if not self.rmq_cfg_path.exists():
            raise RuntimeError(f"RMQ_CFG_PATH 不存在：{self.rmq_cfg_path}")

        if str(self.mnx_lib_root) not in sys.path:
            sys.path.append(str(self.mnx_lib_root))

        # 延後 import（確保 sys.path 已加入）
        from NexSig.xRabbit import RabbitMq, ExChangeType, MsgAdapter  # type: ignore

        self._RabbitMq = RabbitMq
        self._ExChangeType = ExChangeType
        self._MsgAdapter = MsgAdapter

        self._mq: Optional[RabbitMq] = None

    def _init_mq(self):
        import orjson
        from queue import Queue
        from threading import Lock

        RabbitMq = self._RabbitMq
        ExChangeType = self._ExChangeType
        MsgAdapter = self._MsgAdapter

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

        mq_cfg = orjson.loads(self.rmq_cfg_path.read_bytes())

        ex_str = str(mq_cfg[0].get('mqExType', 'direct')).lower()
        ex_map = {
            'fanout': ExChangeType.fanout,
            'direct': ExChangeType.direct,
            'topic': ExChangeType.topic,
            'headers': ExChangeType.headers,
        }
        ex_type = ex_map.get(ex_str, ExChangeType.direct)

        mq = MqSend(
            mq_cfg[0]['mqHost'], mq_cfg[0]['mqPort'],
            mq_cfg[0]['mqExchange'], ex_type,
            mq_cfg[0]['mqQueue'], mq_cfg[0]['mqVirtualHost'],
            mq_cfg[0]['mqUser'], mq_cfg[0]['mqPwd'],
            mq_cfg[0]['mqRouteKey'],
            MsgAdapter.blockingConn, Queue(), Lock()
        )
        mq.isReceive = False

        conn_strs = []
        for node in mq_cfg:
            if node['mqVirtualHost'] == '/':
                conn_strs.append(
                    f"amqp://{node['mqUser']}:{node['mqPwd']}@{node['mqHost']}:{node['mqPort']}/"
                )
            else:
                conn_strs.append(
                    f"amqp://{node['mqUser']}:{node['mqPwd']}@{node['mqHost']}:{node['mqPort']}/{node['mqVirtualHost']}"
                )

        mq.start(conn_strs)
        self._mq = mq

    def publish(self, doc: dict) -> None:
        if self._mq is None:
            self._init_mq()

        msg = json.dumps(doc, ensure_ascii=False)
        self._mq.publish_msg(msg)  # type: ignore
