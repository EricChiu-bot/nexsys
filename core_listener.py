import queue
import threading
import orjson

from NexSig.xRabbit import RabbitMq, ExChangeType, MsgAdapter
from NexCore.xLog import NexLog
from NexCore.xUtil import Util
from NexSto.xMongo import Mongo



#
# Global Variables
#
inMsgQueue = queue.Queue()
msgLock = threading.Lock()
theLog = NexLog('CoreListener').get_logger()

#******************************************************************************#
#+class: MqRecv
#******************************************************************************#
class MqRecv(RabbitMq):
    #@logger.catch
    @theLog.catch
    def __init__(self, _host, _queue, _virtualHost, _msgAdapter, 
                 _procQueue, _mutex):
        self.queue = _procQueue
        self.lock = _mutex

        super().__init__(_host, _queue, -_virtualHost, _msgAdapter)
        
    #@logger.catch
    @theLog.catch
    def __init__(self, _host, _port, _exchange, _exType : ExChangeType,
                 _queue, _virtualHost, _user, _pwd, 
                 _routeKey, _msgAdapter, _procQueue, _mutex):
        self.queue = _procQueue
        self.lock = _mutex

        super().__init__(_host, _port, _exchange, _exType, 
                         _queue, _virtualHost, _user, _pwd, 
                         _routeKey, _msgAdapter)
        

    #@logger.catch
    @theLog.catch
    def start_consuming(cls):
        cls._start_consuming()
        
    @theLog.catch
    def _handle_cb(cls, _channel, _method, _header, _body):
        try:
            if (_header is not None):
                if (_header.headers is not None and
                    _header.headers.get('MSG') == 'reply'):
                    return
                
            msg = bytes(_body).decode('ascii')
            msgLen = len(msg)
            
            if (msg.endswith(',')):
                msg = msg[:msgLen - 1]
                
            ts = Util().get_timestamp()
            
            cls.lock.acquire()
            cls.queue.put(msg)
            cls.lock.release()
            
            theLog.info('Received message bytes:: %d' % msgLen);
            print(" [x] Received message [{curts}]: {inmsg}".format(curts=ts, inmsg=msg))        
            
            #_channel.basic_ack(delivery_tag = _method.delivery_tag)
        except Exception as _err:
            theLog.critical("[_handle_cb] Exception: %s" % _err)



#@logger.catch
@theLog.catch
def init_mq(_cfgFile:str = 'rabbitmq_cfg.json'):    
    #******************************************************************************#
    # for Initiate RabbitMQ
    #******************************************************************************#
    with open(_cfgFile, "rb") as file:
        mqCfg = orjson.loads(file.read())    
    
    
    #ExChangeType.direct.name
    mqExType = mqCfg[0]['mqExType']
    exType:ExChangeType =  ExChangeType.direct

    if (str(mqExType).startswith("fanout")):
        exType = ExChangeType.fanout
    elif (str(mqExType).startswith("direct")):
        exType = ExChangeType.direct
    elif (str(mqExType).startswith("topic")):
        exType = ExChangeType.topic
    elif (str(mqExType).startswith("headers")):
        exType = ExChangeType.headers
    else:
        exType = ExChangeType.match

    # using queue direclty, so keep Exchange as empty
    #mq = MqRecv('NokidaSrv1', 5672, '', 'myTestQueue', 'Resource', 'orbit', 'xUser', MsgAdapter.blockingConn)
    # using Exchange and make queue as empty becuase system will auto generate the queue for the exchange
    mq = MqRecv(mqCfg[0]['mqHost'], mqCfg[0]['mqPort'], 
                mqCfg[0]['mqExchange'], 
                exType,
                mqCfg[0]['mqQueue'], 
                mqCfg[0]['mqVirtualHost'], 
                mqCfg[0]['mqUser'], mqCfg[0]['mqPwd'],
                mqCfg[0]['mqRouteKey'],
                MsgAdapter.blockingConn, inMsgQueue, msgLock)
    
    #mq.routingKey = mqCfg[0]['mqRouteKey']
        
    
    # Support multiple nodes' connection
    connStr = []
    for node in mqCfg:
        if (node['mqVirtualHost'] == '/'):
            connStr.append('amqp://{name}:{pwd}@{host}:{port}/'.format(name=node['mqUser'], 
                                                                       pwd=node['mqPwd'], 
                                                                       host=node['mqHost'], 
                                                                       port=node['mqPort']))            
        else:
            connStr.append('amqp://{name}:{pwd}@{host}:{port}/{viHost}'.format(name=node['mqUser'], 
                                                                               pwd=node['mqPwd'], 
                                                                               host=node['mqHost'], 
                                                                               port=node['mqPort'],
                                                                               viHost=node['mqVirtualHost']))
    mq.start(connStr)

    mq.start_consuming()
    mq.stop()


    
#******************************************************************************#
# program starting point
#******************************************************************************#
if __name__ == "__main__": 
    #
    mongo = Mongo(inMsgQueue, msgLock)
    mongo.start()

    #
#    init_mq('./NexCore/rabbitmq_cfg.json')


# 中斷退出
import os

if __name__ == "__main__":
    try:
        init_mq('./NexCore/rabbitmq_cfg.json')
    except KeyboardInterrupt:
        print("stop the listener", flush=True)
        os._exit(0)

