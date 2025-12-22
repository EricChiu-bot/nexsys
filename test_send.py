
import queue
import threading
import orjson
import pika
import time
#from time import sleep

from NexSig.xRabbit import RabbitMq, ExChangeType, MsgAdapter
from NexCore.xLog import NexLog

#
# Global Variables
#
inMsgQueue = queue.Queue()
msgLock = threading.Lock()
theLog = NexLog('MqSend').get_logger()

#******************************************************************************#
#+class: MqSend
#******************************************************************************#
class MqSend(RabbitMq):
    @theLog.catch
    def __init__(self, _host, _queue, _virtualHost, _msgAdapter, 
                 _procQueue, _mutex):
        self.queue = _procQueue
        self.lock = _mutex

        super().__init__(_host, _queue, -_virtualHost, _msgAdapter)
        

    @theLog.catch
    def __init__(self, _host, _port, _exchange, _exType : ExChangeType,
                 _queue, _virtualHost, _user, _pwd, 
                 _routeKey, _msgAdapter, _procQueue, _mutex):
        self.queue = _procQueue
        self.lock = _mutex

        super().__init__(_host, _port, _exchange, _exType, 
                         _queue, _virtualHost, _user, _pwd, 
                         _routeKey, _msgAdapter)


    @theLog.catch
    def publish_msg(self, _msg):
        val = {}
        val['CMD'] = 'flush'
        #headers = {'CMD', 'poll'}
        props = pika.BasicProperties(content_type='text/plain',
                                     delivery_mode=2,
                                     headers = val)
        
        self._publish_msg(_msg, props)
        
        # for volume send out message testing
        #for idx in range(550):
            #self._publish_msg(_msg + str(idx) + ': ' + Util().get_timestamp())


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

    #
    mq = MqSend(mqCfg[0]['mqHost'], mqCfg[0]['mqPort'], 
                mqCfg[0]['mqExchange'], 
                exType,
                mqCfg[0]['mqQueue'], 
                mqCfg[0]['mqVirtualHost'], 
                mqCfg[0]['mqUser'], mqCfg[0]['mqPwd'],
                mqCfg[0]['mqRouteKey'],
                MsgAdapter.blockingConn, inMsgQueue, msgLock)
    
    mq.isReceive = False

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
    
    for cnt in range(10):
        mq.publish_msg("Hello world... how a beautiful day [%d]!!" % cnt)
        time.sleep(0.5)



#******************************************************************************#
# program starting point
#******************************************************************************#
if __name__ == "__main__": 
    #pass
    init_mq('./NexCore/rabbitmq_cfg.json')