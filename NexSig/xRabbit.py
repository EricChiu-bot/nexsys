# cython: language_level=3

import pika
import random
from NexCore.xUtil import Util

from enum import Enum


class ConnState(Enum):
    isOpen = 1
    isClosed = 2

class MsgAdapter(Enum):
    blockingConn = 1
    selectConn = 2

class ExChangeType(Enum):
    fanout = 1
    direct = 2
    topic = 3
    headers = 4
    match = 5


#*****************************************************************************#
#+class RabbitMq
#*****************************************************************************#
class RabbitMq:
    isReceive = True
    sendMsg = ''
    
    def __init__(self, _host, _queue, _virtualHost, _routeKey, _msgAdapter: MsgAdapter):
        self.msgAdapter = _msgAdapter
        self.queueName_ = _queue
        self.tmpQueue = ''
        self.channel_ = None
        #self.init()
        self.exchange_ = ''
        self.exType = ExChangeType.direct.name
        self.routingKey = _routeKey
        self.contentType = 'text/plain'        
        self.parameters_ = pika.ConnectionParameters(virtual_host=_virtualHost)
        
        if (self.msgAdapter == MsgAdapter.selectConn):
            self.connection_ = pika.SelectConnection(self.parameters_, 
                                                     on_open_callback = self.on_connected)


    def __init__(self, _host, _port, _exchange, _exType:ExChangeType, 
                 _queue, _virtualHost, 
                 _user, _pwd, _routeKey, _msgAdapter: MsgAdapter):
        self.msgAdapter = _msgAdapter
        self.key_ = pika.credentials.PlainCredentials(_user, _pwd)
        self.queueName_ = _queue
        self.tmpQueue = ''
        self.channel_ = None
        self.virtualHost = _virtualHost
        #self.init()
        self.exchange_ = _exchange
        self.exType = _exType.name #ExChangeType.direct.name
        self.routingKey = _routeKey
        self.contentType = 'text/plain'
        self.parameters_ = pika.ConnectionParameters(host=_host, port=_port,
                                                     virtual_host=_virtualHost,
                                                     credentials=self.key_)
        if (self.msgAdapter == MsgAdapter.selectConn):
            self.connection_ = pika.SelectConnection(self.parameters_, 
                                                     on_open_callback = self.on_connected)

    '''
    def init(cls):
        self.channel_ = None
        self.queue_ = None
        self.parameters_ = None
        self.connection_ = None        
    '''
    
    def start(cls, _connStr):
        # blocked_connection_timeout
        # add_on_connection_blocked_callback(callback)
        # add_on_connection_unblocked_callback(callback)
        # parameters = pika.URLParameters('amqp://orbit:xUser@NokidaSrv1:5672/%2F')
        # parameters = pika.URLParameters('amqp://orbit:xUser@NokidaSrv2:5672/%2F')
        

        try:
            if (cls.msgAdapter == MsgAdapter.blockingConn):
                if (not _connStr is None and len(_connStr) > 0):
                    allNodes = []
                    #for node in kw.values():
                    for node in _connStr:
                        allNodes.append(pika.URLParameters(node))
                        
                    random.shuffle(allNodes)
                    cls.connection_ = pika.BlockingConnection(allNodes)
                else:
                    cls.connection_ = pika.BlockingConnection(parameters=cls.parameters_)
                    
                cls.on_channel_open(cls.connection_.channel())
            elif (cls.msgAdapter == MsgAdapter.selectConn):
                cls.connection_.ioloop.start()
        except Exception as _ex:
            raise _ex
        
    def stop(cls):
        try:
            if cls.channel_.is_open:
                cls.channel_.close()
            if cls.connection_.is_open:
                cls.connection_.close()
                
            if cls.msgAdapter == MsgAdapter.selectConn:
                cls.connection_.ioloop.start()
        except Exception as _ex:
            raise _ex
        
    
    def con_state(cls) -> ConnState:
        if (cls.connection_.is_closed):
            return ConnState.isClosed
        elif (cls.connection_.is_open):
            return ConnState.isOpen
        
    def ch_state(cls) -> ConnState:
        if (cls.channel_.is_closed):
            return ConnState.isClosed
        elif (cls.channel_.is_open):
            return ConnState.isOpen


    def on_connected(cls, _conn):
        _conn.channel(on_open_callback = cls.on_channel_open)
        

    def on_channel_open(cls, _channel):
        cls.channel_ = _channel
        
        try:
            cls.channel_.basic_qos(prefetch_count = 1, global_qos = False)
            
            if cls.exchange_ == '' or cls.exchange_ == None:
                if (cls.msgAdapter == MsgAdapter.selectConn):
                    cls.channel_.queue_declare(queue = cls.queueName_, durable = True,
                                               exclusive = False, auto_delete = False,
                                               callback = cls.on_queue_declared)      
                    
                    #if cls.isReceive == False:
                    #    cls._publish_msg(cls.sendMsg)
                else:
                    cls.channel_.queue_declare(queue = cls.queueName_, durable = True,
                                               exclusive = False, auto_delete = False)
                                               #callback = cls.on_queue_declared)
                    if cls.isReceive == True:
                        cls.on_queue_declared()
                
            else:
                cls.channel_.exchange_declare(exchange = cls.exchange_,
                                              exchange_type = cls.exType,durable = True)
                
                #
                if (cls.queueName_ is not None):
                    cls.channel_.queue_declare(queue=cls.queueName_, durable=True)
                else:
                    cls.tmpQueue = cls.channel_.queue_declare(queue = '',
                                                              durable= True)
                    cls.queueName_ = str(cls.tmpQueue.method.queue)
                
                #
                if (cls.msgAdapter == MsgAdapter.selectConn):
                    cls.channel_.queue_bind(exchange = cls.exchange_,
                                            queue = cls.queueName_,
                                            callback = cls.on_exchange_declared)  
                    
                    #if cls.isReceive == False:
                    #    cls._publish_msg(cls.sendMsg)                    
                else:
                    cls.channel_.queue_bind(exchange = cls.exchange_,
                                            queue = cls.queueName_,
                                            routing_key= cls.routingKey)
                                            #callback = cls.on_exchange_declared)
                    if cls.isReceive == True:
                        cls.on_exchange_declared()
                    
        except Exception as _ex:
            raise _ex
        
        
    def on_exchange_declared(cls):
        cls.channel_.basic_consume(queue = cls.queueName_,
                                   auto_ack = True,
                                   on_message_callback = cls._handle_cb)
        
    
    def on_queue_declared(cls):        
        cls.channel_.basic_consume(queue = cls.queueName_, 
                                   auto_ack = True,
                                   on_message_callback = cls._handle_cb)
    
    
    def del_queue(cls):
        if (cls.channel_.is_oepn):
            try:
                cls.channel_.queue_delete(queue = cls.queueName_)
            except:
                print('delete queue [%1] fail!', cls.queueName_)
                
    
    def purge_message(cls):
        if (cls.channel_.is_open):
            try:
                cls.channel_.queue_purge(queue = cls.queueName_)
            except:
                print('purge message of queue [%1] fail', cls.queueName_)
                
                
                
        
    #
    # protected member fucntion
    #
    def _start_consuming(cls):
        #if cls.msgType == MsgType.byQueue:
        #            
        #else:
        try:
            cls.channel_.start_consuming()
        #KeyboardInterrupt
        except Exception as _ex:
            cls.channel_.stop_consuming()
            cls.stop()
            raise _ex
        
    
    def _stop_consuming(cls):
        cls.channel_.stop_consuming()
        

    # for user to override to handle call back message by themselves
    def _handle_cb(cls, _channel, _method, _hader, _body):
        pass
    

    def _publish_msg(cls, _msg, _props):
        try:
            cls.channel_.basic_publish(exchange = cls.exchange_,
                                       routing_key = cls.routingKey,
                                       body = _msg,
                                       properties = _props
                                       )
            
        except pika.exceptions.UnroutableError as _routeErr:
            raise _routeErr
        
        except Exception as _ex:
            cls.stop()
            raise _ex
        
            
