import threading
import queue
import time
import orjson
import paho.mqtt.client as mqtt

from loguru import logger


#*****************************************************************************#
#+class MqttC
#*****************************************************************************#
@logger.catch
class MqttC(threading.Thread):
    def __init__(cls, _mqttSrv:str, _mqttPort:int,
                 _mqttTrans:str, _mqttTopic:str, 
                 _queue:queue.Queue, _mutex:threading.Lock):
        try:
            threading.Thread.__init__(cls)
            #cls.amr = _amr
            cls.mqttTopic = _mqttTopic
            cls.mqttSrv = _mqttSrv
            cls.mqttPort = _mqttPort
            cls.mqttTrans = _mqttTrans
            #self.mqtt_connect()

            cls.procQueue = _queue
            cls.lock = _mutex
            cls.isConnected = False


        except Exception as _err:
            logger.critical('[MqttC.__init__] Exception: %s' % _err)

    def on_message(cls, _mqttc, data, _msg):
        pass

    def on_connect(cls, _mqttc, _data, _flags, _rc):
        if (_rc == 0):
            cls.isConnected = True
        else:
            logger.warning('[Mqttc] MQTT establish connection fail')
        #print("rc: " + str(rc))
                
    def on_disconnnect(cls, _mqttc, _data, _rc):
        cls.isConnected = False
        return
        
        
    def on_publish(cls, _client, _data, _result):
        #print("out result: " + str(_result))
        pass


    def on_cb_message(self, mqttc, data, msg):
        #print("mqtt msg: " + str(msg.payload))
        #msg = msg.payload.decode('utf-8')
        global isBackWard
        global isReverse
        global execCmd

        execCmd = orjson.loads(msg.payload)['action']

        print('coming command... {}' % execCmd)


    def mqtt_client(cls) -> mqtt.Client:    
        try:
            client:mqtt.Client = mqtt.Client('Saturn.Cmd', cls.mqttTrans)

            client.message_callback_add(cls.mqttTopic, cls.on_cb_message)

            client.on_connect = cls.on_connect
            client.on_disconnect = cls.on_disconnnect
            client.on_message = cls.on_message

            client.connect(cls.mqttSrv, cls.mqttPort)
            client.subscribe(cls.mqttTopic)

            client.loop_start()
            cls.isConnected = True
            
            return client
        except:
            return None
        
    def run(cls):
        try:            
            mqttc = cls.mqtt_client()
            #mqttc.connect(cls.mqttSrv, cls.mqttPort)
            #mqttc.subscribe(cls.mqttTopic)

            #mqttc.loop_forever()
            while (not mqttc == None):
                try:
                    if (not cls.isConnected):
                        logger.warning('[Loc.run] MQTT lost connection')
                        time.sleep(5)
                        continue

                    cls.lock.acquire()
                    while (not cls.procQueue.empty()):
                        msg = cls.procQueue.get()

                        sendOut = "abc|xyz" + msg['position']['x']
                        mqttc.publish(cls.mqttTopic, sendOut)

                        cls.lock.release()
                        time.sleep(0.1)

                except Exception as _err:
                    logger.critical('[Loc.run] Exception: %s' % _err)

        except Exception as _err:
            logger.critical('[Cmd.run] Exception: %s' % _err)