

import queue
import threading
import time
import orjson

from NexCore.xUtil import Util
from NexCore.xLog import NexLog
from NexSto.xMongo import Mongo, OpType, SrvType



#
# Global Variables
#
inMsgQueue = queue.Queue()
msgLock = threading.Lock()
#theLog = NexLog('MyMongo').get_logger()



    
#******************************************************************************#
# program starting point
#******************************************************************************#
if __name__ == "__main__": 
    # MongoDb insert operation
    mongo = Mongo(inMsgQueue, msgLock, 'NexCore', 'mongo_cfg-NT.json', 
                  OpType.procInsert, SrvType.modern)
    
    ## if you want to continuous insert the doc into MongoDb through inMsgQueue
    # 1.) Turn on the isUsingQueue = True
    isUsingQueue = False
    # 2.) Tun on the mongo.start() in another thread to hanlde the data in queue
    #mongo.start()

    #
    # for testing purpose
    tgts = []
    for cnt in range(1, 5):
        #srcData = '''{"id":"xxx", "dt":"20250818", "ts":"1917111000222", 
        # "data":{"key1":"value1","key2": 21,"key3":"value3","key4":[1,11,111],"key5": true,"key6": null}}'''
        #body = orjson.dumps(srcData)

        body = {}
        body['myId'] = cnt
        body['dt'] = Util.get_dt()
        body['ts'] = Util.get_timestamp()

        child = {}
        child['key1'] = 'value1'
        child['key2'] = 21 + cnt
        child['key3'] = [1,11,111]
        body['data'] = child
        
        tgts.append(body)

        # put data into MongoDb Queue
        if (isUsingQueue):
            msgLock.acquire()

            inMsgQueue.put(body)

            msgLock.release()

        #
        time.sleep(0.3)

    # for single once operation
    if (not isUsingQueue):
        mongo.insert_many(tgts)