
import queue
import threading

from NexSig.xEvent import Consumer
from NexSto.xMongo import WatchMongo, Mongo, OpType, SrvType


#
# Global Variables
#
inMsgQueue = queue.Queue()
msgLock = threading.Lock()
#theLog = NexLog('WatchMongo').get_logger()


#******************************************************************************#
#+ MyEyes
#! for your Event Driven call back...handle the fire_event function call
#******************************************************************************#
class MyEyes(Consumer):
    def __init__(cls):
        pass

    def fire_event(cls, _data):
        #return super().fire_event(_data)
        #print(_data)
        if (_data['operationType'] == 'insert'):
            print("Document Changed: ", _data['fullDocument'])
        else:
            print(_data)



#******************************************************************************#
# program starting point
#******************************************************************************#
if __name__ == '__main__':
    mongo = Mongo(inMsgQueue, msgLock, 'NexCore', 'mongo_cfg-NT.json', 
                OpType.procInsert, SrvType.modern)
    #mongo.start()
    
    # MongoDb Change Stream manipulation
    #cursor = mongo.get_collection().watch()

    myEyes = MyEyes()
    #watch = WatchMongo(mongo.get_collection(), None, mongo.get_cur_mongo_client())
    watch = WatchMongo(None, mongo.get_cur_db(), None, False)
    #watch = WatchMongo(None, None, mongo.get_cur_mongo_client())
    watch.register(myEyes)
