# cython: language_level=3

import threading
import queue
import time
import urllib.parse
import re
import pymongo
import orjson

from enum import Enum
from NexCore.xCfg import Cfg
from NexSig.xEvent import EventHandler, Consumer

import datetime


class SrvType(Enum):
    classic = 1
    modern = 2

class OpType(Enum):
    procInsert = 1
    procDel = 2
    procUpdate = 3

#******************************************************************************#
#+class: Mongo
#! for concreate MongoDb instance and suuport the manipulation
#! 1. support the streaming paradigm by a shared Queue
#! 2. support one-time operation like insert/insert_many and so on
#******************************************************************************#
class Mongo(threading.Thread):
    def __init_priv__(self, _cfgPath='NexCore', _cfgFile='mongo_cfg.json', 
                      _opType : OpType = 1, _dbType:SrvType = 1):
        threading.Thread.__init__(self)

        mongoCfg = Cfg(_cfgPath, _cfgFile).get_cfg()
        self.opType = _opType

        #with open(_cfgFile, "rb") as file:
        #    mongoCfg = orjson.loads(file.read())
            
        self.username = urllib.parse.quote_plus(mongoCfg[0]['user'])
        self.password = urllib.parse.quote_plus(mongoCfg[0]['pwd'])

        # if (_dbType == SrvType.classic.value):
        #     mongoStr = "mongodb://%s:%s@"
        #     for host in mongoCfg:
        #         mongoStr += '{host}:{port},'.format(host=host['host'], port = host['port'])
        # else:
        #     mongoStr = "mongodb+srv://%s:%s@"
        #     for host in mongoCfg:
        #         mongoStr += '{host},'.format(host=host['host'])
            
        # mongoStr = mongoStr[:len(mongoStr) - 1]
        # #self.client = pymongo.MongoClient("mongodb://%s:%s@RabbitMQ01:27017,RabbitMQ02:27017,RabbitMQ03:27017" 
        # self.client = pymongo.MongoClient(mongoStr%(self.username, self.password)) 
        


        ###
        mongoCfg = Cfg(_cfgPath, _cfgFile).get_cfg()
        self.opType = _opType

        self.username = urllib.parse.quote_plus(mongoCfg[0]['user'])
        self.password = urllib.parse.quote_plus(mongoCfg[0]['pwd'])

        # ★ 這邊不用再組什麼多節點字串，直接拿第一個 host/port
        host = mongoCfg[0]['host']
        localhost = mongoCfg[0]['localhost']

        port_1 = mongoCfg[0]['port_1']
        port_2 = mongoCfg[0]['port_2']
        port_3 = mongoCfg[0]['port_3']

        # ★ 關鍵：鎖死連這個 IP:port，並指定 authSource=admin + directConnection=true

        # uri = f"mongodb://{self.username}:{self.password}@{host}:{port}/admin?replicaSet=rs0&authSource=admin"
##
        # mongoCfg = Cfg(_cfgPath, _cfgFile).get_cfg()
        # mode = mongoCfg[0].get("mode", "single")  # single / rs

        # if mode == "single":
        #      uri = f"mongodb://{host}:{port_1}"
        # else:
        #      uri = (
        #      f"mongodb://{self.username}:{self.password}@"
        #      f"{host}:{port_1},{host}:{port_2},{host}:{port_3}/admin"
        #      f"?replicaSet=rs0&authSource=admin"
        #     )
##
        # uri = f"mongodb://{host}:{port}"  #暫時不使用帳號密碼登入，等archi好了後再改回去
        uri = f"mongodb://{self.username}:{self.password}@{localhost}:{port_1}"

        # 給個 timeout，避免卡太久
        self.client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
        ###


        #self.db = self.client.forRpt
        #self.coll = self.db.Resource
        self.db = self.client.get_database(mongoCfg[0]['dbName'])
        if (self.db == None or self.db == 'NULL'):
            self.db = self.client.cube

        self.coll = self.db.get_collection(mongoCfg[0]['collName'])
        if (self.coll == None or self.coll == 'NULL'):
            self.coll = self.db.Resource
        
        self.queue = queue.Queue(102400)

    #
    # CTOR
    #
    '''def __init__(self, _cfgPath='NexCore', _cfgFile='mongo_cfg.json', _opType : OpType = 1):
        self.__init_priv__(_cfgPath, _cfgFile, _opType)

        self.procQueue = queue.Queue()
        self.lock = threading.Lock()
    '''

    def __init__(self, _queue, _mutex, 
                 _cfgPath='NexCore', _cfgFile='mongo_cfg.json', 
                 _opType : OpType = 1, _dbType:SrvType = 1):
        self.__init_priv__(_cfgPath, _cfgFile, _opType, _dbType)

        self.lock = _mutex
        self.procQueue = _queue

    def get_collection(self):
        return self.coll
    
    def get_cur_db(self):
        return self.db
        
    def get_cur_mongo_client(self):
        return self.client
    

    def run(self):
        while True:
            try:
                self.lock.acquire()
                
                while (not self.procQueue.empty()):
                    body = self.procQueue.get()
                    #self.proc_msg(body)
                    self.queue.put(body)
                    
                self.lock.release()   
                
                #
                match self.opType:
                    case OpType.procInsert.value:
                        self.proc_inst()
                    case OpType.procDel.value:
                        self.proc_del()
                    case OpType.procUpdate.value:
                        self.proc_update()

                    case _:
                        self.proc_inst()
                        

                time.sleep(0.5)
                
            except Exception as _err:
                #logger.critical("Exception: %s" % _err)   
                raise _err         


    def insert_many(self, _srcData:any) -> list:
        try:
            #self.coll.insert_one()
            result = self.coll.insert_many(_srcData)
            return result.inserted_ids

        except Exception as _ex:
            raise _ex


    def find(self, _tgtKey:str, _tgtVal:str, _rtnCnt:int = 1,
              _sortById = pymongo.DESCENDING)->dict:
        #resutl = None
        rtn = []

        try:
            if (not _tgtKey is None or len(_tgtKey) > 0):
                #with self.coll.find({}).limit(_rtnCnt).sort('_id', _sortById) as Cursor:

                with self.coll.find({_tgtKey:_tgtVal}).limit(_rtnCnt).sort('_id', _sortById) as Cursor:
                    #if (not result is None and result.count() > 0):
                    for doc in Cursor:
                        #rtn = result.next()
                        rtn.append(doc)

        except Exception as _err:
            raise _err
        
        finally:
            return rtn


    def marshal_data(_src) -> dict:
        pass
    

    # def proc_inst(self):
    #     try:
    #         while (not self.queue.empty()): 
    #             tgts = []
                
    #             body = self.queue.get()
    #             #body = body.replace(" ","").replace("ISODate(",'').replace(")}","}")

    #             if (type(body) is dict):
    #                 tgts.append(body)
    #             elif (type(body) is bytes):                                   
    #                 tgts.append(orjson.loads(body))
    #             else:
    #                 tgts.append(body)
                
    #             #print (tgt)
    #             #self.coll.insert_many(tgts)
    #             self.insert_many(tgts)
                
        
    #     except Exception as _err:
    #         #logger.critical("[proc_msg] Exception: %s" % _err)
    #         raise _err

###
    def proc_inst(self):
        """
        從 queue 取出 MQ 訊息，轉成 storage schema：

        {
          "timestamp_ms": <int>,
          "conn_name": "user_default" 或 envelope.conn_name,
          "site_uid": "001",
          "device_uid": "0001",
          "payload": { temperature, humidity, ... }
        }
        """
        try:
            while not self.queue.empty():
                body = self.queue.get()

                # 1) 統一轉成 dict --------------------------------
                if isinstance(body, dict):
                    msg = body

                elif isinstance(body, (bytes, bytearray)):
                    msg = orjson.loads(body)

                elif isinstance(body, str):
                    try:
                        msg = orjson.loads(body)
                    except Exception as e:
                        msg = {"_raw": body, "_parse_error": str(e)}
                else:
                    msg = {"_raw": body, "_parse_error": f"unsupported type {type(body).__name__}"}

                # 2) 拆 envelope / payload ------------------------
                env = msg.get("envelope", {}) or {}
                payload = msg.get("payload", {}) or {}

                # 3) site_uid / device_uid 對應 -------------------
                # 先吃新版欄位 site_uid / device_uid，
                # 沒有的話退回舊的 site_id / site_code、device_id / device_code
                site_uid = (
                    env.get("site_uid")
                    or env.get("site_id")
                    or env.get("site_code")
                )
                device_uid = (
                    env.get("device_uid")
                    or env.get("device_id")
                    or env.get("device_code")
                )

                # 4) conn_name：優先用 envelope.conn_name，否則預設值
                conn_name = env.get("conn_name") or "user_default"

                # 5) timestamp_ms：用 envelope.ts 轉成 epoch 毫秒 -----
                timestamp_ms = None
                ts_str = env.get("ts")
                if ts_str:
                    try:
                        # 支援 ...Z / ...+00:00
                        if ts_str.endswith("Z"):
                            ts_str = ts_str.replace("Z", "+00:00")
                        dt = datetime.datetime.fromisoformat(ts_str)
                        timestamp_ms = int(dt.timestamp() * 1000)
                    except Exception as e:
                        print(f"[MONGO] timestamp_ms parse error: {e}", flush=True)

                # 6) payload：整包丟進去（這裡是 temp/RH，就會只有那兩個欄位）
                payload_doc = dict(payload)

                # 7) 組成要寫進 Mongo 的 doc ----------------------
                doc = {
                    "timestamp_ms": timestamp_ms,   # 實際在 Mongo 會是 int
                    "conn_name":    conn_name,
                    "site_uid":     site_uid,
                    "device_uid":   device_uid,
                    "payload":      payload_doc,
                }

                # 8) 寫進 Mongo（一次一筆，用 insert_many） -------
                try:
                    print(f"[MONGO] insert_many 1 docs into {self.db.name}.{self.coll.name}", flush=True)
                    self.insert_many([doc])
                except Exception as e:
                    print(f"[MONGO] insert_many ERROR: {e}", flush=True)

        except Exception as _err:
            print(f"[MONGO] proc_inst TOP ERROR: {_err}", flush=True)
            raise _err



###




    def proc_del(self):
        pass

    def proc_update(self):
        pass



#*****************************************************************************#
#+class MongoWatch
#! for pub/sub base to watch the Mongo Db or Coll I/O completed event
#*****************************************************************************#
class WatchMongo:
    def __init__(cls, _coll: pymongo.collection = None, 
                 _db: pymongo.database = None, 
                 _client: pymongo.MongoClient = None, _onlyInst = True):
        cls.name = 'WatchMongo'
        cls.isRun = False

        cls.tgtColl = _coll
        cls.tgtDb = _db
        cls.client = _client

        
        cls.pipeline = None
        cls.resumeToken = None
        cls.cursor = None

        if (_onlyInst):
            cls.pipeline = [{"$match": {"operationType": "insert"}}]
        #cls.pipeline = [{"$match": {"operationType": "insert", "fullDocument.status": "new"}}]
        #filtered_change_stream = collection.watch(pipeline=pipeline)

        #
        if (not cls.tgtColl is None):
            cls.cursor = cls.tgtColl.watch(
                pipeline=cls.pipeline, resume_after=cls.resumeToken)
        elif (not cls.tgtDb is None):
            cls.cursor = cls.tgtDb.watch(
                pipeline=cls.pipeline, resume_after=cls.resumeToken)
        else:
            cls.cursor = cls.client.watch(
                pipeline=cls.pipeline, resume_after=cls.resumeToken)

        # Create Event Handler to enable the Event Driven behavior
        cls.evtHandler = EventHandler('WatchMongo')
        cls.evtHandler.start_handler()

        # Turn on the Watch Thread to continuous watching the target MongoDb's ChangeStream
        cls.isRun = True
        cls.procThd_ = threading.Thread(target=cls.start_watch, name=cls.name, args={})
        cls.procThd_.start()


    def start_watch(cls):
        while cls.isRun:
            try:
                #doc = next(cls.cursour)
                for doc in cls.cursor:
                    cls.evtHandler.has_event(doc)
                    cls.resumeToken = cls.cursor.resume_token
                    #print(doc)

            except Exception as _err:
                cls.isRun = False
                raise _err

    def stop_wathc(cls):
        if (cls.cursor is not None):
            cls.cursor.close()


    def register(cls, _srcObj:Consumer):
        cls.evtHandler.subscribe(_srcObj)

    def un_register(cls, _srcObj:Consumer):
        cls.evtHandler.un_subscribe(_srcObj)

    




class MongoWatch(threading.Thread):
    def __init__(cls, _tgtDb, _tgtColl, 
                 _cfgPath='NexCore', _cfgFile='mongo_cfg.json', 
                 _opType : OpType = 1, _dbType:SrvType = 1, _chkDb=False, _chkColl=True):
        
        threading.Thread.__init__(cls)

        cls.tgtColl = _tgtColl
        cls.tgtDb = _tgtDb     

        cls.watchColl = _chkColl
        cls.watchDb = _chkDb

        if (cls.watchColl):
            cls.cursour = _tgtColl.watch()
        elif (cls.watchDb): 
            cls.cursour = _tgtDb.watch()

        #
        cls.dispatcher = EventHandler(cls.eqpName)
        cls.dispatcher.start_handler()  

    def run(cls):
        isRun = True

        while isRun:
            try:
                doc = next(cls.cursour)
                cls.dispatcher.has_event(doc)
                print(doc)

            except Exception as _err:
                isRun = False
                raise _err
           
    def reg_notify(cls, _srcObj):
        pass