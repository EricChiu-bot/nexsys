import requests
import threading
import queue

from loguru import logger
from NexCore.xCfg import get_cfg


#*****************************************************************************#
#+class Rest
#*****************************************************************************#
'''
    restC = Rest(restQ, restMtx,
                 get_cfg()['tgtRestSrv'], 
                 get_cfg()['tgtRestHeader'])
    restC.start()
'''
@logger.catch
class Rest(threading.Thread):
    def __init__(cls, _queue:queue.Queue, _mutex:threading.Lock,
                        _tgtRestUrl:str, _tgtRestHeader:dict):
        threading.Thread.__init__(cls)
        cls.lock = _mutex
        cls.procQ = _queue
        cls.restUrl = _tgtRestUrl
        cls.restHeader = _tgtRestHeader

    def run(cls):
       
        while(True):
            try:
                pass

            except Exception as _err:
                logger.critical('[Rest.run] Exception: %s' % _err)

