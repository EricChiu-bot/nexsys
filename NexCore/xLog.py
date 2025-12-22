
import os

from pathlib import Path
from time import sleep
from datetime import date
from loguru import logger
from enum import Enum

class NexLog:
    def __init__(cls, _logPrefix:str, _logDir:str='Log'):
        if (_logPrefix == None):
            _logPrefix = "NexCore_"
        else:
            _logPrefix += "_"
        
        logFile = _logPrefix + date.today().isoformat() + ".log"
        #curDir = os.getcwd()
        curDir = Path.cwd()
        logDir = os.path.join(curDir, _logDir)
        
        if (not os.path.exists(logDir)):
            os.mkdir(logDir)
            
        logFile = os.path.join(logDir, logFile)    
        logger.add(logFile, level="INFO", format="{time:YYYY-MM-DD at HH:mm:ss} {level} {message}", 
                   filter="", backtrace="True", diagnose="False", rotation="5MB", colorize=True)
              
    def get_logger(cls):
        return logger