import os
import orjson


#*****************************************************************************#
#+class Cfg
#!param1: target config file path
#!param2: configuration file name
#*****************************************************************************#
class Cfg:
    def __init__(cls, _path:str, _cfgFile:str):
        cls.path = _path
        cls.file = _cfgFile

        try:
            tgtFile = os.path.join(_path, _cfgFile)
            
            with open(tgtFile, "rb") as file:
                cls.config = orjson.loads(file.read())
        
        except Exception as _err:
            print(f'[Cfg] Exception : {_err}')
            return None


    def get_cfg(cls):
        return cls.config


### for direct using the configuration file###
def get_cfg(_cfgFile:str = 'core.json') -> any:
    try:
        with open(_cfgFile, 'rb') as file:
            return orjson.loads(file.read())
        
    except Exception as _err:
        print(f'[get_cfg] Exception: {_err}')
        return None