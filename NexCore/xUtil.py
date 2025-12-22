import datetime
import socket
import platform

import orjson, dataclasses, typing


#
# Data type
#
@dataclasses.dataclass
class Detail:
    valueType: str
    key: str
    value: str


@dataclasses.dataclass
class Tag:
    name: str


@dataclasses.dataclass
class PayLoad:
    srcItemId: str
    nodeGrpId: str
    data: str
    timestamp: str
    nodeName: str
    valueType: str
    unit: str

@dataclasses.dataclass
class PayLoads:
    payloads: typing.List[PayLoad]


#*****************************************************************************#
#+class MakeJson
#*****************************************************************************#
class MakeJson:
    def __init__(cls):
        cls.unxTs = UnixTs()

    def marshal(cls, _srcData:dict):
        rtn = None

        try:
            for key, val in _srcData.items():
                for item1, item2, item3 in val.itmes():
                    rtn = orjson.dumps(PayLoads([PayLoad('A001', 'AMR_Grp_001', 'str(batPower)', 'ts', 'Battery_Power', '0', '%'),
                                                 PayLoad('A002', 'AMR_Grp_001', 'str(batVolt)', 'ts', 'Battery_Voltage', '0', 'Volt')]))
        except:
            pass

        return rtn


#*****************************************************************************#
#+class UnixTs
#*****************************************************************************#
class UnixTs:
    def __init__(cls):
        pass
        
    def get_ts(self, _tzHours = -8) -> str:
        tgtTs : str = ''

        try:
            now = datetime.datetime.now() + datetime.timedelta(hours=_tzHours)
            ts = datetime.datetime.timestamp(now)

        except:
            ts = None
            ts = datetime.datetime.timestamp(datetime.now() +
                                            datetime.timedelta(hours=_tzHours))
        
        finally:
            tgtTs = str(ts).replace('.','')[:10]

        return tgtTs


    def from_ts(self, _unixTs:int, _isUtc=False) -> str:
        try:
            if (_isUtc):
                return str(datetime.datetime.utcfromtimestamp(_unixTs))
            else:
                return str(datetime.datetime.fromtimestamp(_unixTs))

        except:
            return str(datetime.datetime.now())




#*****************************************************************************#
#+class Util
#*****************************************************************************#
class Util:
    def __init__(self):
        self.hostName = socket.gethostname()
        #self.hostName = platform.node()
    
    
    @staticmethod
    def get_timestamp():
        now = datetime.datetime.now()
        return now.strftime('%Y.%m.%d %H:%M:%S')        
    
    @staticmethod
    def get_dt():
        now = datetime.datetime.now()
        return now.strftime('%Y.%m.%d')
    
    def get_host_name(this):
        #name = platform.node()
        #return name
        if (len(this.hostName) == 0):
            this.hostName = socket.gethostname()
            
        return this.hostName
    
    
    def get_host_ip(this):
        sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        try:
            #host = socket.gethostname()
            #ip = socket.gethostbyname(host)
            ips = sckt.getsockname()
            for val in ips:
                if str(val).startswith('127'):
                    continue
                else:
                    ip = val
                    break
        except socket.error as err:
            print("get_host_ip exception: {}".format(err))
            ip = '127.0.0.1'
        finally:
            #pass
            sckt.close()
            
        return ip
