



'''***for testing xRpt***
import os
from util.util import Cfg
from impl.mariadb import Maria
from impl.rpt import PlotLine, PlotStack
import threading
import time
import queue


class Scenario:
    def __init__(cls, _procQueue, _mutex):
        cls.procQueue = _procQueue
        cls.lock = _mutex
        cls.execThd = threading.Thread(target=cls.execute)

    def execute(cls):
        pass

    def start_run(cls):
        cls.execThd.start()



class MyPlot(Scenario):
    #def __init__(cls):
    #    pass
    maxVal:float = []
    minVal:float = []
    avgVal:float = []

    allVal:int = []
    cntVal:int = []    

    localQ:queue = queue.Queue(1024)

    def execute(cls):
        #return super().execute()
        plt = PlotLine('nelson_test')

        while(True):                    
            cls.lock.acquire()                
            while (not cls.procQueue.empty()):
                cls.localQ.put(cls.procQueue.get())
                #tgtVal =  cls.procQueue.get()
            cls.lock.release()
            

            while (not cls.localQ.empty()):
                yAxisMin1 = 0
                yAxisMax1 = 0
                yAxisMin2 = 0
                yAxisMax2 = 0

                #if (tgtVal != None):
                tgtVal:str = cls.localQ.get()    
                procVal = tgtVal.split(',')

                #
                if (len(cls.avgVal) > 30): del cls.avgVal[0]
                yAxisMin1 = cls.chk_max(yAxisMin1, float(procVal[0]))
                yAxisMax1 = cls.chk_max(yAxisMax1, float(procVal[0]))
                cls.avgVal.append(float(procVal[0]))

                if (len(cls.maxVal) > 30): del cls.maxVal[0]
                yAxisMin1 = cls.chk_min(yAxisMin1, float(procVal[1]))
                yAxisMax1 = cls.chk_max(yAxisMax1, float(procVal[1]))
                cls.maxVal.append(float(procVal[1]))

                if (len(cls.minVal) > 30): del cls.minVal[0]
                yAxisMin1 = cls.chk_min(yAxisMin1, float(procVal[2]))
                yAxisMax1 = cls.chk_max(yAxisMax1, float(procVal[2]))
                cls.minVal.append(float(procVal[2]))

                #
                if (len(cls.allVal) > 30): del cls.allVal[0]
                yAxisMin2 = cls.chk_max(yAxisMin2, int(procVal[3]))
                yAxisMax2 = cls.chk_max(yAxisMax2, int(procVal[3]))
                cls.allVal.append(int(procVal[3]))

                if (len(cls.cntVal) > 30): del cls.cntVal[0]
                yAxisMin2 = cls.chk_min(yAxisMin2, int(procVal[4]))
                yAxisMax2 = cls.chk_max(yAxisMax2, int(procVal[4]))
                cls.cntVal.append(int(procVal[4]))


            ###
            if (len(cls.allVal) > 0):                
                xAxis1 = len(cls.maxVal)
                xAxis2 = len(cls.maxVal)               

                plt.draw(xAxis1, xAxis2, 
                         round(yAxisMin1), round(yAxisMax1), round(yAxisMin2), round(yAxisMax2),
                    cls.maxVal, cls.minVal, cls.avgVal, 
                    cls.cntVal, cls.allVal)

            time.sleep(0.1)

    def chk_min(cls, _src, _tgt):
        if (_src < _tgt):
            return _src
        else:
            return _tgt

    def chk_max(cls, _src, _tgt):
        if (_src < _tgt):
            return _tgt
        else:
            return _src

class MyTest(Scenario):
    #def __init__(cls):
    #    pass
    def execute(cls):
        #return super().execute()
        recIdx = 0

        while(True):
            mariaDb = Maria(dbHost, dbPort, dbUser, dbPwd, dbName)
            if (mariaDb.get_cur() == None): continue

            try:            
                recIdx = recIdx + 1
                print("Current query count ==> " + str(recIdx))

                sql1 = 'SELECT avg(cast('+ dbColName +' as FLOAT)) FROM ' + dbTblName
                result = mariaDb.get_data(sql1)
                val1:float = float(result[0][0])
                print(f"The AVG is: {val1}")
                #for item in result:
                #    print(f"The average is: {item}")

                sql2 = 'SELECT max(cast(' + dbColName + ' as FLOAT)) FROM ' + dbTblName
                result = mariaDb.get_data(sql2)
                val2:float = float(result[0][0])
                print(f"The MAX is: {val2}")

                sql3 = 'SELECT min(cast(' + dbColName +  ' as FLOAT)) FROM ' + dbTblName
                result = mariaDb.get_data(sql3)
                val3:float = float(result[0][0])
                print(f"The MIN is: {val3}")

                sql4 = 'SELECT COUNT(' + dbColName + ') FROM ' + dbTblName
                result = mariaDb.get_data(sql4)
                val4:int = int(result[0][0])
                print(f"The Total is: {val4}")

                sql5 = 'SELECT count(DISTINCT(' + dbColName + ')) FROM ' + dbTblName
                result = mariaDb.get_data(sql5)
                val5:int = int(result[0][0])
                # for testing
                if (val4 > 3000): val5 = round(val4 - 3000)
                print(f"The DISTINCT Count is: {val5}")

                #
                tgtVal:str = str(val1) + ',' + str(val2) + ',' + str(val3) + ',' + str(val4) + ',' + str(val5)
                cls.lock.acquire()                
                cls.procQueue.put(tgtVal)
                cls.lock.release()
            
            except Exception as _err:
                print(f'Exception: {_err}')

            mariaDb.stop()

            time.sleep(10)




dbHost:str
dbPort:int
dbUser:str
dbPwd:str
dbName:str
dbTblName:str
dbColName:str

inMsgQueue = queue.Queue()
msgMtx = threading.Lock()
#******************************************************************************#
# program starting point
#******************************************************************************#
if __name__ == "__main__":
    tgtLoc = os.path.join(os.getcwd(), 'config')
    myCfg = Cfg(tgtLoc, "cfg.json").get_cfg()

    for setting in myCfg:
        dbHost = setting['target']['host']
        dbPort = setting['target']['port']
        dbUser = setting['target']['dbuser']
        dbPwd = setting['target']['dbpwd']

        dbName = setting['dbname']
        dbTblName = setting['tblname']
        for col in setting['column']:
            dbColName = col
        #print(myCfg[0]['target']['host'])
        #print(myCfg[0]['target']['port'])
        #print(myCfg[0]['target']['dbuser'])
        #print(myCfg[0]['target']['dbpwd'])

        #
        #print(myCfg[0]['dbname'])
        #print(myCfg[0]['tblname'])
        #for col in myCfg[0]['column']:
        #    print(col)

    ####
    myTest = MyTest(inMsgQueue, msgMtx)
    myTest.start_run()

    myPlot = MyPlot(inMsgQueue, msgMtx)
    myPlot.start_run()

    #plt = PlotStack(0, 0)
    #plt.draw()

    #plt = PlotLine(0, 0)
    #plt.draw()

    print("hello world")

'''



if __name__ == '__main__':
    pass