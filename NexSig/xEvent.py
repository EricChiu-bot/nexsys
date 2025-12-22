# cython: language_level=3

import threading

from pathlib import Path
from time import sleep
from queue import Queue


#******************************************************************************#
#+class: EvtType
#! For simplified and unified producer and consumer data type
#******************************************************************************#
class EvtType:
    def __init__(cls, _name, _evtValue, **kwargs):
        cls.name = _name
        cls.value = _evtValue

    def comp_data(cls):
        pass



#******************************************************************************#
#+class: Consumer
#******************************************************************************#
class Consumer:
    def __init__(cls, _name):
        cls.name = _name
        
        
    def fire_event(cls, _data):
        pass
    
    
    
#******************************************************************************#
#+class: EventHandler
#******************************************************************************#
class EventHandler:
    __consumers = []

    def __init__(cls, _name:str):
        cls.name_ = _name
        
        cls.evtQueue_ = Queue()
        cls.evtFlag_ = threading.Event()
        cls.procThd_ = threading.Thread(target=cls.dispatch_evt, name=cls.name_, args={})
        #cls.procThd_.setDaemon(True)
        cls.evtFlag_.clear()
        cls.startRun_ = False
        
    def start_handler(cls):
        cls.startRun_ = True
        cls.procThd_.start()
        
    def stop_handler(cls):
        while (not cls.evtQueue_.empty()):
            sleep(0.1)
            
        cls.startRun_ = False
        cls.evtFlag_.set()
        
    #Using Class variables to store all the input consumers
    def subscribe(cls, _consumer:Consumer):
        EventHandler.__consumers.append(_consumer)
        
    def un_subscribe(cls, _consumer:Consumer):
        EventHandler.__consumers.remove(_consumer)
        

    def has_event(cls, _data):
        cls.evtQueue_.put(_data)
        cls.evtFlag_.set()
        
    def dispatch_evt(cls):
        while (cls.startRun_):
            cls.evtFlag_.wait()
            cls.evtFlag_.clear()
            
            while (not cls.evtQueue_.empty()):
                evtData = cls.evtQueue_.get()
                
                for consumer in EventHandler.__consumers:
                    consumer.fire_event(evtData)