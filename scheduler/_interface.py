from abc import abstractmethod, ABCMeta
from connector import *
from model import *
from typing import Any, List, Dict

class Scheduler(metaclass=ABCMeta):
    '''
    Scheduler interface
    '''
    @abstractmethod
    def check_args(self, args : dict[str,str]) -> bool:
        return False

    @abstractmethod
    def schedule(self, name : str, 
                 reader : Connector, 
                 writer : Connector,
                 model : BaseModel,
                 model_instance_id : str, 
                 query: dict[str, dict[str,Any]],
                 args : dict[str, str]) -> None:
        '''
        query : {query_name : {query_args_key : query_args_value}}
        args : {schedule_args_key : schedule_args_value}
        '''
        pass

    @abstractmethod
    def stop(self, name : str):
        pass



class ScheduledTask():
    '''
    Runnable task handle, executed by scheduler
        query: {'hash-id': {'key':'value',...}}
        args: scheduler.args
    '''
    def __init__(self, name : str,
                 reader : Connector, 
                 writer : Connector, 
                 model : BaseModel, 
                 model_instance : str,
                 query : Dict[str, Dict[str, Any]],
                 args : Dict[str, str],
                 next_trigger_t : int) :
        '''
        query : {query_name : {query_args_key : query_args_value}}
        args : {schedule_args_key : schedule_args_value}
        '''
        self.name = name
        self.reader = reader
        self.writer = writer
        self.model = model
        self.model_instance = model_instance
        self.query = query
        self.args = args
        self.next_trigger_t = next_trigger_t

    def __gt__(self, other : ScheduledTask):
        return self.next_trigger_t > other.next_trigger_t
    
    def __lt__(self, other : ScheduledTask):
        return self.next_trigger_t < other.next_trigger_t
    
    def __eq__(self, other : ScheduledTask):
        return self.next_trigger_t == other.next_trigger_t

    def update(self, reader : Connector, 
                 writer : Connector, 
                 model : BaseModel, 
                 model_instance : str,
                 query : Dict[str, Dict[str, Any]],
                 args : Dict[str, str],
                 next_trigger_t : int):
        if reader is not None:
            self.reader = reader
        if model is not None:
            self.writer = writer
        if model_instance is not None and model is not None:
            self.model = model
            self.model_instance = model_instance
        if query is not None:
            self.query = query
        if args is not None:
            self.args = args
        if next_trigger_t is not None and next_trigger_t > 0:
            self.next_trigger_t = next_trigger_t
