from abc import abstractmethod, ABCMeta
from typing import Any, Dict, List, Tuple
import pandas as pd
import copy

class InferResult():

    '''
    self.data是一个dataframe，列包含：['ds', 'y', 'yhat', 'yhat_lower', 'yhat_upper', 'anomaly_score']
    '''

    def __init__(self, series_id : str, ds : List[int], y : List[float], yhat : List[float], 
                 yhat_lower : List[float], yhat_upper : List[float], anomaly_score : List[float],
                 extra_metrics : dict[str, List[Any]]) -> None:
        column = ['ds', 'y', 'yhat', 'yhat_lower', 'yhat_upper', 'anomaly_score']
        data = [ds, y, yhat, yhat_lower, yhat_upper, anomaly_score]
        if extra_metrics is not None and extra_metrics.__len__ > 0:
            column.extend(extra_metrics.keys())
            data.extend(extra_metrics.values())
        self.data = pd.DataFrame(columns=column, data=zip(*data))
        self.series_id = series_id

    def to_metrics(self, metrics_prefix : str, query_name : str, query_labels : dict) -> Tuple[List[str],List[Dict[str,str]],List[Dict[str,str]]]:
        '''
        return:
            [metrics_1, ...], [{timestamp_unix_seconds: value}, ...], [{label: value}, ...]
        '''
        metrics_names = []
        values = []
        for column in self.data.columns:
            if str(column) in ['ds']:
                continue
            metrics_name = metrics_prefix + "_" + column
            value = {}
            ts_sec = self.data['ds'].astype('int64').divide(1e9)
            value = self.data[column]
            to_write = {str(k):str(v) for k,v in zip(ts_sec, value)}
            metrics_names.append(metrics_name)
            values.append(to_write)
        query_labels['__for'] = query_name
        labels = [query_labels] * len(metrics_names)
        return metrics_names, values, labels
    
    def to_metrics_prom(self, metrics_prefix : str, query_labels : dict) -> Dict:
        result = {"data": {"result": []}}
        for column in self.data.columns:
            if str(column) in ['ds']:
                continue
            metrics_name = metrics_prefix + "_" + column
            labels = copy.deepcopy(query_labels)
            labels["__name__"] = metrics_name
            ts_sec = self.data['ds'].astype('int64').divide(1e9)
            value = self.data[column]
            to_write = [[k,str(v)] for k,v in zip(ts_sec, value)]
            result["data"]["result"].append({"metric": labels, "values": to_write})
        return result

class BaseModel(metaclass=ABCMeta):
    """
    Base Model interface
    - model : type = prophet
        - instance 1 (argument set 1)
        - instance 2 (argument set 2)
        - ...

    [IMPORTANT] the implementation should implement a '__json__' method to enable json serialization
    """
    @abstractmethod
    def check_args(self, args : dict[str,Any]) -> bool:
        return False
    
    @abstractmethod
    def create_instance(self, args : dict[str,Any]) -> str:
        '''
        input : instance args
        output : instance id
        '''
        return None

    @abstractmethod
    def remove_instance(self, instance_id : str) -> bool:
        '''
        delete model instance by its id
        '''
        return False
    
    @abstractmethod
    def infer(self, instance : str, y : Dict[str, pd.DataFrame]) -> Dict[str, InferResult]:
        '''
        instance: instance id
        y: {series_id: query_result}
        return: {series_id: infer_result}
        '''
        return None
    
    @abstractmethod
    def fit(self, instance : str, y : Dict[str, pd.DataFrame]) -> bool:
        '''
        instance: instance id
        y: {series_id: Dataframe{columns=[ts,value,...]}}  ts: 'yyyy-MM-dd HH:mm:ss'  value: float
        '''
        return False
    
    @abstractmethod
    def save_checkpoint(self) -> bool:
        '''
        save checkpoint, called by scheduler
        '''
        return False
    
    @abstractmethod
    def load_checkpoint(self) -> bool:
        '''
        load checkpoint, called by scheduler
        '''
        return False