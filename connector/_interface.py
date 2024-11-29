from abc import abstractmethod, ABCMeta
from typing import Dict, Tuple, Any, List
import sys

import pandas as pd

sys.path.append("..")
import common

class Connector(metaclass=ABCMeta): 
    """
    Connector interface
    """
    @abstractmethod
    def check_query_args(self, args : dict[str,Any]):
        '''
        Must contain 'queries' args
        Optional 'sampling_period', 'query_length' args
        '''
        if not args.__contains__('queries'):
            return False
        if 'sampling_period' in args and not common.check_time_range_str(args['sampling_period']):
            return False
        if 'query_length' in args and not common.check_time_range_str(args['query_length']):
            return False
        return True

    @abstractmethod
    def query_series(self, tenant : str, query_name : str, queries : str, sampling_period : str, query_length: str) -> Tuple[Dict[str,pd.DataFrame],Dict[str,Dict[str,str]]]:
        '''
        query series data
        return:
            Exception, {'series_id': returned dataframe}, {'series_id': {'label_key': 'label_value'}}
        '''
        pass

    @abstractmethod
    def insert_series(self, tenant : str, metrics : List[str], labels : List[dict], values : List[Dict[str,str]]) -> bool:
        '''
        metrics: metric list
        labels: label dict, list
        values: { timestamp_unix_seconds : value }, list
        '''
        pass
    
