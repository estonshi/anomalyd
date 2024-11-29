from typing import Dict, Tuple, Any, List
import pandas as pd

from connector import _interface

class Stdio(_interface.Connector):

    def __init__(self):
        self.id = 1

    def check_query_args(self, args : dict[str,Any]):
        return True

    def query_series(self, tenant : str, query_name : str, queries : str, sampling_period : str, query_length: str) -> Tuple[Dict[str,pd.DataFrame],Dict[str,Dict[str,str]]] :
        return {}, {}

    def insert_series(self, tenant : str, metrics : List[str], labels : List[dict], values : List[Dict[str,str]]) -> bool:
        for i in range(len(metrics)):
            metric = metrics[i]
            label = labels[i]
            value = values[i]
            print(f"{tenant} {metric} {label} {value}")