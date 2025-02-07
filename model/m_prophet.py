import sys
import model._interface as _interface
import uuid
import random
import threading
from typing import Any, Dict, List, Tuple
from prophet import Prophet

import pandas as pd
import logging

sys.path.append("..")
logger = logging.getLogger(__name__)

class ProphetModel(_interface.BaseModel):

    lock = threading.Lock()

    def __init__(self) -> None:
        # {instance_id : {series_id : model instance}}
        self.instances : Dict[str, Dict[str,Prophet]] = {}
        # {instance_id : {args map}}
        self.instances_args : Dict[str, Dict[str,Any]] = {}

    def check_args(self, args : dict[str,Any]) -> bool:
        try:
            pmodel = Prophet(**args)
        except Exception as e:
            return False
        return True
    
    def create_instance(self, args : dict[str,Any]) -> str:
        instance_id = uuid.uuid4()
        self.instances_args[instance_id] = args
        self.instances[instance_id] = {}
        return instance_id

    def remove_instance(self, instance_id : str) -> bool:
        if instance_id in self.instances:
            del self.instances[instance_id]
        if instance_id in self.instances_args:
            del self.instances_args[instance_id]
        # 20% probability to copy to a new dict
        if random.randint(1, 100) > 80:
            with ProphetModel.lock:
                self.instances = dict(self.instances)
                self.instances_args = dict(self.instances_args)
        return True
    
    def infer(self, instance : str, y : Dict[str, pd.DataFrame]) -> Dict[str, _interface.InferResult]:
        if instance not in self.instances:
            return None
        series_map = self.instances[instance]
        if len(series_map) == 0:
            return None
        result_map : Dict[str, _interface.InferResult] = {}
        for series_id, df in y.items():
            if series_id not in series_map:
                continue
            model = series_map[series_id]
            predicted = model.predict(df[['ds']])
            score = self.__evaluate_anomaly_score(predicted, df)
            result = _interface.InferResult(series_id, list(predicted['ds']), list(df['y']), 
                                            list(predicted['yhat']), list(predicted['yhat_lower']),
                                            list(predicted['yhat_upper']), score, None)
            result_map[series_id] = result
        return result_map

    def __evaluate_anomaly_score(self, predicted : pd.DataFrame, y : pd.DataFrame) -> List[float]:
        score = 2*(y['y']-predicted['yhat'])/(predicted['yhat_upper']-predicted['yhat_lower'])
        return list(score)
    
    def fit(self, instance : str, y : Dict[str, pd.DataFrame]) -> bool:
        if instance not in self.instances_args:
            return False
        if instance not in self.instances:
            self.instances[instance] = {}
        for series_id, data in y.items():
            if not ('y' in data.columns and 'ds' in data.columns):
                logger.error("[ProphetModel](Fit) invalid fit dataset (series_id=%s), lack of columns", series_id)
                continue
            try:
                model = Prophet(**self.instances_args[instance])
                model.fit(data)
                self.instances[instance][series_id] = model
            except Exception as e:
                logger.error("[ProphetModel](Fit) fitting error : %s", e)
                return False
        return True
            
    def save_checkpoint(self) -> bool:
        return False

    def load_checkpoint(self) -> bool:
        return False