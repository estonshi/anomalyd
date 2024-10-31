import sys
import _interface
import uuid
from typing import Any, Dict, List, Tuple
from prophet import Prophet

import pandas as pd
import logging

sys.path.append("..")


class ProphetModel(_interface.BaseModel):

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
        instance_id = uuid.uuid().hex
        self.instances_args[instance_id] = args
        return instance_id

    def remove_instance(self, instance_id : str) -> bool:
        if instance_id in self.instances:
            del self.instances[instance_id]
        if instance_id in self.instances_args:
            del self.instances_args[instance_id]
        return True
    
    def infer(self, instance : str, y : Dict[str, pd.DataFrame]) -> Dict[str, _interface.InferResult]:
        series_map = self.instances[instance]
        if series_map is None:
            return None
        result_map : Dict[str, _interface.InferResult] = {}
        for series_id, df in y.items():
            model = series_map[series_id]
            if model is None:
                continue
            predicted = model.predict(df['ds'])
            score = self.__evaluate_anomaly_score(predicted, df)
            result = _interface.InferResult(series_id, list(predicted['ds']), list(df['y']), 
                                            list(predicted['yhat']), list(predicted['yhat_lower']),
                                            list(predicted['yhat_upper']), score, None)
            result_map[series_id] = result
        return result_map

    def __evaluate_anomaly_score(self, predicted : pd.DataFrame, y : pd.DataFrame) -> List[float]:
        score = 2*(y['y']-predicted['yhat']).abs()/(predicted['yhat_upper']-predicted['yhat_lower'])
        return list(score)
    
    def fit(self, instance : str, y : Dict[str, pd.DataFrame]) -> bool:
        if self.instances_args[instance] is None:
            return False
        if self.instances[instance] is None:
            self.instances[instance] = {}
        try:
            model = Prophet(**self.instances_args[instance])
        except Exception as e:
            logging.error("[ProphetModel](Fit) invalid model parameters ! error : %s", e)
            return False
        for series_id, data in y.items():
            if not ('y' in data.columns and 'ds' in data.columns):
                logging.error("[ProphetModel](Fit) invalid fit dataset (series_id=%s), lack of columns", series_id)
                continue
            try:
                self.instances[instance][series_id] = model.fit(y)
            except Exception as e:
                logging.error("[ProphetModel](Fit) fitting error : %s", e)
                continue
            