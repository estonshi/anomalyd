import logging
import sys
import scheduler._interface as _interface
import time
import copy

from typing import Any, List, Dict
from connector import *
from model import *

import pandas as pd


sys.path.append('..')
import common

logger = logging.getLogger(__name__)

class Once(_interface.Scheduler):

    def __init__(self):
        self.anomaly_metrics_prefix = common.output_metrics_prefix
    
    def check_args(self, args : dict[str,str]) -> bool:
        if args['fit_window'] is None or not common.check_time_range_str(args['fit_window']):
            return False
        if args['infer_window'] is None or not common.check_time_range_str(args['infer_window']):
            return False
        return True
    
    def __run_fit(self, sch_task : _interface.ScheduledTask) -> bool:
        y_all : Dict[str, pd.DataFrame] = {}
        for query_name in sch_task.query.keys():
            query_args_raw = sch_task.query[query_name]
            queries = query_args_raw['queries']
            query_len = sch_task.args['fit_window']
            sampling_period = None
            if 'sampling_period_fit' not in query_args_raw:
                sampling_period = common.default_time_window(query_len, 250)
            else:
                sampling_period = query_args_raw['sampling_period_fit']
            data, _ = sch_task.reader.query_series(sch_task.tenant, query_name, queries, sampling_period, query_len)
            if data is None:
                logger.warning("[Scheduler](Once) query: %s, return none", query_name)
                continue
            if data.__len__ == 0:
                logger.warning("[Scheduler](Once) query: %s, return empty result", query_name)
                continue
            y_all.update(data)
        if len(y_all) == 0:
            return True
        try:
            success = sch_task.model.fit(sch_task.model_instance, y_all)
            if not success:
                logger.warning("[Scheduler](Once) fit: failed, model = %s, ", sch_task.model.__class__, sch_task.model_instance)
            return success
        except Exception as e:
            logger.error("[Scheduler](Once) fit: error occurred, %s", e)
            return False

    def __run_infer(self, sch_task : _interface.ScheduledTask) -> Dict:
        y_all : Dict[str, pd.DataFrame] = {}
        y_label_all : Dict[str, Dict[str, str]] = {}
        original = {}
        for query_name in sch_task.query.keys():
            query_args_raw = sch_task.query[query_name]
            queries = query_args_raw['queries']
            query_len = sch_task.args['infer_window']
            sampling_period = None
            if 'sampling_period_infer' not in query_args_raw:
                sampling_period = sch_task.args['infer_every']
            else:
                sampling_period = query_args_raw['sampling_period_infer']
            y_map, y_label_map = sch_task.reader.query_series(sch_task.tenant, query_name, queries, sampling_period, query_len)
            if y_map is None:
                logger.warning("[Scheduler](Once) query: %s, none returned y", query_name)
                continue
            if y_map.__len__ == 0:
                logger.warning("[Scheduler](Once) query: %s, empty returned y", query_name)
                continue
            y_all.update(y_map)
            y_label_all.update(y_label_map)
            original[query_name] = self.__to_metric_prom(y_all, y_label_all)
        if len(y_all) == 0:
            return {}
        result = {"original": original, "anomaly": {}}
        try:
            hat = sch_task.model.infer(sch_task.model_instance, y_all)
            if hat is None:
                logger.warning("[Scheduler](Once) query:, %s, model: %s, empty inferer", query_name, sch_task.model.__class__)
                return result
            for sid, infer_result in hat.items():
                metrics = infer_result.to_metrics_prom(self.anomaly_metrics_prefix, y_label_all[sid])
                result["anomaly"][query_name] = metrics
            return result
        except Exception as e:
            logger.error("[Scheduler](Once) infer: error occurred, %s", e)
            return result
        
    def __to_metric_prom(self, y_all: dict[str, pd.DataFrame], y_label_all: dict[str, Dict[str, str]]) -> Dict:
        result = {"data": {"result": []}}
        for sid, y in y_all.items():
            labels = copy.deepcopy(y_label_all[sid])
            ts_sec = y['ds'].astype('int64').divide(1e9)
            value = y['y']
            to_write = [[k,str(v)] for k,v in zip(ts_sec, value)]
            result["data"]["result"].append({"metric": labels, "values": to_write})
        return result
        
    def schedule(self, name : str, tenant : str, reader : Connector, writer : Connector,
                 model : BaseModel, model_args : Dict[str,Any],
                 query : dict[str, dict[str, Any]], args : dict[str, str]) -> None:
        # create model instance
        model_instance_id = model.create_instance(model_args)
        next_fit_t = time.time()
        next_infer_t = time.time()
        # create task
        fit_task = _interface.ScheduledTask(name, tenant, reader, writer, model, model_instance_id, query, args, next_fit_t)
        infer_task = _interface.ScheduledTask(name, tenant, reader, writer, model, model_instance_id, query, args, next_infer_t)
        # submit
        if not self.__run_fit(fit_task):
            return 'Fitting failed'
        result =  self.__run_infer(infer_task)
        common.threadlocal.result = result
        # delete model instance
        model.remove_instance(model_instance_id)

    def stop(self, name) -> bool:
        raise RuntimeError("Not implemented")