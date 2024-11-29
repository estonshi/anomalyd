import logging
import sys
import scheduler._interface as _interface
import time
import heapq

from typing import Any, List, Dict
from connector import *
from model import *

from concurrent.futures import ThreadPoolExecutor
import pandas as pd


sys.path.append('..')
import common

logger = logging.getLogger(__name__)

class Periodical(_interface.Scheduler):

    workers_pool : ThreadPoolExecutor = ThreadPoolExecutor()
    active_task : Dict[str, List[_interface.ScheduledTask]] = {}

    def __init__(self):
        self.anomaly_metrics_prefix = common.output_metrics_prefix
        self.model_to_infer = []
        self.model_to_fit = []
        heapq.heapify(self.model_to_infer)
        heapq.heapify(self.model_to_fit)
        Periodical.workers_pool.submit(Periodical.time_wheel, self)

    def time_wheel(self):
        logger.info("[Scheduler](Periodical) time wheel started")
        while True:
            current_time = time.time()
            infer_task = None
            fit_task = None
            if len(self.model_to_infer) > 0:
                infer_task : _interface.ScheduledTask = self.model_to_infer[0]
            if len(self.model_to_fit) > 0:
                fit_task : _interface.ScheduledTask = self.model_to_fit[0]
            try:
                continuing = False
                if infer_task is not None and infer_task.next_trigger_t < current_time:
                    Periodical.workers_pool.submit(self.__run_infer, sch_task = infer_task)
                    # update next_trigger_t
                    infer_task.next_trigger_t = common.parse_time_range_str(infer_task.args['infer_every']) + time.time()
                    heapq.heapify(self.model_to_infer)
                    # continue to pop task from heap until it doesn't reach its execution time
                    continuing = True
            except Exception as e:
                logger.error("[Scheduler](Periodical) inferring task error : task = %s, %s", infer_task.name, e)
            try:
                if fit_task is not None and fit_task.name not in Periodical.active_task:
                    Periodical.workers_pool.submit(self.__run_fit, sch_task = fit_task)
                    # update next_trigger_t
                    fit_task.next_trigger_t = common.parse_time_range_str(fit_task.args['fit_every']) + time.time()
                    heapq.heapify(self.model_to_fit)             
                    # continue to pop task from heap until it doesn't reach its execution time
                    continuing = True
            except Exception as e:
                logger.error("[Scheduler](Periodical) fitting task error : task = %s, %s", fit_task.name, e)
            if not continuing:
                time.sleep(10)

    def __run_infer(self, sch_task : _interface.ScheduledTask):
        y_all : Dict[str, pd.DataFrame] = {}
        y_label_all : Dict[str, Dict[str, str]] = {}
        for query_name in sch_task.query.keys():
            query_args = sch_task.query[query_name]
            query_args['query_length'] = sch_task.args['infer_window']
            if 'sampling_period' not in query_args:
                query_args['sampling_period'] = sch_task.args['infer_every']
            y_map, y_label_map = sch_task.reader.query_series(tenant=sch_task.tenant, query_name=query_name, **query_args)
            if y_map is None:
                logger.warning("[Scheduler](Periodical) query: %s, none returned y", query_name)
                continue
            if y_map.__len__ == 0:
                logger.warning("[Scheduler](Periodical) query: %s, empty returned y", query_name)
                continue
            y_all.update(y_map)
            y_label_all.update(y_label_map)
        if len(y_all) == 0:
            return 
        hat = sch_task.model.infer(sch_task.model_instance, y_all)
        if hat is None:
            logger.warning("[Scheduler](Periodical) query:, %s, model: %s, empty inferer", query_name, sch_task.model.__class__)
            return
        for sid, infer_result in hat.items():
            metrics, values, labels = infer_result.to_metrics(self.anomaly_metrics_prefix, query_name, sid, y_label_all[sid])
            sch_task.writer.insert_series(sch_task.tenant, metrics, labels, values)
        return

    def __run_fit(self, sch_task : _interface.ScheduledTask):
        y_all : Dict[str, pd.DataFrame] = {}
        for query_name in sch_task.query.keys():
            query_args = sch_task.query[query_name]
            if 'query_period' not in query_args:
                query_args['query_length'] = sch_task.args['fit_window']
            if 'sampling_period' not in query_args:
                query_args['sampling_period'] = common.default_time_window(query_args['query_period'], 250)
            data, _ = sch_task.reader.query_series(tenant=sch_task.tenant, query_name=query_name, **query_args)
            if data is None:
                logger.warning("[Scheduler](Periodical) query: %s, return none", query_name)
                continue
            if data.__len__ == 0:
                logger.warning("[Scheduler](Periodical) query: %s, return empty result", query_name)
                continue
            y_all.update(data)
        if len(y_all) == 0:
            return 
        try:
            success = sch_task.model.fit(sch_task.model_instance, y_all)
            if not success:
                logger.warning("[Scheduler](Periodical) fit: failed, model = %s, ", sch_task.model.__class__, sch_task.model_instance)
        except Exception as e:
            logger.error("[Scheduler](Periodical) fit: error occurred, %s", e)
    
    def check_args(self, args : dict[str,str]) -> bool:
        if args['fit_window'] is None or not common.check_time_range_str(args['fit_window']):
            return False
        if args['infer_every'] is None or not common.check_time_range_str(args['infer_every']):
            return False
        if args['fit_every'] is None:
            args['fit_every'] = args['infer_every']
        elif not common.check_time_range_str(args['fit_every']):
            return False
        return True
    
    def schedule(self, name : str, tenant : str, reader : Connector, writer : Connector,
                 model : BaseModel, model_args : Dict[str,Any],
                 query : dict[str, dict[str, Any]], args : dict[str, str]) -> None:
        if not self.check_args(args):
            raise ValueError("[Scheduler](Periodical) args error !")
        # update task ?
        if name in Periodical.active_task:
            self.stop(name)
        # create model instance
        model_instance_id = model.create_instance(model_args)
        # schedule
        next_fit_t = time.time()
        next_infer_t = common.parse_time_range_str(args['infer_every']) + time.time()
        fit_task = _interface.ScheduledTask(name, tenant, reader, writer, model, model_instance_id, query, args, next_fit_t)
        infer_task = _interface.ScheduledTask(name, tenant, reader, writer, model, model_instance_id, query, args, next_infer_t)
        heapq.heappush(self.model_to_fit, fit_task)
        heapq.heappush(self.model_to_infer, infer_task)
        Periodical.active_task[name] = [fit_task, infer_task]

    def stop(self, name) -> bool:
        if name in Periodical.active_task:
            fit_task, infer_task = Periodical.active_task[name]
            try:
                self.model_to_fit.remove(fit_task)
                self.model_to_infer.remove(infer_task)
            except Exception:
                return False
            heapq.heapify(self.model_to_fit)
            heapq.heapify(self.model_to_infer)
            del Periodical.active_task[name]
            return True
        else:
            return False