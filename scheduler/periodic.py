import logging
import sys
import scheduler._interface as _interface
import time
import heapq

from typing import Any, List, Dict
from connector import *
from model import *

from threading import Thread
from concurrent.futures import ThreadPoolExecutor
import pandas as pd


sys.path.append('..')
import tools

class Periodical(_interface.Scheduler):

    workers_pool : ThreadPoolExecutor = ThreadPoolExecutor()
    active_task : Dict[str, _interface.ScheduledTask] = {}

    def __init__(self, anomaly_metrics_prefix = 'anomaly_detected'):
        if anomaly_metrics_prefix is None or anomaly_metrics_prefix == "":
            raise ValueError("[Scheduler](Periodical) init error: anomaly_metrics_prefix should not be empty")
        self.anomaly_metrics_prefix = anomaly_metrics_prefix
        self.model_to_infer = []
        self.model_to_fit = []
        heapq.heapify(self.model_to_infer)
        heapq.heapify(self.model_to_fit)
        t = Thread(target=Periodical.time_wheel, args=(self,))
        t.start()

    @staticmethod
    def time_wheel(periodical : Periodical):
        logging.info("[Scheduler](Periodical) time wheel started")
        while True:
            current_time = time.time()
            infer_task = None
            fit_task = None
            if len(periodical.model_to_infer) > 0:
                infer_task : _interface.ScheduledTask = periodical.model_to_infer[0]
            if len(periodical.model_to_fit) > 0:
                fit_task : _interface.ScheduledTask = periodical.model_to_fit[0]
            try:
                if infer_task is not None and infer_task.next_trigger_t >= current_time:
                    if task.name in Periodical.active_task:
                        Periodical.workers_pool.submit(periodical.__run_infer, sch_task = infer_task)
                        # update next_trigger_t
                        infer_task.next_trigger_t = tools.parse_time_range_str(infer_task.args['infer_every']) + time.time()
                    else:
                        heapq.heappop(periodical.model_to_infer)
                if fit_task is not None and fit_task.next_trigger_t >= current_time:
                    if task.name in Periodical.active_task:
                        Periodical.workers_pool.submit(periodical.__run_fit, sch_task = fit_task)
                        # update next_trigger_t
                        fit_task.next_trigger_t = tools.parse_time_range_str(fit_task.args['fit_every']) + time.time()
                    else:
                        heapq.heappop(periodical.model_to_fit)
            except Exception as e:
                logging.error("[Scheduler](Periodical) error in time wheel : %s", e)
            time.sleep(1)

    def __run_infer(self, sch_task : _interface.ScheduledTask):
        sampling_period = sch_task.args['infer_every']
        query_length = sampling_period
        y_all : Dict[str, pd.DataFrame] = {}
        y_label_all : Dict[str, Dict[str, str]] = {}
        for query_name in sch_task.query.keys():
            #query_name = list(sch_task.query.keys())[0]
            queries = sch_task.query[query_name]['queries']
            err, y_map, y_label_map = sch_task.reader.query_series(query_name, queries, sampling_period, query_length)
            if err is not None:
                logging.error("[Scheduler](Periodical) failed to query series, %s", err.__str__())
                continue
            if y_map.__len__ == 0:
                logging.warning("[Scheduler](Periodical) query: %s, empty returned y", query_name)
                continue
            y_all.update(y_map)
            y_label_all.update(y_label_map)
        hat = sch_task.model.infer(sch_task.model_instance, y_all)
        if hat is None:
            logging.warning("[Scheduler](Periodical) query:, %s, model: %s, empty inferer", query_name, sch_task.model.__class__)
            return
        for sid, infer_result in hat.items():
            metrics, values, labels = infer_result.to_metrics(self.anomaly_metrics_prefix, query_name, sid, y_label_all[sid])
            sch_task.writer.insert_series(metrics, labels, values)
        return

    def __run_fit(self, sch_task : _interface.ScheduledTask):
        #query_name = list(sch_task.query.keys())[0]
        y_all : Dict[str, pd.DataFrame] = {}
        for query_name in sch_task.query.keys():
            query_args = sch_task.query[query_name]
            if 'query_period' not in query_args:
                query_args['query_period'] = sch_task.args['fit_window']
            if 'sampling_period' not in query_args:
                query_args['sampling_period'] = tools.default_time_window(query_args['query_period'], 250)
            err, data, _ = sch_task.reader.query_series(**query_args)
            if err is not None:
                logging.error("[Scheduler](Periodical) query: %s, run into error: %s", query_name, err)
                continue
            if data.__len__ == 0:
                logging.warning("[Scheduler](Periodical) query: %s, return empty result", query_name)
                continue
            y_all.update(data)
        try:
            success = sch_task.model.fit(sch_task.model_instance, y_all)
            if not success:
                logging.warning("[Scheduler](Periodical) fit: failed, model = %s, ", sch_task.model.__class__, sch_task.model_instance)
        except Exception as e:
            logging.error("[Scheduler](Periodical) fit: error occurred, %s", e)
    
    def check_args(self, args : dict[str,str]) -> bool:
        if args['fit_window'] is None or not tools.check_time_range_str(args['fit_window']):
            return False
        if args['infer_every'] is None or not tools.check_time_range_str(args['infer_every']):
            return False
        if args['fit_every'] is None:
            args['fit_every'] = args['infer_every']
        elif not tools.check_time_range_str(args['fit_every']):
            return False
        return True
    
    def schedule(self, name : str, reader : Connector, writer : Connector,
                 model : BaseModel, model_instance_id : str,
                 query : dict[str, dict[str, Any]], args : dict[str, str]) -> None:
        if not self.check_args(args):
            raise ValueError("[Scheduler](Periodical) args error !")
        if name in Periodical.active_task:
            Periodical.active_task[name].update(reader, writer, model, model_instance_id, query, args)
        next_fit_t = time.time()
        next_infer_t = tools.parse_time_range_str(args['infer_every']) + time.time()
        fit_task = _interface.ScheduledTask(name, reader, writer, model, model_instance_id, query, args, next_fit_t)
        infer_task = _interface.ScheduledTask(name, reader, writer, model, model_instance_id, query, args, next_infer_t)
        heapq.heappush(self.model_to_fit, fit_task)
        heapq.heappush(self.model_to_infer, infer_task)
        Periodical.active_task[name] = 1

    def stop(self, name) -> None:
        if name in self.task_map:
            del Periodical.active_task[name]