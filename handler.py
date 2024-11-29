import functools
from flask import Flask
from flask import request
from typing import Any
import flask
import os
import yaml
import common
import time
import logging
import sys

import connector
import model
import scheduler

sys.path.append("..")
import common

logger = logging.getLogger(__name__)
app = Flask(__name__)
readers : dict[str, connector.Connector] = {}
writers : dict[str, connector.Connector] = {}
models : dict[str, model.BaseModel] = {}
schedulers : dict[str, scheduler.Scheduler] = {}

def init_global_configs(data_folder : str, 
                        reader : dict[str, connector.Connector], 
                        writer: dict[str, connector.Connector], 
                        model : dict[str, model.BaseModel],
                        scheduler : dict[str, scheduler.Scheduler]):
    global readers, writers, models, schedulers
    readers = reader
    writers = writer
    models = model
    schedulers = scheduler
    common.check_home_folder(data_folder)

def start_app(port : int):
    app.run(port=port, host='0.0.0.0')

def require_url_args(*required_args : str):
    def decorator(func):
        @functools.warps(func)
        def wrapper(*args, **kwargs):
            for arg in required_args:
                if arg not in request.args or request.args[arg] is None or request.args[arg] == "":
                    return flask.abort(400)
        return wrapper
    return decorator

def check_submit(task : dict[str,Any]) -> bool :
    if task['name'] is None or task['name'] == "":
        return False
    if task['reader_name'] not in readers or task['writer_name'] not in writers \
        or task['model_name'] not in models or task['scheduler_name'] not in schedulers:
        return False
    if task['query_name'] is None or not readers[task['reader_name']].check_query_args(task['query_args']):
        return False
    if not models[task['model_name']].check_args(task['model_args']):
        return False
    if not schedulers[task['scheduler_name']].check_args(task['scheduler_args']):
        return False
    return True

def resume_task():
    logger.info("Loading task configs ...")
    tenant_dir_list = os.listdir(common.task_folder)
    for tenant in tenant_dir_list:
        folder = os.path.join(common.task_folder, tenant)
        fp_list = os.listdir(folder)
        for fname in fp_list:
            fpath = os.path.join(folder, fname)
            with open(fpath, 'r') as f:
                task = yaml.safe_load(f)
                succ = __add_scheduled_task(task, tenant)
                if not succ:
                    logger.warning("Failed to load task: task_config = %s", tenant, fpath)

def __add_scheduled_task(task, tenant) -> bool:
    if task['model_name'] not in models:
        return False
    # get model object
    model = models[task['model_name']]
    # schedule
    schedulers[task['scheduler_name']].schedule(name = str(tenant) + "_" + task['name'],
                                                tenant = str(tenant),
                                                reader=readers[task['reader_name']], 
                                                writer=writers[task['writer_name']],
                                                model=model, 
                                                model_args=task['model_args'],
                                                query={task['query_name']:task['query_args']},
                                                args=task['scheduler_args']) 
    return True

@app.route('/submit/<int:tenant>', methods=['POST'])
def submit_task(tenant : int):
    task = {}
    task['name'] = request.json.get('name') # str
    task['reader_name'] = request.json.get('reader') # str
    task['writer_name'] = request.json.get('writer') # str
    task['model_name'] = request.json.get('model')   # str
    task['model_args'] = request.json.get('model_args') # dict
    task['scheduler_name'] = request.json.get('scheduler')  # str
    task['scheduler_args'] = request.json.get('scheduler_args')   # dict
    task['query_name'] = request.json.get('query_name')   # str
    task['query_args'] = request.json.get('query_args')   # dict
    tenant_dir = os.path.join(common.task_folder, str(tenant))
    if check_submit(task):
        if not os.path.isdir(tenant_dir):
            os.makedirs(tenant_dir)
    else:
        return flask.abort(400, "bad configuration !")
    with open(os.path.join(tenant_dir, task['name']+'.yaml'), 'w') as f:
        yaml.safe_dump(task, f)
    # task scheduling
    succ = __add_scheduled_task(task, tenant)
    if not succ:
        return flask.abort(400, "invalid model config !")
    return 'success'

@app.route('/stop/<int:tenant>', methods=['POST'])
def stop_task(tenant : int):
    scheduler_name = request.json.get('scheduler')  # str
    if scheduler_name not in schedulers:
        return flask.abort(400, "unknown scheduler !")
    task_name = request.json.get('name') # str
    tenant_dir = os.path.join(common.task_folder, str(tenant))
    task_file = os.path.join(tenant_dir, task_name + ".yaml")
    succ = schedulers[scheduler_name].stop(str(tenant) + "_" + task_name)
    if not succ:
        return flask.abort(400, "no task to stop")
    if os.path.isfile(task_file):
        os.remove(task_file)
    return 'success'