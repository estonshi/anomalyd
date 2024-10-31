import functools
from flask import Flask
from flask import request
from typing import Any
import flask
import os
import yaml
import tools
import time

import connector
import model
import scheduler

app = Flask(__name__)
home_folder = os.path.dirname(os.path.abspath(__file__))
readers : dict[str, connector.Connector] = {}
writers : dict[str, connector.Connector] = {}
models : dict[str, model.BaseModel] = {}
schedulers : dict[str, scheduler.Scheduler] = {}

def init_global_configs(task_folder : str, 
                        reader : dict[str, connector.Connector], 
                        writer: dict[str, connector.Connector], 
                        model : dict[str, model.BaseModel],
                        scheduler : dict[str, scheduler.Scheduler]):
    global home_folder, readers, writers, models, schedulers
    if task_folder is not None:
        home_folder = task_folder
    readers = reader
    writers = writer
    models = model
    schedulers = scheduler

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
    if readers[task['reader_name']] is None or writers[task['writer_name']] is None \
        or models[task['model_name']] is None or schedulers[task['scheduler_name']] is None:
        return False
    if task['query_name'] is None or not readers[task['reader_name']].check_query_args(task['query_args']):
        return False
    if not models[task['model_name']].check_args(task['model_args']):
        return False
    if not schedulers[task['scheduler_name']].check_args(task['scheduler_args']):
        return False
    return True

@app.route('/submit/<int:tenant>', methods=['POST'])
def submit_task(tenant : int):
    task_id = tools.map_hash({'tenant':tenant, 'timestamp': time.time_ns})
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
    tenant_dir = os.path.join(home_folder, str(tenant))
    if check_submit(task):
        if not os.path.isdir(tenant_dir):
            os.makedirs(tenant_dir)
    else:
        return flask.abort(400, "bad configuration !")
    with open(os.path.join(tenant_dir, task['name']+'.yaml'), 'w') as f:
        yaml.dump(task, f)
    # create model instance
    model = models[task['model_name']]
    ins_id = model.create_instance(task['model_args'])
    # schedule
    schedulers[task['scheduler_name']].schedule(name = tenant + "_" + task['name'],
                                                reader=readers[task['reader_name']], 
                                                writer=writers[task['writer_name']],
                                                model=model, 
                                                model_instance_id=ins_id,
                                                query={task['query_name']:task['query_args']},
                                                args=task['scheduler_args']) 
    # return task id
    return task_id

@app.route('/stop/<int:tenant>', methods=['POST'])
def stop_task(tenant : int):
    scheduler_name = request.json.get('scheduler')  # str
    if scheduler_name not in schedulers:
        return flask.abort(400, "unknown scheduler !")
    task_name = request.json.get('name') # str
    tenant_dir = os.path.join(home_folder, str(tenant))
    task_file = os.path.join(tenant_dir, task_name + ".yaml")
    if os.path.isfile(task_file):
        os.remove(task_file)
    schedulers[scheduler_name].stop(task_name)