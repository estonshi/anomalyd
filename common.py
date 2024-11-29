import sys
import os
import hashlib
import re

home_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
task_folder = None
model_folder = None
output_metrics_prefix = "_model_output"

def check_home_folder(specified_home_folder = None):
    global home_folder, task_folder, model_folder
    if specified_home_folder is not None:
        if not os.path.isdir(specified_home_folder):
            raise IOError("Unknown home path : %s", specified_home_folder)
        home_folder = specified_home_folder
    task_folder = os.path.join(home_folder, 'task')
    model_folder = os.path.join(home_folder, 'model')
    if not os.path.isdir(home_folder):
        os.makedirs(home_folder)
    if not os.path.isdir(task_folder):
        os.mkdir(task_folder)
    if not os.path.isdir(model_folder):
        os.mkdir(model_folder)

def map_hash(map : dict) -> str :
    raw_str = ""
    for k in sorted(map):
        raw_str += str(k) + ":" + str(map[k]) + ";"
    return hashlib.md5(raw_str.encode(encoding='utf-8')).hexdigest()

def check_time_range_str(time_range : str):
    return re.match(r'^([0-9])+(s|m|h|d|w)+$', time_range) is not None
    
def parse_time_range_str(time_range : str) -> int :
    '''
    parse time range like '3s'/'4m'/'2h'/'5d'/'1w' to seconds
    '''
    if time_range.endswith('s'):
        return int(time_range.strip('s'))
    elif time_range.endswith('m'):
        return int(time_range.strip('m')) * 60
    elif time_range.endswith('h'):
        return int(time_range.strip('h')) * 3600
    elif time_range.endswith('d'):
        return int(time_range.strip('d')) * 3600 * 24
    elif time_range.endswith('w'):
        return int(time_range.strip('w')) * 3600 * 24 * 7
    else:
        raise ValueError("Unknown format")

def default_time_window(whole_period : str, points : int) -> str:
    seconds = parse_time_range_str(whole_period)
    window_sec = seconds / points
    if window_sec <= 0:
        window_sec = 1
    return str(window_sec) + "s"