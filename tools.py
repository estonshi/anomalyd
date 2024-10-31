import sys
import hashlib
import re


def map_hash(map : dict) -> str :
    raw_str = ""
    for k in sorted(map):
        raw_str += k + ":" + map[k] + ";"
    return hashlib.md5(raw_str.encode(encoding='utf-8')).hexdigest()

def check_time_range_str(time_range : str):
    return re.match(r'^([0-9])+(s|m|h|d|w)+$', time_range)
    
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