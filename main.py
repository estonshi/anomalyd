import yaml
import argparse

from connector import *
from model import *
import handler
from scheduler import *

def load_config(file_path : str) -> dict:
    data = None
    with open(file_path, 'r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = "anamoly detection service")
    parser.add_argument("-c", "--config", type=str, help="config file path, default is config.yaml", default="config.yaml")
    parser.add_argument("-m", "--home", type=str, help="Home folder, default is current folder", default=None)
    parser.add_argument("-p", "--web.listen-address", type=int, help="address on which to expose web interface, default 8450", default=8450)
    args = parser.parse_args()

    configs = load_config(args.config)
    readers : dict[str, Connector] = {}
    writers : dict[str, Connector] = {}
    models : dict[str, BaseModel] = {}
    schedulers : dict[str, Scheduler] = {}

    for cnt in configs['connector']:
        if cnt['pipeline'] == 'reader':
            readers[cnt['name']] = globals()[cnt['class']](**cnt['params'])
        elif cnt['pipeline'] == 'writer':
            writers[cnt['name']] = globals()[cnt['class']](**cnt['params'])
    for m in configs['model']:
        models[m['name']] = globals()[m['class']]()
    for sch in configs['scheduler']:
        schedulers[sch['name']] = globals()[m['class']]()

    handler.init_global_configs(args.home, reader=readers, writer=writers, model=models, scheduler = schedulers)
    handler.start_app()

    
