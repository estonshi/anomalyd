import argparse
import pandas as pd
from prophet import Prophet
import yaml
import time
from typing import Any, Dict, List, Tuple
from vm import Victoriametrics
import sys
import logging


model_dict = {}
task_last_fit = {}
task_last_infer = {}

def __evaluate_anomaly_score(predicted : pd.DataFrame, y : pd.DataFrame) -> List[float]:
    score = 2*(y['y']-predicted['yhat']).abs()/(predicted['yhat_upper']-predicted['yhat_lower'])
    return list(score)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "anamoly detection service")
    parser.add_argument("--config", type=str, help="config file", default=".\model_test_config.yaml")
    parser.add_argument("--vmHost", type=str, default="http://192.168.235.26:8427/")
    parser.add_argument("--vmQUser", type=str, default="vmselect")
    parser.add_argument("--vmQPwd", type=str, default="MZf8x593u")
    parser.add_argument("--vmIUser", type=str, default="vminsert")
    parser.add_argument("--vmIPwd", type=str, default="K4OQS0ieqT")
    parser.add_argument("--vmTenant", type=str, default='3027850591')
    args = parser.parse_args()

    logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = None
    with open(args.config, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    vm = Victoriametrics(datasource_url = args.vmHost,
                 tenant_id= args.vmTenant,
                 health_path = "health",
                 user_query = args.vmQUser,
                 pwd_query = args.vmQPwd,
                 user_insert = args.vmIUser,
                 pwd_insert = args.vmIPwd,
                 timeout = "30s")


    while(True):
        
        current = time.time()
        for c in config:
            try:
                mname = c["name"]
                fit_period = c["fit_period"]
                fit_step = c["fit_step"]
                infer_period = c["infer_period"]
                infer_step = c["infer_step"]
                if mname not in task_last_fit:
                    task_last_fit[mname] = 0
                if mname not in task_last_infer:
                    task_last_infer[mname] = 0
                # fit
                last_fit = task_last_fit[mname]
                if last_fit + fit_period < current:
                    logger.info("Fitting: start, %s", mname)
                    e, result, label = vm.query_series(mname, c["query"], str(fit_step)+"s", str(fit_period)+"s")
                    if e is None:
                        for hashkey in result:
                            data = result[hashkey]
                            label = label[hashkey]
                            model_key = mname + "." + hashkey
                            if model_key not in model_dict:
                                model_dict[model_key] = Prophet()
                            model = model_dict[model_key]
                            try:
                                model.fit(data)
                                task_last_fit[mname] = current
                            except Exception as e:
                                logger.error("Error while fit model: " + str(e))
                            logger.info("Fitting: end, %s", mname)
                    else:
                        logger.error("query error: " + str(e))
            except Exception as e:
                logger.error("Fit error: " + str(e))
            # infer
            try:
                last_infer = task_last_infer[mname]
                if last_infer + infer_period < current:
                    logger.info("Inferring: start, %s", mname)
                    e, result, label = vm.query_series(mname, c["query"], str(infer_step)+"s", str(infer_period)+"s")
                    if e is None:
                        for hashkey in result:
                            data2 = result[hashkey]
                            label2 = label[hashkey]
                            model_key = mname + "." + hashkey
                            if model_key not in model_dict:
                                continue
                            model = model_dict[model_key]
                            predicted = model.predict(data2.loc[:,['ds']])
                            score = __evaluate_anomaly_score(predicted, data2)
                            ts_sec = data2['ds'].astype('int64').divide(1e9).tolist()
                            to_write = {str(k):str(v) for k,v in zip(ts_sec, score)}
                            vm.insert_series([mname], [label2], [to_write])
                            task_last_infer[mname] = current
                    logger.info("Inferring: end, %s", mname)
            except Exception as e:
                logger.error("Infer error: " + str(e))
                raise e

        time.sleep(10)

    
