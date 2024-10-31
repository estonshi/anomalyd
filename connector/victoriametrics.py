import logging
import sys
import re
import requests
import base64
import time
import pandas as pd
from urllib.parse import urlencode, urljoin
from typing import Tuple, Dict, Any, List
import connector._interface as _interface

sys.path.append("..")
import tools

class Victoriametrics(_interface.Connector):
    def __init__(self,
                 datasource_url = "http://localhost:8481/",
                 tenant_id= None,
                 health_path = "health",
                 user = None,
                 password = None,
                 timeout = "30s"
                 ) -> None:
        self.datasource_url = datasource_url
        self.tenant_id = tenant_id
        self.user = user
        self.password = password
        self.timeout = timeout
        self.health_path = health_path
        self.down = False
        self.__valid_inputs()
        #self.__health_check()

    def __valid_inputs(self) -> None:
        if not re.match(r'^((http|https)://)([a-zA-Z0-9.-])+:([0-9])+/?', self.datasource_url):
            raise ValueError("[CONFIG](Victoriametrics) 'datasource_url' is invalid")
        if not tools.check_time_range_str(self.timeout):
            raise ValueError("[CONFIG](Victoriametrics) 'timeout' is invalid")
        
    def __health_check(self) -> None:
        url =  urljoin(self.datasource_url, self.health_path)
        headers = {}
        down_time = 0
        if self.user is not None:
            headers['Authorization'] = 'Basic' + base64.b64encode(f"{self.user}:{self.password}".encode("utf-8")).decode("ascii")
        while True:
            res = requests.get(url=url, headers=headers)
            if res.status_code == 200:
                time.sleep(60)
                continue
            else:
                logging.error("[CONFIG](Victoriametrics) service '%s' is down ...", self.datasource_url)
                down_time += 1
                if down_time > 30:
                    logging.error("[CONFIG](Victoriametrics) stop checking service '%s'", self.datasource_url)
                    self.down = True
                    return
                time.sleep(120)

    def __get_ingest_url(self):
        if self.tenant_id is not None and len(self.tenant_id) > 0:
            return self.datasource_url + "insert/" + self.tenant_id + "/prometheus/api/v1/import/prometheus"
        else:
            return self.datasource_url + "api/v1/import/prometheus"
        
    def __get_query_url(self):
        if self.tenant_id is not None and len(self.tenant_id) > 0:
            return self.datasource_url + "select/" + self.tenant_id + "/prometheus/api/v1/query_range"
        else:
            return self.datasource_url + "api/v1/query_range"

    def check_query_args(self, args : dict[str,Any]):
        return super().check_query_args(args=args)

    def query_series(self, query_name : str, queries : str, sampling_period : str, query_length: str) -> Tuple[Exception,Dict[str,pd.DataFrame],Dict[str,Dict[str,str]]] :
        if self.down:
            return ValueError("[CONFIG](Victoriametrics) datasource is down"), None, None
        if not tools.check_time_range_str(sampling_period) or \
            tools.check_time_range_str(query_length):
            return ValueError("[CONFIG](Victoriametrics) 'sampling_period' or 'query_length' is invalid"), None, None
        end = time.time()
        start = end - tools.parse_time_range_str(query_length)
        queries_params = {"query": urlencode(queries), "start": start, "end": end, "step": sampling_period}
        url = self.__get_query_url()
        headers = {"Accept-Encoding":"gzip, deflate"}
        if self.user is not None:
            headers['Authorization'] = 'Basic' + base64.b64encode(f"{self.user}:{self.password}".encode("utf-8")).decode("ascii")
        res = requests.get(url=url, params=queries_params, headers=headers, timeout=tools.parse_time_range_str(self.timeout))
        data = res.json()
        if data["data"]["result"] is None or len(data["data"]["result"]) == 0 :
            return ValueError("No data returned"), None, None
        result : Dict[str,pd.DataFrame] = {}
        labels : Dict[str,Dict[str,str]] = {}
        for metric in data["data"]["result"]:
            label : dict = metric["metric"]
            if query_name is not None and query_name != '':
                label['__name__'] = query_name
            sid = tools.map_hash(labels)
            df = pd.DataFrame(columns=['ds', 'y'], data=metric["values"])
            df['ds'] = pd.to_datetime(df['ds'], unit='s')
            result[sid] = df
            label.pop('__name__')
            labels[sid] = label
        return None, result, labels
        
    def insert_series(self, query_names : List[str], labels : List[dict], values : List[dict[str,str]]) -> bool:
        if self.down:
            return False
        data_total = []
        url = self.__get_ingest_url()
        for idx, query_name in enumerate(query_names):
            label = labels[idx]
            value = values[idx]
            label_s = "{" + ",".join([k+"=\""+v+"\"" for k,v in label.items()]) + "}"
            for time_s, val in value:
                data = query_name.strip() + label_s + " " + val + " " + time_s
                data_total.append(data)
        headers = {"Accept-Encoding":"gzip, deflate", "Content-Type": "text/plain"}
        if self.user is not None:
            headers['Authorization'] = 'Basic' + base64.b64encode(f"{self.user}:{self.password}".encode("utf-8")).decode("ascii")
        res = requests.put(url=url, headers=headers, data="\n".join(data_total))
        return res.status_code <= 205 and res.status_code >= 200        

            