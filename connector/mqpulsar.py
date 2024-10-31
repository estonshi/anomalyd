import sys
import time
from typing import Dict, Tuple, Any, List
import pulsar
import re
import pandas as pd
import connector._interface as _interface

sys.path.append("..")
import tools

class Pulsar(_interface.Connector):
    def __init__(self,
                 datasource_url = "pulsar://localhost:6650",
                 jwt_token = None,
                 timeout = "3s",
                 target_topic = None,
                ) -> None:
        self.datasource_url = datasource_url
        self.jwt_token = jwt_token
        self.timeout = timeout
        self.target_topic = target_topic
        self.client = None
        self.producer = None
        self.__valid_inputs()
        self.__init_conn()

    def __del__(self):
        if self.producer is not None:
            self.producer.close()
        if self.client is not None:
            self.client.close()

    def __valid_inputs(self) -> None:
        if not re.match(r'^(pulsar://)([a-zA-Z0-9.-])+:([0-9])+/?', self.datasource_url):
            raise ValueError("[CONFIG](Pulsar) 'datasource_url' is invalid")
        if not tools.check_time_range_str(self.timeout):
            raise ValueError("[CONFIG](Pulsar) 'timeout' is invalid")
        
    def __init_conn(self) -> None:
        auth = None
        if self.jwt_token is not None:
            auth = pulsar.AuthenticationToken(self.jwt_token)
        self.client = pulsar.Client(service_url=self.datasource_url, 
                               authentication=auth,
                               operation_timeout_seconds=tools.parse_time_range_str(self.timeout), 
                               connection_timeout_ms=tools.parse_time_range_str(self.timeout)*1000)
        if self.target_topic is not None:
            self.producer = self.client.create_producer(topic=self.target_topic, 
                                                        send_timeout_millis=tools.parse_time_range_str(self.timeout)*1000)
            
    def check_query_args(self, args: dict[str, Any]):
        return False
            
    def query_series(self, query_name : str, queries : str, sampling_period : str, query_length: str) -> Tuple[Exception,Dict[str,pd.DataFrame],Dict[str,Dict[str,str]]] :
        return RuntimeError("[CONFIG](Pulsar) Pulsar cannot query metrics !"), None, None

    def insert_series(self, query_names : List[str], labels : List[dict], values : List[dict[str,str]]) -> bool:
        data_total = []
        for idx, query_name in enumerate(query_names):
            label = labels[idx]
            value = values[idx]
            label_s = "{" + ",".join([k+"=\""+v+"\"" for k,v in label.items()]) + "}"
            for time_s, val in value:
                data = query_name.strip() + label_s + " " + val + " " + time_s
                data_total.append(data)
        try:
            self.producer.send("\n".join(data_total))
            return True
        except:
            return False