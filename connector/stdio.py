

class Stdio(_interface.Connector):

    def __init__(self):
        self.id = 1

    def check_query_args(self, args : dict[str,Any]):
        return True

    def query_series(self, query_name : str, queries : str, sampling_period : str, query_length: str) -> Tuple[Exception,Dict[str,pd.DataFrame],Dict[str,Dict[str,str]]] :
        return None, {}, {}

    def insert_series(self, query_names : List[str], labels : List[dict], values : List[Dict[str,str]]) -> bool:
        for i in range(len(query_names)):
            metric = query_names[i]
            label = labels[i]
            value = values[i]
            print(f"{metric} {label} {value}")