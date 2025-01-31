import json
import os
class Parser:
    def __init__(self, job_name):
        self.job_name = job_name
        self.job_file_path = f"../configs/{job_name.lower()}/{job_name.lower()}.json"
        with open(self.job_file_path) as f:
            self.config = self.json_expandvars(json.load(f))
        self.spark_config = dict(self.config['spark_config'])
        self.job_config = dict(self.config['job_config'])
        self.export_config = dict(self.config['export_config'])
        self.logging_config = dict(self.config['logging_config'])

    def json_expandvars(self,o):
        if isinstance(o, dict):
            return {self.json_expandvars(k): self.json_expandvars(v) for k, v in o.items()}
        elif isinstance(o, list):
            return [self.json_expandvars(v) for v in o]
        elif isinstance(o, str):
            return os.path.expandvars(o)
        else:
            return o

    def get_spark_config(self):
        return self.spark_config
    
    def get_job_config(self):
        return self.job_config

    def get_export_config(self):
        return self.export_config

    def get_logging_config(self):
        return self.logging_config
    
    def get_job_name(self):
        return self.job_name
    
    def get_connection_config(self, connection):
        with open(f"../connections/{connection.lower()}.json") as f:
            return dict(self.json_expandvars(json.load(f)))
