import json
class Parser:
    def __init__(self, job_name):
        self.job_name = job_name
        self.job_file_path = f"../configs/{job_name}/{job_name}.json"
        with open(self.job_file_path) as f:
            data = json.load(f)
        self.spark_config = dict(data['spark_config'])
        self.job_config = dict(data['job_config'])
        self.export_config = dict(data['export_config'])
        self.logging_config = dict(data['logging_config'])
    
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
