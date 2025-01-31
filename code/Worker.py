from Parser import Parser
from Spark import Spark
from Logger import Logger
class Worker:
    def __init__(self, job_name):
        self.parser = Parser(job_name)
        self.logger = Logger(self.parser)
        self.spark = Spark(self.parser)
        self.job_config = self.parser.get_job_config()
        self.export_config = self.parser.get_export_config()
        self.memory = {}
    
    def start(self):
        self.logger.log_start()
        self.spark.start()
        for step in self.job_config['steps']:
            self.execute(step)
        self.export()
        self.spark.stop()
        self.logger.log_stop()
    
    def execute(self, step):
        if step['type'] == 'fetch':
            self.logger.log_event("INFO", f"Fetching {step['df_name']}")
            self.fetch(step)
            self.logger.log_event("INFO", f"Fetched {step['df_name']}")
        elif step['type'] == 'script':
            self.logger.log_event("INFO", f"Executing {step['format']} script {step['script']}")
            self.script(step)
            self.logger.log_event("INFO", f"Executed {step['format']} script {step['script']}")
        elif step['type'] == 'load':
            self.logger.log_event("INFO", f"Loading {step['df_name']}")
            self.load(step)
            self.logger.log_event("INFO", f"Loaded {step['df_name']}")
    
    def fetch(self, step):
        self.memory[step['df_name']] = self.spark.fetch(step['format'], step['df_name'], step['options'])
    
    def load(self, step):
        if step['format'] == 'jdbc':
            connection = self.parser.get_connection_config(step['connection'])
            for key, val in connection.items():
                step['options'][key] = val
        self.spark.load(self.memory[step['df_name']], step['format'], step['mode'], step['options'])
    
    def export(self):
        for export_item in self.export_config['export_list']:
            self.logger.log_event("INFO", f"Exporting {export_item['name']}")
            self.spark.export(self.memory[export_item['name']], export_item)
            self.logger.log_event("INFO", f"Exported {export_item['name']}")

    def script(self, step):
        exec(open(f"./scripts/{step['script']}", ).read(), step['options'])

if __name__ == '__main__':
    import sys
    worker = Worker(sys.argv[1])
    worker.start()