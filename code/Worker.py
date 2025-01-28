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
            self.memory[step['df_name']] = self.spark.fetch(step['format'], step['df_name'], step['options'])
    
    def export(self):
        for export_item in self.export_config['export_list']:
            self.logger.log_event("INFO", f"Exporting {export_item['name']}")
            self.spark.export(self.memory[export_item['name']], export_item)
            self.logger.log_event("INFO", f"Exported {export_item['name']}")


if __name__ == '__main__':
    import sys
    worker = Worker(sys.argv[1])
    worker.start()