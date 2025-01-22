from Parser import Parser
from Spark import Spark
from Logger import Logger
class Worker:
    def __init__(self, job_name):
        self.parser = Parser(job_name)
        self.logger = Logger(self.parser)
        self.spark = Spark(self.parser)
        self.job_config = self.parser.get_job_config()
        self.memory = {}
    
    def start(self):
        self.logger.log_start()
        self.spark.start()
        for step in self.job_config['steps']:
            self.execute(step)
        self.spark.stop()
        self.logger.log_stop()
    
    def execute(self, step):
        if step['type'] == 'fetch':
            self.memory[step['df_name']] = self.spark.fetch(step['format'], step['df_name'], step['options'])
            print(self.memory[step['df_name']].get_spark_df().show())


if __name__ == '__main__':
    import sys
    worker = Worker(sys.argv[1])
    worker.start()