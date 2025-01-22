from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from Dataframe import Dataframe
class Spark:
    def __init__(self, parser):
        self.spark_config = parser.get_spark_config()
        self.job_name = parser.get_job_name()

    def start(self):
        conf = SparkConf().setAppName(self.job_name).setMaster(self.spark_config['spark.master'])
        for key, val in self.spark_config['config'].items():
            conf.set(key, val)
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    def stop(self):
        self.spark.stop()

    def get_spark_instance(self):
        return self.spark
    
    def fetch(self, format, df_name, options):
        df = None
        if format == 'rest':
            #call rest api
        else:
            df = self.spark.read.format(format).options(**options).load()
        return Dataframe(df_name, df)