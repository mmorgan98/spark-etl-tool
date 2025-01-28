from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import requests
import json
from datetime import datetime, timezone
from Dataframe import Dataframe
class Spark:
    def __init__(self, parser):
        self.spark_config = parser.get_spark_config()
        self.job_name = parser.get_job_name()
        self.export_replace_config = {
            "[DATE]": datetime.now(timezone.utc).strftime("%Y-%m-%d")
        }

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
        if format.lower() == 'rest':
            response = requests.get(options['url'], params=options['params'], headers=options['headers'])
            if options['file_format'] == 'json':
                json_response = response.json()[options['data_key']]
                df = self.spark.createDataFrame(json_response)
        else:
            df = self.spark.read.format(format).options(**options).load()
        return Dataframe(df_name, df)
    
    def export(self, df, options):
        for key in self.export_replace_config.keys():
            options['file_name'] = options['file_name'].replace(key, self.export_replace_config[key])
        if options['format'].lower() == 'csv':
            with open(f"{options['path']}{options['file_name']}.{options['format']}", 'w') as f:
                f.write(df.get_spark_df().toPandas().to_csv(index=False))