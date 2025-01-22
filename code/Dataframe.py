class Dataframe:
    def __init__(self, df_name, spark_df):
        self.df_name = spark_df
        self.spark_df = spark_df
    
    def get_df_name(self):
        return self.df_name
    
    def get_spark_df(self):
        return self.spark_df