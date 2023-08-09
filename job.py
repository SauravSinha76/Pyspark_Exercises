from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from udfs import get_english_name, get_start_year, get_trend



class BirdsETLJob:
    input_path = '/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/birds.csv'

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[1]")
                                          .appName("BirdsETLJob")
                                          .getOrCreate())

    def extract(self):
        input_schema = StructType([StructField("Species", StringType()),
                                   StructField("Category", StringType()),
                                   StructField("Period", StringType()),
                                   StructField("Annual percentage change", DoubleType())
                                   ])
        return self.spark_session.read.csv(self.input_path, header=True, schema=input_schema)

    def transform(self, df):
        df.show()
        df2 =df.select(get_english_name(col('Species')).name('species'),col('category'),
                        get_start_year(col('Period')).name('collected_from_year'),
                           col('Annual percentage change').name('annual_percentage_change'),
                        get_trend(col('annual_percentage_change')).name('trend'))
        df2.show()
    def run(self):
        return self.transform(self.extract())



etl = BirdsETLJob()
etl.run()