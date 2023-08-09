from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test.csv").getOrCreate()

spark.range(1,10,2).show()