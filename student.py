from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("student").getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv("/Users/infoobjects/SparkWorkSpace/WordCount/src/resource/students.csv")

df.show()

result = df.groupBy("student_name").agg(sum("marks").alias("total"),
                                        avg("marks").alias("avg_marks"))

result.show()

spark.range(1,10,2).show()