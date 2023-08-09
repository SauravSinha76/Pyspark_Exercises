from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("WindowFunction").getOrCreate()

data = spark.read\
    .option("header","true")\
    .csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/employee.csv")

data.show()

data.distinct().show()
data.dropDuplicates(["salary"]).show()
data.groupBy("salary").agg(countDistinct("name")).show()


window_fn = Window.partitionBy("department").orderBy(desc("salary"))

row_num = data.withColumn("row_num", row_number().over(window_fn))

row_num.show()

row_num.filter("row_num == 1").show()


rank_num = data.withColumn("rank", rank().over(window_fn))

rank_num.show()

data.withColumn("dense_rank",dense_rank().over(window_fn)).show()

data.withColumn("lag",lag("salary",1).over(window_fn)).show()

data.withColumn("lead",lead("salary",1).over(window_fn)).show()


window_fn_spec = Window.partitionBy("department")

data.groupBy("department").agg(avg("salary").alias("avg"),
                               max("salary").alias("max"),
                               min("salary").alias("min"),
                               sum("salary").alias("total")).show()
