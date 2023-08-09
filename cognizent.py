from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Cognizent").getOrCreate()

df =  spark.read.option("header","true").csv('/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/cog.csv')

df.show()

df1 = df.withColumn("Name",explode(split("Name", ";")))

df1.show()

df1.groupBy("ID").agg(concat_ws(";",collect_list("Name"))).alias("name_bc").orderBy("ID").show()