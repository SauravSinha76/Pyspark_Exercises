from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Cast test").getOrCreate()

df = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/cast_test.csv")

df.show()

df.printSchema()

df.write.mode("overwrite").parquet("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/out/cast/")

df1 = spark.read.parquet("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/out/cast/")

rdf = df1.selectExpr("name","country","cast(zip_code as int) zip_code","cast(salary as double) salary")
rdf.show()
rdf.printSchema()