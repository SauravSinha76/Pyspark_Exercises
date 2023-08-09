from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Cast test").getOrCreate()

df = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/call_data.csv")

df.show()

df.printSchema()

df.createOrReplaceTempView("swap")

spark.sql("Select *, rank() over (partition by caller_id order by call_time desc) as r from swap").show()