from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Udf").getOrCreate()
df = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/customer.csv")
df.show()

@udf(returnType=StringType())
def upperCase(str):
    return str.upper()

# df_1 = df.withColumn("upperCaseName",upperCase(col("customer_name")))
spark.udf.register("uppercaseUDF",upperCase)
df.createOrReplaceTempView("customer")
df_1 = spark.sql("Select *,uppercaseUDF(customer_name) from customer")
df_1.show()

