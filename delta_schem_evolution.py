from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

emp = spark.read.json("/Users/infoobjects/SparkWorkSpace/WordCount/src/resource/employees.json")
emp.show()

fisrt = emp.filter(col('id') == '1')

fisrt.write.format('delta')\
            .mode('overwrite')\
            .save("/Users/infoobjects/PycharmProjects/SimplePySparkApp/employes")

second = spark.read.format('delta')\
            .load("/Users/infoobjects/PycharmProjects/SimplePySparkApp/employes")
second.explain()
second.show()
second.printSchema()

third = fisrt.withColumn('department', lit("IT"))
third.show()
third.write.format('delta')\
            .mode('append')\
            .option("mergeSchema", "true")\
            .save("/Users/infoobjects/PycharmProjects/SimplePySparkApp/employes")


forth = spark.read.format('delta')\
            .load("/Users/infoobjects/PycharmProjects/SimplePySparkApp/employes")

forth.printSchema()
forth.show()
fifth = forth.withColumn("department", forth.department.cast(IntegerType()))
fifth.show()
fifth.printSchema()
#
# fifth.write.format('delta')\
#             .mode('append')\
#             .option("mergeSchema", "true")\
#             .save("/Users/infoobjects/PycharmProjects/SimplePySparkApp/employes")
#
# six = spark.read.format('delta')\
#             .load("/Users/infoobjects/PycharmProjects/SimplePySparkApp/employes")
# six.printSchema()
# six.show()
