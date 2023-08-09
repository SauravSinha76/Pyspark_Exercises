from delta import *
from delta.tables import *
from pyspark.sql.functions import *

builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()


emp = spark.read.json("/Users/infoobjects/SparkWorkSpace/WordCount/src/resource/employees.json")
emp.show()
emp.printSchema()

DeltaTable.createOrReplace(spark) \
  .tableName("default.employees") \
  .addColumn("id", "INT") \
  .addColumn("name", "STRING") \
  .addColumn("salary", "LONG") \
  .location("/tmp/delta/employees") \
  .execute()

casted = emp.selectExpr("cast(id as string) id","name","cast(salary as string)")

casted.printSchema()
casted.write.format('delta')\
            .mode("append")\
            .option("mergeSchema", "true")\
            .save("/tmp/delta/employees")

forth = spark.read.format('delta')\
            .load("/tmp/delta/employees")

# val df3 = newemDetails.selectExpr("cast(id as int) id" ,"cast(name as string) name","cast(salary as int) salary",
#       "cast(location as string) location")

forth.show()
forth.printSchema()

spark.sql("Select * from default.employees").show()




# spark.sql("Select * from employees").show()