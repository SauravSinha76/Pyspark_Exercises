from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType


builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()


schema = StructType()\
      .add("id", IntegerType(), True)\
      .add("name", StringType(), True)\
      .add("salary", DoubleType(), True)

df = spark.readStream.schema(schema)\
    .json("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/cloudfile")

df_1 =df.withColumn("department",col=lit('IT'))


df_1.writeStream\
        .trigger(once=True)\
        .format("delta")\
        .outputMode("append")\
        .option("mergeSchema","true")\
        .option("checkpointLocation", "/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/_checkpoints/")\
        .start("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/cloudfile_out/").awaitTermination()
  # .option("checkpointLocation", "/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/_checkpoints/")\
  # .start("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/cloudfile_out/")

