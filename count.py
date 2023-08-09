# Decorator
import time

from pyspark.sql import SparkSession


def get_time(func):
    def inner_get_time() -> str:
        start_time = time.time()
        func()
        end_time = time.time()
        return (f"Execution time: {(end_time - start_time)*1000} ms")
    print(inner_get_time())

spark = SparkSession.builder.appName("Count_Decorator").getOrCreate()

df = spark \
    .read \
    .format("csv") \
    .option("header", True) \
    .load("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/trx.csv")
df.show()


# Get count(1) performance
from pyspark.sql.functions import lit, count
@get_time
def x(): df.groupBy("product_ID").count().write.format("noop").mode("overwrite").save()