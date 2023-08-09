from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re
from pyspark.sql.types import BooleanType

logFile = "/Users/infoobjects/Documents/Software/spark-3.2.1-bin-hadoop3.2/README.md"
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# data = spark.read.text(logFile).cache()
#
# data.show()


@udf(returnType=BooleanType())
def validWord(word):
    if not word or not word.strip() or not bool(re.match('^[a-zA-Z0-9]+$', word.strip())):
        return False
    else:
        return True

#
# data2 = data.withColumn('word', explode(split(col('value'), '\\s+'))) \
#     .filter(validWord(col('word'))) \
#     .groupBy('word') \
#     .count() \
#     .sort('count', ascending=False)
# data2.show()

# .filter((length(trim(col('word'))) > 0) & (col('word').rlike('^[a-zA-Z0-9]+$')))


data = spark.read.text(logFile).rdd.cache()

# data.toDF().show()

words = data.flatMap(lambda x: x.value.split()).filter(lambda x : validWord(x)).map(lambda x: (x, 1))

count = words.reduceByKey(lambda a, b:  a + b)

count.toDF().sort(col('_2'), ascending=False).show()
