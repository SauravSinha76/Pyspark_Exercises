from pyspark.sql import SparkSession

logFile = "/Users/infoobjects/Documents/Software/spark-3.2.1-bin-hadoop3.2/README.md"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

data = spark.sql("select CAST(123.11233 AS DECIMAL(5,2))")
data.show()

data1 = spark.sql("select CAST(123.11233 AS NUMERIC(10,5))")

data1.show()

spark.stop()
