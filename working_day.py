from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("workingDays").getOrCreate()

pay_df = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/paymentdate.csv")
pay_df.show()


holiday = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/holiday.csv")
holiday.show()
x =holiday.select("holiday_date").collect()
print(x)
holiday_br = spark.sparkContext.broadcast(holiday.collect())


df1 = pay_df.withColumn("dayOfweek",dayofweek("date"))
df1.show()

full = df1.join(holiday, df1.date == holiday.holiday_date, "left")

full.filter(full.holiday_date.isNull()).show()

full.withColumn("nextDate", when(full.holiday_date.isNotNull(), next_day(full.date, 'Mon'))
                .otherwise(full.date)).show()

