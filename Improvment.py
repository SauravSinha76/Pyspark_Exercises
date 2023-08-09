from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Improvment").getOrCreate()

df = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/test.csv")
df1 = df[['TestId']]
df1.show()
print(df1.schema)
df.show()

df.createOrReplaceTempView("tests")

spark.sql(""" Select TestId from 
                (select TestId, case when (Marks - old_marks) > 0 then true else false end as improved from
                (Select TestId,Marks,lag(Marks) over(order by TestId) as old_Marks from tests)) where improved == 'true'""").explain()