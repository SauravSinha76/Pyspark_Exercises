from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("emp_duration").getOrCreate()

df = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/swap.csv")

df.show()
df.printSchema()

df.createOrReplaceTempView("swap")

new_df = spark.sql("""Select *, case when status == 'Logout' then cast(to_timestamp(swap_time) as long) - cast(to_timestamp(last_login) as long) end 
                   from (select *,lag(swap_time) over (partition by empid order by swap_time) as last_login  from swap)""")

new_df.show()