from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("first_n_last").getOrCreate()

call = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/call_data.csv")

call.show()

call.createOrReplaceTempView("call")

spark.sql("""
        Select user_id1,
                            first_value(user_id2) over(partition by user_id1, date(call_time) 
                            order by call_time) as first_caller from
                            (select caller_id as user_id1, recipient_id as user_id2, call_time from call) 
                                      

        """).show()