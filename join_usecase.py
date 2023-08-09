from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Join").getOrCreate()

customerdf = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/customer.csv")

customerdf.show()

orderdf = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/order.csv")
orderdf.show()


inner = customerdf.join(orderdf,customerdf.customer_id == orderdf.customer_id,"inner")
inner.show()


outer = customerdf.join(orderdf,customerdf.customer_id == orderdf.customer_id,"outer")
outer.show()