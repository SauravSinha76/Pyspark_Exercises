from pyspark.sql import SparkSession
import pickle

spark = SparkSession.builder.appName("Work").getOrCreate()

trx_df = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/trx.csv")
adv_df = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/adv.csv")
product = spark.read.option("header","true")\
    .csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/product.csv")
pp = spark.read.option("header","true")\
    .csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/PricePoint.csv")



trx_df.show()

adv_df.show()

trx_df.createTempView("trx")
adv_df.createTempView("adv")
product.createTempView("pro")
pp.createTempView("pp")

spark.sql("""Select distinct(adv.external_ID),Name,Type,City,State,Parent_Firm,price_point,cnt from adv join
        (Select pp.price_point,adv.external_ID,count(*) as cnt from 
        trx join pro join pp join adv on trx.product_ID == pro.product_ID and pro.price == pp.price_point and trx.internal_ID == adv.internal_ID 
        group by pp.price_point,adv.external_ID) rs on adv.external_ID == rs.external_ID """).show()
# rs_df = spark.sql("""Select distinct(adv.external_ID),Name,Type,City,State,Parent_Firm,cnt from adv join
#                         (Select adv.external_ID,count(*) as cnt from trx join adv on trx.internal_ID == adv.internal_ID
#                         group by adv.external_ID) rs on adv.external_ID == rs.external_ID""")


# rs_df.show()



# import pandas as pd
# import pickle
# data = pd.read_parquet('/Users/infoobjects/Downloads/test_2/data')
# model = pickle.load(open("/Users/infoobjects/Downloads/test_2/models.pickle",'rb'))
# pre_date = model.predict(data)
# pre_date.take(1)