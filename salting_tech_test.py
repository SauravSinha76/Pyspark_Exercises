from pyspark.sql import SparkSession
import random

def generate_fact_data(counter=100):
    fact_records = []
    dim_keys = ["D100", "D101", "D102", "D103", "D104"]
    order_ids = ["ORD" + str(i) for i in range(1001, 1010)]
    qty_range = [i for i in range(10, 120)]
    for i in range(counter):
        _record = [i, random.choice(order_ids), random.choice(dim_keys), random.choice(qty_range)]
        fact_records.append(_record)
    return fact_records


spark = SparkSession.builder.appName("Salting Test").getOrCreate()


fact_records = generate_fact_data(200)
dim_records = [
    ["D100", "Product A"],
    ["D101", "Product B"],
    ["D102", "Product C"],
    ["D103", "Product D"],
    ["D104", "Product E"]
]
_fact_cols = ["id", "order_id", "prod_id", "qty"]
_dim_cols = ["prod_id", "prod_name"]

# Generate Fact Data Frame
fact_df = spark.createDataFrame(data = fact_records, schema=_fact_cols)
len(fact_df.columns)

fact_df.printSchema()
fact_df.show(10, truncate = False)

# Generate Prod Dim Data Frame
dim_df = spark.createDataFrame(data = dim_records, schema=_dim_cols)
dim_df.printSchema()
dim_df.show(10, False)

# Set Spark parameters - We have to turn off AQL to demonstrate Salting
spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.shuffle.partitions", 5)
# Check the parameters
print(spark.conf.get("spark.sql.adaptive.enabled"))
print(spark.conf.get("spark.sql.shuffle.partitions"))

# Lets join the fact and dim without salting
joined_df = fact_df.join(dim_df, on="prod_id", how="leftouter")
joined_df.show(10, False)

# Check the partition details to understand distribution
from pyspark.sql.functions import spark_partition_id, count
partition_df = joined_df.withColumn("partition_num", spark_partition_id()).groupBy("partition_num").agg(count("id"))
partition_df.show()

import random
from pyspark.sql.functions import udf
# UDF to return a random number every time
def rand(): return random.randint(0, 4) #Since we are distributing the data in 5 partitions
rand_udf = udf(rand)
# Salt Data Frame to add to dimension
salt_df = spark.range(0, 5)
salt_df.show()

# Salted Fact
from pyspark.sql.functions import lit, expr, concat
salted_fact_df = fact_df.withColumn("salted_prod_id", concat("prod_id",lit("_"), lit(rand_udf())))
salted_fact_df.show(10, False)


# Salted DIM
salted_dim_df = dim_df.join(salt_df, how="cross").withColumn("salted_prod_id", concat("prod_id", lit("_"), "id")).drop("id")
salted_dim_df.show()

# Lets make the salted join now
salted_joined_df = salted_fact_df.join(salted_dim_df, on="salted_prod_id", how="leftouter")
salted_joined_df.show(10, False)

# Check the partition details to understand distribution
from pyspark.sql.functions import spark_partition_id, count
partition_df = salted_joined_df.withColumn("partition_num", spark_partition_id()).groupBy("partition_num") \
    .agg(count(lit(1)).alias("count")).orderBy("partition_num")
partition_df.show()