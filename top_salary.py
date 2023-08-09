from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("top_3").getOrCreate()

emp = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/emp.csv")

depart = spark.read.option("header","true").csv("/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/departmnet.csv")

emp.createOrReplaceTempView("employee")
depart.createOrReplaceTempView("department")

spark.sql("Select department_id,name,salary,dense_rank() over(partition by department_id order by salary desc) as rank from employee").show()
spark.sql("""
        With agg as (Select department_id,name,salary,dense_rank() over(partition by department_id order by salary desc) as rank from employee)
        select d.name as department, a.name, a.salary from
        agg as a join department as d on d.id == a.department_id
        where a.rank < 4

            """).show()