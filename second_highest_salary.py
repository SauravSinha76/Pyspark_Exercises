from pyspark.sql import SparkSession

data_file = '/Users/infoobjects/PycharmProjects/SimplePySparkApp/resources/employee.csv'
spark = SparkSession.builder.appName("second_highest_salary").getOrCreate()

employee = spark.read.option("header","true").csv(data_file)
employee.show()

employee.createTempView("employee")

ranked_employees = spark.sql("""with ramked as 
                                (Select *,dense_rank() over(partition by department order by salary desc) as r from employee) 
                                select * from ramked where r == 1""")
ranked_employees.show()

# filter_data = ranked_employees.filter("r==2")
#
# filter_data.show()
