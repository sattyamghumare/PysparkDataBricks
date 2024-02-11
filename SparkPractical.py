# Databricks notebook source
spark

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder.appName("AirLines").getOrCreate()

# COMMAND ----------

file_path = "/FileStore/tables/airlines.csv"

options = {
    "header" : "true",
    "inferSchema" : "true",
    "delimiter" : ",",
    #"mode" = "PERMISSIVE"
    #"mode": "FAILFAST"
    #"columnNameOfCorruptRecord": "_corrupt_record"
    "columnNameOfCorruptRecord": "_corrupt_record",
    "badRecordsPath": "/FileStore/tables/bad_records"
   # "nullValues": ["", "NA", "null"],
   # "nullable": "true"
}

df = spark.read.format("csv").options(**options).load(file_path)

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

df_with_id = df.withColumn("Id", monotonically_increasing_id())
df_with_id.show(6, truncate=False)
df_with_id.write.mode("overwrite").parquet("/FileStore/tables/parquet/")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))


# COMMAND ----------

from pyspark.accumulators import AccumulatorParam
accumulator_var = spark.sparkContext.accumulator(0)
print(f"value of accumalator Varible is : {accumulator_var}")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.window import Window
data = [("Alice", "Sales", 5000),
        ("Bob", "Marketing", 6000),
        ("Charlie", "Sales", 7000),
        ("David", "Marketing", 5500),
        ("Eva", "Sales", 4500),
        ("Frank", "Marketing", 8000)]

columns = ["Employee", "Department", "Salary"]
df = spark.createDataFrame(data, columns)

# COMMAND ----------

window_spec = Window.partitionBy("Department").orderBy("Salary")
df_transformed = df.withColumn("RowNumber", row_number().over(window_spec)) \
                  .withColumn("TotalSalary", sum("Salary").over(window_spec)) \
                  .withColumn("AvgSalary", avg("Salary").over(window_spec))

# Show the result
df_transformed.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sum, avg, col

# Initialize Spark session
spark = SparkSession.builder.appName("WindowFunctionExample").getOrCreate()

# Sample DataFrame
data = [("Alice", "Sales", 5000),
        ("Bob", "Marketing", 6000),
        ("Charlie", "Sales", 7000),
        ("David", "Marketing", 5500),
        ("Eva", "Sales", 4500),
        ("Frank", "Marketing", 8000)]

columns = ["Employee", "Department", "Salary"]
df = spark.createDataFrame(data, columns)

# Define a window specification
window_spec = Window.partitionBy("Department").orderBy("Salary")

# Use window functions to compute row_number, sum, and avg
df_transformed = df.withColumn("RowNumber", row_number().over(window_spec)) \
                  .withColumn("TotalSalary", sum("Salary").over(window_spec)) \
                  .withColumn("AvgSalary", avg("Salary").over(window_spec))

# Show the result
df_transformed.show()

# Stop the Spark session


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

# Initialize Spark session
spark = SparkSession.builder.appName("SecondHighestSalaryExample").getOrCreate()

# Sample DataFrame
data = [("Alice", "Sales", 5000),
        ("Bob", "Marketing", 6000),
        ("Charlie", "Sales", 7000),
        ("David", "Marketing", 5500),
        ("Eva", "Sales", 4500),
        ("Frank", "Marketing", 8000)]

columns = ["Employee", "Department", "Salary"]
df = spark.createDataFrame(data, columns)

# Define a window specification partitioned by "Department" and ordered by "Salary" in descending order
window_spec = Window.partitionBy("Department").orderBy(col("Salary").desc())


# Use dense_rank to find the second-highest salary
df_transformed = df.withColumn("Rank", dense_rank().over(window_spec))

# Filter for rows with rank 2 (second-highest salary)
second_highest_salary_df = df_transformed.filter(col("Rank") == 2)

# Show the result
second_highest_salary_df.show(1)

# Stop the Spark session


# COMMAND ----------

df_transformed.orderBy(col("Rank").desc()).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("GroupByExample").getOrCreate()

# Sample DataFrame
data = [("Alice", "Sales", 5000),
        ("Bob", "Marketing", 6000),
        ("Charlie", "Sales", 7000),
        ("David", "Marketing", 5500),
        ("Eva", "Sales", 4500),
        ("Frank", "Marketing", 8000)]

columns = ["Employee", "Department", "Salary"]
df = spark.createDataFrame(data, columns)

# Use groupBy on "Department" and orderBy on "Salary" within each group
window_spec = Window().orderBy(col("Salary").desc())

# Use row_number to assign a unique row number within each group
df_transformed = df.withColumn("RowNumber", row_number().over(window_spec))

# Show the result



# COMMAND ----------

df.show()

# COMMAND ----------

df_transformed.filter(col("RowNumber") == 2).show()
