# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, avg, when, expr
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sparkConfig = SparkConf().setAppName("AccentureData").setMaster("local[2]")
spark = SparkSession.builder.config(conf=sparkConfig).getOrCreate()

# COMMAND ----------

# Sample JSON data
json_data = '''
{
  "employees": [
    {
      "id": 1,
      "name": "John",
      "position": "Developer"
    },
    {
      "id": 2,
      "name": "Jane",
      "position": "Data Scientist"
    },
    {
      "id": 3,
      "name": "Bob",
      "position": "Analyst"
    }
  ]
}
'''

# Now, you can use this variable (json_data) in your PySpark operations


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a Spark session
spark = SparkSession.builder.appName("YourAppName").getOrCreate()

# Corrected JSON data with a key "employees"
json_data = [
    {"id": 1, "name": "John", "position": "Developer"},
    {"id": 2, "name": "Jane", "position": "Data Scientist"},
    {"id": 3, "name": "Bob", "position": "Analyst"}
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("position", StringType(), True)
])

# Create a DataFrame with the specified schema
df = spark.read.schema(schema).json(spark.sparkContext.parallelize([json_data]))

# Show the DataFrame
df.show()


# COMMAND ----------

selected_columns = df.select("id", 'name')
print("DataFrame with selected columns:")
selected_columns.show()

# COMMAND ----------

filtered_data = df.filter(df["id"] > 2 )
print("Filtered DataFrame:")
filtered_data.show()

# COMMAND ----------

df_with_new_column = df.withColumn("role_length", length(df["position"]))
print("DataFrame with a new column (role_length):")
df_with_new_column.show()

# COMMAND ----------

# Action: Aggregation - Calculate the average ID
average_id = df.agg(avg("id")).collect()[0][0]
print(f"average_id : {average_id}")

# COMMAND ----------

rowCount = df.count()
print(f"Number of rows in file are : {rowCount}")

# COMMAND ----------

sorted_df = df.orderBy(col("id").desc())
print("Sorted DataFrame:")
sorted_df.show()

# COMMAND ----------

grouped_df = df.groupBy("position").count()
print("Grouped DataFrame:")
grouped_df.show()

# COMMAND ----------

df_category = df.withColumn("Category", when(col("position") == "Developer", "IT").otherwise("Non-IT"))
df_category.show()

# COMMAND ----------

data1 = [
    {"id": 1, "name": "John", "position": "Developer"},
    {"id": 2, "name": "Jane", "position": "Data Scientist"},
    {"id": 3, "name": "Bob", "position": "Analyst"}
]

# Sample data for the second DataFrame
data2 = [
    {"id": 1, "department": "IT"},
    {"id": 2, "department": "Data Science"},
    {"id": 4, "department": "Finance"}
]

# COMMAND ----------

data1 = [
    {"id": 1, "name": "John", "position": "Developer", "salary": 80000, "company": "ABC Inc"},
    {"id": 2, "name": "Jane", "position": "Data Scientist", "salary": 100000, "company": "XYZ Corp"},
    {"id": 3, "name": "Bob", "position": "Analyst", "salary": 75000, "company": "ABC Inc"}
]

# Sample data for the second DataFrame
data2 = [
    {"id": 1, "department": "IT", "company": "ABC Inc"},
    {"id": 2, "department": "Data Science", "company": "XYZ Corp"},
    {"id": 4, "department": "Finance", "company": "ABC Inc"}
]

# Define the schema for both DataFrames
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema1 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("position", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("company", StringType(), True)
])

schema2 = StructType([
    StructField("id", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("company", StringType(), True)
])
df1 = spark.createDataFrame(data1, schema=schema1)

# Create the second DataFrame
df2 = spark.createDataFrame(data2, schema=schema2)
print("DataFrame 1:")
df1.show()

print("DataFrame 2:")
df2.show()

# COMMAND ----------

inner_join_result = df1.join(df2, on="id", how="inner")
inner_join_result.show()

# COMMAND ----------

left_join_result = df1.join(df2, on="id", how="left")
print("Left Join Result:")
left_join_result.show()

# COMMAND ----------

right_join_result = df1.join(df2, on="id", how="right").select("id", "name", "salary", "department")

print("right Join Result:")
right_join_result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Average Salary

# COMMAND ----------

average_salary = df1.agg(avg("salary")).collect()[0][0]
print(f"Average Salary is : {average_salary}")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("YourAppName").getOrCreate()

# Sample data
data = [
    {"id": 1, "name": "John", "salary": 80000},
    {"id": 2, "name": "Jane", "salary": 100000},
    {"id": 3, "name": "Bob", "salary": 75000},
    {"id": 4, "name": "Alice", "salary": 90000}
]

# Define the schema for the DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)
df.orderBy(col("salary").desc()).show()
# Find the second-highest salary
second_highest_salary = df.orderBy(col("salary").desc()).select("salary").limit(1).collect()[0][0]

print(f"The second-highest salary is: {second_highest_salary}")

# COMMAND ----------

df_with_bonus = df.withColumn("salary_with_bonus", col("salary") + 20000)
df_with_bonus.show()

# COMMAND ----------

df_with_bonus = df_with_bonus.withColumn("bonus", col("salary_with_bonus") - col("salary"))
df_with_bonus.show()

# COMMAND ----------

metro_cities = ["Mumbai", "Bangalore", "Chennai", "Delhi", "Kolkata", "Hyderabad"]
df_with_city_type = df.withColumn

# COMMAND ----------

data = [
    {"id": 1, "name": "John", "location": "Mumbai", "salary": 80000},
    {"id": 2, "name": "Jane", "location": "Bangalore", "salary": 100000},
    {"id": 3, "name": "Bob", "location": "Pune", "salary": 75000},
    {"id": 4, "name": "Alice", "location": "Hyderabad", "salary": 90000},
    {"id": 5, "name": "Charlie", "location": "Chennai", "salary": 95000}
]

rdd = spark.sparkContext.parallelize(data)
filtered_rdd = rdd.filter(lambda x: x["salary"] > 80000)

names_rdd = rdd.map(lambda x: x["name"])

print(filtered_rdd.collect())
print("\nNames RDD:")
print(names_rdd.collect())

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("YourAppName").getOrCreate()

# Sample data for the RDD
data = [
    {"id": 1, "name": "John", "salary": 80000},
    {"id": 2, "name": "Jane", "salary": 100000},
    {"id": 3, "name": "Bob", "salary": 75000},
    {"id": 4, "name": "Alice", "salary": 90000},
    {"id": 5, "name": "Charlie", "salary": 95000}
]

# Create an RDD from the data
rdd = spark.sparkContext.parallelize(data)

# Extract the salary values
salary_values = rdd.map(lambda x: x["salary"])

# Calculate the sum and count using reduce
sum_salary = salary_values.reduce(lambda x, y: x + y)
count_salary = salary_values.count()

# Calculate the average
average_salary = sum_salary / count_salary

# Display the results
print("Salary values:", salary_values.collect())
print("Average Salary:", average_salary)


# COMMAND ----------

print("RDD Content:")
for row in rdd.collect():
    print(row)

# COMMAND ----------


