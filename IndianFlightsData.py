# Databricks notebook source
spark

# COMMAND ----------

import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder\
    .appName("Indian Flights Data")\
        .getOrCreate()

IndianFlightsDataPath= "/FileStore/tables/IndianFlightsData.csv"
airportsDataPath = "/FileStore/tables/airports.csv"



# COMMAND ----------

IndianFlightsDf = spark.read.options(header=True, inferSchema =True, delimiter =",", badRecordsPath="/FileStore/tables/")\
    .csv(IndianFlightsDataPath)
airportsDf = spark.read.options(header=True, inferSchema=True, delimiter=",")\
    .csv(airportsDataPath)

# COMMAND ----------

IndianFlightsDf.show()

# COMMAND ----------

airportsDf.show(truncate=False)

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/20240211T144733/bad_records"))

# COMMAND ----------

bad_records_path ="dbfs:/FileStore/tables/20240211T144733/bad_records/part-00000-3d0ee19c-54f2-4bef-ba21-72fb130de44b"
bad_records_df = spark.read.json(bad_records_path)
bad_records_df.show(truncate=False)

# COMMAND ----------

cleaned_df = IndianFlightsDf.dropna()
cleaned_df.show()

# COMMAND ----------

IndianFlightsDf = IndianFlightsDf \
    .fillna("N/A", subset= ["FlightNumber", "Airline", "Source", "Destination", "DepartureTime", "ArrivalTime", "Duration", "Price"])

IndianFlightsDf.show()

# COMMAND ----------

IndianFlightsDf = IndianFlightsDf\
    .withColumn("Price", when(IndianFlightsDf["Price"].isNull(), "N/A").otherwise(IndianFlightsDf["Price"]))

IndianFlightsDf.show()

# COMMAND ----------

IndianFlightsDf = IndianFlightsDf.\
    withColumn("DiscountPercentage", when(IndianFlightsDf["Price"] > 4500, "8").otherwise("2"))
IndianFlightsDf.show()

# COMMAND ----------

IndianFlightsDf = IndianFlightsDf \
    .withColumn("Price", when(IndianFlightsDf["Price"].isNull(), "N/A").otherwise(IndianFlightsDf["Price"]))

# Add a new "DiscountPrice" column based on the discount conditions
IndianFlightsDf = IndianFlightsDf \
    .withColumn("DiscountPrice", 
                when((col("DiscountPercentage") == 8), col("Price") * 0.92)
                .when((col("DiscountPercentage") == 2), col("Price") * 0.98)
                .otherwise(col("Price")))
IndianFlightsDf = IndianFlightsDf.dropna().drop("Discount")
IndianFlightsDf.show()


# COMMAND ----------

IndianFlightsDf.write.partitionBy("Source").parquet("/FileStore/tables/source")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/parquet/"))

# COMMAND ----------

grouped_by_source_df = IndianFlightsDf.groupBy("Source").agg(count("*").alias("count"))
grouped_by_source_df.show()

# COMMAND ----------

airportsDf.show(truncate=False)

# COMMAND ----------

Joined_flight_data = IndianFlightsDf.join(airportsDf, IndianFlightsDf["Source"] == airportsDf["City"], "inner")
columns_to_drop = ["Duration", "Price", "DiscountPercentage", "DiscountPrice"]
joined_data = Joined_flight_data.drop(*columns_to_drop).orderBy("DepartureTime")
joined_data.show(truncate=False)

# COMMAND ----------

filtered_joined_data = joined_data.filter(col("Airline") != "N/A")

# Group by "Airline" and calculate the count
joined_Airline_data = filtered_joined_data.groupBy("Airline").agg(count("*").alias("Number_Of_Flights"))

# Show the result
joined_Airline_data.show()

# COMMAND ----------

# Assuming IndianFlightsDf is your DataFrame
# Repartition into 4 partitions
repartitioned_data = IndianFlightsDf.repartition("4")


# COMMAND ----------

# Assuming repartitioned_data is your DataFrame
display(repartitioned_data)


# COMMAND ----------

IndianFlightsDf.show()

# COMMAND ----------

Window_spec = Window.orderBy(col("price").desc())
Indians_flights_window = IndianFlightsDf.withColumn("Price_rank", percent_rank().over(Window_spec))
Indians_flights_window.select("FlightNumber", "Airline", "Price","DiscountPrice","Price_rank").show()

# COMMAND ----------

Indians_flights_window.select("FlightNumber", "Airline", "Price","DiscountPrice","Price_rank").filter(col("price_Rank")==2).show()
