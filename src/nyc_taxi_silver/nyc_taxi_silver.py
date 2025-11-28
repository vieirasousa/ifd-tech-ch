# Imports
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StringType, IntegerType, DoubleType
import pyspark.sql.functions

# Creating Silver schema to acommodate Silver datasets
spark.sql("CREATE SCHEMA IF NOT EXISTS ifood_db_ws.silver")

# Creating table, setting descriptions to fill Table and columns metadata for the SQL Catalog
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS ifood_db_ws.silver.taxi_trips_refined (
    vendor_id STRING COMMENT 'Code indicating the TPEP provider that produced the record.',
    tpep_pickup_datetime TIMESTAMP COMMENT 'Timestamp for the moment the meter was engaged (pickup time).',
    tpep_dropoff_datetime TIMESTAMP COMMENT 'Timestamp for the moment the meter was disengaged (dropoff time).',
    total_amount DOUBLE COMMENT 'The final total amount charged to passengers, including taxes, tolls, and other fees. Does not include cash tips.',
    passenger_count INT COMMENT 'The number of passengers reported in the vehicle.'
  )
  USING DELTA
  COMMENT 'This is a Silver table from NYC Yellow Taxi trip records, with cleansed data and correctly typed columns, built on top of the Core Taxi table "ifood_db_ws.core.nyc_yellow_taxi_trips". This table will be used as a direct source for the Analytics layer.'
""")

# Loading Core Taxi table
core_taxi_trips = spark.table("ifood_db_ws.core.nyc_yellow_taxi_trips")

# Filtering and cleansing Core Taxi table
taxi_silver = core_taxi_trips \
    .where(col("total_amount") >= 0) \
    .withColumn("total_amount", col("total_amount").cast(DoubleType())) \
    .where(col("passenger_count").isNotNull()) \
    .withColumn("passenger_count", col("passenger_count").cast(IntegerType())) \
    .where(col("tpep_pickup_datetime") < col("tpep_dropoff_datetime"))

# Writing Silver table do the Data Warehouse
taxi_silver.write.mode("append").saveAsTable("ifood_db_ws.silver.taxi_trips_refined")