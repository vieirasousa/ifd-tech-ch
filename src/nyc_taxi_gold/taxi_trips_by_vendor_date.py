# Imports
from pyspark.sql.functions import col, lit, when, avg
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType

# Creating Gold schema to persist refined and aggregated data
spark.sql("CREATE SCHEMA IF NOT EXISTS ifood_db_ws.gold")

# Creating table, setting descriptions to fill Table and columns metadata for the SQL Catalog
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS ifood_db_ws.gold.taxi_trips_by_vendor_date (
    vendor_id STRING COMMENT 'Code indicating the TPEP provider that produced the record.',
    trip_date DATE COMMENT 'Date the trip started',
    total_trips INT COMMENT 'Total trips for the vendor on the given date',
    is_above_average BOOLEAN COMMENT 'Flag indicating if the total trips for the vendor on the given date is above the average.'
  )
  USING DELTA
  COMMENT 'NYC Yellow Taxi trips, aggregated by vendor and start date of each trip.'
""")

# Loading Silver table as source for this Gold
taxi_silver = spark.table("ifood_db_ws.silver.taxi_trips_refined")

# Transforming and aggregating data to obtain a table ready for consumption in dashboards - Stage 1
nyc_yellow_taxi_by_vendor_date_stg1 = taxi_silver \
.where(col("source") == "yellow") \
.select( \
    col("vendor_id"), \
    col("pickup_datetime").cast("date").alias("trip_date") \
).groupBy("vendor_id", "trip_date").count().orderBy(col("trip_date")) \
    .withColumnRenamed("count", "total_trips") \
    .withColumn("total_trips", col("total_trips").cast(IntegerType())
)

# Obtaining average trips
avg_trips = nyc_yellow_taxi_by_vendor_date_stg1.agg(avg("total_trips")).collect()[0][0]

# Creating final table with average trips
nyc_yellow_taxi_by_vendor_date_final = nyc_yellow_taxi_by_vendor_date_stg1.withColumn("is_above_average", when(col("total_trips") > avg_trips, lit(True)).otherwise(lit(False)))

# Writing the final Gold table to the Data Warehose
nyc_yellow_taxi_by_vendor_date_final.write.mode("append").saveAsTable("ifood_db_ws.gold.yellow_taxi_trips_by_vendor_date")