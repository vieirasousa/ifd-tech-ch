from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Creating Spark Session
spark = SparkSession.builder.appName("DataWranglingApp").getOrCreate()

# Loading dataset - We are loading local CSVw files to peform the analysis locally, avoiding cloud storage and computing costs 
yellow_taxi = spark.read.format("csv").option("header", "true").load("./data/2023_yellow_taxi_trip.csv")
green_taxi = spark.read.format("csv").option("header", "true").load("./data/2023_green_taxi_trip.csv")

# Converting names and data types from Yellow Taxi dataset
yt = yellow_taxi \
    .withColumnRenamed(
        "VendorID", "vendor_id"
    ) \
    .withColumn(
        "tpep_pickup_datetime", 
        to_timestamp(col("tpep_pickup_datetime"), "MM/dd/yyyy hh:mm:ss a")
    ) \
    .withColumn(
        "tpep_dropoff_datetime", 
        to_timestamp(col("tpep_dropoff_datetime"), "MM/dd/yyyy hh:mm:ss a")
    ) \
    .withColumn(
        "passenger_count", 
        col("passenger_count").cast("integer")
    ) \
    .withColumn(
        "total_amount",
        regexp_replace(col("total_amount"), ",", "").cast("double")
    )

# Converting names and data types from Yellow Taxi dataset
gt = green_taxi \
    .withColumnRenamed(
        "VendorID", "vendor_id"
    ) \
    .withColumn(
        "lpep_pickup_datetime", 
        to_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy hh:mm:ss a")
    ) \
    .withColumn(
        "lpep_dropoff_datetime", 
        to_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy hh:mm:ss a")
    ) \
    .withColumn(
        "passenger_count", 
        col("passenger_count").cast("integer")
    ) \
    .withColumn(
        "total_amount",
        regexp_replace(col("total_amount"), ",", "").cast("double")
    )


# Analysing the data

# Printing statistics for the total amount per month
yt \
    .select("tpep_dropoff_datetime", "total_amount") \
    .groupBy(month("tpep_dropoff_datetime").alias("month")) \
    .agg(sum("total_amount").alias("total_amount_sum")) \
    .select("total_amount_sum") \
    .describe() \
    .show()

# Reproducing the same statistcs as above, but formatted to avoid scientific notation
monthly = yt \
    .select("tpep_dropoff_datetime", "total_amount") \
    .groupBy(month("tpep_dropoff_datetime").alias("month")) \
    .agg(sum("total_amount").alias("total_amount_sum"))

# Generating stats with two decimals (no scientific notation)
stats = monthly.select("total_amount_sum").agg(
    count("total_amount_sum").alias("count"),
    mean("total_amount_sum").alias("mean"),
    stddev("total_amount_sum").alias("stddev"),
    min("total_amount_sum").alias("min"),
    max("total_amount_sum").alias("max")
)

# Printing the final statistics with no scientific notation.
# The average monthly total amount by yellow taxis in NYC during 2023 is about $90 million.
formatted_stats = stats.select(
    col("count"),
    format_number(col("mean"), 2).alias("mean"),
    format_number(col("stddev"), 2).alias("stddev"),
    format_number(col("min"), 2).alias("min"),
    format_number(col("max"), 2).alias("max")
)

formatted_stats.show(truncate=False)

# Combining both yellow and green taxis datasets for further analysis
all_taxis = \
    yt \
    .withColumn("source", lit("yellow")) \
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
    .unionByName(gt \
                 .withColumn("source", lit("green"))
                 .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
                 .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
                 )


# Printing average passenger count per hour of the day, only for May, considering all taxis in the fleet, both Yellow and Green
all_taxis \
    .select("pickup_datetime", "passenger_count") \
    .where(month("pickup_datetime") == 5) \
    .groupBy(hour("pickup_datetime").alias("hour")) \
    .agg(avg("passenger_count").alias("avg_passenger_count")) \
    .orderBy("hour") \
    .show(24)