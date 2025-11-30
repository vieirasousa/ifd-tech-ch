# Imports
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, lit, when
import requests
import json
import time

# Creating Core schema to persist semiraw data
spark.sql("CREATE SCHEMA IF NOT EXISTS ifood_db_ws.core")

# Creating Delta table, setting descriptions to fill Table and columns metadata for the SQL Catalog, and setting the write location to the data lake
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS ifood_db_ws.core.nyc_green_taxi_trips (
    vendor_id STRING COMMENT 'Code indicating the TPEP provider that produced the record.',
    lpep_pickup_datetime TIMESTAMP COMMENT 'Timestamp for the moment the meter was engaged (pickup time).',
    lpep_dropoff_datetime TIMESTAMP COMMENT 'Timestamp for the moment the meter was disengaged (dropoff time).',
    total_amount FLOAT COMMENT 'The final total amount charged to passengers, including taxes, tolls, and other fees. Does not include cash tips.',
    passenger_count FLOAT COMMENT 'The number of passengers reported in the vehicle.'
  )
  USING DELTA
  LOCATION 'abfss://raw@ifdchallenge.dfs.core.windows.net/nyc_taxi/green'
  COMMENT 'NYC Green Taxi trip records Dataset. This is a Core table with semi-raw data fetched from the NYC Taxi API, as a number of columns was dropped and we set a start and an end date, creating a subset from the original data. This table will be an audit source for the Data Engineering team, to be used as reference whenever dataset debugging is needed.'
""")

# Loading config file 
with open("/Workspace/Users/arturvieirasousa@gmail.com/ifd-tech-ch/src/config.json", "r") as f:
    conf_file = json.load(f)

# Defining parameters
app_token = conf_file["app_token"]
start_date = conf_file["start_date"]
end_date = conf_file["end_date"]
api_url = conf_file["api_url_green"]

# Defining limits to the api calls
batch_size = 50000
wait_time = 5

# Defining SOQL Query
order_by = "lpep_pickup_datetime"
select_clause = "vendorid, passenger_count, total_amount, lpep_pickup_datetime, lpep_dropoff_datetime"
where_clause = f"lpep_pickup_datetime >= '{start_date}' AND lpep_dropoff_datetime <= '{end_date}'"

# Fetch Data
offset = 0
total_rows_fetched = 0

while True:
    api_params = {
        "$$app_token": app_token,
        "$select": select_clause,
        "$where": where_clause,
        "$limit": batch_size,
        "$offset": offset,
        "$order": f"{order_by} ASC"
    }

    try:
        response = requests.get(api_url, params=api_params)
        response.raise_for_status()

        data_rows = response.json()
        
        # Creating a PySpark DataFrame
        df = spark.createDataFrame(data_rows)

        # Converting data types lost during the API call
        ready_to_append = df.withColumnRenamed("VendorID", "vendor_id") \
        .withColumn("vendor_id", col("vendor_id").cast("string")) \
        .withColumn("lpep_pickup_datetime", col("lpep_pickup_datetime").cast("timestamp")) \
        .withColumn("lpep_dropoff_datetime", col("lpep_dropoff_datetime").cast("timestamp")) \
        .withColumn("total_amount", col("total_amount").cast("float")) \
        .withColumn("passenger_count", col("passenger_count").cast("float"))
        
        # Appending data to both the Data Warehouse and the data lake, to be used as an easy audit source
        ready_to_append.write \
            .mode("append") \
            .saveAsTable("ifood_db_ws.core.nyc_green_taxi_trips")
        
        rows_in_batch = len(data_rows)

        if rows_in_batch == 0:
            print("\nExtraction complete: the API returned no rows.")
            break

        # Adding the current batch to the main list

        total_rows_fetched += rows_in_batch
        
        print(f"Batch {offset // batch_size + 1} with {rows_in_batch} rows fetched. Total rows fetched: {total_rows_fetched}")

        # After the last batch was added to the list containing all data, 
        # if the batch size is lower than the limit defined on the config file, the process will stop
        if rows_in_batch < batch_size:
            print("Extraction complete.")
            break

        # Define the new offset, for the next iteration
        offset += batch_size
        
        print(f"Pausing for {wait_time} seconds...")
        time.sleep(wait_time)

    except requests.exceptions.RequestException as e:
        print(f"\nError fetching data at offset {offset}: {e}")
        break
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        break

print(f"\nData Processing")
print(f"Total {total_rows_fetched} rows were fetched through the API.")