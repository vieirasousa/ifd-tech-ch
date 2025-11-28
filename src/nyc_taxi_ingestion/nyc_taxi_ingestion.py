# Creating Core schema to persist semiraw data
spark.sql("DROP TABLE IF EXISTS ifood_db_ws.core.nyc_yellow_taxi")

spark.sql("""CREATE TABLE IF NOT EXISTS ifood_db_ws.core.nyc_yellow_taxi (
  vendor_id STRING,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  total_amount FLOAT,
  passenger_count FLOAT
)""")

# Imports
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, lit, when
import requests
import json
import time

# Loading config file 
with open("/Workspace/Users/arturvieirasousa@gmail.com/ifd-tech-ch/src/config.json", "r") as f:
    conf_file = json.load(f)

# Defining parameters
app_token = conf_file["app_token"]
start_date = conf_file["start_date"]
end_date = conf_file["end_date"]
api_url = conf_file["api_url"]

# Defining limits to the api calls
batch_size = 50000
wait_time = 5

# Defining SOQL Query
order_by = "tpep_pickup_datetime"
select_clause = "vendorid, passenger_count, total_amount, tpep_pickup_datetime, tpep_dropoff_datetime"
where_clause = f"tpep_pickup_datetime >= '{start_date}' AND tpep_dropoff_datetime <= '{end_date}'"

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
        .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
        .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp")) \
        .withColumn("total_amount", col("total_amount").cast("float")) \
        .withColumn("passenger_count", col("passenger_count").cast("float"))

        ready_to_append.write.mode("append").saveAsTable("ifood_db_ws.core.nyc_yellow_taxi")
        
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