# iFood - Technical Challenge

### Architecture

###### This document details the architecture for this project and the decisions made to develop it.
-------------------------------------------------------------------------
## Overview

#### The model
The architecture designed for this project borrows concepts from different models. The evolving stages over a raw set of data comes notoriously from a Medallion architeture, with its raw/bronze, silver and gold layers, but on the raw data we took some liberties, as for this case we didn't need all the columns or rows present on the original dataset. 

With that orientation, we dropped the raw layer in favor of a "core" layer, which derivatives from the raw data, but with some minor filtering and dropped columns, and made it generally available for direct SQL querying (to allow for quick validation efforts), in contrast to raw data, which is not commonly accessible from a SQL engine, due to its size, quality and other specificities.

#### Components and resources used
This pipeline was developed with PySpark (with some embedded Spark SQL DDL code for the Data Warehouse component) scripts, to work on a Databricks environment, leveraging its Big Data capabilities, orchestration tools, and its SQL engine as well. Excluding cloud resources related to permissions (such as connectors, policies, roles and secretsm which are dependent on the cloud provider), the pipeline is comprised of the following components:

* A Databricks workspace, with Data Warehouse and Unit Catalog capabilities to orchestrate and run scripts.
* A storage solution (such as Azure Blob or AWS S3 Bucket) on which the data lake is structured.
* The following PySpark scripts, in their respective folders:
  * nyc_taxi_ingestion/nyc_taxi_ingestion_yellow.py
  * nyc_taxi_ingestion/nyc_taxi_ingestion_green.py
  * nyc_taxi_silver/nyc_taxi_silver.py
  * nyc_taxi_gold/nyc_taxi_trips_by_vendor_date.py
* A config file (config.json) containing API keys and parameters.

In order to reduce the need for external dependencies, which add complexity and unecessary steps to the pipeline's setup, we opted for using only native PySpark libraries installed on Databricks instances. The requirements.txt file present on the root folder is to be used by the analysis script and is not requirement for the ETL pipeline.

#### Specificities regarding date range for data extraction
Complying with the project requirements (see [README.md](https://github.com/vieirasousa/ifd-tech-ch/blob/main/README.md)), we worked with a subset of the original data, using only rows generated between January 2023 and May 2023, a date range defined by two parameters in the config file ("start_date" and "end_date"). That setup doesn't allow for scheduled subsequent runs as it is because it would always read the same date range present on the config file. Therefore, if we hipothetically intend to make this extraction dynamic and recurrent, we would have to create an additional resource to dynamically update "start_date" and "end_date" according to business needs.

#### High-level design of the flow
The scripts comprising the flow are orchestrated as tasks, on a Databricks Job, as shown on the high-level design below. Make sure to open the image in a new tab so texts are readable:

![High-level design for the NYC Taxi architecture](https://github.com/vieirasousa/ifd-tech-ch/blob/main/HLD_ifood_taxi_architecture.png "HLD")

-------------------------------------------------------------------------
## About the scripts

###### Below we describe the scripts functions inside the pipeline and other specificities.

- **nyc_taxi_ingestion/nyc_taxi_ingestion_yellow.py**: Creates the Core schema (if it doesn't exist), the *core_yellowcore.nyc_yellow_taxi_trips* table definition as a Delta table, makes API calls for the source of Yellow Taxi data, saves the result to a variable and change DataTypes in order to make the data more manageable; ultimately, saves the final result to the Core schema, which also creates delta parquet files on the data lake. It uses pre-querying with “Socrata Query Language” (SoQL) on the API calls to extract only the necessary portion of the original data, considering the project requirements (see [README.MD](https://github.com/vieirasousa/ifd-tech-ch/blob/main/README.md)).

- **nyc_taxi_ingestion/nyc_taxi_ingestion_green.py**: Does the same as above, but using as source an endpoint for Green Taxi data.

- **nyc_taxi_silver/nyc_taxi_silver.py**: Cleanses data and fixes types separatly for yellow and green taxis, leveraging Spark's parallel processing capabilities. Renames columns to allow for merging the two datasets seamlessly. Writes the result to the *silver.nyc_taxi_silver* Delta table, which reflets both on the Data Lake and the Data Warehouse.

- **nyc_taxi_gold/taxi_trips_by_vendor_date.py**: Cleanses data and fixes types separatly for yellow and green taxis, leveraging Spark's parallel processing capabilities. Renames columns to allow for merging the two datasets into a single one, enrich the result with aggregations and business logics and writes the result to the *gold.taxi_trips_by_vendor_date* dataset.

- **nyc_taxi_ingestion/config.json**: Holds parameters needed for the API calls, which extract data from the source. Fields are self-explanatory.

-------------------------------------------------------------------------
## About the datasets

###### The rationale behind the data model and each dataset

**Core datasets:** hold smaller subsets of the raw data, filtered by start and end date, with a fraction of the original columns, and no cleansing or tranformations applied to it. It's needed to enable quick audit and debugging efforts without fetching all the original data. Datatypes were pre-defined to accomodate unexpected values in the columns and to spare resources (the default stringType coming from the json is more resource-demanding than any other data types we've chosen), but further transformations are needed to make this data useful for Analytics teams.

**Silver dataset:** Built on top of the Core tables, this dataset receives treatment, for null values, for entries which contain errors, and ultimately applying the correct data type definition for the needed columns. It also joins Yellow and Green taxi data, which came from different API endpoints, and had different names for their pickup and dropoff columns. This dataset can be consumed directly by the Analytics team with Databricks SQL. The treatment stages present on this layer are described below:

* Rows with values lesser then zero on "total_amount" were removed. Those rows indicated some sort of "negative" payments, probably due to errors on the services side.
* Entries with pickup datetime greater then dropoff datetime were removed, as they indicated trips that were finished before they even started.
* Entries with pickup datetime equal to dropoff datetime were removed, as they indicated trips that were started and finished in the exact moment.

We had other suspicious entries, like rows with passenger_count superior to 8 individuals in a single trip, but since we can't indicate for sure if this is a real word possibility, we've kept the data expecting data analystis to verify this.

**Gold dataset:** Successor to the Silver dataset, this is a pre-processed table with relevant aggregated data, by vendor_id and trip_date, with a business rule over it (if a vendor/month presents a number of trips above the overall average, it will receive true for "above_average" column), ready to be consumed directly by reports, alerts and dashboards, and also be consumed with SQL queries.