# iFood - Technical Challenge

### Architecture

###### This document details the architecture for this project and the decisions made to develop it.
-------------------------------------------------------------------------
## Overview

The architecture designed for this project borrows concepts from different models. The evolving stages over a raw set of data comes notoriously from a Medallion architeture, with its raw/bronze, silver and gold layers, but on the raw data we took some liberties, as for this case we didn't need all the columns or rows present on the original dataset. With that orientation, we dropped the raw layer in favor of a "core" layer, which derivatives from the raw data, but with some minor filtering and dropped columns, and made it generally available for direct SQL querying (to allow for quick validation efforts), in contrast to raw data, which is not commonly accessible from a SQL engine, due to its size, quality and other specificities.

This pipeline was developed to work on a Databricks environment, leveraging its Big Data capabilities, orchestration tools, and its SQL engine as well. The following scripts comprise the flow, orchestrated on a Databricks Job, as shown in the image below:


![High-level design for the NYC Taxi architecture](https://github.com/vieirasousa/ifd-tech-ch/blob/main/HLD_ifood_taxi_architecture.png "HLD")


- **nyc_taxi_ingestion/nyc_taxi_ingestion_yellow.py**: Makes API calls for the source of Yellow Taxi data, change DataTypes in order to make the data more manageable, and save the result to the Core schema. It uses pre-querying with “Socrata Query Language” (SoQL) on the API calls to extract only the necessary portion of the original data, considering the project requirements (see README.MD).

- 

The pipeline starts with API calls, bringing separately Yellow and Green taxi filtered data to the Core schema



1. Create a solution to ingest raw data from https://www.nyc.gov/site/tlc/about/tlc-
trip-record-data.page , with the following guidelines:
  1. PySpark must be used at some point.


Rationale

Core dataset: holds smaller subsets of the raw data, filtered by start and end date, with a fraction of the original columns, and no cleansing or tranformation applied to it. It's needed to enable audit and debugging efforts without fetching all the original data. Datatypes were pre-defined to accomodate unexpected values in the columns and to spare resources (the default stringType coming from the json is more resource-demanding than any other data types we've chosen), but further transformations are needed to make this data useful for Analytics teams.


Silver dataset: Built on top of the Core tables, this dataset receives treatment, for null values, for entries which contain errors, and ultimately applying the correct data type definition for the needed columns. It also joins Yellow and Green taxi data, which came from different API endpoints, and had different names for their pickup and dropoff columns. This dataset can be consumed directly by the Analytics team. The treatments present on this layer are described below:

* Rows with values lesser then zero on "total_amount" were removed.Those rows indicated some sort of "negative" payments,  probably due to errors in the services side.
* Entries with "tpep_pickup_datetime" greater then "tpep_dropoff_datetime" were removed, as they indicated trips that were finished before they even started.
* Entries with "tpep_pickup_datetime" equal to "tpep_dropoff_datetime" were removed, as they indicated trips that were started and finished in the exact moment.

We had other suspicious entries, like rows with passenger_count superior to 8 individuals in a single trip, but since we can't indicate for sure if this is a real word possibility, we've kept the data expecting data analystis to verify this.

Gold dataset: Successor to the Silver dataset, this is a pre-processed table with relevant aggregated data, by vendor_id and trip_date, with a business rule over it, ready to be consumed directly by dashboards.

