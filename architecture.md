# iFood - Technical Challenge

### Overview

###### This project is part of a hiring process for iFood, ocurring between November and December 2024.
-------------------------------------------------------------------------
## Case requirements

This technical case has the following requirements:

1. Create a solution to ingest raw data from https://www.nyc.gov/site/tlc/about/tlc-
trip-record-data.page , with the following guidelines:
  1. PySpark must be used at some point.


Rationale

Core dataset: holds a  smaller subset of the raw data, filtered by start and end date, with a subset of the original columns, and no cleansing or tranformation applied to it. It's needed to enable audit and debugging efforts without fetching all the original data. Datatypes were pre-defined to accomodate unexpected values in the columns and to spare resources (the default stringType coming from the json is more resource-demanding than any other data types we've chosen), but further transformations are needed to make this data useful for Analytics teams.


Silver dataset: Built on top of the Core table, this dataset receives treatment for null values, filters out entries which contain errors and apply the correct data type definition for the needed columns. This dataset can be consumed directly by the Analytics team. The treatments present on this layer are described below:

* Rows with values lesser then zero on "total_amount" were removed.Those rows indicated some sort of "negative" payments,  probably due to errors in the services side.
* Entries with "tpep_pickup_datetime" greater then "tpep_dropoff_datetime" were removed, as they indicated trips that were finished before they even started.
* Entries with "tpep_pickup_datetime" equal to "tpep_dropoff_datetime" were removed, as they indicated trips that were started and finished in the exact moment.

We had other suspicious entries, like rows with passenger_count superior to 8 individuals in a single trip, but since we can't indicate for sure if this is a real word possibility, we've kept the data expecting data analystis to verify this.

Gold dataset: Successor to the Silver dataset, this is a pre-processed table with relevant aggregated data, by vendor_id and trip_date, with a business rule over it, ready to be consumed directly by dashboards.

