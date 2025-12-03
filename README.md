# NYC Yellow Taxi Data Project
CIS 9440 - NYC Yellow Taxi Data Sourcing, Storage, and Modeling

### Project Overview
This repository holds my work for Assignments 1 and 2 of CIS 9440, where I focused on sourcing, storing, transforming, and modeling NYC Yellow Taxi Trip Data (Jan - Mar 2025), and built a cloud-based data warehouse. 

The goal of the project is to learn how to handle real-world data from start to finish, starting with sourcing the raw TLC trip files, storing them in PostgreSQL, transforming and cleaning them, and then modeling the data using a star schema. After building the warehouse locally, the project expands into the cloud by loading the transformed tables into BigQuery and creating a cloud-based version of the data warehouse. Throughout the process, I worked on building Python scripts for sourcing and ETL, creating SQL scripts for the raw, staging, and warehouse layers, designing dimension and fact tables, validating data quality, and documenting the entire pipeline through a data dictionary and data-mapping sheet. Altogether, this project demonstrates how raw trip data becomes structured, validated, and ready for analytics across both local and cloud environments.

### Data Source
The data is sourced from the official NYC Taxi & Limousine Commission (TLC):
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

# Assignment 1

## Data Sourcing
I handled the data sourcing part of the project in Python. My script pulls the January–March 2025 Parquet files straight from the TLC website and then loads them into my PostgreSQL database through pgAdmin 4. This let me bring in the raw data quickly and keep everything in one place for the modeling steps.

## Storage
I created a raw schema in PostgreSQL to store the downloaded TLC files without modifying the values. The main table, raw.yellow_taxi_trips_2025, includes all the fields from the TLC data dictionary along with a surrogate key and two metadata columns (source_file and load_date). I intentionally avoided strict constraints here so the raw table could hold whatever values appear in the files, even if they’re unusual or slightly messy.

## Modeling
After the raw table was set up, I designed a small data warehouse under a new schema called dw. I followed a star-schema structure, since it works well for trip-based data and keeps analysis simple. The fact table, fact_taxi_trips, contains the main numeric measures for each trip.

A few of the dimension tables describe different aspects of a trip—such as time, location, vendor, payment type, rate code, and the store-and-forward flag. Each dimension uses a surrogate key which links back to the fact table. This makes it easier to analyze trips by the hour, day, month, payment method, pickup or dropoff zone, and more.

## SQL Scripts
The repository includes separate SQL files for each step:

* create_raw_schema.sql – Sets up the raw schema and ingestion table

* dw_schema.sql – Creates all dimension tables and the fact table

* load_dimensions.sql – Loads lookup values and derived datetime components

* load_fact_taxi_trips.sql – Builds the fact table by joining to dimension surrogate keys

# Assignment 2

This assignment expanded the pipeline to include full transformation, data validation, and loading into a Cloud Data Warehouse (Google BigQuery).

## Transformation
All transformations were conducted in PostgreSQL through SQL. The following data cleaning steps were taken:
* standardized timestamp formats
* ensured numeric columns had correct data types
* normalized categorical fields
* handled missing or invalid values

Next, I performed the following data validation steps:
* Removed any trips with negative distance or fares
* validated location IDS against the TLC lookup table downloaded from the TLC website
* filtered passenger counts that fell under 1-6
* ensured valid datetimes
* performed deduplication to keep the most recent records
* added derived columns (including trip_duration_min, tip_percent, extra_charges, total_amount_no_mta_tax)

## Cloud Data Warehouse (Google Cloud BigQuery)
For the cloud Datawarehouse requirement, the transformed dimensional tables and fact table were exported from PostgreSQL and loaded into Google Cloud's BigQuery.

The BigQuery Dataset created:
Dataset name: dw
Project: cis9440-taxi-dw

Tables that were loaded into BigQuery include:
* dw.dim_datetime
* dw.dim_location
* dw.dim_ratecode
* dw.dim_payment_type
* dw.dim_vendor
* dw.dim_store_and_fwd_flag
* dw.fact_taxi_trips
* dw.sampled_taxi_data

A python ETL script was created for moving data from the local PostgreSQL warehouse to the BigQuery cloud warehouse. This script loads each dw table from PostgreSQL into BigQuery using BigQuery Python Client. 

## Scripts used for these steps:
* Transformation Script: Transformation.sql
*	BigQuery DDL: create_dw_tables_bigquery.sql
*	ETL Script: load_postgres_to_bigquery.py
*	API Python Script: API.py

## Dashoards for Datasets

For the visualization component, I connected Tableau directly to my BigQuery data warehouse. Since my warehouse contains all the cleaned and transformed NYC Yellow Taxi data for January through March 2025, this allowed me to build visualizations using the live data stored in BigQuery instead of relying on local files or extracts. I used Tableau’s built-in BigQuery connector, authenticated with my Google Cloud project, and created a custom SQL query to pull the fields needed for analysis.

Once connected, I built several interactive visuals that reflect the structure of my warehouse, including a line chart of daily trip volume, a column chart comparing boroughs, a pie chart showing payment type distribution, and a heat map summarizing trip density by hour and day of week. I also added a date filter so users can adjust the time window and automatically update the charts. This setup demonstrates that my data flows all the way from the warehouse into Tableau and supports interactive, real-time analysis.

* Tableau Dashboard Link: [Tableau Dashboard - Taxi Dataset](https://public.tableau.com/views/TableauTaxiDataset/Dashboard1?:language=en-GB&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)
* Quick Sight Dashboard Link: https://us-east-2.quicksight.aws.amazon.com/sn/accounts/252056599601/dashboards/5c98a137-0320-4f2f-9622-e3e1990187fa?directory_alias=cis9440-ditzhaki 


# Tools Used

* Python (specifically by using requests, pandas, pyarrow, psycopg2, sqlalchemy, google-cloud-bigquery, dotenv libraries)

* SQL (via pgAdmin 4 and BigQuery SQL)

* Google BigQuery (Cloud Data Warehouse)

* GitHub
