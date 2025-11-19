# NYC Yellow Taxi Data Project
CIS 9440 - NYC Yellow Taxi Data Sourcing, Storage, and Modeling


### Project Overview
This repository holds my work for the assignments where we focused on sourcing, storing, and modeling NYC Yellow Taxi trip data. The dataset includes a lot of useful details about taxi rides across the city such as pickup and dropoff times, locations, fares, and other trip-level information that helps with analysis.

### Data Source
The data is sourced from the official NYC Taxi & Limousine Commission (TLC):
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

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

## Tools Used

* Python (specifically by using requests, pandas, pyarrow, psycopg2 libraries)

* PostgreSQL (via pgAdmin 4)

* GitHub

* SQL for all modeling and transformation steps
