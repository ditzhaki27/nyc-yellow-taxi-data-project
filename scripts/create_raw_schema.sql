CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.yellow_taxi_trips_2025 (
    trip_id               BIGSERIAL PRIMARY KEY,

    VendorID              INTEGER,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count       INTEGER,
    trip_distance         DOUBLE PRECISION,  -- no precision limit now
    RatecodeID            INTEGER,
    store_and_fwd_flag    CHAR(1),
    PULocationID          INTEGER,
    DOLocationID          INTEGER,
    payment_type          INTEGER,
    fare_amount           NUMERIC(10,2),
    extra                 NUMERIC(10,2),
    mta_tax               NUMERIC(10,2),
    tip_amount            NUMERIC(10,2),
    tolls_amount          NUMERIC(10,2),
    improvement_surcharge NUMERIC(10,2),
    total_amount          NUMERIC(10,2),
    congestion_surcharge  NUMERIC(10,2),
    airport_fee           NUMERIC(10,2),
    cbd_congestion_fee    NUMERIC(10,2),

    source_file           TEXT,
    load_date             DATE DEFAULT CURRENT_DATE
);

-- 1) Total rows
SELECT COUNT(*) 
FROM raw.yellow_taxi_trips_2025;

-- 2) Trips per month
SELECT
    DATE_TRUNC('month', tpep_pickup_datetime) AS month,
    COUNT(*) AS trips
FROM raw.yellow_taxi_trips_2025
GROUP BY 1
ORDER BY 1;

UPDATE raw.yellow_taxi_trips_2025
SET source_file = CONCAT(
        'yellow_tripdata_2025-',
        TO_CHAR(tpep_pickup_datetime, 'MM'),
        '.parquet'
    )
WHERE source_file IS NULL;

SELECT source_file, COUNT(*)
FROM raw.yellow_taxi_trips_2025
GROUP BY source_file
ORDER BY source_file;