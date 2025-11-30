CREATE SCHEMA IF NOT EXISTS staging;

-- Step 1: Unified Date Format (YYYY-MM-DD) & Proper Timestamps
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'raw'
  AND table_name = 'yellow_taxi_trips_2025'
ORDER BY ordinal_position;

DROP TABLE IF EXISTS dw.dim_datetime CASCADE;

CREATE TABLE dw.dim_datetime (
    datetime_sk        BIGSERIAL PRIMARY KEY,   -- surrogate key
    datetime_ts        timestamp NOT NULL,      -- full timestamp
    date               date NOT NULL,          -- YYYY-MM-DD
    year               integer NOT NULL,
    quarter            integer NOT NULL,
    month              integer NOT NULL,
    day                integer NOT NULL,
    day_of_week        integer NOT NULL,       -- 0=Sunday, 1=Monday...
    hour               integer NOT NULL
);

SELECT 
    MIN(tpep_pickup_datetime) AS min_pickup,
    MAX(tpep_pickup_datetime) AS max_pickup
FROM raw.yellow_taxi_trips_2025;

INSERT INTO dw.dim_datetime (
    datetime_ts,
    date,
    year,
    quarter,
    month,
    day,
    day_of_week,
    hour
)
SELECT
    ts AS datetime_ts,
    ts::date AS date,
    EXTRACT(YEAR FROM ts)::int AS year,
    EXTRACT(QUARTER FROM ts)::int AS quarter,
    EXTRACT(MONTH FROM ts)::int AS month,
    EXTRACT(DAY FROM ts)::int AS day,
    EXTRACT(DOW FROM ts)::int AS day_of_week,
    EXTRACT(HOUR FROM ts)::int AS hour
FROM generate_series(
    (SELECT date_trunc('hour', MIN(tpep_pickup_datetime)) FROM raw.yellow_taxi_trips_2025),
    (SELECT date_trunc('hour', MAX(tpep_pickup_datetime)) FROM raw.yellow_taxi_trips_2025),
    interval '1 hour'
) AS ts;


-- Confirm that pickup/dropoff timestamps are proper timestamp types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'raw'
  AND table_name = 'yellow_taxi_trips_2025'
  AND column_name IN ('tpep_pickup_datetime', 'tpep_dropoff_datetime');

-- Confirm values work as timestamps
SELECT 
    tpep_pickup_datetime,
    tpep_dropoff_datetime
FROM raw.yellow_taxi_trips_2025
ORDER BY tpep_pickup_datetime
LIMIT 5;

-- Verify dim_datetime table stores YYYY-MM-DD correctly
SELECT date, datetime_ts
FROM dw.dim_datetime
ORDER BY datetime_ts
LIMIT 10;

-- Confirm date column is typed correctly
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'dw'
  AND table_name = 'dim_datetime'
  AND column_name = 'date';

-- Ensure no improperly formatted dates remain
-- Look for values that failed conversion (Null where there shouldn't be)
SELECT COUNT(*)
FROM raw.yellow_taxi_trips_2025
WHERE tpep_pickup_datetime IS NULL
   OR tpep_dropoff_datetime IS NULL;
-- Verify that only valid PostgreSQL timestamps exist
 SELECT 
    MIN(tpep_pickup_datetime),
    MAX(tpep_pickup_datetime)
FROM raw.yellow_taxi_trips_2025;

-- Step 2: Split date into units (already done previously)

-- Step 3: Remove Nulls
CREATE OR REPLACE VIEW staging.clean_yellow_taxi_trips AS
SELECT *
FROM raw.yellow_taxi_trips_2025
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
  AND trip_distance IS NOT NULL
  AND trip_distance >= 0
  AND passenger_count IS NOT NULL
  AND passenger_count BETWEEN 1 AND 6;

-- Step 4: Remove Duplicate Rows
CREATE OR REPLACE VIEW staging.clean_yellow_taxi_trips_dedup AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY load_date DESC
        ) AS rn
    FROM staging.clean_yellow_taxi_trips
)
SELECT *
FROM ranked
WHERE rn = 1;

-- Step 5: Verify data against reference data
-- make sure location IDs exist in a TLC zone lookup
-- assume currency = USD

-- Load TLC Taxi zone lookup
CREATE TABLE raw.taxi_zone_lookup (
    LocationID integer PRIMARY KEY,
    Borough    text,
    Zone       text,
    service_zone text
);

-- check for invalid location IDS
-- Pickup
SELECT COUNT(*) AS invalid_pickup_ids
FROM staging.clean_yellow_taxi_trips_dedup t
LEFT JOIN raw.taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
WHERE z.LocationID IS NULL;

-- Dropoff
SELECT COUNT(*) AS invalid_dropoff_ids
FROM staging.clean_yellow_taxi_trips_dedup t
LEFT JOIN raw.taxi_zone_lookup z
  ON t.DOLocationID = z.LocationID
WHERE z.LocationID IS NULL;

-- Output is 0 invalid IDS for both pickup and dropoff

-- Step 6. Correct data types for numeric and categorical fields
-- inspect current types in raw
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'raw'
  AND table_name = 'yellow_taxi_trips_2025';

-- convert types for some columns
ALTER TABLE raw.yellow_taxi_trips_2025
ALTER COLUMN store_and_fwd_flag TYPE char(1);

-- Step 7. Add derived columns in the fact table
DROP TABLE IF EXISTS dw.fact_taxi_trips CASCADE;

CREATE TABLE dw.fact_taxi_trips (
    trip_sk                   BIGSERIAL PRIMARY KEY,
    trip_id                   BIGINT,        -- from raw data
    datetime_pickup_sk        BIGINT NOT NULL,
    datetime_dropoff_sk       BIGINT NOT NULL,
    pickup_location_sk        BIGINT NOT NULL,
    dropoff_location_sk       BIGINT NOT NULL,
    ratecode_sk               BIGINT NOT NULL,
    payment_type_sk           BIGINT NOT NULL,
    vendor_sk                 BIGINT NOT NULL,
    store_and_fwd_flag_sk     BIGINT NOT NULL,

    passenger_count           integer,
    trip_distance             numeric(10,2),
    fare_amount               numeric(10,2),
    extra                     numeric(10,2),
    mta_tax                   numeric(10,2),
    tip_amount                numeric(10,2),
    tolls_amount              numeric(10,2),
    improvement_surcharge     numeric(10,2),
    congestion_surcharge      numeric(10,2),
    total_amount              numeric(10,2),

    -- Derived columns:
    trip_duration_min         numeric(10,2),
    tip_percent               numeric(10,4),
    extra_charges             numeric(10,2),
    total_amount_no_mta_tax   numeric(10,2)
);

-- compute derived columns in your load script
INSERT INTO dw.fact_taxi_trips (
    trip_id,
    datetime_pickup_sk,
    datetime_dropoff_sk,
    pickup_location_sk,
    dropoff_location_sk,
    ratecode_sk,
    payment_type_sk,
    vendor_sk,
    store_and_fwd_flag_sk,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    total_amount,
    trip_duration_min,
    tip_percent,
    extra_charges,
    total_amount_no_mta_tax
)
SELECT
    t.trip_id,
    dp.datetime_sk AS datetime_pickup_sk,
    dd.datetime_sk AS datetime_dropoff_sk,
    pl.location_sk AS pickup_location_sk,
    dl.location_sk AS dropoff_location_sk,
    r.ratecode_sk,
    p.payment_type_sk,
    v.vendor_sk,
    s.store_and_fwd_flag_sk,        -- ✅ now this exists
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.congestion_surcharge,
    t.total_amount,

    EXTRACT(EPOCH FROM (t.tpep_dropoff_datetime - t.tpep_pickup_datetime))/60.0 AS trip_duration_min,
    CASE 
        WHEN t.fare_amount > 0 THEN t.tip_amount / t.fare_amount 
        ELSE NULL 
    END AS tip_percent,
    COALESCE(t.mta_tax,0) 
      + COALESCE(t.improvement_surcharge,0)
      + COALESCE(t.congestion_surcharge,0) AS extra_charges,
    (t.total_amount - COALESCE(t.mta_tax,0)) AS total_amount_no_mta_tax
FROM staging.clean_yellow_taxi_trips_valid_locations t
JOIN dw.dim_datetime dp
  ON dp.datetime_ts = date_trunc('hour', t.tpep_pickup_datetime)
JOIN dw.dim_datetime dd
  ON dd.datetime_ts = date_trunc('hour', t.tpep_dropoff_datetime)
JOIN dw.dim_location pl
  ON pl.location_id = t.pulocationid
JOIN dw.dim_location dl
  ON dl.location_id = t.dolocationid
JOIN dw.dim_ratecode r
  ON r.ratecode_id = t.ratecodeid
JOIN dw.dim_payment_type p
  ON p.payment_type_id = t.payment_type
JOIN dw.dim_vendor v
  ON v.vendor_id = t.vendorid
JOIN dw.dim_store_and_fwd_flag s
  ON s.flag_value = t.store_and_fwd_flag;

-- Step 8. Sum of two or more columns
-- Already done previously




