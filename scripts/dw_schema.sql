-- create data warehouse

CREATE SCHEMA IF NOT EXISTS dw;

-- Dimension: datetime

CREATE TABLE dw.dim_datetime (
    datetime_sk      BIGSERIAL PRIMARY KEY,
    datetime_value   TIMESTAMP NOT NULL UNIQUE,
    date_value       DATE,
    year             INTEGER,
    month            INTEGER,
    day              INTEGER,
    hour             INTEGER,
    dow              INTEGER,  -- day of week (0–6)
    is_weekend       BOOLEAN
);


-- Dimension: location

CREATE TABLE dw.dim_location (
    location_sk      BIGSERIAL PRIMARY KEY,
    location_id      INTEGER NOT NULL UNIQUE,
    borough          TEXT,
    zone             TEXT,
    service_zone     TEXT
);


-- Dimension: rate code

CREATE TABLE dw.dim_ratecode (
    ratecode_sk      BIGSERIAL PRIMARY KEY,
    ratecode_id      INTEGER NOT NULL UNIQUE,
    description      TEXT
);


-- Dimension: payment type

CREATE TABLE dw.dim_payment_type (
    payment_type_sk  BIGSERIAL PRIMARY KEY,
    payment_type_id  INTEGER NOT NULL UNIQUE,
    description      TEXT
);


-- Dimension: vendor

CREATE TABLE dw.dim_vendor (
    vendor_sk        BIGSERIAL PRIMARY KEY,
    vendor_id        INTEGER NOT NULL UNIQUE,
    vendor_name      TEXT
);


-- Dimension: store-and-forward flag

CREATE TABLE dw.dim_store_and_fwd_flag (
    flag_sk          BIGSERIAL PRIMARY KEY,
    flag_value       CHAR(1) NOT NULL UNIQUE, -- 'Y' or 'N'
    description      TEXT
);


-- Fact table: taxi trips

CREATE TABLE dw.fact_taxi_trips (
    trip_sk               BIGSERIAL PRIMARY KEY,

    -- foreign keys to dimensions
    vendor_sk             BIGINT REFERENCES dw.dim_vendor(vendor_sk),
    pickup_datetime_sk    BIGINT REFERENCES dw.dim_datetime(datetime_sk),
    dropoff_datetime_sk   BIGINT REFERENCES dw.dim_datetime(datetime_sk),
    pu_location_sk        BIGINT REFERENCES dw.dim_location(location_sk),
    do_location_sk        BIGINT REFERENCES dw.dim_location(location_sk),
    ratecode_sk           BIGINT REFERENCES dw.dim_ratecode(ratecode_sk),
    payment_type_sk       BIGINT REFERENCES dw.dim_payment_type(payment_type_sk),
    store_and_fwd_sk      BIGINT REFERENCES dw.dim_store_and_fwd_flag(flag_sk),

    -- measures
    trip_distance         DOUBLE PRECISION,
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

    -- lineage
    source_file           TEXT,
    load_date             DATE
);