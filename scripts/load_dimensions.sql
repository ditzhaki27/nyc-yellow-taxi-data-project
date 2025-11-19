-- Load dimensions

-- dim_datetime: from pickup + dropoff timestamps

INSERT INTO dw.dim_datetime (
    datetime_value, date_value, year, month, day, hour, dow, is_weekend
)
SELECT DISTINCT
    dt AS datetime_value,
    DATE(dt)                        AS date_value,
    EXTRACT(YEAR  FROM dt)::INT     AS year,
    EXTRACT(MONTH FROM dt)::INT     AS month,
    EXTRACT(DAY   FROM dt)::INT     AS day,
    EXTRACT(HOUR  FROM dt)::INT     AS hour,
    EXTRACT(DOW   FROM dt)::INT     AS dow,
    (EXTRACT(DOW FROM dt) IN (0,6)) AS is_weekend
FROM (
    SELECT tpep_pickup_datetime  AS dt FROM raw.yellow_taxi_trips_2025
    UNION
    SELECT tpep_dropoff_datetime AS dt FROM raw.yellow_taxi_trips_2025
) AS x
WHERE dt IS NOT NULL
ON CONFLICT (datetime_value) DO NOTHING;


-- dim_ratecode: static mapping

INSERT INTO dw.dim_ratecode (ratecode_id, description)
VALUES
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
    (99, 'Unknown')
ON CONFLICT (ratecode_id) DO NOTHING;


-- dim_payment_type: static mapping

INSERT INTO dw.dim_payment_type (payment_type_id, description)
VALUES
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip')
ON CONFLICT (payment_type_id) DO NOTHING;


-- dim_vendor: basic mapping using VendorID codes

INSERT INTO dw.dim_vendor (vendor_id, vendor_name)
VALUES
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'VeriFone Inc'),
    (6, 'CMT Taxi'),
    (7, 'VeriFone Taxi')
ON CONFLICT (vendor_id) DO NOTHING;


-- dim_store_and_fwd_flag: 'Y' or 'N'

INSERT INTO dw.dim_store_and_fwd_flag (flag_value, description)
VALUES
    ('Y', 'Store and forward trip'),
    ('N', 'Not a store and forward trip')
ON CONFLICT (flag_value) DO NOTHING;
