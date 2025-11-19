-- proper fact table with surrogate keys pointing to all dimensions

INSERT INTO dw.fact_taxi_trips (
    vendor_sk,
    pickup_datetime_sk,
    dropoff_datetime_sk,
    pu_location_sk,
    do_location_sk,
    ratecode_sk,
    payment_type_sk,
    store_and_fwd_sk,

    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,

    source_file,
    load_date
)
SELECT
    v.vendor_sk,
    d_pick.datetime_sk,
    d_drop.datetime_sk,
    pu.location_sk,
    do_.location_sk,
    r.ratecode_sk,
    p.payment_type_sk,
    f.flag_sk,

    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,

    t.source_file,
    t.load_date
FROM raw.yellow_taxi_trips_2025 t
LEFT JOIN dw.dim_vendor              v     ON v.vendor_id       = t.VendorID
LEFT JOIN dw.dim_datetime            d_pick ON d_pick.datetime_value  = t.tpep_pickup_datetime
LEFT JOIN dw.dim_datetime            d_drop ON d_drop.datetime_value  = t.tpep_dropoff_datetime
LEFT JOIN dw.dim_location            pu    ON pu.location_id    = t.PULocationID
LEFT JOIN dw.dim_location            do_   ON do_.location_id   = t.DOLocationID
LEFT JOIN dw.dim_ratecode            r     ON r.ratecode_id     = t.RatecodeID
LEFT JOIN dw.dim_payment_type        p     ON p.payment_type_id = t.payment_type
LEFT JOIN dw.dim_store_and_fwd_flag  f     ON f.flag_value      = t.store_and_fwd_flag;
