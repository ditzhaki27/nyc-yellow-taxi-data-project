-- Create Tables

-- dim_location
CREATE TABLE `cis9440-taxi-dw.dw.dim_location`
(
  location_sk INT64,
  location_id INT64,
  borough STRING,
  zone STRING,
  service_zone STRING
);

-- dim_datetime
CREATE TABLE `cis9440-taxi-dw.dw.dim_datetime`
(
  datetime_sk INT64,
  datetime_ts TIMESTAMP,
  date DATE,
  year INT64,
  quarter INT64,
  month INT64,
  day INT64,
  day_of_week INT64,
  hour INT64
);

-- dim_payment_type
CREATE TABLE `cis9440-taxi-dw.dw.dim_payment_type`
(
  payment_type_sk INT64,
  payment_type_id INT64,
  description STRING
);

-- dim_ratecode
CREATE TABLE `cis9440-taxi-dw.dw.dim_ratecode`
(
  ratecode_sk INT64,
  ratecode_id INT64,
  description STRING
);

-- dim_store_and_fwd_flag
CREATE TABLE `cis9440-taxi-dw.dw.dim_store_and_fwd_flag`
(
  store_and_fwd_flag_sk INT64,
  flag_value BOOL,
  flag_description STRING
);

-- dim_vendor
CREATE TABLE `cis9440-taxi-dw.dw.dim_vendor`
(
  vendor_sk INT64,
  vendor_id INT64,
  vendor_name STRING
);

-- fact_taxi_trips
CREATE EXTERNAL TABLE `cis9440-taxi-dw.dw.fact_taxi_trips`
OPTIONS(
  format="CSV",
  uris=["https://drive.google.com/open?id=1wcWHUnpwqoSetwdtTV81jg1SLps75UKQ"]
);