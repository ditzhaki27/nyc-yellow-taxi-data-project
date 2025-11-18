# Import necessary Libraries\n",
import os
import requests
import pandas as pd
import psycopg2
from io import StringIO

# Data Sourcing

# Base URL for NYC TLC Yellow Taxi trip data\n",
base_url = \"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{}.parquet\
    
# Year and months to download (focusing on the first 3 months of the year)\n",
year = 2025
months = ["01", "02", "03"]  # Jan, Feb, Mar

# Folder structure
folder_path = "data"
os.makedirs(folder_path, exist_ok = True)


def download_file(url, save_path):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(save_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            print(f"SUCCESS: Downloaded {save_path}")
        else:
            print(f"ERROR: Failed to download {url} (Status code: {response.status_code})")
    except Exception as e:
        print(f"EXCEPTION: Error downloading {url} -> {e}")

print(df.columns.tolist())


# Storage

# PostgreSQL connection details
def get_postgres_connection():
    return psycopg2.connect(
        host = "localhost",
        dbname = "nyc_taxi",
        user = "postgres",
        password = "diana6508",
        port = 5432
    )

# Column ordering
columns_order = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee"]

# Download + Load pipeline
for month in months:
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    url = base_url.format(year, month)
    save_path = os.path.join(folder_path, file_name)
    download_file(url, save_path)

    # read parquet
    print(f"Reading Parquet: {file_name}")
    df = pd.read_parquet(save_path)

    # clean & reorder columns
    print("Columns in Parquet:", df.columns.tolist())

    # Ensure optional columns exist; default to 0 if missing
    optional_zero_cols = ["airport_fee", "cbd_congestion_fee"]
    for col in optional_zero_cols:
        if col not in df.columns:
            df[col] = 0.0

    required_cols = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee"
    ]

    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"Required column {col} is missing from {file_name}")

    # Reorder columns to match the table load order exactly
    df = df[required_cols]

    # Cast integer-like columns so they don't look like 1.0 in CSV
    int_cols = [
        "VendorID",
        "passenger_count",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "payment_type"
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    # load into postgresql
    print(f"Loading into PostgreSQL: {file_name}")

    conn = get_postgres_connection()
    cur = conn.cursor()

    # Use raw schema by default
    cur.execute("SET search_path TO raw, public;")

    # Write df to CSV buffer
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    # Explicit COPY column list
    copy_sql = """
        COPY yellow_taxi_trips_2025 (
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee,
            cbd_congestion_fee
        )
        FROM STDIN WITH (FORMAT csv)
    """

    cur.copy_expert(copy_sql, buffer)

    conn.commit()
    cur.close()
    conn.close()

    print(f"SUCCESS: Loaded {file_name}")
