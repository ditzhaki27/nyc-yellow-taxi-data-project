import os
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery

# --- Postgres credentials from environment variables ---
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB = os.getenv("PG_DB", "nyc_taxi")  # default if not set
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")

if not PG_USER or not PG_PASSWORD:
    raise RuntimeError("PG_USER and PG_PASSWORD environment variables must be set.")

pg_url = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(pg_url)

# Connect to BigQuery
client = bigquery.Client()
project_id = "cis9440-taxi-dw"
dataset_id = "dw"

tables = [
    "dim_datetime",
    "dim_location",
    "dim_ratecode",
    "dim_payment_type",
    "dim_vendor",
    "dim_store_and_fwd_flag",
    "fact_taxi_trips"
]

for table_name in tables:
    print(f"Loading {table_name}...")

    # 3. Read from Postgres
    df = pd.read_sql(f"SELECT * FROM dw.{table_name}", con=pg_engine)

    # 4. Define BigQuery destination
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    # 5. Load DataFrame into BigQuery
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for the job to complete

    print(f"Loaded {len(df)} rows into {table_id}")
