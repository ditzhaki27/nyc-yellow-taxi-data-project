# api/export_csv_api.py

from fastapi import FastAPI, Response
from google.cloud import bigquery
import csv
from io import StringIO

app = FastAPI()

project_id = "cis9440-taxi-dw"
table_id = "cis9440-taxi-dw.dw.sampled_taxi_data"

client = bigquery.Client(project=project_id)

@app.get("/export-trips")
def export_trips():
    """
    Returns a CSV export of daily trip counts between Jan 1 and Mar 31, 2025.
    """

    query = f"""
    SELECT trip_date, COUNT(*) AS trip_count
    FROM `{table_id}`
    GROUP BY trip_date
    ORDER BY trip_date;
    """

    query_job = client.query(query)
    rows = query_job.result()

    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["trip_date", "trip_count"])
    for row in rows:
        writer.writerow([row["trip_date"], row["trip_count"]])

    return Response(
        content=buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=trip_summary.csv"},
    )
