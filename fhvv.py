from google.cloud import bigquery
from google.cloud import storage
import os
import pandas as pd
import numpy as np
import tempfile

"""
This Script moves NYC Taxi Trips data from GCS Bucket to BigQuery Table
"""

BUCKET = os.getenv("GCP_GCS_BUCKET", "main001")
project_id = "celestial-gist-375110"
dataset_name = "MainTaxiData"
services = "fhv"

# Define table schema
schema = [
    #bigquery.SchemaField("vendor_id", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("pickup_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("dropoff_time", "TIMESTAMP", mode="NULLABLE"),
    #bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pickup_location_id", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("dropoff_location_id", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("season", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pickup_period", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("dropoff_period", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("day_of_week", "STRING", mode="NULLABLE"),
]


def create_bq_table():
    table_id = f"{project_id}.{dataset_name}.{services}_tripdata"
    client = bigquery.Client()
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


def gcs_to_bigquery():
    bucket_name = BUCKET
    foldername = "fhv/2019"

    # Retrieve all blobs with a prefix matching the file.
    storage_client = storage.Client(project=project_id)  # Specify project ID
    bucket = storage_client.get_bucket(bucket_name)

    # List blobs iterate in folder
    blobs = bucket.list_blobs(prefix=foldername)

    print(f"Downloading files from {bucket_name}")

    for blob in blobs:
        print(blob.name)

        # Create a temporary file
        temp_dir = "C:\Temp"
        os.makedirs(temp_dir, exist_ok=True)
        _, temp_filename = tempfile.mkstemp(suffix=".parquet", dir=temp_dir)
        print("Temporary file:", temp_filename)

        blob.download_to_filename(temp_filename)

        df = pd.read_parquet(temp_filename)
        print(df.head())

        # Data cleaning and transformations
        df = df.rename(columns={
           # 'VendorID': 'vendor_id',
            'pickup_datetime': 'pickup_time',
            'dropOff_datetime': 'dropoff_time',
            'PUlocationID': 'pickup_location_id',
            'DOlocationID': 'dropoff_location_id'
        })

        df = df.drop_duplicates()
        df.dropna()

        # Convert to pandas datetime for further exploration and conversion
        df['pickup_time'] = pd.to_datetime(df['pickup_time'])
        df['dropoff_time'] = pd.to_datetime(df['dropoff_time'])

        # Categorizing the trips into season (Summer & Winter)
        conditions = [
            (df['pickup_time'].between('2019-01-01', '2019-02-28')) & (df['dropoff_time'].between('2019-01-01', '2019-02-28')),
            (df['pickup_time'].between('2019-03-01', '2019-05-31')) & (df['dropoff_time'].between('2019-03-01', '2019-05-31'))
        ]

        choices = ['Winter', 'Summer']

        df['season'] = np.select(conditions, choices, default='Other')

        # Period of the day (Late Night, Morning, Afternoon, Evening)
        df['pickup_period'] = pd.cut(
            df['pickup_time'].dt.hour,
            bins=[0, 4, 11, 16, 23],
            labels=['Late Night', 'Morning', 'Afternoon', 'Evening'],
            right=False
        )

        df['dropoff_period'] = pd.cut(
            df['dropoff_time'].dt.hour,
            bins=[0, 4, 11, 16, 23],
            labels=['Late Night', 'Morning', 'Afternoon', 'Evening'],
            right=False
        )

        # Day of the week
        df['day_of_week'] = df['pickup_time'].dt.day_name()

        # Upload data to BigQuery
        table_id = f"{project_id}.{dataset_name}.{services}_tripdata"
        client = bigquery.Client(project=project_id)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND"
        )
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Waits for the job to complete.

        print(f"Data uploaded to BigQuery table: {table_id}")


if __name__ == "__main__":
    create_bq_table()
    gcs_to_bigquery()
