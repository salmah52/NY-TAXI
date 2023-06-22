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
services ="green"

# Define table schema
schema = [
    bigquery.SchemaField("vendor_id", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("pickup_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("dropoff_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("passengers", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("distance", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("rate_code_id", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pickup_location_id", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("dropoff_location_id", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("payment_type", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("fare", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("extra", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("mta_tax", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("tip", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("tolls_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("improvement_surcharge", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_amount", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("congestion_surcharge", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("season", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pickup_period", "STRING", mode="NULLABLE"),  
    bigquery.SchemaField("dropoff_period", "STRING", mode="NULLABLE"),  # Added dropoff_period field
    bigquery.SchemaField("day_of_week", "STRING", mode="NULLABLE"),  # Added day_of_week field
]


def create_bq_table():
    table_id = f"{project_id}.{dataset_name}.{services}_tripdata"
    client = bigquery.Client()
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


def gcs_to_bigquery():
    bucket_name = BUCKET
    foldername = "green/2019"

    # Retrieve all blobs with a prefix matching the file.
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # List blobs iterate in folder
    blobs = bucket.list_blobs(prefix=foldername)

    print(f"Downloading files from {bucket_name}")

    for blob in blobs:
        print(blob.name)

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmpfile:
            destination_uri = tmpfile.name
            print(f"Temporary file: {destination_uri}")
            blob.download_to_filename(destination_uri)

            df = pd.read_parquet(destination_uri)
            print(df.head())
            print(df.columns.values)

            # Data cleaning
            df = df.rename(
                columns={
                    'VendorID': 'vendor_id',
                    'lpep_pickup_datetime': 'pickup_time',
                    'lpep_dropoff_datetime': 'dropoff_time',
                    'passenger_count': 'passengers',
                    'RatecodeID': 'rate_code_id',
                    'PULocationID': 'pickup_location_id',
                    'DOLocationID': 'dropoff_location_id',
                    'trip_distance': 'distance',
                    'fare_amount': 'fare',
                    'tip_amount': 'tip'
                }
            )

            # Remove duplicates
            df = df.drop_duplicates()

            # Replacing missing rows in the rate_code_id column with the most common rate code ID
            df['rate_code_id'] = df['rate_code_id'].fillna(df['rate_code_id'].mode()[0])

            # Dropping all rows with zero in the distance, fare, total_amount and improvement charge column
            df = df[df['distance'] > 0]
            df = df[df['fare'] > 0]

            df.dropna()
            df = df.drop(['ehail_fee'], axis=1)
            df = df.drop(['trip_type'], axis=1)

            # Convert to pandas datetime for further exploration and conversion
            df['pickup_time'] = pd.to_datetime(df['pickup_time'])
            df['dropoff_time'] = pd.to_datetime(df['dropoff_time'])

            # Categorizing the trips into season (Summer & Winter)
            conditions = [
                (df['pickup_time'].between('2019-01-01', '2019-02-28')) & (df['dropoff_time'].between('2019-01-01', '2019-02-28')),
                (df['pickup_time'].between('2019-03-01', '2019-05-31')) & (df['dropoff_time'].between('2019-03-01', '2019-05-31')),
                (df['pickup_time'].between('2019-06-01', '2019-08-31')) & (df['dropoff_time'].between('2019-06-01', '2019-08-31')),
                (df['pickup_time'].between('2019-09-01', '2019-11-30')) & (df['dropoff_time'].between('2019-09-01', '2019-11-30')),
                (df['pickup_time'].between('2019-12-01', '2019-12-31')) & (df['dropoff_time'].between('2019-12-01', '2019-12-31'))
            ]
            values = ['winter', 'spring', 'summer', 'autumn', 'winter']
            df['season'] = np.select(conditions, values)

            # Categorize the trips into pickup period of the day
            pickup_conditions = [
                (df['pickup_time'].dt.hour >= 0) & (df['pickup_time'].dt.hour < 6),
                (df['pickup_time'].dt.hour >= 6) & (df['pickup_time'].dt.hour < 12),
                (df['pickup_time'].dt.hour >= 12) & (df['pickup_time'].dt.hour < 18),
                (df['pickup_time'].dt.hour >= 18) & (df['pickup_time'].dt.hour <= 23)
            ]

            pickup_values = ['midnight', 'morning', 'afternoon', 'evening']
            df['pickup_period'] = np.select(pickup_conditions, pickup_values)

            # Categorize the trips into dropoff period of the day
            dropoff_conditions = [
                (df['dropoff_time'].dt.hour >= 0) & (df['dropoff_time'].dt.hour < 6),
                (df['dropoff_time'].dt.hour >= 6) & (df['dropoff_time'].dt.hour < 12),
                (df['dropoff_time'].dt.hour >= 12) & (df['dropoff_time'].dt.hour < 18),
                (df['dropoff_time'].dt.hour >= 18) & (df['dropoff_time'].dt.hour <= 23)
            ]
            dropoff_values = ['midnight', 'morning', 'afternoon', 'evening']
            df['dropoff_period'] = np.select(dropoff_conditions, dropoff_values)

            # Categorize the trips into day of the week (Monday to Sunday)
            df['day_of_week'] = df['pickup_time'].dt.day_name()

            # Construct a BigQuery client object
            bqclient = bigquery.Client()

            # Full path to the existing table: project.dataset.table_name
            table_id = f"{project_id}.{dataset_name}.{services}_tripdata"

            # Set up a job configuration
            job_config = bigquery.LoadJobConfig(autodetect=False)

            # Submit the job
            job = bqclient.load_table_from_dataframe(df, table_id, job_config=job_config)

            # Wait for the job to complete and then show the job results
            job.result()

            # Read back the properties of the table
            table = bqclient.get_table(table_id)
            print("Table:", table.table_id, "has", table.num_rows, "rows and", len(table.schema), "columns")
            print("JOB SUCCESSFUL")


if __name__ == "__main__":
    create_bq_table()
    gcs_to_bigquery
