"""
Data Toolkit program

A command line program to manage interactions of BigQuery and GPU training machines.

NOTE It should use the service account bq2file@pfsdb3.iam.gserviceaccount.com, but
right now it uses whatever that is in /shared/key
"""

import gzip
import json
import os
from pathlib import Path
from pprint import pprint
from urllib.parse import urlparse
from tqdm import tqdm
from google.cloud import bigquery, storage
from google.cloud.storage import Bucket
from jsonargparse import CLI

KEY_ORIG = f'../../ml/shared/key.json'
GCP_KEYFILE_PATH = KEY_ORIG

PROJECT_ID = "pfsdb3"


def make_export_url( table: str, bucket: str):
    return f"gs://{bucket}/{table}/*"


def setup_google_cloud():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEYFILE_PATH

def list_tables(dataset_id):
    client = bigquery.Client()
    dataset_id = f'{PROJECT_ID}.{dataset_id}'
    tables = client.list_tables(dataset_id)

    print("Tables contained in '{}':".format(dataset_id))
    for table in tables:
        print("\t{}".format( table.table_id))
    tables = client.list_tables(dataset_id)
    return  [table.table_id for table in tables]

def export_table(dataset: str, table: str, bucket: str):
    # Initialize BigQuery and Cloud Storage clients
    bq_client = bigquery.Client()

    # Set your project ID, dataset ID, table ID, and bucket name
    destination_uri = make_export_url( table,bucket)
    print(f'Bucket in progress: {destination_uri}')
    # add / to the end to force the export to create a directory

    # Set the job configuration
    job_config = bigquery.ExtractJobConfig(
        destination_format="PARQUET", compression="GZIP"
    )

    # Start the export job
    job = bq_client.extract_table(
        source=bq_client.dataset(dataset).table(table),
        destination_uris=[destination_uri],
        job_config=job_config,
    )

    # print(f"Started job with id {job.job_id}")

    # Wait for the job to complete
    job.result()

    print(f"Exported table {table} to {destination_uri}")

def create_bucket_class_location(bucket_name):
    """
    Create a new bucket in the US region with the coldline storage
    class
    """
    # bucket_name = "your-new-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "Standard"
    new_bucket = storage_client.create_bucket(bucket, location="us-west1")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

def export_dataset(dataset: str ='4_train_amd'):
    """Exports datset to GCS

    Args:
        dataset: Name of dataset.
    """
    tables = list_tables(dataset)
    bucket = f'export_pqt_{dataset}'
    print (f'Exporting to bucket : {bucket}')
    client = storage.Client()
    exists = Bucket(client, bucket).exists()
    if not exists:
        create_bucket_class_location(bucket)
    for table in tables:
        export_table(dataset,table,bucket)
    print(f'\n\nTo download bucket please run command below in terminal:\n\n\tgsutil -m cp -r gs://{bucket} .')

if __name__ == "__main__":
    setup_google_cloud()
    CLI(export_dataset)
