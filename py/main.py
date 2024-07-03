
from google.cloud import bigquery
from google.cloud import storage
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import nltk
from io import BytesIO
import zipfile
import os
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/Chris/Dropbox/keys/supremecourt-428223-68829818aa13.json"

storage_client = storage.Client()

def extract_csv_urls(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    links = soup.find_all('a')
    rda_url = []
    for link in links:
        href = link.get('href')
        if href and ".csv" in href:
            rda_url.append(href)
    return rda_url

def download_and_unzip_to_gcs(url, bucket_name):
    """Downloads a ZIP file, unzips it, and uploads new CSV files to a GCS bucket.

    Args:
        url: The URL of the ZIP file to download.
        bucket_name: The name of the Google Cloud Storage bucket.
    """

    response = requests.get(url)

    if response.status_code == 200:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            for file_info in zip_file.infolist():
                if file_info.filename.endswith(".csv"):
                    blob_name = file_info.filename

                    # Check if the file already exists in the bucket
                    if not bucket.blob(blob_name).exists():
                        csv_data = zip_file.read(file_info)
                        blob = bucket.blob(blob_name)
                        blob.upload_from_string(csv_data)
                        print(f"CSV file {blob_name} uploaded to gs://{bucket_name}/{blob_name}")
                    else:
                        print(f"The file {blob_name} already exists in the bucket. Skipping this upload.")

    else:
        print(f"Failed to download file from {url}. Status code: {response.status_code}")




def create_and_load_table(project_id, dataset_id, table_id, bucket_name, csv_file_name):
    """Creates a BigQuery table and loads data from a CSV file in GCS.

    Args:
        project_id (str): Your Google Cloud project ID.
        dataset_id (str): The ID of the dataset to create the table in.
        table_id (str): The ID of the table to create.
        bucket_name (str): The name of the GCS bucket containing the CSV file.
        csv_file_name (str): The name of the CSV file in the bucket.
    """

    # Construct BigQuery client and table reference
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True
    )
    uri = f"gs://{bucket_name}/{csv_file_name}"

    # Create table (if it doesn't exist)
    try:
        client.get_table(table_ref)  # Check if table exists
    except:
        table = bigquery.Table(table_ref)
        client.create_table(table)
        print(f"Created table {table_id}")

    # Load data from GCS
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for job to complete
    print(f"Loaded {load_job.output_rows} rows to {table_id}")

def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Fetch all blobs at once and convert to a list
    blobs = list(bucket.list_blobs())

    # Return the names of the blobs
    blob_names = [blob.name for blob in blobs]
    return blob_names

blob_names = list_blobs(bucket_name)



def main_function(event, context):
    base_url = "http://scdb.wustl.edu/"
    url = "http://scdb.wustl.edu/data.php"
    rda_url = extract_csv_urls(URL)

    # Construct absolute URLs
    complete_urls = [base_url + url for url in rda_url]

    project_id = 'supremecourt-428223'
    dataset_id = 'untouched_data'
    bucket_name = "untouched_court_data"

    for url in complete_urls:
        uploaded_blobs = download_and_unzip_to_gcs(url, bucket_name)

        for blob_name in uploaded_blobs:
            create_and_load_table(project_id, dataset_id, bucket_name, blob_name)

