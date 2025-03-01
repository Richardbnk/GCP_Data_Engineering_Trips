"""
This Python script is an ETL pipeline designed to ingest large-scale trip data from 
a CSV file into Google BigQuery, ensuring scalability and efficiency through batch 
processing and partitioning.
"""

import os
import re
import time
import warnings
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# variables
PROJECT_ID = "data-project-452300"
DATASET_ID = "challenge"
TABLE_NAME = "raw_trips"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
SERVICE_ACCOUNT_FILE = r"data-project-452300-e2c341ffd483.json"
FILE_PATH = "trips.csv"
CHUNK_SIZE = 100000  # load data in chuncks

# set GCP credentials
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


# supress warnings
warnings.simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)


def create_bq_table(ddl_file):
    """Run DDL for table creation."""

    with open(ddl_file, "r") as ddl_file:
        ddl_query = ddl_file.read()
    query_job = bq_client.query(ddl_query)
    query_job.result()


def load_table_to_bigquery(df, table_id):
    """Append data from DataFrame to BigQuery in chunks for scalability."""

    start_time = time.time()

    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # append data
            autodetect=True,  # automatically detect schema
        )

        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        duration = time.time() - start_time
        return duration

    except Exception as e:
        print(f"🔹Failed to upload table chunk: {e}")
        return 0  # return a default value instead of None


def data_ingestion(file_path):
    """Loads chunks and processes information efficiently."""

    print("\n\n🔹 Starting data ingestion for trips.csv")

    total_rows = (
        sum(1 for _ in open(file_path)) - 1
    )  # estimate total rows (minus header)
    total_chunks = (total_rows // CHUNK_SIZE) + 1  # estimate number of chunks

    with tqdm(
        total=total_chunks,
        desc="- Processing Chunks",
        unit="chunk",
        bar_format="{l_bar}{bar} [{elapsed}<{remaining}]",
    ) as pbar:

        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):

            chunk["datetime"] = pd.to_datetime(chunk["datetime"])  # convert datetime

            chunk["origin_coord"] = chunk["origin_coord"].apply(
                clean_coordinates
            )  # fix origin coordinates

            chunk["destination_coord"] = chunk["destination_coord"].apply(
                clean_coordinates
            )  # fix destination coordinates

            processing_time = load_table_to_bigquery(
                chunk, TABLE_ID
            )  # load chuck data to BigQuery

            pbar.update(1)  # update progress bar

            pbar.set_postfix({"Last Batch Time": f"{processing_time:.2f}s"})

    time.sleep(1)
    print("🔹 Data ingestion completed successfully.")


def clean_coordinates(coord):
    """Extracts latitude & longitude from 'POINT (lat lon)' format."""

    try:
        coord = coord.replace("POINT (", "").replace(")", "")
        lon, lat = map(float, coord.split())
        return f"POINT({lon} {lat})"  # convert to BigQuery GEOGRAPHY format
    except:
        return None  # replace possible errors


if __name__ == "__main__":

    # create raw trip table with partition and clustering for better performance
    create_bq_table(ddl_file="sql/ddl/trips_ddl.sql")

    # make the ETL of table trips from CSV to Big Query
    df = data_ingestion(FILE_PATH)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
