import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# load credentials
PROJECT_ID = "data-project-452300"
DATASET_ID = "challenge"
TABLE_NAME = "raw_trips"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
SERVICE_ACCOUNT_FILE = "data-project-452300-e2c341ffd483.json"

# authenticate with Google Cloud
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


def query_bigquery_table(query):
    """Runs a SQL query on BigQuery and returns results as a DataFrame."""
    query_job = bq_client.query(query)  # run the query
    return query_job.to_dataframe()  # convert result to Pandas DataFrame


def print_report(title, data):
    """Formats the report for better readability."""
    print("\n\n" + "=" * 80)
    print(f"{title}")
    print("=" * 80)

    if isinstance(data, pd.Series):
        for index, value in data.items():
            print(f"🔹 {index}: {value:,}")
    else:
        print(data)


if __name__ == "__main__":

    print("\n\n##################################################################")
    print("\n🔹 Selecting table for Data Visualization.")

    # query to get all records from trips table
    query = f"SELECT * FROM `{TABLE_ID}`"  # adjust limit if needed

    # run query and store results in a DataFrame
    df = query_bigquery_table(query)

    print("🔹 Table loaded into memory.")

    # convert datetime column to pandas datetime format
    df["datetime"] = pd.to_datetime(df["datetime"])

    # extract relevant time-based features
    df["hour"] = df["datetime"].dt.hour
    df["day_of_week"] = df["datetime"].dt.day_name()
    df["month"] = df["datetime"].dt.month_name()

    # generate reports
    print_report("TOTAL TRIPS PER REGION", df["region"].value_counts())
    print_report("TOTAL TRIPS PER DATASOURCE", df["datasource"].value_counts())
    print_report(
        "TOTAL TRIPS PER HOUR OF THE DAY", df["hour"].value_counts().sort_index()
    )
    print_report("TOTAL TRIPS PER DAY OF THE WEEK", df["day_of_week"].value_counts())
    print_report("TOTAL TRIPS PER MONTH", df["month"].value_counts())

    print("\n##################################################################")
    print("✅ REPORT GENERATION COMPLETE ✅")
    print("##################################################################\n\n")
