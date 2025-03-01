import matplotlib.pyplot as plt
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


if __name__ == "__main__":

    print("\n##################################################################")

    print("🔹 Select table for Data Vizualization.")

    # query to get all records from trips table
    query = f"SELECT * FROM `{TABLE_ID}`"  # limit to avoid excessive data

    # run query and store results in a DataFrame
    df = query_bigquery_table(query)

    print("🔹 Table loaded into memory.")

    # convert datetime column to pandas datetime format
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Extract relevant time-based features
    df["hour"] = df["datetime"].dt.hour
    df["day_of_week"] = df["datetime"].dt.day_name()
    df["month"] = df["datetime"].dt.month_name()

    # count trips per region
    region_counts = df["region"].value_counts()

    # count trips per datasource
    datasource_counts = df["datasource"].value_counts()

    # count trips per hour of the day
    hourly_counts = df["hour"].value_counts().sort_index()

    # count trips per day of the week
    day_of_week_counts = df["day_of_week"].value_counts()

    # count trips per month
    month_counts = df["month"].value_counts()

    print("🔹 Creating matplotlib charts.")

    # plot Trips per Region
    plt.figure(figsize=(10, 6))
    plt.bar(region_counts.index, region_counts.values, color="blue")
    plt.xlabel("Region")
    plt.ylabel("Number of Trips")
    plt.title("🚖 Total Trips per Region")
    plt.xticks(rotation=45)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.show()

    # plot Trips per Datasource
    plt.figure(figsize=(10, 6))
    plt.bar(datasource_counts.index, datasource_counts.values, color="green")
    plt.xlabel("Datasource")
    plt.ylabel("Number of Trips")
    plt.title("📡 Total Trips per Datasource")
    plt.xticks(rotation=45)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.show()

    # plot Trips per Hour of the Day
    plt.figure(figsize=(10, 6))
    plt.plot(hourly_counts.index, hourly_counts.values, marker="o", linestyle="-")
    plt.xlabel("Hour of the Day")
    plt.ylabel("Number of Trips")
    plt.title("⏳ Trips per Hour of the Day")
    plt.xticks(range(0, 24))
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.show()

    # plot Trips per Day of the Week
    plt.figure(figsize=(10, 6))
    plt.bar(day_of_week_counts.index, day_of_week_counts.values, color="purple")
    plt.xlabel("Day of the Week")
    plt.ylabel("Number of Trips")
    plt.title("📅 Trips per Day of the Week")
    plt.xticks(rotation=45)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.show()

    # plot Trips per Month
    plt.figure(figsize=(10, 6))
    plt.bar(month_counts.index, month_counts.values, color="orange")
    plt.xlabel("Month")
    plt.ylabel("Number of Trips")
    plt.title("📆 Trips per Month")
    plt.xticks(rotation=45)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.show()
