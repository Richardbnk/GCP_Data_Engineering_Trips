import os
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


def execute_sql_file(sql_file_path, parameters={}):
    """
    Executes a SQL file in BigQuery with optional parameters and returns the result as a Pandas DataFrame.
    """
    with open(sql_file_path, "r") as file:
        query = file.read()

    # convert parameters into BigQuery query parameters
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            (
                bigquery.ScalarQueryParameter(key, "STRING", value)
                if isinstance(value, str)
                else bigquery.ScalarQueryParameter(key, "FLOAT64", value)
            )
            for key, value in parameters.items()
        ]
    )

    # execute the query
    query_job = bq_client.query(query, job_config=job_config)
    result_df = query_job.to_dataframe()

    return result_df


# define parameters for different queries
bounding_box_params = {
    "min_lat": 7.49,
    "max_lat": 13.00,
    "min_lon": 44.0,
    "max_lon": 48.0,
}

region_params = {"region": "Prague"}

# run queries for different use cases
print("\n🔹 Running: Group Similar Trips")
grouped_trips_df = execute_sql_file(os.path.join("sql", "group_similar_trips.sql"))

print("\n🔹 Running: Weekly Average Trips (Bounding Box)")
weekly_avg_bbox_df = execute_sql_file(
    os.path.join("sql", "weekly_avg_trips_bounding_box.sql"),
    bounding_box_params,
)

print("\n🔹 Running: Weekly Average Trips by Region")
weekly_avg_region_df = execute_sql_file(
    os.path.join("sql", "weekly_avg_trips_region.sql"), region_params
)

print("\n🔹 Running: Latest Datasource From Common Regions")
latest_datasource_from_common_regions = execute_sql_file(
    os.path.join("sql", "latest_datasource_from_common_regions.sql"), region_params
)


print("\n🔹 Running: Regions where cheap_mobile Appeared")
regions_of_cheap_mobile_df = execute_sql_file(
    os.path.join("sql", "regions_of_cheap_mobile.sql"), region_params
)


print("\n##################################################################")


# display results
print("\n🔹 Grouped Trips (Similar Trips per Region & Time of Day):")
print(grouped_trips_df.head())

print(
    "\n🔹 Weekly Average Trips (Bounding Box):",
    weekly_avg_bbox_df["weekly_avg_trips"][0],
)

print(
    "\n🔹 Weekly Average Trips (Region - Prague):",
    weekly_avg_region_df["weekly_avg_trips"][0],
)

print("\n##################################################################")

print(
    "\n🔹 Latest Datasource From Common Regions:\n",
    latest_datasource_from_common_regions.head(),
)


print(
    "\n🔹 Regions Where cheap_mobile Appeared:\n",
    regions_of_cheap_mobile_df.head(),
)
