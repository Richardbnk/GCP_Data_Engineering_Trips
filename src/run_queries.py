import os
from tqdm import tqdm
from google.cloud import bigquery
from google.oauth2 import service_account

# load credentials
PROJECT_ID = "data-project-452300"
DATASET_ID = "challenge"
TABLE_NAME = "raw_trips"
TABLE_NAME_GROUPED = "grouped_trips"

TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
TABLE_ID_GROUPED = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_GROUPED}"

SERVICE_ACCOUNT_FILE = r"data-project-452300-e2c341ffd483.json"
CHUNK_SIZE = 100000  # load data in chuncks

# authenticate with Google Cloud
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


def create_bq_table(ddl_file):
    """Run DDL for table creation."""

    with open(ddl_file, "r") as ddl_file:
        ddl_query = ddl_file.read()
    query_job = bq_client.query(ddl_query)
    query_job.result()


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


def load_dataframe_to_bigquery(df, table_id):
    """Loads a Pandas DataFrame into BigQuery."""

    job = bq_client.load_table_from_dataframe(df, table_id)
    job.result()


def data_ingestion_from_dataframe(df, table_id):
    """Loads a DataFrame in chunks to BigQuery."""
    chunk_size = 10000
    total_chunks = (len(df) // chunk_size) + 1

    print("\n🔹 Starting data ingestion for grouped trips...")
    with tqdm(
        total=total_chunks,
        desc=" - Processing Chunks",
        unit="chunk",
        bar_format="{l_bar}{bar} [{elapsed}<{remaining}]",
    ) as pbar:

        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            load_dataframe_to_bigquery(chunk, table_id)
            pbar.update(1)

    print("🔹 Data ingestion from DataFrame completed successfully.")


def print_report(title, data):
    """Formats the report for better readability."""
    print("\n\n" + "=" * 80)
    print(f"{title}")
    print("=" * 80)

    try:
        if isinstance(data, pd.Series):
            for index, value in data.items():
                print(f"🔹 {index}: {value:,}")
        else:
            print(data)
    except:
        print(data)


# define parameters for different queries
bounding_box_params = {
    "min_lat": 7.49,
    "max_lat": 13.00,
    "min_lon": 44.0,
    "max_lon": 48.0,
}

region_params = {"region": "Prague"}

# create grouped similar trip table with partition and clustering for better performance
print("\n🔹 Running: Group Similar Trips")
create_bq_table(ddl_file="sql/ddl/grouped_trips.sql")
grouped_trips_df = execute_sql_file(os.path.join("sql", "group_similar_trips.sql"))
data_ingestion_from_dataframe(df=grouped_trips_df, table_id=TABLE_ID_GROUPED)

print("\n🔹 Running: Weekly Average Trips (Bounding Box)")
weekly_avg_bbox_df = execute_sql_file(
    os.path.join("sql", "weekly_avg_trips_bounding_box.sql"),
    bounding_box_params,
)

print("\n🔹 Running: Weekly Average Trips by Region")
weekly_avg_region_df = execute_sql_file(
    os.path.join("sql", "weekly_avg_trips_region.sql"), region_params
)

print("🔹 Running: Latest Datasource From Common Regions")
latest_datasource_from_common_regions = execute_sql_file(
    os.path.join("sql", "latest_datasource_from_common_regions.sql"), region_params
)

print("🔹 Running: Regions where cheap_mobile Appeared")
regions_of_cheap_mobile_df = execute_sql_file(
    os.path.join("sql", "regions_of_cheap_mobile.sql"), region_params
)


print("\n##################################################################")

print("\nResults:")


# display results
print_report(
    "🔹 Grouped Trips (Similar Trips per Region & Time of Day):",
    grouped_trips_df.head(),
)

print_report(
    "🔹 Weekly Average Trips (Bounding Box):",
    weekly_avg_bbox_df["weekly_avg_trips"][0],
)

print_report(
    "🔹 Weekly Average Trips (Region - Prague):",
    weekly_avg_region_df["weekly_avg_trips"][0],
)

print_report(
    "🔹 Latest Datasource From Top 2 most Common Regions:",
    latest_datasource_from_common_regions.head(),
)

print_report(
    "🔹 Regions Where cheap_mobile Appeared:",
    regions_of_cheap_mobile_df.head(),
)
