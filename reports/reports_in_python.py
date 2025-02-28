import re
import time
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
CHUNK_SIZE = 30  # load data in chuncks

# set GCP credentials
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


def query_bigquery_table(query):
    """Runs a SQL query on BigQuery and returns results as a DataFrame."""
    query_job = bq_client.query(query)  # Run the query
    return query_job.to_dataframe()  # Convert result to Pandas DataFrame


def create_trips_table(ddl_file):
    """Creates the trip table on big query."""

    bq_client = bigquery.Client()

    with open(ddl_file, "r") as ddl_file:
        ddl_query = ddl_file.read()
    query_job = bq_client.query(ddl_query)
    query_job.result()  # Wait for completion


def load_table_to_bigquery(df, table_id):
    """Uploads DataFrame to BigQuery in chunks for scalability."""
    start_time = time.time()

    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )

        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        duration = time.time() - start_time
        return duration

    except Exception as e:
        print(f"Failed to upload table chunk: {e}")
        return 0


def data_ingestion(file_path):
    """Loads large CSV in chunks and processes efficiently."""
    print("Starting data ingestion...")

    total_rows = (
        sum(1 for _ in open(file_path)) - 1
    )  # estimate total rows (minus header)
    total_chunks = (total_rows // CHUNK_SIZE) + 1  # etimate number of chunks

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
            )  # fix coordinates
            chunk["destination_coord"] = chunk["destination_coord"].apply(
                clean_coordinates
            )  # fix coordinates

            processing_time = load_table_to_bigquery(
                chunk, TABLE_ID
            )  # load to BigQuery
            pbar.update(1)  # update progress bar
            pbar.set_postfix(
                {"Last Batch Time": f"{processing_time:.2f}s"}
            )  # show last batch time

    print("Data ingestion completed successfully.")


def clean_coordinates(coord):
    """Extracts latitude & longitude from 'POINT (lat lon)' format."""
    try:
        coord = coord.replace("POINT (", "").replace(")", "")
        lon, lat = map(float, coord.split())
        return f"POINT({lon} {lat})"  # convert to BigQuery GEOGRAPHY format
    except:
        return None  # handle malformed data


def get_time_of_day(hour):

    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour <= 18:
        return "Afternoon"
    else:
        return "Night"


def round_coordinate(coordinate):

    # clean coordinate string and get latitude and longitude
    coord = re.sub(r"[^0-9.\s]", "", coordinate).split(" ")
    latitude = float(coord[0])
    longitude = float(coord[1])

    # round latitude and longitude by 1 decimal positions (1 decimal == 11 km)
    return round(latitude, 1), round(longitude, 1)


def group_similar_trips(df):

    # extract hour and categorize time of day
    df["time_of_day"] = df["datetime"].dt.hour.apply(get_time_of_day)

    # retreive latitude and longitude for similar origin and destination
    # it create a grid with blocks of lat and long at every 11 km,
    # and the people inside each block will be considered
    # as the same origin or same destination
    df["origin_latitude"], df["origin_longitude"] = zip(
        *df["origin_coord"].apply(lambda x: round_coordinate(x))
    )
    df["destination_latitude"], df["destination_longitude"] = zip(
        *df["destination_coord"].apply(lambda x: round_coordinate(x))
    )

    # group by origin, destination, and time of day
    grouped_df = (
        df.groupby(
            [
                "region",  # include region in grouping
                "time_of_day",
                "origin_latitude",
                "origin_longitude",
                "destination_latitude",
                "destination_longitude",
            ]
        )
        .agg(
            trip_count=("datasource", "count"),  # Count trips
            # concatenate cars that would do the same trip at a similar time of day
            datasources=("datasource", lambda x: ", ".join(set(x))),
        )
        .reset_index()
    )

    # filter trips where trip_count > 1 and print
    multiple_trips = grouped_df[grouped_df["trip_count"] > 1]

    print("##################################################################")
    print(
        "\nTrips with similar origin, destination, and time of day are now grouped. These are grouped the trips:\n"
    )
    print(multiple_trips, "\n")

    return grouped_df


def weekly_avg_trips(df, bounding_box=None, region=None, location_filter="origin"):
    if location_filter not in ("origin", "destination", "both"):
        raise ValueError(
            'Invalid "location_filter" parameter. Choose "origin", "destination", or "both".'
        )

    # ensure datetime is in proper format
    df = df.copy(deep=True)
    df["datetime"] = pd.to_datetime(df["datetime"])

    # apply bounding box filter
    if bounding_box:
        min_lat, max_lat, min_lon, max_lon = bounding_box

        if location_filter == "origin":
            mask = (
                (df["origin_latitude"] >= min_lat)
                & (df["origin_latitude"] <= max_lat)
                & (df["origin_longitude"] >= min_lon)
                & (df["origin_longitude"] <= max_lon)
            )
        elif location_filter == "destination":
            mask = (
                (df["destination_latitude"] >= min_lat)
                & (df["destination_latitude"] <= max_lat)
                & (df["destination_longitude"] >= min_lon)
                & (df["destination_longitude"] <= max_lon)
            )
        elif location_filter == "both":
            mask = (
                (df["origin_latitude"] >= min_lat)
                & (df["origin_latitude"] <= max_lat)
                & (df["origin_longitude"] >= min_lon)
                & (df["origin_longitude"] <= max_lon)
            ) | (
                (df["destination_latitude"] >= min_lat)
                & (df["destination_latitude"] <= max_lat)
                & (df["destination_longitude"] >= min_lon)
                & (df["destination_longitude"] <= max_lon)
            )
        df = df.loc[mask].copy()

    # apply region filter
    if region:
        df = df.loc[df["region"] == region].copy()

    # if filtered dataframe is empty, return 0.0
    if df.empty:
        print("No trips found for the given area and filter criteria.")
        return 0.0

    # extract week
    df["datetime"] = df["datetime"].dt.tz_localize(None)  # remove timezone
    df["week_year"] = df["datetime"].dt.to_period("W").astype(str)  # convert to period
    # week grouping

    # print("\ndebug: trips per week (raw count)")
    weekly_trips = df.groupby("week_year").size()
    # print(weekly_trips)

    # calculate weekly average
    weekly_avg = weekly_trips.mean()

    return weekly_avg


def print_weekle_average_trips_cenarios():

    print("#################################################################\n")
    # define bounding box (example)
    bounding_box = (7.49, 13.00, 44.0, 48.0)
    print(
        f"For Bounding box of (lat: {bounding_box[0]} to {bounding_box[1]} | lon {bounding_box[2]} to {bounding_box[3]})"
    )
    print("Considering 'Origin' locations only.\n")

    # compute weekly average for bounding box based on origin
    weekly_avg = weekly_avg_trips(
        df, bounding_box=bounding_box, location_filter="origin"
    )
    print(f"Weekly Average Trips (origin): {weekly_avg:.2f}\n")

    print("#################################################################")

    # compute weekly average for bounding box based on BOTH (either origin or destination)
    weekly_avg = weekly_avg_trips(df, location_filter="both", region="Prague")
    print(f"\nFor Prague region - Weekly Average Trips (both): {weekly_avg:.2f}")

    # compute weekly average for bounding box based on BOTH (either origin or destination)
    weekly_avg = weekly_avg_trips(df, location_filter="both", region="Turin")
    print(f"\nFor Turin region - Weekly Average Trips (both): {weekly_avg:.2f}")

    # compute weekly average for bounding box based on BOTH (either origin or destination)
    weekly_avg = weekly_avg_trips(df, location_filter="both", region="Hamburg")
    print(f"\nFor Hamburg region - Weekly Average Trips (both): {weekly_avg:.2f}")


if __name__ == "__main__":

    # query to get all records from trips table
    query = f"SELECT * FROM `{TABLE_ID}`"

    # run query and store results in a DataFrame
    df = query_bigquery_table(query)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

    # calculate and print similar group trips
    df_similar_trips = group_similar_trips(df)

    print_weekle_average_trips_cenarios()
