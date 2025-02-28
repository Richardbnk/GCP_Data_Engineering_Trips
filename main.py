import os
import pandas as pd
from google.cloud import bigquery
import re
import warnings

warnings.simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)


# constants
PROJECT_ID = "data-project-452300"
DATASET_ID = "challenge"
TABLE_ID = "raw_trips"

SERVICE_ACCOUNT_FILE = r"data-project-452300-e2c341ffd483.json"

# set Google Cloud Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

FILE_PATH = "trips.csv"


def create_trips_table(ddl_file):
    """Creates the trip table on big query."""

    bq_client = bigquery.Client()

    with open(ddl_file, "r") as ddl_file:
        ddl_query = ddl_file.read()
    query_job = bq_client.query(ddl_query)
    query_job.result()  # Wait for completion


def data_ingestion(file_path):

    print("Data ingestion started.")
    df = pd.read_csv(file_path)

    df["datetime"] = pd.to_datetime(df["datetime"])

    print(f"Table loaded with {len(df)} rows.\n")

    return df


def load_table_to_bigquery(df, table_id):

    print(f"Importing raw table to Big Query at: {table_id}")

    bq_client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, autodetect=True
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Table finished loading.\n")


def get_time_of_day(hour):

    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour <= 18:
        return "Afternoon"
    else:
        return "Night"


def round_coordinate(coordinate):

    # clean coordinate string and get latitude and longitude
    coord = re.sub(r"[^0-9.\s]", "", coordinate)
    coord = coord.split(" ")
    latitude = float(coord[1])
    longitude = float(coord[2])

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
        "\nTrips with similar origin, destination, and time of day are now grouped. These are the trips:\n"
    )
    print(multiple_trips, "\n")

    return grouped_df


def weekly_avg_trips(df, bounding_box=None, region=None, location_filter="origin"):
    if location_filter not in ("origin", "destination", "both"):
        raise ValueError(
            'Invalid "location_filter" parameter. Choose "origin", "destination", or "both".'
        )

    # Ensure datetime is in proper format
    df = df.copy(deep=True)
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Apply bounding box filter
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
    df["week_year"] = df["datetime"].dt.to_period("W").astype(str)  # week grouping

    # print("\ndebug: trips per week (raw count)")
    weekly_trips = df.groupby("week_year").size()
    # print(weekly_trips)

    # calculate weekly average
    weekly_avg = weekly_trips.mean()

    print(f"Weekly Average Trips ({location_filter}): {weekly_avg:.2f}\n")
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
    weekly_avg_trips(df, bounding_box=bounding_box, location_filter="origin")

    print("#################################################################")
    # define bounding box (example)
    bounding_box = (7.49, 13.00, 44.0, 48.0)
    print(f"\nFor Prague region only\n")

    # compute weekly average for bounding box based on BOTH (either origin or destination)
    weekly_avg_trips(df, location_filter="both", region="Prague")


# MAIN CODE
# if __name__ == "__main__":

# create raw trip table
create_trips_table(ddl_file="ddl.sql")

df = data_ingestion(FILE_PATH)

table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

load_table_to_bigquery(df, table_id)

# calculate and print similar group trips
df_similar_trips = group_similar_trips(df)

print_weekle_average_trips_cenarios()
