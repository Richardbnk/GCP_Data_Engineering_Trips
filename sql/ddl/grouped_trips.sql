CREATE OR REPLACE TABLE  `data-project-452300.challenge.grouped_trips` (
    region STRING,
    time_of_day STRING,
    origin_latitude FLOAT64,
    origin_longitude FLOAT64,
    destination_latitude FLOAT64,
    destination_longitude FLOAT64,
    trip_count INT64,
    datasources STRING,
)
CLUSTER BY region, time_of_day, datasources;
