CREATE OR REPLACE TABLE `data-project-452300.challenge.raw_trips` (
    region STRING,
    origin_coord GEOGRAPHY, 
    destination_coord GEOGRAPHY, 
    datetime TIMESTAMP,
    datasource STRING
)
PARTITION BY DATE(datetime)
CLUSTER BY region, datasource;
