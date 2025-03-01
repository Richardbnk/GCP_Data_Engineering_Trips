WITH trips_with_time_of_day AS (
    SELECT 
        region,
        CASE 
            WHEN EXTRACT(HOUR FROM datetime) BETWEEN 5 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM datetime) BETWEEN 12 AND 18 THEN 'Afternoon'
            ELSE 'Night'
        END AS time_of_day,
        ROUND(ST_Y(origin_coord), 1) AS origin_latitude,
        ROUND(ST_X(origin_coord), 1) AS origin_longitude,
        ROUND(ST_Y(destination_coord), 1) AS destination_latitude,
        ROUND(ST_X(destination_coord), 1) AS destination_longitude,
        datasource,
    FROM `data-project-452300.challenge.raw_trips`
)

SELECT 
    region,
    time_of_day,
    origin_latitude,
    origin_longitude,
    destination_latitude,
    destination_longitude,
    COUNT(*) AS trip_count,
    STRING_AGG(DISTINCT datasource, ', ') AS datasources
FROM trips_with_time_of_day
GROUP BY 1,2,3,4,5,6
-- HAVING trip_count > 1 # Filter only more than one trip at similar variables
ORDER BY trip_count DESC;
