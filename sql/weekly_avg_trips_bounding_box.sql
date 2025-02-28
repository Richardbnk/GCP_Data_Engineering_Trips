WITH weekly_avg_trips_by_bounding_box AS (
    SELECT 
        DATE_TRUNC(DATE(datetime), WEEK(MONDAY)) AS week,  -- Use Monday as the reference day for the week
        ROUND(ST_Y(origin_coord), 1) AS origin_latitude,
        ROUND(ST_X(origin_coord), 1) AS origin_longitude,
        COUNT(*) AS weekly_trips
    FROM `data-project-452300.challenge.raw_trips`
    WHERE 
        -- Apply bounding box filter for ORIGIN (after extracting lat/lon)
        ROUND(ST_Y(origin_coord), 1) BETWEEN 40 AND 50
        AND ROUND(ST_X(origin_coord), 1) BETWEEN 7 AND 11
    GROUP BY week, origin_latitude, origin_longitude
)

SELECT 
    ROUND(AVG(weekly_trips), 1) AS weekly_avg_trips
FROM weekly_avg_trips_by_bounding_box;
