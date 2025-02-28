
WITH filtered_trips AS (
    SELECT 
        DATE_TRUNC(DATE(datetime), WEEK(MONDAY)) AS week,  -- Use monday as the reference day for the week
        COUNT(*) AS weekly_trips
    FROM `data-project-452300.challenge.raw_trips`
    WHERE 
        region = 'Prague'
    GROUP BY week
)

SELECT 
    ROUND(AVG(weekly_trips), 1) AS weekly_avg_trips
FROM filtered_trips;
