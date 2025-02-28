WITH commonly_regions AS (
    -- Count occurrences of each region
    SELECT region, COUNT(*) AS number_of_trips
    FROM `data-project-452300.challenge.raw_trips`
    GROUP BY region
    ORDER BY number_of_trips DESC
    LIMIT 2
),
latest_datasource AS (
    -- Find the latest datasource from the top 2 regions
    SELECT 
        region,
        datasource,
        datetime,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY datetime DESC) AS row_num
    FROM `data-project-452300.challenge.raw_trips`
    WHERE region IN (SELECT region FROM commonly_regions)
)
SELECT region, datasource, datetime
FROM latest_datasource
WHERE row_num = 1
ORDER BY datetime DESC;
