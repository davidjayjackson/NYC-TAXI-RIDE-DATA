USE nyctaxis
-- I want to return the rows with the maximum ride distance and any row with the ride distance <= 0

WITH max_trip AS (
  SELECT MAX(trip_distance) AS max_distance
  FROM main.yellowcab low
)
SELECT *
FROM main.yellowcab low
WHERE trip_distance <= 0 OR trip_distance = (SELECT max_distance FROM max_trip);

-- write the duckdb SQL code to calculate the 0.25,0.5, 0.75 quartiles and IQR 
-- for trip_distance in the Yellowcab data 

-- Calculate Q1, Q2, Q3, and IQR for trip_distance
-- Calculate descriptive statistics for trip_distance
WITH statistics AS (
  SELECT 
    MIN(trip_distance) AS min_trip_distance,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance) AS q1,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance) AS q2,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) AS q3,
    MAX(trip_distance) AS max_trip_distance,
    AVG(trip_distance) AS avg_trip_distance,
    (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) -
     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance)) AS iqr
  FROM yellowcab
)
SELECT min_trip_distance, q1, q2, q3, max_trip_distance, avg_trip_distance, iqr
FROM statistics;

--- by YEAR 
-- Calculate descriptive statistics for trip_distance by year
WITH statistics AS (
  SELECT 
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    MIN(trip_distance) AS min_trip_distance,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance) AS q1,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance) AS q2,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) AS q3,
    MAX(trip_distance) AS max_trip_distance,
    AVG(trip_distance) AS avg_trip_distance,
    (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) -
     PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance)) AS iqr
  FROM yellowcab
  GROUP BY year
)
SELECT year, min_trip_distance, q1, q2, q3, max_trip_distance, avg_trip_distance, iqr
FROM statistics
ORDER BY year;

-- Create a new table to store yearly distance quartiles and IQR
CREATE VIEW yearly_distance_quants AS (
  WITH statistics AS (
    SELECT 
      EXTRACT(YEAR FROM pickup_datetime) AS year,
      MIN(trip_distance) AS min_trip_distance,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance) AS q1,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance) AS q2,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) AS q3,
      MAX(trip_distance) AS max_trip_distance,
      AVG(trip_distance) AS avg_trip_distance,
      (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) -
       PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance)) AS iqr
    FROM yellowcab
    GROUP BY year
  )
  SELECT year, min_trip_distance, q1, q2, q3, max_trip_distance, avg_trip_distance, iqr
  FROM statistics
  ORDER BY year
);

-- Show the contents of the new table
SELECT * FROM yearly_distance_quants;

-- Create quant table for fare_amount by YEAR 
-- Create a new table to store yearly fare amount quartiles and IQR
CREATE VIEW yearly_fare_amount_quants AS (
  WITH statistics AS (
    SELECT 
      EXTRACT(YEAR FROM pickup_datetime) AS year,
      MIN(fare_amount) AS min_fare_amount,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY fare_amount) AS q1,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fare_amount) AS q2,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fare_amount) AS q3,
      MAX(fare_amount) AS max_fare_amount,
      AVG(fare_amount) AS avg_fare_amount,
      (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fare_amount) -
       PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY fare_amount)) AS iqr
    FROM yellowcab
    GROUP BY year
  )
  SELECT year, min_fare_amount, q1, q2, q3, max_fare_amount, avg_fare_amount, iqr
  FROM statistics
  ORDER BY year
);

-- Show the contents of the new table
SELECT * FROM yearly_fare_amount_quants;

-- Create a date table that has date,year,month and quarter using the start_date column
-- add a auto-increment column that starts with 100 and goes up by 1

-- Create a new table with an auto-increment column
-- Create a new table with an auto-increment column and an index for start_date
CREATE VIEW dim_calendar AS (
  SELECT
   -- ROW_NUMBER() OVER (ORDER BY start_date) + 99 AS id,
    start_date AS date,
    EXTRACT(YEAR FROM start_date) AS year,
    EXTRACT(MONTH FROM start_date) AS month,
    CASE
      WHEN EXTRACT(MONTH FROM start_date) <= 3 THEN '1Q'
      WHEN EXTRACT(MONTH FROM start_date) <= 6 THEN '2Q'
      WHEN EXTRACT(MONTH FROM start_date) <= 9 THEN '3Q'
      ELSE '4Q'
    END AS quarter
  FROM yellowcab -- Replace with your actual table name
  GROUP BY date, year,month, quarter
);

-- Create an index for the start_date column
-- CREATE INDEX idx_date ON dim_calendar(date);



-- Create date/hour table
CREATE VIEW dim_hour AS (
  SELECT
    pickup_datetime AS pickup_datetime,
    start_date,
    EXTRACT(HOUR FROM pickup_datetime) AS hour
  FROM yellowcab 
 GROUP BY pickup_datetime,start_date,hour);

SELECT * FROM dim_calendar LIMIT 25;


SELECT cal.year,cal.quarter,count(*) as ride_count
FROM yellowcab as yel
INNER JOIN dim_calendar as cal
ON cal.date = yel.start_date 
GROUP BY cal.year,cal.quarter
ORDER BY cal.year,cal.quarter;


SELECT cal.year,cal.month,count(*) as ride_count
FROM yellowcab as yel
INNER JOIN dim_calendar as cal
ON cal.date = yel.start_date 
GROUP BY cal.year,cal.month
ORDER BY cal.year,cal.month;

SELECT * FROM db_cleaner limit 10;
