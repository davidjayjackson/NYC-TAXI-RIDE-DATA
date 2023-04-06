-- Create a view to aggregate demand by hour of day
CREATE VIEW hourly_demand AS
SELECT DATE_TRUNC('hour', pickup_datetime) AS pickup_hour,
       COUNT(*) AS demand
FROM yellowcab
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- Run a linear regression to predict demand patterns
CREATE MODEL demand_prediction AS (
  SELECT pickup_hour,
         demand,
         ROW_NUMBER() OVER (ORDER BY pickup_hour) AS row_num
  FROM hourly_demand
);

-- Train the model
TRAIN demand_prediction USING (
  SELECT row_num, pickup_hour::FLOAT, demand::FLOAT
  FROM demand_prediction
);

-- Generate predictions for future hours
SELECT TO_CHAR(pickup_hour, 'YYYY-MM-DD HH24:00:00') AS pickup_hour,
       PREDICT_LINEAR_REGRESSION('demand_prediction', row_num, ARRAY[row_num]) AS predicted_demand
FROM (
  SELECT (SELECT MAX(row_num) FROM demand_prediction) + 1 + GENERATE_SERIES(1, 24) AS row_num,
         CURRENT_DATE + INTERVAL '1 day' + INTERVAL '1 hour' * GENERATE_SERIES(0, 23) AS pickup_hour
) AS subquery;
