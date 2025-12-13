-- Query 1: Average fare by passenger count
SELECT passenger_count, AVG(fare_amount)
FROM fact_trips
GROUP BY passenger_count;

-- Query 2: Total revenue per pickup location
SELECT pickup_location_id, SUM(total_amount)
FROM fact_trips
GROUP BY pickup_location_id;

-- Query 3: Trips per day
SELECT date(tpep_pickup_datetime) AS trip_date, COUNT(*)
FROM fact_trips
GROUP BY trip_date;

