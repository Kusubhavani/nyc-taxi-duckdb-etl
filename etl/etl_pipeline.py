import duckdb
import os

# Create output folder
os.makedirs("output", exist_ok=True)

# Connect to DuckDB
con = duckdb.connect()

# ===============================
# Load Parquet data
# ===============================
con.execute("""
DROP TABLE IF EXISTS trips_raw;
CREATE TABLE trips_raw AS
SELECT *
FROM read_parquet('data/yellow_tripdata_2023-01.parquet')
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL;
""")

# ===============================
# Date Dimension
# ===============================
con.execute("""
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date AS
SELECT DISTINCT
    DATE(tpep_pickup_datetime) AS date_id,
    YEAR(tpep_pickup_datetime) AS year,
    MONTH(tpep_pickup_datetime) AS month,
    DAY(tpep_pickup_datetime) AS day,
    WEEKDAY(tpep_pickup_datetime) AS weekday
FROM trips_raw;
""")

# ===============================
# Location Dimension
# ===============================
con.execute("""
DROP TABLE IF EXISTS dim_location;
CREATE TABLE dim_location AS
SELECT DISTINCT location_id
FROM (
    SELECT PULocationID AS location_id FROM trips_raw
    UNION
    SELECT DOLocationID AS location_id FROM trips_raw
)
WHERE location_id IS NOT NULL;
""")

# ===============================
# Fact Table
# ===============================
con.execute("""
DROP TABLE IF EXISTS fact_trips;
CREATE TABLE fact_trips AS
SELECT
    ROW_NUMBER() OVER (ORDER BY tpep_pickup_datetime) AS trip_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    DATE(tpep_pickup_datetime) AS date_id
FROM trips_raw;
""")

# ===============================
# Export to Parquet
# ===============================
con.execute("""
COPY fact_trips TO 'output/fact_trips.parquet' (FORMAT PARQUET);
""")

con.execute("""
COPY dim_date TO 'output/dim_date.parquet' (FORMAT PARQUET);
""")

con.execute("""
COPY dim_location TO 'output/dim_location.parquet' (FORMAT PARQUET);
""")

# Close connection
con.close()

print("✅ ETL completed successfully!")
