import duckdb
import os

os.makedirs("output", exist_ok=True)

con = duckdb.connect()

# =========================
# Load Parquet (NOT CSV)
# =========================
con.execute("""
CREATE TABLE trips_raw AS
SELECT * FROM read_parquet('data/yellow_tripdata_2023-01.parquet');
""")

# =========================
# Date Dimension
# =========================
con.execute("""
CREATE TABLE dim_date AS
SELECT DISTINCT
    date_trunc('day', tpep_pickup_datetime) AS date,
    year(tpep_pickup_datetime) AS year,
    month(tpep_pickup_datetime) AS month,
    day(tpep_pickup_datetime) AS day,
    weekday(tpep_pickup_datetime) AS weekday
FROM trips_raw;
""")

# =========================
# Location Dimension
# =========================
con.execute("""
CREATE TABLE dim_location AS
SELECT DISTINCT
    PULocationID AS location_id
FROM trips_raw;
""")

# =========================
# Fact Table
# =========================
con.execute("""
CREATE TABLE fact_trips AS
SELECT
    row_number() OVER () AS trip_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id
FROM trips_raw;
""")

# =========================
# Export to Parquet
# =========================
con.execute("COPY fact_trips TO 'output/fact_trips.parquet' (FORMAT PARQUET);")
con.execute("COPY dim_date TO 'output/dim_date.parquet' (FORMAT PARQUET);")
con.execute("COPY dim_location TO 'output/dim_location.parquet' (FORMAT PARQUET);")

print("✅ ETL completed successfully!")
