import duckdb
import os

INPUT_PARQUET = "data/yellow_tripdata_2023-01.parquet"
OUTPUT_DIR = "output"

os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()

# 1) Inspect columns in the Parquet
cols = con.execute(f"PRAGMA show_columns('{INPUT_PARQUET}');").fetchall()
print("Columns in Parquet:")
for c in cols:
    print("-", c[0], c[1])

# Build a simple trips_raw table using only what actually exists.
# For now, assume:
#   column0 = pickup datetime
# and there is NO dropoff datetime in the file.
# Adjust this mapping after you see the printed columns.
con.execute("DROP TABLE IF EXISTS trips_raw;")
con.execute(f"""
    CREATE TABLE trips_raw AS
    SELECT
        column0::TIMESTAMP AS tpep_pickup_datetime
    FROM read_parquet('{INPUT_PARQUET}')
    WHERE column0 IS NOT NULL;
""")

# Date dimension
con.execute("DROP TABLE IF EXISTS dim_date;")
con.execute("""
CREATE TABLE dim_date AS
SELECT DISTINCT
    DATE(tpep_pickup_datetime) AS date_id,
    YEAR(tpep_pickup_datetime) AS year,
    MONTH(tpep_pickup_datetime) AS month,
    DAY(tpep_pickup_datetime) AS day,
    WEEKDAY(tpep_pickup_datetime) AS weekday
FROM trips_raw;
""")

# Location dimension: these will be NULL unless your file actually has these columns.
con.execute("DROP TABLE IF EXISTS dim_location;")
con.execute("""
CREATE TABLE dim_location AS
SELECT DISTINCT location_id
FROM (
    SELECT NULL::INT AS location_id
)
WHERE location_id IS NOT NULL;
""")

# Fact table
con.execute("DROP TABLE IF EXISTS fact_trips;")
con.execute("""
CREATE TABLE fact_trips AS
SELECT
    ROW_NUMBER() OVER (ORDER BY tpep_pickup_datetime) AS trip_id,
    tpep_pickup_datetime,
    tpep_pickup_datetime AS tpep_dropoff_datetime,
    NULL::INT AS passenger_count,
    NULL::DOUBLE AS trip_distance,
    NULL::DOUBLE AS fare_amount,
    NULL::DOUBLE AS total_amount,
    NULL::INT AS pickup_location_id,
    NULL::INT AS dropoff_location_id,
    DATE(tpep_pickup_datetime) AS date_id
FROM trips_raw;
""")

# Export
con.execute(f"COPY fact_trips TO '{OUTPUT_DIR}/fact_trips.parquet' (FORMAT PARQUET);")
con.execute(f"COPY dim_date TO '{OUTPUT_DIR}/dim_date.parquet' (FORMAT PARQUET);")
con.execute(f"COPY dim_location TO '{OUTPUT_DIR}/dim_location.parquet' (FORMAT PARQUET);")

con.close()
print("✅ ETL completed (with minimal schema).")
