import duckdb
import os

# ===============================
# Config
# ===============================
INPUT_PARQUET = "data/yellow_tripdata_2023-01.parquet"
OUTPUT_DIR = "output"

# Create output folder
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Connect to DuckDB
con = duckdb.connect()

# ===============================
# Inspect schema and normalize
# ===============================
# Get actual column names from the parquet
cols = con.execute(f"PRAGMA show_columns('{INPUT_PARQUET}');").fetchall()
# cols is a list of (column_name, type, ...)

print("Detected columns in Parquet:")
for c in cols:
    print("-", c[0], c[1])

# If the file already has proper NYC taxi column names, we can use it directly.
# Otherwise, we need to rename columns here.
# For safety, create a normalized view that aliases columns to expected names.

# Try to detect if tpep_pickup_datetime exists
col_names = {c[0].lower(): c[0] for c in cols}

def get(col_key, default=None):
    """Return actual column name if present, else default."""
    return col_names.get(col_key.lower(), default)

# Build a SELECT that maps whatever is present to standard names.
# Adjust these mappings if your Parquet file has different column names.
select_expr = f"""
SELECT
    {get('tpep_pickup_datetime', get('column0'))}::TIMESTAMP AS tpep_pickup_datetime,
    {get('tpep_dropoff_datetime', get('column1', get('column0')))}::TIMESTAMP AS tpep_dropoff_datetime,
    {get('passenger_count', 'NULL')}::INT AS passenger_count,
    {get('trip_distance', 'NULL')}::DOUBLE AS trip_distance,
    {get('fare_amount', 'NULL')}::DOUBLE AS fare_amount,
    {get('total_amount', 'NULL')}::DOUBLE AS total_amount,
    {get('PULocationID', 'NULL')}::INT AS PULocationID,
    {get('DOLocationID', 'NULL')}::INT AS DOLocationID
FROM read_parquet('{INPUT_PARQUET}')
"""

# ===============================
# Load normalized data
# ===============================
con.execute("""
DROP TABLE IF EXISTS trips_raw;
""")

con.execute(f"""
CREATE TABLE trips_raw AS
{select_expr}
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
con.execute(f"""
COPY fact_trips TO '{OUTPUT_DIR}/fact_trips.parquet' (FORMAT PARQUET);
""")

con.execute(f"""
COPY dim_date TO '{OUTPUT_DIR}/dim_date.parquet' (FORMAT PARQUET);
""")

con.execute(f"""
COPY dim_location TO '{OUTPUT_DIR}/dim_location.parquet' (FORMAT PARQUET);
""")

# Close connection
con.close()

print("✅ ETL completed successfully!")
