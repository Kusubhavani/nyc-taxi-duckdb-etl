import duckdb
import os

os.makedirs("output", exist_ok=True)
con = duckdb.connect()

# CSV HAS NO HEADERS ? map columns manually
con.execute("""
CREATE OR REPLACE TABLE trips_raw AS
SELECT
    column0::TIMESTAMP AS tpep_pickup_datetime,
    column1::TIMESTAMP AS tpep_dropoff_datetime,
    column2::INTEGER AS passenger_count,
    column3::DOUBLE AS trip_distance,
    column4::DOUBLE AS fare_amount,
    column5::DOUBLE AS total_amount,
    column6::INTEGER AS PULocationID,
    column7::INTEGER AS DOLocationID
FROM read_csv_auto('data/yellow_tripdata.csv', header=false);
""")

con.execute("""
CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    date(tpep_pickup_datetime) AS date,
    year(tpep_pickup_datetime) AS year,
    month(tpep_pickup_datetime) AS month,
    day(tpep_pickup_datetime) AS day,
    weekday(tpep_pickup_datetime) AS weekday
FROM trips_raw
WHERE tpep_pickup_datetime IS NOT NULL;
""")

con.execute("""
CREATE OR REPLACE TABLE dim_location AS
SELECT DISTINCT
    PULocationID AS location_id
FROM trips_raw
WHERE PULocationID IS NOT NULL;
""")

con.execute("""
CREATE OR REPLACE TABLE fact_trips AS
SELECT
    row_number() OVER () AS trip_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id
FROM trips_raw
WHERE tpep_pickup_datetime IS NOT NULL;
""")

con.execute("COPY fact_trips TO 'output/fact_trips.parquet' (FORMAT PARQUET);")
con.execute("COPY dim_date TO 'output/dim_date.parquet' (FORMAT PARQUET);")
con.execute("COPY dim_location TO 'output/dim_location.parquet' (FORMAT PARQUET);")

print("? ETL completed successfully")
