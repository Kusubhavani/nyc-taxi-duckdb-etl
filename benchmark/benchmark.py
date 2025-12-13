import duckdb
import time
import os

con = duckdb.connect()

# Ensure data folder exists
os.makedirs("../data", exist_ok=True)

# 1) Create CSV from Parquet (one-time per run)
con.execute("""
COPY (SELECT * FROM read_parquet('../output/fact_trips.parquet'))
TO '../data/yellow_tripdata.csv'
(WITH HEADER, FORMAT CSV);
""")

# 2) CSV benchmark
con.execute("""
CREATE OR REPLACE TABLE trips_csv AS
SELECT * FROM read_csv_auto('../data/yellow_tripdata.csv');
""")

start = time.time()
con.execute("SELECT AVG(fare_amount) FROM trips_csv;").fetchall()
csv_time = time.time() - start

# 3) Parquet benchmark
con.execute("""
CREATE OR REPLACE TABLE trips_parquet AS
SELECT * FROM read_parquet('../output/fact_trips.parquet');
""")

start = time.time()
con.execute("SELECT AVG(fare_amount) FROM trips_parquet;").fetchall()
parquet_time = time.time() - start

print(f"CSV Query Time: {csv_time:.6f} seconds")
print(f"Parquet Query Time: {parquet_time:.6f} seconds")

con.close()
