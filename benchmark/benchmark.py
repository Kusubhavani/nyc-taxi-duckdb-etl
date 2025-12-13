import duckdb
import time

con = duckdb.connect()

# CSV Benchmark
con.execute("CREATE TABLE trips_csv AS SELECT * FROM read_csv_auto('data/yellow_tripdata.csv');")

start = time.time()
con.execute("SELECT AVG(fare_amount) FROM trips_csv;").fetchall()
csv_time = time.time() - start

# Parquet Benchmark
con.execute("CREATE TABLE trips_parquet AS SELECT * FROM read_parquet('output/fact_trips.parquet');")

start = time.time()
con.execute("SELECT AVG(fare_amount) FROM trips_parquet;").fetchall()
parquet_time = time.time() - start

print(f"CSV Query Time: {csv_time}")
print(f"Parquet Query Time: {parquet_time}")
