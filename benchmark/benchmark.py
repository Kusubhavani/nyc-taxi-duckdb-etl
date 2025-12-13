import duckdb
import time

con = duckdb.connect()

# Parquet Benchmark
con.execute("CREATE OR REPLACE TABLE trips_parquet AS SELECT * FROM read_parquet('output/fact_trips.parquet');")

start = time.time()
con.execute("SELECT AVG(fare_amount) FROM trips_parquet;").fetchall()
parquet_time = time.time() - start

print(f"Parquet Query Time: {parquet_time:.6f} seconds")

con.close()
