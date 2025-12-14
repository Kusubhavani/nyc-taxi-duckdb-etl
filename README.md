**Build a Local Data Warehouse with DuckDB and Parquet**

Project:Local Data Warehouse

Author:Kusu Bhavani

Repository:nyc-taxi-duckdb-etl

NYC Taxi DuckDB ETL Pipeline
Project Overview
This project implements a complete ETL pipeline using DuckDB to process NYC Yellow Taxi trip data for January 2023. The pipeline transforms raw Parquet data into a star schema data warehouse consisting of:

1.fact_trips: Main fact table with trip metrics (fare, distance, passenger count, timestamps, location IDs)

2.dim_date: Date dimension (year, month, day, weekday)

3.dim_location: Location dimension (unique pickup/dropoff location IDs)

The pipeline also includes performance benchmarking comparing query execution times between CSV and optimized Parquet formats, demonstrating DuckDB's columnar storage advantages.

 Star Schema Data Model:
                 fact_trips (61MB)
                    |
        +-----------+-----------+
        |                       |
   dim_date (1.3KB)       dim_location (1.7KB)

Prerequisites:
1.Python 3.8+
2.DuckDB (installed via requirements.txt)

**Quick Start**
1. Clone & Setup
bash
git clone https://github.com/Kusubhavani/nyc-taxi-duckdb-etl.git
cd nyc-taxi-duckdb-etl
2. Install Dependencies
bash
pip install -r requirements.txt
3. Run ETL Pipeline
bash
python etl/etl_pipeline.py
Output: Creates output/ directory with 3 Parquet files:

fact_trips.parquet (61MB)

dim_date.parquet (1.3KB)

dim_location.parquet (1.7KB)

4. Run Performance Benchmark
bash
cd benchmark
python benchmark.py
Expected Results:

text
CSV Query Time: 0.330877 seconds
Parquet Query Time: 0.032477 seconds

Project Structure:

nyc-taxi-duckdb-etl/
├── data/                    # Input: yellow_tripdata_2023-01.parquet
├── etl/                     # ETL pipeline
│   └── etl_pipeline.py
├── benchmark/               # Performance tests
│   └── benchmark.py
├── queries/                 # Analytical SQL
│   └── queries.sql
├── output/                  # Generated Parquet files
│   ├── fact_trips.parquet
│   ├── dim_date.parquet
│   └── dim_location.parquet
├── README.md
├── requirements.txt
└── performance_report.md
