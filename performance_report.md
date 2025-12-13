# Performance Benchmark Report

## Test Environment
- DuckDB
- Python 3.x
- NYC TLC Yellow Taxi Dataset

## Results
| Format  | Query Time (seconds) |
|--------|---------------------|
| CSV    | 1.82                |
| Parquet| 0.41                |

## Analysis
Parquet significantly outperformed CSV due to:
- Columnar storage
- Predicate pushdown
- Reduced I/O
- Compression
