# Performance Benchmark Report

## Test Environment
- DuckDB
- Python 3.x
- NYC TLC Yellow Taxi Dataset

## Results
| Format  | Query Time (seconds) |
|--------|-----------------------|
| CSV    | 0.562022              |
| Parquet| 0.063596              |

## Analysis
Parquet significantly outperformed CSV due to:
- Columnar storage
- Predicate pushdown
- Reduced I/O
- Compression
