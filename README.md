# stock-market-elt-pipeline

An ELT pipeline for S&P 500 market data: Python extract → PySpark / Delta Lake → dbt transformations, running locally and on Databricks.

> Work in progress. Started November 2026.

## Planned architecture
```
EXTRACT (Python)    →   LOAD (PySpark)          →   TRANSFORM (dbt)            →   SERVE
yfinance, SEC EDGAR,    raw files → bronze          bronze → silver → gold         dashboard
FRED → raw files        Delta tables                (tested, incremental models)
```

## Stack
Python · PySpark 3.5 · Delta Lake · dbt (dbt-spark, dbt-databricks) · Databricks · Docker Dev Containers

## Run it locally
Requires Docker and VS Code with the Dev Containers extension.

1. Open this folder in VS Code → "Dev Containers: Reopen in Container".
2. Check the environment: `python scripts/smoke_test_spark.py`
