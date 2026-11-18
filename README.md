# stock-market-elt-pipeline

An end-to-end **ELT pipeline for S&P 500 market data**. Python extracts daily prices, corporate actions, company fundamentals and macro data from public APIs. PySpark loads the raw files into **Delta Lake** bronze tables, and **dbt** transforms them into tested silver and gold models. It's developed locally in a Docker dev container and deployed on **Databricks**.

> 🚧 **Work in progress** (started November 2026). The extract stage for yfinance and SEC EDGAR is done; FRED, the load stage and dbt are next. See [Roadmap](#roadmap).

## Why this project
Market data is a good stress test for a data pipeline:
- **Sources disagree on shape.** Prices are tabular, SEC fundamentals are deeply nested JSON with dynamic keys, and macro series come at different frequencies.
- **History gets restated.** Yahoo rewrites old prices after splits and dividends.
- **Correctness is checkable.** `high >= low`, `price > 0`, and one row per ticker per day.

The aim is to handle these the way a production data team would: idempotent loads, raw data kept unchanged, all business logic in tested dbt models, and the same code running locally and in the cloud.

## Architecture
```
EXTRACT (Python)            LOAD (PySpark)                TRANSFORM (dbt)                  SERVE
APIs → raw files       →    raw files → bronze Delta  →   bronze → silver → gold      →    dashboard
data/raw/<source>/          + _ingested_at,               staging / intermediate /
<run_date>/                   _source_file                marts, snapshots, tests
        │
        └── uploaded to a Databricks Unity Catalog Volume (raw data does not stay on the dev machine)
```

| Source | Data | Raw format |
|---|---|---|
| [yfinance](https://github.com/ranaroussi/yfinance) | Daily OHLCV prices, splits, dividends for ~500 S&P 500 tickers | Parquet, one file per dataset per run |
| [SEC EDGAR](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) | Company info (`submissions`) and all XBRL fundamentals (`companyfacts`) | Gzipped JSON Lines, API responses unchanged |
| [FRED](https://fred.stlouisfed.org/docs/api/fred/) | Macro series (rates, inflation, unemployment, volatility) | *planned* |

## Design decisions
Each decision, with its reason, is recorded in the daily notes in [`docs/plans/`](docs/plans/). The main ones so far:

- **ELT, not ETL.** Extract scripts don't transform anything: prices stay unadjusted (`auto_adjust=False`), and the SEC responses are stored exactly as returned. Split and dividend adjustment, type casting and tag selection all happen in dbt, where they're versioned and tested.
- **Idempotent runs.** Each run writes to `data/raw/<source>/<run_date>/` through a `.tmp` folder that is renamed only when complete. Re-running a day replaces that day's files instead of duplicating them, and a crashed run never leaves partial data visible.
- **Streaming for large sources.** SEC companyfacts is ~2 GB of JSON for 500 companies. The extract writes each response to a gzip stream as it arrives, so peak memory stays under 200 MB and the files are 15× smaller.
- **Polite API clients.** Self-throttling below the SEC rate limit, retries with exponential backoff for temporary errors (429/5xx), no retry for permanent ones (403), and timeouts on every request.
- **Few, larger files.** One file per dataset per run rather than one per ticker (3 files instead of ~1,500), which suits Spark better.
- **Secrets stay out of git.** API keys and the SEC contact User-Agent come from a git-ignored `.env`.

## Stack
Python 3.11 · pandas · PySpark 3.5 · Delta Lake 3.3 · dbt (dbt-spark locally, dbt-databricks in the cloud) · Databricks Free Edition · Docker Dev Containers · pytest · ruff

## Repository layout
```
extract/          Python API pulls → raw files (yfinance_extract.py, sec_edgar_extract.py)
load/             PySpark: raw files → bronze Delta tables
dbt/              dbt project: staging / intermediate / marts, snapshots, tests
scripts/          one-off helpers (Spark + Delta smoke test)
tests/            pytest unit tests (no network calls)
notebooks/        exploration notebooks (outputs stripped by nbstripout)
data/reference/   small reference data (S&P 500 ticker list)
docs/plans/       daily plans, decisions and results
```

## Run it locally
You need Docker and VS Code with the Dev Containers extension.

1. Open the folder in VS Code → **Dev Containers: Reopen in Container**.
2. Check Spark and Delta: `python scripts/smoke_test_spark.py` should print `OK`.
3. Create your `.env` from the template (`.env` is git-ignored) and fill in the values:
   ```bash
   cp .env.example .env
   ```
   | Variable | Used by | How to get it |
   |---|---|---|
   | `SEC_USER_AGENT` | SEC EDGAR extract | Your project name and contact email, e.g. `"stock-market-elt-pipeline you@example.com"`. SEC requires it ([policy](https://www.sec.gov/os/accessing-edgar-data)). |
   | `FRED_API_KEY` | FRED extract | Free. Sign in at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) → **Request API Key**. It is 32 characters. |
4. Run the extracts. A few tickers or companies first, then everything:
   ```bash
   python extract/yfinance_extract.py --tickers AAPL MSFT BRK-B --start 2026-11-01
   python extract/sec_edgar_extract.py --ciks 0000320193 0000789019
   python extract/sec_edgar_extract.py          # all S&P 500 companies, ~10 min
   python extract/fred_extract.py               # 5 macro series, a few seconds
   ```
5. Run the tests: `pytest`

## Roadmap
- [x] Dev container, Spark + Delta smoke test
- [x] yfinance extract (prices, splits, dividends) to raw Parquet
- [x] SEC EDGAR extract (company info, fundamentals) to raw JSON Lines
- [ ] FRED extract and upload of raw runs to a Databricks Volume
- [ ] PySpark load to bronze Delta tables (idempotent, with metadata columns)
- [ ] dbt: staging, intermediate, marts, `dim_company` snapshot (SCD2), data tests
- [ ] Databricks: dbt-databricks target and a scheduled Databricks Job
- [ ] Dashboard, GitHub Actions CI, daily scheduled extract

## How it's developed
Each topic is built on its own branch (`feature/<topic>`) and merged through a pull request. The PR description documents what changed, why, the decisions made, and the results.
