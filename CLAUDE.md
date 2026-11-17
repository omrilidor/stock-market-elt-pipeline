# Stock Market ELT Pipeline — project guide for Claude

## What this project is
A portfolio data engineering project (for GitHub and my resume). It is an **ELT** pipeline for stock market data, built with **PySpark** and **dbt**. It runs locally first and is then ported to **Databricks Free Edition**.

I work on it Sunday to Thursday, about **2 hours a day**. **I write the code myself; you (Claude) guide me**: break the work into small steps, explain what each piece should do and why, give hints rather than full solutions, then review and run what I wrote. I make the decisions. Housekeeping (plan files, verified dependency pins) you may do directly.

**Today's plan and progress are in `docs/plans/`.** Read the newest file there at the start of each session.

## Architecture
```
EXTRACT (Python)        LOAD (PySpark)                 TRANSFORM (dbt)                 SERVE
APIs → raw files   →    raw files → bronze Delta   →   bronze → silver → gold     →    dashboard
data/raw/<source>/      schema + metadata columns      staging / intermediate /
<date>/                 only, no business logic        marts, snapshots, tests
```
- **Sources:** yfinance (daily prices, splits, dividends for the S&P 500), SEC EDGAR (company info, fundamentals), FRED (macro data).
- **Load:** append to bronze Delta tables and add `_ingested_at` and `_source_file`. Re-running a day must not create duplicates.
- **Transform:** dbt only. Adjust prices for splits and dividends in dbt, not during extract (keep it ELT). Use incremental `merge` models, a `dim_company` snapshot, and tests (`unique`, `not_null`, `high >= low`, `price > 0`).
- **Two dbt targets:** `local` (dbt-spark, session method) and `databricks` (dbt-databricks). Write Spark SQL that runs on both.

## Folder layout
```
extract/          Python API pulls → raw files
load/             PySpark: raw files → bronze Delta tables
dbt/              dbt project (created in week 2)
scripts/          one-off helpers (smoke_test_spark.py)
tests/            pytest tests
data/raw/         git-ignored
data/lakehouse/   git-ignored (Delta tables)
data/reference/   small reference files (e.g. sp500_tickers.csv)
docs/plans/       daily plans and progress
.github/          PR template
```

## Environment
- Windows host. Everything runs in a **VS Code Dev Container** (Docker). Nothing project-related is installed on Windows itself.
  - Definition: `.devcontainer/devcontainer.json` + `.devcontainer/Dockerfile`.
  - Base: Python 3.11 on Debian 12, Java 17 (`JAVA_HOME=/usr/lib/jvm/default-java`).
  - Python packages are installed into the image from `requirements.txt`. **After changing `requirements.txt`, rebuild the container** (Ctrl+Shift+P → "Dev Containers: Rebuild Container"). No venv is needed inside the container.
- Pinned versions: `pyspark==3.5.*`, `delta-spark==3.3.*`, `dbt-spark[session]`.
- Spark/Delta check: `python scripts/smoke_test_spark.py` should print `OK`.
- Spark UI is forwarded on port 4040 while a Spark session is running.
- `data/raw/`, `data/lakehouse/`, `dbt/target/` and `*.env` are git-ignored. Never commit secrets or API keys.

## Known gotchas
- yfinance adjusts prices by default (`auto_adjust=True`). Always pass `auto_adjust=False` so we keep the raw prices.
- Wikipedia returns 403 to Python's default User-Agent. Fetch the page with `requests` and a custom `User-Agent` header, then `pd.read_html(StringIO(html))`.
- Databricks Free Edition only allows outbound internet to approved domains. Extract runs locally or in GitHub Actions and uploads raw files to a Unity Catalog Volume.
- Databricks Free Edition: serverless compute only, one 2X-Small SQL warehouse, up to 5 concurrent job tasks.

## How to work with me
- **Explain as you go.** I must be able to explain every line in an interview. After each change, give a short summary of what you did and why, and point out any data engineering concept involved (idempotency, partitioning, incremental models, SCD2, and so on).
- **Ask before design decisions** (table layout, partitioning, naming, new tools or dependencies). Implement routine code without asking.
- **Small steps.** One working piece at a time. Run it and show me the result before moving on.
- **Commits:** small, with clear messages. Only commit or push when I say so.
- **Branches and PRs (from Nov 18):** never work directly on `main`. Each topic gets its own branch named `feature/<topic>` (e.g. `feature/fred-extract`, `feature/upload-to-volume`), created from an up-to-date `main` at the start of the work. When the topic is finished, push the branch and open a PR with `gh pr create`. The description follows `.github/pull_request_template.md` (what, why, decisions, results, how to test, concepts) and links the day's plan file. Merge only when I say so, then switch back to `main` and pull.
- **Before giving me any link, install command or version pin, check that it works.**
- **End of each day:** when I say I'm done, update today's file in `docs/plans/` with what got finished, then write the next work day's plan (2 hours, time-blocked, with a "done when" checklist) as a new file there. Fridays and Saturdays are days off.

## Timeline (Sun–Thu, 2 hours a day)
| Week | Dates | Focus |
|---|---|---|
| 1 | Nov 15–19 | Setup, repo, Databricks account, extract scripts |
| 2 | Nov 22–26 | PySpark load to bronze, start dbt (sources, staging) |
| 3 | Nov 29–Dec 3 | dbt: intermediate models, marts, snapshots, tests, docs |
| 4 | Dec 6–10 | Port to Databricks: Volume, load, dbt-databricks, Databricks Job |
| 5 | Dec 13–17 | Dashboard, GitHub Actions CI, daily scheduled extract, README |
| 6 | Dec 20–24 | Buffer, stretch goals, update resume and LinkedIn |
