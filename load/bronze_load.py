"""Load one raw extract run into bronze Delta tables (the L in ELT).

Volume run folder -> temp folder (download_run) -> one bronze table per file:
    <source>/<run_date>/prices.parquet  ->  stock_market.bronze_<source>_prices

Each row gets run_date, _source_file and _ingested_at. No business logic here:
cleaning, deduplication and split/dividend adjustment happen in dbt.
Re-loading a run replaces only that run's partition (replaceWhere), so it never
creates duplicates.

Usage: python load/bronze_load.py --source yfinance --run-date 2026-11-16
"""

import argparse
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from extract.download_raw import download_run
from extract.upload_raw import VOLUME_ROOT, volume_path
from load.spark_session import build_spark


SCHEMA = "stock_market"
SOURCES = ["yfinance"]


def add_metadata(df: DataFrame, run_date: str, source_file: str) -> DataFrame:
    """Add the bronze metadata columns to a DataFrame read from one raw file.

    run_date:      the run folder's date, as a date. Partition column and replaceWhere key.
    _source_file:  source_file, the file's Volume path (lineage). Passed in rather than
                   taken from _metadata.file_path, which locally would be the deleted
                   temp folder. One DataFrame = one file, so a constant is exact.
    _ingested_at:  when the row was loaded (audit).
    """
    return (
        df.withColumn("run_date", F.to_date(F.lit(run_date)))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_bronze(df: DataFrame, table: str, run_date: str) -> None:
    """Write one run's rows to a bronze Delta table, replacing only that run.

    Idempotent: loading the same run again replaces its run_date partition in one
    atomic Delta commit, so row counts never double and other runs are untouched.
    The first write creates the table (managed, registered in the metastore).
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"run_date = '{run_date}'")
        .partitionBy("run_date")
        .saveAsTable(table)
    )


def parse_args() -> argparse.Namespace:
    """Read --source and --run-date from the command line (both required)."""
    parser = argparse.ArgumentParser(
        description="Load one raw extract run into bronze Delta tables."
    )
    parser.add_argument("--source", required=True, choices=SOURCES)
    parser.add_argument(
        "--run-date", required=True, help="folder name, YYYY-MM-DD"
    )

    return parser.parse_args()


def load_file(
        spark: SparkSession,
        path: Path,
        source: str,
        run_date: str,
        tmp_root: Path
) -> None:
    """Load one downloaded raw file into its bronze table.

    The table name comes from the file name: prices.parquet -> stock_market.bronze_<source>_prices.
    tmp_root is the temp folder the run was downloaded into; it is used to turn the local
    path back into the file's Volume path for _source_file.
    """
    table = f"{SCHEMA}.bronze_{source}_{path.stem}"
    df = spark.read.parquet(str(path))
    df = add_metadata(df, run_date, volume_path(path, tmp_root, VOLUME_ROOT))
    write_bronze(df, table, run_date)
    print(f"{path.name} -> {table}")


def main() -> None:
    """Download the run into a temp folder, then load every file into its bronze table.

    Download happens before Spark starts, so a missing run fails in a second instead of
    after Spark's startup. Loading stays inside the with-block because Spark reads the
    files lazily, during the write. spark.stop() in finally releases the Derby metastore
    lock even if a load fails.
    """
    args = parse_args()
    load_dotenv()
    client = WorkspaceClient()
    with tempfile.TemporaryDirectory() as tmp:
        tmp_root = Path(tmp)
        paths = download_run(client, args.source, args.run_date, tmp_root)
        spark = build_spark(f"bronze-load-{args.source}")
        try:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")
            for path in paths:
                load_file(spark, path, args.source, args.run_date, tmp_root)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()