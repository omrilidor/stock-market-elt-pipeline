"""Tests for load/bronze_load.py against a real local Spark + Delta session.

The session uses its own temp lakehouse (warehouse dir + Derby metastore), so the
tests never touch data/lakehouse/ or its metastore lock. Starting Spark takes
~10-15 s, so the session is shared by all tests in this file (scope="module").
"""

from datetime import date

import pandas as pd
import pytest

from load import bronze_load as bl
from load.spark_session import build_spark

RUN = "2026-11-16"
METADATA_COLUMNS = ["run_date", "_source_file", "_ingested_at"]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    lakehouse = tmp_path_factory.mktemp("lakehouse")
    session = build_spark("pytest-bronze-load", lakehouse_dir=lakehouse)
    # getOrCreate() would silently reuse an already-running session with another
    # lakehouse. Fail loudly instead of writing test tables to the wrong place.
    assert str(lakehouse) in session.conf.get("spark.sql.warehouse.dir")
    session.sparkContext.setLogLevel("ERROR")
    session.sql(f"CREATE DATABASE IF NOT EXISTS {bl.SCHEMA}")
    yield session
    session.stop()


def tiny_df(spark, n_rows: int):
    """A small DataFrame shaped like raw prices: ticker, close."""
    return spark.createDataFrame([(f"T{i}", float(i)) for i in range(n_rows)], ["ticker", "close"])


def rows_per_run(spark, table: str) -> dict[str, int]:
    rows = spark.sql(f"SELECT run_date, count(*) AS n FROM {table} GROUP BY run_date").collect()
    return {str(r.run_date): r.n for r in rows}


# --- add_metadata -----------------------------------------------------------------


def test_add_metadata_adds_the_three_columns(spark):
    df = bl.add_metadata(tiny_df(spark, 2), RUN, "/Volumes/x/prices.parquet")

    assert df.columns == ["ticker", "close", *METADATA_COLUMNS]
    assert dict(df.dtypes)["run_date"] == "date"
    assert dict(df.dtypes)["_ingested_at"] == "timestamp"
    row = df.first()
    assert row.run_date == date(2026, 11, 16)
    assert row._source_file == "/Volumes/x/prices.parquet"
    assert row._ingested_at is not None


# --- write_bronze -----------------------------------------------------------------


def test_write_bronze_twice_keeps_row_count(spark):
    table = f"{bl.SCHEMA}.test_idempotent"
    for _ in range(2):
        bl.write_bronze(bl.add_metadata(tiny_df(spark, 3), RUN, "f"), table, RUN)

    assert spark.table(table).count() == 3
    history = spark.sql(f"DESCRIBE HISTORY {table}").collect()
    assert len(history) == 2  # two Delta versions: create, then replace


def test_write_bronze_replaces_only_its_own_run(spark):
    table = f"{bl.SCHEMA}.test_replace_where"
    bl.write_bronze(bl.add_metadata(tiny_df(spark, 3), "2026-11-16", "f"), table, "2026-11-16")
    bl.write_bronze(bl.add_metadata(tiny_df(spark, 2), "2026-11-17", "f"), table, "2026-11-17")

    # Reload 11-16 with fewer rows: 11-16 is replaced, 11-17 is untouched.
    bl.write_bronze(bl.add_metadata(tiny_df(spark, 1), "2026-11-16", "f"), table, "2026-11-16")

    assert rows_per_run(spark, table) == {"2026-11-16": 1, "2026-11-17": 2}


def test_write_bronze_partitions_by_run_date(spark):
    table = f"{bl.SCHEMA}.test_partitioning"
    bl.write_bronze(bl.add_metadata(tiny_df(spark, 1), RUN, "f"), table, RUN)

    detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
    assert detail.partitionColumns == ["run_date"]


# --- load_file --------------------------------------------------------------------


def test_load_file_names_table_after_file_and_records_volume_path(spark, tmp_path):
    raw = tmp_path / "yfinance" / RUN / "prices.parquet"
    raw.parent.mkdir(parents=True)
    pd.DataFrame({"ticker": ["AAPL", "MSFT"], "close": [1.0, 2.0]}).to_parquet(raw)

    bl.load_file(spark, raw, "yfinance", RUN, tmp_path)

    df = spark.table(f"{bl.SCHEMA}.bronze_yfinance_prices")
    assert df.count() == 2
    assert {r._source_file for r in df.select("_source_file").collect()} == {
        f"/Volumes/workspace/stock_market/raw/yfinance/{RUN}/prices.parquet"
    }
