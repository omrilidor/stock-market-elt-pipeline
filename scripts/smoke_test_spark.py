"""Check that Spark and Delta Lake work in this environment.

Writes a small Delta table, reads it back, and prints the result.
Run with: python scripts/smoke_test_spark.py
"""

import shutil
import tempfile

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("smoke-test")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    # Adds the Delta Lake jars that match the installed delta-spark version.
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    path = tempfile.mkdtemp(prefix="delta-smoke-")
    try:
        spark.range(10).write.format("delta").mode("overwrite").save(path)
        count = spark.read.format("delta").load(path).count()
        assert count == 10, f"expected 10 rows, got {count}"
        print(f"OK: Spark {spark.version} wrote and read a Delta table ({count} rows).")
    finally:
        spark.stop()
        shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    main()
