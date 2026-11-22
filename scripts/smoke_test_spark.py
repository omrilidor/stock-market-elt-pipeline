"""Check that Spark and Delta Lake work in this environment.

Writes a small Delta table, reads it back, and prints the result.
Run with: python scripts/smoke_test_spark.py
"""

import shutil
import tempfile

from load.spark_session import build_spark


def main() -> None:
    spark = build_spark("smoke-test")
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
