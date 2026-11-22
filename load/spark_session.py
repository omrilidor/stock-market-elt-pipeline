"""Build the shared Spark session with Delta Lake enabled.

Used by the smoke test, the bronze load and the tests, so the Spark and Delta
config lives in one place.
"""

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

LAKEHOUSE_DIR = Path(__file__).resolve().parents[1]/"data"/"lakehouse"


def build_spark(app_name: str = "stock-market-elt") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(LAKEHOUSE_DIR))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL", 
            f"jdbc:derby:;databaseName={LAKEHOUSE_DIR}/metastore_db;create=true")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={LAKEHOUSE_DIR}")
        .enableHiveSupport()
    )
    # Adds the Delta Lake jars that match the installed delta-spark version.
    return configure_spark_with_delta_pip(builder).getOrCreate()
