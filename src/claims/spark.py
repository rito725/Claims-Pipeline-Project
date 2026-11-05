"""SparkSession factory: Delta enabled, sized for an 8 GB laptop."""
from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

POSTGRES_JDBC = "org.postgresql:postgresql:42.7.4"


def get_spark(app: str = "claims") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.snapshotPartitions", "4")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=[POSTGRES_JDBC]).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
