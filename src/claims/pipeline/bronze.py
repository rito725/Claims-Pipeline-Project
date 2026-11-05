"""Incremental extract from Postgres into Bronze Delta, partitioned by batch.

Bronze keeps rows exactly as extracted. Replacing by batch partition is what makes a rerun safe.
The write does not enable schema evolution, so a renamed source column fails hard here instead
of quietly creating a new column and leaving the old one null.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from claims import config, db
from claims.pipeline import control

TECHNICAL = {"row_id", "updated_at", "source_file"}


def row_hash(df: DataFrame) -> F.Column:
    """Hash business columns only: a reloaded row gets new technical values but the same hash."""
    cols = sorted(c for c in df.columns if c not in TECHNICAL)
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~null~")) for c in cols]), 256)


def extract_table(spark: SparkSession, table: str, low, high) -> DataFrame:
    query = f"(select * from {table} where updated_at > '{low.isoformat()}' and updated_at <= '{high.isoformat()}') src"
    return (
        spark.read.format("jdbc")
        .option("url", db.jdbc_url()).option("dbtable", query)
        .option("user", config.PG_USER).option("driver", "org.postgresql.Driver")
        .option("fetchsize", 10000)
        .load()
    )


def run(spark: SparkSession, batch_id: str) -> dict[str, int]:
    highs, counts = {}, {}
    for table in config.SOURCE_TABLES:
        low = control.watermark(table)
        high = control.source_high(table) or low
        df = extract_table(spark, table, low, high)
        df = (df.withColumn("_row_hash", row_hash(df))
                .withColumn("_batch_id", F.lit(batch_id))
                .withColumn("_extracted_at", F.current_timestamp()))
        path = config.bronze(table)
        writer = df.write.format("delta").partitionBy("_batch_id")
        if DeltaTable.isDeltaTable(spark, path):
            writer.mode("overwrite").option("replaceWhere", f"_batch_id = '{batch_id}'").save(path)
        else:
            writer.mode("overwrite").save(path)
        counts[table] = spark.read.format("delta").load(path).where(F.col("_batch_id") == batch_id).count()
        highs[table] = high
        print(f"  bronze.{table}: {counts[table]} rows ({low.isoformat()} .. {high.isoformat()}]")
    control.record_extract(batch_id, highs, counts)
    return counts


def batch_rows(spark: SparkSession, table: str, batch_id: str) -> DataFrame:
    return spark.read.format("delta").load(config.bronze(table)).where(F.col("_batch_id") == batch_id)


def active_rows(spark: SparkSession, table: str, batch_id: str) -> DataFrame:
    """Bronze rows for the batch minus anything triage has quarantined."""
    rows = batch_rows(spark, table, batch_id)
    q = config.quarantine(table)
    if table == "claims" and DeltaTable.isDeltaTable(spark, q):
        held = spark.read.format("delta").load(q).select("row_id").distinct()
        rows = rows.join(held, "row_id", "left_anti")
    return rows
