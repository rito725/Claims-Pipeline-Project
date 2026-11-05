"""Silver: typed, deduplicated, idempotent. Claims and encounters upsert; patients are SCD2."""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from claims import config
from claims.pipeline.bronze import active_rows

OPEN_END = "timestamp'9999-12-31 00:00:00'"
BEGINNING = "timestamp'1900-01-01 00:00:00'"
SCD2_TRACKED = ["last_name", "address", "city", "state", "zip", "income"]


def latest_per_key(df: DataFrame, key: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col("updated_at").desc(), F.col("_row_hash"))
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def upsert(spark: SparkSession, df: DataFrame, path: str, key: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        return
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(df.alias("s"), f"t.{key} = s.{key}")
     .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
     .whenNotMatchedInsertAll()
     .execute())


def build_claims(spark: SparkSession, batch_id: str) -> None:
    df = active_rows(spark, "claims", batch_id)
    df = (latest_per_key(df, "claim_id")
          .withColumn("service_date", F.to_date("service_date"))
          .withColumn("status", F.upper(F.trim("status")))
          .drop("_extracted_at"))
    upsert(spark, df, config.silver("claims"), "claim_id")


def build_lookup(spark: SparkSession, batch_id: str, table: str, key: str) -> None:
    df = latest_per_key(active_rows(spark, table, batch_id), key).drop("_extracted_at")
    upsert(spark, df, config.silver(table), key)


def build_dim_patient(spark: SparkSession, batch_id: str) -> None:
    """SCD Type 2. A new version opens at the source update time; replaying a batch changes nothing."""
    path = config.silver("dim_patient")
    incoming = (latest_per_key(active_rows(spark, "patients", batch_id), "patient_id")
                .withColumn("attr_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in SCD2_TRACKED]), 256))
                .drop("_row_hash", "_batch_id", "_extracted_at"))

    def with_surrogate(df: DataFrame) -> DataFrame:
        return df.withColumn("patient_sk", F.sha2(F.concat_ws("|", "patient_id", F.col("valid_from").cast("string")), 256))

    if not DeltaTable.isDeltaTable(spark, path):
        first = (incoming.withColumn("valid_from", F.expr(BEGINNING)).withColumn("valid_to", F.expr(OPEN_END))
                 .withColumn("is_current", F.lit(True)))
        with_surrogate(first).write.format("delta").mode("overwrite").save(path)
        return

    table = DeltaTable.forPath(spark, path)
    current = table.toDF().where("is_current").select("patient_id", F.col("attr_hash").alias("cur_hash"))
    joined = incoming.join(current, "patient_id", "left")
    new_patients = joined.where("cur_hash is null").drop("cur_hash")
    changed = joined.where("cur_hash is not null and cur_hash <> attr_hash").drop("cur_hash")

    inserts = (changed.withColumn("valid_from", F.col("updated_at"))
               .unionByName(new_patients.withColumn("valid_from", F.expr(BEGINNING)))
               .withColumn("valid_to", F.expr(OPEN_END)).withColumn("is_current", F.lit(True)))
    inserts = with_surrogate(inserts).withColumn("_merge_key", F.lit(None).cast("string"))
    closes = (changed.withColumn("valid_from", F.col("updated_at")).withColumn("valid_to", F.col("updated_at"))
              .withColumn("is_current", F.lit(False)).withColumn("patient_sk", F.lit(None).cast("string"))
              .withColumn("_merge_key", F.col("patient_id")))

    (table.alias("t")
     .merge(inserts.unionByName(closes).alias("s"), "t.patient_id = s._merge_key and t.is_current")
     .whenMatchedUpdate(set={"valid_to": "s.valid_to", "is_current": "false"})
     .whenNotMatchedInsert(values={c: f"s.{c}" for c in table.toDF().columns})
     .execute())


def run(spark: SparkSession, batch_id: str) -> None:
    build_lookup(spark, batch_id, "payers", "payer_id")
    build_lookup(spark, batch_id, "encounters", "encounter_id")
    build_dim_patient(spark, batch_id)
    build_claims(spark, batch_id)
    print("  silver updated")
