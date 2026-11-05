"""Gold star schema. Rebuilt from Silver each batch, so it is always reproducible."""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from claims import config


def run(spark: SparkSession, batch_id: str) -> None:
    read = lambda name: spark.read.format("delta").load(config.silver(name))
    claims, patients = read("claims"), read("dim_patient")
    payers, encounters = read("payers"), read("encounters")

    # As-of join: each claim points at the patient version in force on its service date.
    p = patients.select("patient_id", "patient_sk", "birth_date", "state", "valid_from", "valid_to")
    fact = (claims.alias("c")
            .join(p.alias("p"),
                  (F.col("c.patient_id") == F.col("p.patient_id"))
                  & (F.col("c.service_date") >= F.to_date("p.valid_from"))
                  & (F.col("c.service_date") < F.to_date("p.valid_to")),
                  "left")
            .join(encounters.select("encounter_id", "encounter_class").alias("e"), "encounter_id", "left")
            .select(
                "c.claim_id", "p.patient_sk", "c.patient_id", "c.payer_id", "c.encounter_id",
                "e.encounter_class", "c.service_date",
                F.date_trunc("month", "c.service_date").cast("date").alias("service_month"),
                "c.claim_type", "c.status", "c.diagnosis_code",
                "c.charge_amount", "c.payer_paid_amount", "c.patient_paid_amount", "c.outstanding_amount",
                F.floor(F.months_between("c.service_date", "p.birth_date") / 12).cast("int").alias("patient_age_at_service"),
                F.lit(batch_id).alias("_gold_batch_id"),
            ))
    fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(config.gold("fact_claims"))

    fact = spark.read.format("delta").load(config.gold("fact_claims"))
    (fact.join(payers.select("payer_id", F.col("name").alias("payer_name")), "payer_id", "left")
     .groupBy("service_month", "payer_id", "payer_name")
     .agg(F.count("*").alias("claim_count"), F.sum("charge_amount").alias("total_charged"),
          F.sum("payer_paid_amount").alias("total_payer_paid"), F.sum("patient_paid_amount").alias("total_patient_paid"),
          F.sum("outstanding_amount").alias("total_outstanding"))
     .withColumn("payer_paid_ratio", F.round(F.col("total_payer_paid") / F.col("total_charged"), 4))
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(config.gold("agg_payer_month")))

    print(f"  gold.fact_claims: {fact.count()} rows")
