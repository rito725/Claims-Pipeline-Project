"""Great Expectations checks for every layer.

Each check also carries `bad_row_sql`, the same rule as a Spark SQL predicate. That one line is
what lets the agent sample offending rows and quarantine exactly what the check rejected.
"""
from __future__ import annotations

from dataclasses import dataclass

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.data_context.types.base import ProgressBarsConfig
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from claims import config
from claims.failures import FailedCheck
from claims.pipeline.bronze import active_rows, batch_rows

ALLOWED_STATUS = ["CLOSED", "BILLED", "OPEN", "PENDING", "REJECTED"]
NULL_TOLERANCE = 0.99      # payer_id baseline is 0% null, so tolerate at most 1%


@dataclass
class Check:
    name: str
    table: str
    expectation: object
    bad_row_sql: str | None = None


def _column(exp) -> str | None:
    return getattr(exp, "column", None)


def validate(df: DataFrame, checks: list[Check]) -> list[FailedCheck]:
    """Run checks against one dataframe with an ephemeral GX context."""
    ctx = gx.get_context(mode="ephemeral")
    ctx.variables.progress_bars = ProgressBarsConfig(globally=False)
    batch = (ctx.data_sources.add_spark("spark").add_dataframe_asset("frame")
             .add_batch_definition_whole_dataframe("whole").get_batch({"dataframe": df}))
    failed: list[FailedCheck] = []
    for check in checks:
        result = batch.validate(check.expectation, result_format="SUMMARY")
        ok = "PASS" if result.success else "FAIL"
        print(f"  [{ok}] {check.table}.{check.name}")
        if not result.success:
            observed = {k: v for k, v in result.result.items()
                        if k in ("observed_value", "element_count", "unexpected_count", "unexpected_percent",
                                 "missing_count", "missing_percent", "partial_unexpected_counts")}
            if result.exception_info and result.exception_info.get("raised_exception"):
                observed["exception"] = str(result.exception_info.get("exception_message"))[:300]
            failed.append(FailedCheck(
                name=check.name, table=check.table, column=_column(check.expectation),
                expectation=type(check.expectation).__name__, observed=observed,
                bad_row_sql=check.bad_row_sql,
            ))
    return failed


def flag_duplicates(spark: SparkSession, df: DataFrame) -> DataFrame:
    """A row is a duplicate if the same claim content is already in Silver, or earlier in this batch.

    Same claim_id with a different hash is a legitimate correction and is not flagged.
    """
    w = Window.partitionBy("claim_id", "_row_hash").orderBy("row_id")
    df = df.withColumn("_rn", F.row_number().over(w))
    path = config.silver("claims")
    if DeltaTable.isDeltaTable(spark, path):
        seen = spark.read.format("delta").load(path).select("claim_id", "_row_hash").withColumn("_seen", F.lit(True))
        df = df.join(seen, ["claim_id", "_row_hash"], "left")
    else:
        df = df.withColumn("_seen", F.lit(False))
    return (df.withColumn("is_duplicate", (F.col("_rn") > 1) | F.coalesce(F.col("_seen"), F.lit(False)))
              .drop("_rn", "_seen"))


def bronze_claims_frame(spark: SparkSession, batch_id: str) -> DataFrame:
    return flag_duplicates(spark, active_rows(spark, "claims", batch_id))


def validate_bronze(spark: SparkSession, batch_id: str, expect_new_data: bool) -> list[FailedCheck]:
    failed: list[FailedCheck] = []
    if expect_new_data:
        # Counted before quarantine: "nothing arrived" and "everything was quarantined" differ.
        failed += validate(batch_rows(spark, "claims", batch_id), [
            Check("new_claims_arrived", "bronze.claims",
                  gxe.ExpectTableRowCountToBeBetween(min_value=1)),
        ])
    failed += validate(bronze_claims_frame(spark, batch_id), [
        Check("claim_id_not_null", "bronze.claims",
              gxe.ExpectColumnValuesToNotBeNull(column="claim_id"), "claim_id IS NULL"),
        Check("patient_id_not_null", "bronze.claims",
              gxe.ExpectColumnValuesToNotBeNull(column="patient_id"), "patient_id IS NULL"),
        Check("payer_id_null_rate", "bronze.claims",
              gxe.ExpectColumnValuesToNotBeNull(column="payer_id", mostly=NULL_TOLERANCE), "payer_id IS NULL"),
        Check("no_duplicate_load", "bronze.claims",
              gxe.ExpectColumnValuesToBeInSet(column="is_duplicate", value_set=[False]), "is_duplicate"),
    ])
    failed += validate(active_rows(spark, "patients", batch_id), [
        Check("patient_id_unique", "bronze.patients", gxe.ExpectColumnValuesToBeUnique(column="patient_id")),
    ])
    return failed


def validate_silver(spark: SparkSession, batch_id: str) -> list[FailedCheck]:
    read = lambda name: spark.read.format("delta").load(config.silver(name))
    failed = validate(read("claims"), [
        Check("claim_id_unique", "silver.claims", gxe.ExpectColumnValuesToBeUnique(column="claim_id")),
        Check("service_date_not_null", "silver.claims", gxe.ExpectColumnValuesToNotBeNull(column="service_date")),
        Check("charge_non_negative", "silver.claims",
              gxe.ExpectColumnValuesToBeBetween(column="charge_amount", min_value=0)),
        Check("payer_paid_non_negative", "silver.claims",
              gxe.ExpectColumnValuesToBeBetween(column="payer_paid_amount", min_value=0)),
        Check("status_valid", "silver.claims",
              gxe.ExpectColumnValuesToBeInSet(column="status", value_set=ALLOWED_STATUS)),
    ])
    failed += validate(read("dim_patient").where("is_current"), [
        Check("one_current_version", "silver.dim_patient", gxe.ExpectColumnValuesToBeUnique(column="patient_id")),
    ])
    return failed


def validate_gold(spark: SparkSession, batch_id: str) -> list[FailedCheck]:
    silver = spark.read.format("delta").load(config.silver("claims"))
    fact = spark.read.format("delta").load(config.gold("fact_claims"))
    agg = spark.read.format("delta").load(config.gold("agg_payer_month"))
    rows = silver.count()
    charged = float(silver.agg(F.sum("charge_amount")).first()[0] or 0)
    failed = validate(fact, [
        Check("claim_id_unique", "gold.fact_claims", gxe.ExpectColumnValuesToBeUnique(column="claim_id")),
        Check("patient_resolved", "gold.fact_claims", gxe.ExpectColumnValuesToNotBeNull(column="patient_sk")),
        Check("rowcount_matches_silver", "gold.fact_claims",
              gxe.ExpectTableRowCountToEqual(value=rows)),
        Check("charges_reconcile_to_silver", "gold.fact_claims",
              gxe.ExpectColumnSumToBeBetween(column="charge_amount", min_value=charged - 0.01, max_value=charged + 0.01)),
    ])
    failed += validate(agg, [
        Check("charges_reconcile_to_silver", "gold.agg_payer_month",
              gxe.ExpectColumnSumToBeBetween(column="total_charged", min_value=charged - 0.01, max_value=charged + 0.01)),
    ])
    return failed
