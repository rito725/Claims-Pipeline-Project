"""The three state-changing actions. Only the graph's `act` node reaches these, after approval.

Targets come from the failure artifact, never from model output, so the model can choose an
action but cannot point it at a different batch or an arbitrary predicate.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from claims import config, db

ACTIONS = ("quarantine_bad_rows", "rerun_partition", "open_incident")


def _pipeline(*args: str) -> tuple[bool, str]:
    proc = subprocess.run([sys.executable, "-m", "claims.pipeline.run", *args],
                          capture_output=True, text=True, env=os.environ.copy())
    tail = "\n".join((proc.stdout + proc.stderr).strip().splitlines()[-12:])
    return proc.returncode == 0, tail


def quarantine_bad_rows(failure: dict, box) -> dict:
    from claims.quality import suites

    checks = [c for c in failure["checks"] if c.get("bad_row_sql") and c["table"] == "bronze.claims"]
    if not checks:
        return {"status": "refused", "detail": "no row-level bronze check to quarantine against"}
    spark = box.spark
    frame = suites.bronze_claims_frame(spark, failure["batch_id"])
    held = 0
    path = config.quarantine("claims")
    for check in checks:
        rows = (frame.where(check["bad_row_sql"])
                .withColumn("_quarantine_reason", F.lit(check["name"]))
                .withColumn("_failure_id", F.lit(failure["failure_id"]))
                .withColumn("_quarantined_at", F.current_timestamp()))
        if DeltaTable.isDeltaTable(spark, path):
            rows = rows.join(spark.read.format("delta").load(path).select("row_id"), "row_id", "left_anti")
        rows = rows.cache()
        held += rows.count()
        rows.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
        rows.unpersist()
    box.close()  # release the driver's memory before the pipeline starts its own JVM
    ok, tail = _pipeline("--from", "validate_bronze")
    return {"status": "succeeded" if ok else "failed", "rows_quarantined": held, "quarantine_table": path,
            "detail": f"quarantined {held} rows; pipeline resumed from validate_bronze and "
                      f"{'committed the batch' if ok else 'failed again'}", "pipeline_tail": tail}


def rerun_partition(failure: dict, box) -> dict:
    box.close()
    args = ["--expect-new-data"] if any(c["name"] == "new_claims_arrived" for c in failure["checks"]) else []
    ok, tail = _pipeline(*args)
    return {"status": "succeeded" if ok else "failed", "pipeline_tail": tail,
            "detail": f"batch {failure['batch_id']} re-extracted from the watermark: "
                      f"{'committed' if ok else 'failed again, new failure artifact written'}"}


def open_incident(failure: dict, box, diagnosis: dict | None = None) -> dict:
    box.close()
    config.INCIDENTS.mkdir(parents=True, exist_ok=True)
    number = len(list(config.INCIDENTS.glob("INC-*.json"))) + 1
    incident_id = f"INC-{number:04d}"
    (config.INCIDENTS / f"{incident_id}.json").write_text(json.dumps({
        "incident_id": incident_id, "failure_id": failure["failure_id"], "status": "open",
        "opened_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "diagnosis": diagnosis,
    }, indent=2, default=str))
    return {"status": "succeeded", "incident_id": incident_id,
            "detail": f"{incident_id} opened; batch {failure['batch_id']} stays open and the watermark has not moved"}


def batch_is_open(batch_id: str) -> bool:
    with db.connect() as conn:
        row = conn.execute("select status from etl.batch where batch_id = %s", (batch_id,)).fetchone()
    return bool(row) and row[0] == "open"


def execute(action: str, failure: dict, box, diagnosis: dict) -> dict:
    # An approval can sit for hours. If the batch moved on meanwhile, the diagnosis is stale.
    if not batch_is_open(failure["batch_id"]):
        box.close()
        return {"status": "refused", "detail": f"batch {failure['batch_id']} is no longer open; nothing was changed"}
    if action == "quarantine_bad_rows":
        return quarantine_bad_rows(failure, box)
    if action == "rerun_partition":
        return rerun_partition(failure, box)
    if action == "open_incident":
        return open_incident(failure, box, diagnosis)
    raise ValueError(f"unknown action {action!r}")
