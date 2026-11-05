"""Assert the lake state after each demo step, so the demo fails loudly if the outcome is wrong.

  python demo/verify.py <step>
"""
from __future__ import annotations

import json
import sys

from pyspark.sql import functions as F

from claims import config, db
from claims.pipeline.silver import build_dim_patient
from claims.spark import get_spark


def source_truth() -> dict:
    """Distinct claims by claim_id from the source, the value Gold must equal."""
    with db.connect() as conn:
        total = conn.execute("select count(distinct claim_id) from claims").fetchone()[0]
        charged = conn.execute(
            "select sum(charge_amount) from (select distinct on (claim_id) claim_id, charge_amount "
            "from claims order by claim_id, row_id) d").fetchone()[0]
    return {"claims": total, "charged": float(charged)}


def main(step: str) -> None:
    s = get_spark("verify")
    read = lambda p: s.read.format("delta").load(p)
    fact = read(config.gold("fact_claims"))
    rows = fact.count()
    distinct = fact.select("claim_id").distinct().count()
    charged = float(fact.agg(F.sum("charge_amount")).first()[0])
    q_path = config.quarantine("claims")
    q = read(q_path) if (config.LAKE / "quarantine" / "claims").exists() else None
    quarantine = {r["_quarantine_reason"]: r["count"] for r in q.groupBy("_quarantine_reason").count().collect()} if q else {}
    dim = read(config.silver("dim_patient"))
    versions = dim.where("not is_current").count()
    state = {"gold_rows": rows, "gold_distinct_claims": distinct, "gold_charged": round(charged, 2),
             "quarantine": quarantine, "closed_patient_versions": versions,
             "patient_versions": dim.count(), "current_patients": dim.where("is_current").count()}
    print(json.dumps(state, indent=1))

    assert rows == distinct, "gold has duplicate claims"
    with db.connect() as conn:
        open_batches = conn.execute("select count(*) from etl.batch where status = 'open'").fetchone()[0]
    print(f"open batches: {open_batches}")

    truth = source_truth()
    if step == "clean":
        assert open_batches == 0 and rows == truth["claims"] and abs(charged - truth["charged"]) < 0.01
    elif step == "scd2":
        assert versions == 25, f"expected 25 closed versions, got {versions}"
        # Replaying the same bronze batch through SCD2 must be a no-op.
        with db.connect() as conn:
            batch = conn.execute(
                "select batch_id from etl.batch where (row_counts->>'patients')::int > 0 "
                "order by opened_at desc limit 1").fetchone()[0]
        before = dim.count()
        build_dim_patient(s, batch)
        after = read(config.silver("dim_patient")).count()
        assert after == before, f"replay added versions: {before} -> {after}"
        print(f"scd2 replay of {batch}: still {after} versions")
    elif step == "schema_drift_held":
        assert open_batches == 1, "batch should be held while the incident is open"
        assert rows < truth["claims"], "gold must not include the held batch"
    elif step == "schema_drift_recovered":
        assert open_batches == 0 and rows == truth["claims"]
    elif step == "null_spike":
        assert open_batches == 0 and quarantine.get("payer_id_null_rate", 0) > 0
        assert fact.where("payer_id is null").count() == 0, "null payer reached gold"
        assert rows == truth["claims"] - quarantine["payer_id_null_rate"]
    elif step == "duplicate":
        assert open_batches == 0 and quarantine.get("no_duplicate_load", 0) > 0
    elif step == "delay":
        assert open_batches == 0
    elif step == "reconciled":
        assert open_batches == 0 and rows == truth["claims"], f"{rows} != {truth['claims']}"
        assert abs(charged - truth["charged"]) < 0.01, f"{charged} != {truth['charged']}"
    print(f"verified: {step}")
    s.stop()


if __name__ == "__main__":
    main(sys.argv[1])
