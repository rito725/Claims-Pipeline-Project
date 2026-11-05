"""Pipeline entry point. Each stage is callable alone (Airflow) or as a chain (CLI, agent reruns).

  python -m claims.pipeline.run                      # all stages
  python -m claims.pipeline.run --stage gold         # one stage on the open batch
  python -m claims.pipeline.run --from validate_bronze  # resume, used after quarantine

Exit code 0 means the batch committed; 1 means a failure artifact was written to runs/failures/.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from claims import config, failures
from claims.failures import Failure
from claims.pipeline import bronze, control, gold, silver
from claims.spark import get_spark

STAGES = ["extract_bronze", "validate_bronze", "silver", "validate_silver", "gold", "validate_gold", "commit"]


class QualityGateFailed(Exception):
    pass


def gate(batch_id: str, stage: str, failed: list) -> None:
    if not failed:
        return
    log = failures.stage_log(batch_id, stage)
    with log.open("a") as fh:
        for c in failed:
            fh.write(f"{datetime.now(timezone.utc).isoformat()} ERROR check={c.table}.{c.name} observed={c.observed}\n")
    names = ", ".join(f"{c.table}.{c.name}" for c in failed)
    Failure(batch_id=batch_id, stage=stage, kind="quality_check", message=f"failed checks: {names}",
            checks=failed, log_path=str(log)).save()
    raise QualityGateFailed(names)


def run_stage(spark, stage: str, batch_id: str, expect_new_data: bool) -> None:
    from claims.quality import suites  # imported late: GX is slow to import

    if stage == "extract_bronze":
        bronze.run(spark, batch_id)
    elif stage == "validate_bronze":
        gate(batch_id, stage, suites.validate_bronze(spark, batch_id, expect_new_data))
    elif stage == "silver":
        silver.run(spark, batch_id)
    elif stage == "validate_silver":
        gate(batch_id, stage, suites.validate_silver(spark, batch_id))
    elif stage == "gold":
        gold.run(spark, batch_id)
    elif stage == "validate_gold":
        gate(batch_id, stage, suites.validate_gold(spark, batch_id))
    elif stage == "commit":
        control.commit_batch(batch_id)
        print(f"  committed {batch_id}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=STAGES)
    ap.add_argument("--from", dest="start", choices=STAGES)
    ap.add_argument("--expect-new-data", action="store_true",
                    help="treat an empty claims batch as a failure (scheduled runs)")
    args = ap.parse_args(argv)
    config.ensure_dirs()

    stages = [args.stage] if args.stage else STAGES[STAGES.index(args.start or STAGES[0]):]
    batch_id = control.open_batch() if stages[0] == "extract_bronze" else control.current_batch()
    spark = get_spark(f"claims-{batch_id}")
    stage = stages[0]
    try:
        for stage in stages:
            print(f"== {batch_id} {stage}", flush=True)
            run_stage(spark, stage, batch_id, args.expect_new_data)
    except QualityGateFailed as exc:
        print(f"QUALITY GATE FAILED at {stage}: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - every crash must become a triage artifact
        failure = failures.record_exception(batch_id, stage, exc)
        print(f"STAGE FAILED at {stage}: {failure.message}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
