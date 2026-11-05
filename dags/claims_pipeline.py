"""Daily claims pipeline with a triage task on failure.

Each stage is its own task, so Airflow shows exactly which gate tripped. Tasks shell into the
project venv because the pipeline's dependencies cannot share Airflow's constrained environment.

The triage task stops at the approval step and exits successfully: a human resumes it with
`python -m claims.agent.cli approve <failure_id>`. The DAG run itself stays failed until then,
which is the honest state for a held batch.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[1]))


def stage(name: str, extra: str = "") -> BashOperator:
    return BashOperator(
        task_id=name,
        bash_command=f'source "{ROOT}/scripts/env.sh" && python -m claims.pipeline.run --stage {name} {extra}',
    )


with DAG(
    dag_id="claims_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=30)},
    tags=["claims"],
) as dag:
    extract = stage("extract_bronze")
    validate_bronze = stage("validate_bronze", "--expect-new-data")
    silver = stage("silver")
    validate_silver = stage("validate_silver")
    gold = stage("gold")
    validate_gold = stage("validate_gold")
    commit = stage("commit")

    triage = BashOperator(
        task_id="triage",
        bash_command=f'source "{ROOT}/scripts/env.sh" && python -m claims.agent.cli start',
        trigger_rule="one_failed",
    )

    # Airflow derives run state from leaf tasks. Without `finished`, triage would be the only leaf
    # and a successful triage would paint a failed pipeline green.
    finished = EmptyOperator(task_id="finished")

    chain = [extract, validate_bronze, silver, validate_silver, gold, validate_gold, commit]
    for upstream, downstream in zip(chain, chain[1:]):
        upstream >> downstream
    commit >> finished
    chain >> triage
