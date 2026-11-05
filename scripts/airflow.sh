#!/usr/bin/env bash
# Run the Airflow CLI with this project's settings, e.g.:
#   scripts/airflow.sh standalone                         UI on http://localhost:8080
#   scripts/airflow.sh dags test claims_pipeline          one full run, no scheduler needed
set -euo pipefail
source "$(dirname "$0")/env.sh"
export AIRFLOW_HOME="$TOOLS_HOME/airflow"           # outside the project: no spaces in sqlite URIs
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=2                 # one Spark JVM at a time is enough on 8 GB
exec "$PROJECT_ROOT/.venv-airflow/bin/airflow" "$@"
