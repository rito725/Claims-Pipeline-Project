#!/usr/bin/env bash
# From an empty checkout to a finished demo.
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$PROJECT_ROOT"
scripts/bootstrap.sh
scripts/pg.sh start
[ -f data/synthea/csv/claims.csv ] || scripts/generate_synthea.sh
[ -x .venv-airflow/bin/airflow ] || scripts/install_airflow.sh
scripts/airflow.sh db migrate >/dev/null 2>&1
demo/run_demo.sh
