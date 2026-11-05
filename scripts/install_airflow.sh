#!/usr/bin/env bash
# Airflow gets its own venv: its constraint file pins versions LangGraph cannot share.
set -euo pipefail
source "$(dirname "$0")/env.sh"
V=3.3.2; PY=3.12
"${PYTHON312:-$HOME/.local/bin/python3.12}" -m venv "$PROJECT_ROOT/.venv-airflow"
"$PROJECT_ROOT/.venv-airflow/bin/python" -m pip -q install -U pip
"$PROJECT_ROOT/.venv-airflow/bin/python" -m pip -q install "apache-airflow==$V" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$V/constraints-$PY.txt"
