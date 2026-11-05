# Sourced by every script. Toolchain lives outside the project: conda-built Postgres
# and venv tooling are unreliable under a path that contains spaces.
export PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
export TOOLS_HOME="${TOOLS_HOME:-$HOME/.local/share/claims-pipeline}"
export CONDA_ENV="$TOOLS_HOME/env"
export JAVA_HOME="$CONDA_ENV/lib/jvm"
export PGDATA="$TOOLS_HOME/pgdata"
export PGHOST=127.0.0.1 PGPORT="${PGPORT:-5433}" PGUSER=claims PGDATABASE=claims
export PATH="$JAVA_HOME/bin:$CONDA_ENV/bin:$PROJECT_ROOT/.venv/bin:$PATH"
export PYTHONPATH="$PROJECT_ROOT/src:$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
if [ -f "$PROJECT_ROOT/.env" ]; then set -a; . "$PROJECT_ROOT/.env"; set +a; fi
