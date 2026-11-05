#!/usr/bin/env bash
# Install everything into user space: no sudo, Docker or Homebrew. Safe to rerun.
# macOS arm64 by default; set MAMBA_PLATFORM=osx-64 or linux-64 elsewhere.
set -euo pipefail
source "$(dirname "$0")/env.sh"
mkdir -p "$TOOLS_HOME/bin" "$TOOLS_HOME/synthea"

if [ ! -x "$TOOLS_HOME/bin/micromamba" ]; then
  curl -sL "https://micro.mamba.pm/api/micromamba/${MAMBA_PLATFORM:-osx-arm64}/latest" | tar -xj -C "$TOOLS_HOME" bin/micromamba
fi
if [ ! -x "$CONDA_ENV/bin/postgres" ]; then
  MAMBA_ROOT_PREFIX="$TOOLS_HOME/mamba" "$TOOLS_HOME/bin/micromamba" create -y -q -p "$CONDA_ENV" -c conda-forge 'postgresql=16' 'openjdk=17'
fi
JAR="$TOOLS_HOME/synthea/synthea-with-dependencies.jar"
if ! unzip -tq "$JAR" >/dev/null 2>&1; then
  curl -L -C - --retry 5 -o "$JAR" https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar
fi
if [ ! -x "$PROJECT_ROOT/.venv/bin/python" ]; then
  PY="${PYTHON312:-$(command -v python3.12 || echo "$HOME/.local/bin/python3.12")}"
  "$PY" -m venv "$PROJECT_ROOT/.venv"
  "$PROJECT_ROOT/.venv/bin/python" -m pip -q install -U pip
  "$PROJECT_ROOT/.venv/bin/python" -m pip -q install -r "$PROJECT_ROOT/requirements.txt"
fi
echo "toolchain ready in $TOOLS_HOME"
