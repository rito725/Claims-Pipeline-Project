#!/usr/bin/env bash
# Full failure demo: resets the source and lake, then runs every scenario and asserts each outcome.
# Assumes Postgres is running and Synthea data exists (scripts/run_all.sh handles both).
set -uo pipefail
source "$(dirname "$0")/../scripts/env.sh"
cd "$PROJECT_ROOT"
Q="$PROJECT_ROOT/scripts/quiet.sh"

py()   { python "$@" 2>&1 | "$Q"; return "${PIPESTATUS[0]}"; }
pipe() { python -m claims.pipeline.run "$@" 2>&1 | "$Q"; return "${PIPESTATUS[0]}"; }
triage() { python -m claims.agent.cli start --auto-approve 2>&1 | "$Q"; }
step() { printf '\n\n########## %s\n' "$1"; }
must_fail() { pipe "$@" && { echo "EXPECTED THE PIPELINE TO FAIL"; exit 1; } || true; }
set -e

step "Reset and initial load"
python -m claims.seed 2>&1 | "$Q"
pipe

step "0. Clean incremental batch"
py demo/inject.py feed 2026_07
pipe --expect-new-data
py demo/verify.py clean

step "0b. SCD2: 25 patients move; replaying must not add versions"
py demo/inject.py patient-changes
pipe
py demo/verify.py scd2

step "1. SCHEMA DRIFT: upstream renames payer_id to payer_ref"
py demo/inject.py feed 2026_08
py demo/inject.py rename-column
must_fail --expect-new-data
triage
py demo/verify.py schema_drift_held
echo "--- upstream reverts the rename after the incident; the held batch reruns"
py demo/inject.py revert-column
pipe --expect-new-data
py demo/verify.py schema_drift_recovered

step "2. NULL SPIKE: file arrives with 20% of payer_id blank"
py demo/inject.py null-spike 2026_09
must_fail --expect-new-data
triage
py demo/verify.py null_spike

step "3. DUPLICATE LOAD: claims_2026_07.csv is loaded a second time"
py demo/inject.py reload 2026_07
must_fail --expect-new-data
triage
py demo/verify.py duplicate

step "4. UPSTREAM DELAY: scheduled run finds no new file, which lands after diagnosis"
must_fail --expect-new-data
python -m claims.agent.cli start 2>&1 | "$Q"
FAILURE=$(python -c "from claims import failures; print(failures.latest()['failure_id'])")
echo "--- the late file lands while the approval is pending"
py demo/inject.py feed 2026_10
python -m claims.agent.cli approve "$FAILURE" --by on-call 2>&1 | "$Q"
py demo/verify.py delay

step "5. AIRFLOW: the sender's corrected rows arrive and the scheduled DAG loads them"
py demo/inject.py resend 2026_09
scripts/airflow.sh dags reserialize >/dev/null 2>&1
scripts/airflow.sh dags test claims_pipeline 2>&1 | grep -E '\[(PASS|FAIL)\]|committed' | sed -E 's/^.*\[info *\] *//; s/ *\[airflow.*$//'
py demo/verify.py reconciled

step "6. AIRFLOW: no new file, so the DAG fails, triage fires and waits for a human"
scripts/airflow.sh dags test claims_pipeline 2>&1 | grep -E 'QUALITY GATE|DIAGNOSIS|AWAITING' | sed -E 's/^.*\[info *\] *//; s/ *\[airflow.*$//' || true
FAILURE=$(python -c "from claims import failures; print(failures.latest()['failure_id'])")
python -m claims.agent.cli reject "$FAILURE" --by on-call --note "sender confirmed no file today" 2>&1 | "$Q"

step "Done. Reports:"
ls -1 runs/reports runs/incidents
