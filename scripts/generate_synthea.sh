#!/usr/bin/env bash
# Generate synthetic claims data. SYNTHEA_PATIENTS defaults to 5000 (~6 min, ~500k claims).
set -euo pipefail
source "$(dirname "$0")/env.sh"
N="${SYNTHEA_PATIENTS:-5000}"
OUT="$PROJECT_ROOT/data/synthea"
mkdir -p "$OUT"
java -Xmx3g -jar "$TOOLS_HOME/synthea/synthea-with-dependencies.jar" -s 42 -cs 42 -p "$N" \
  --exporter.baseDirectory="$OUT" \
  --exporter.csv.export=true --exporter.fhir.export=false \
  --exporter.hospital.fhir.export=false --exporter.practitioner.fhir.export=false \
  --exporter.csv.included_files=patients.csv,payers.csv,encounters.csv,claims.csv,claims_transactions.csv \
  Massachusetts | tail -3
wc -l "$OUT"/csv/*.csv
