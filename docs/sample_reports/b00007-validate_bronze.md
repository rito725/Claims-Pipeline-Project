# Incident b00007-validate_bronze: Upstream delay

| | |
|---|---|
| Batch / stage | `b00007` / `validate_bronze` |
| Detected | 2026-11-05T13:25:35+00:00 |
| Root cause | **upstream_delay** (confidence 0.85) |
| Rows affected | 0 |
| Diagnosed by | offline-rule-planner |

## Cause
- Extract returned 0 new claims for a run that expected data.
- Last file received: claims_2026_07.csv at 2026-11-05 18:53:48.408375+05:30

## Failed signal
`failed checks: bronze.claims.new_claims_arrived`

- `bronze.claims.new_claims_arrived`: observed_value=0

## Impact
No new claims this run; Gold stays correct but stale.

## Action
- Proposed: `rerun_partition`
- Decision: approved by on-call
- Executed: `rerun_partition`
- Result: **succeeded**. batch b00007 re-extracted from the watermark: committed

## Fix
Confirm the feed status with the sender; rerun once the file lands.
