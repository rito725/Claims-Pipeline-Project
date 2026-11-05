# Incident b00009-validate_bronze: Upstream delay

| | |
|---|---|
| Batch / stage | `b00009` / `validate_bronze` |
| Detected | 2026-11-05T13:29:27+00:00 |
| Root cause | **upstream_delay** (confidence 0.85) |
| Rows affected | 0 |
| Diagnosed by | offline-rule-planner |

## Cause
- Extract returned 0 new claims for a run that expected data.
- Last file received: claims_2026_09_resend.csv at 2026-11-05 18:56:44.302192+05:30

## Failed signal
`failed checks: bronze.claims.new_claims_arrived`

- `bronze.claims.new_claims_arrived`: observed_value=0

## Impact
No new claims this run; Gold stays correct but stale.

## Action
- Proposed: `rerun_partition`
- Decision: rejected by on-call (sender confirmed no file today)
- Executed: `none`
- Result: **skipped**. rejected by the approver; batch stays open

## Fix
Confirm the feed status with the sender; rerun once the file lands.
