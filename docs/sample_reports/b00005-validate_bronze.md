# Incident b00005-validate_bronze: Null spike

| | |
|---|---|
| Batch / stage | `b00005` / `validate_bronze` |
| Detected | 2026-11-05T13:22:19+00:00 |
| Root cause | **null_spike** (confidence 0.93) |
| Rows affected | 694 |
| Diagnosed by | offline-rule-planner |

## Cause
- payer_id null rate is 19.64% in b00005 against a 0.0% baseline
- 694 of 3,534 rows affected, from claims_2026_09.csv (694)

## Failed signal
`failed checks: bronze.claims.payer_id_null_rate`

- `bronze.claims.payer_id_null_rate`: 694 of 3,534 rows (19.6%)

## Impact
$1,045,193.31 of charges cannot be attributed to a payer.

## Action
- Proposed: `quarantine_bad_rows`
- Decision: approved by demo-auto-approve
- Executed: `quarantine_bad_rows`
- Result: **succeeded**. quarantined 694 rows; pipeline resumed from validate_bronze and committed the batch

## Fix
Quarantine the null rows, load the clean remainder, and ask the sender to resend the affected rows from claims_2026_09.csv (694).
