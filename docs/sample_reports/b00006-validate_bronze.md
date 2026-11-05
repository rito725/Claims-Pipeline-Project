# Incident b00006-validate_bronze: Duplicate load

| | |
|---|---|
| Batch / stage | `b00006` / `validate_bronze` |
| Detected | 2026-11-05T13:24:09+00:00 |
| Root cause | **duplicate_load** (confidence 0.97) |
| Rows affected | 3,609 |
| Diagnosed by | offline-rule-planner |

## Cause
- 3,609 rows match content already in Silver, from claims_2026_07.csv (3,609)
- claims_2026_07.csv was loaded 2 times with an identical sha256 (2026-11-05 18:49:03.553895+05:30 and 2026-11-05 18:53:48.408375+05:30)

## Failed signal
`failed checks: bronze.claims.no_duplicate_load`

- `bronze.claims.no_duplicate_load`: 3,609 of 3,609 rows (100.0%)

## Impact
$5,463,092.55 of reloaded charges stopped before Silver. Gold is unchanged.

## Action
- Proposed: `quarantine_bad_rows`
- Decision: approved by demo-auto-approve
- Executed: `quarantine_bad_rows`
- Result: **succeeded**. quarantined 3609 rows; pipeline resumed from validate_bronze and committed the batch

## Fix
Quarantine the repeats; add an ingest guard that rejects a sha256 already in etl.ingest_log.
