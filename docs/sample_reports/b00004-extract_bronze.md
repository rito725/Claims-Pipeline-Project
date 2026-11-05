# Incident b00004-extract_bronze: Schema drift

| | |
|---|---|
| Batch / stage | `b00004` / `extract_bronze` |
| Detected | 2026-11-05T13:20:49+00:00 |
| Root cause | **schema_drift** (confidence 0.95) |
| Rows affected | 3,745 |
| Diagnosed by | offline-rule-planner |

## Cause
- compare_schema: only in source ['payer_ref'], only in target ['payer_id']
- 10 of 10 distinct values in payer_ref also exist in payer_id, and it sits at the same ordinal (3 vs 3)

## Failed signal
`AnalysisException: [DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing to the Delta table.
- A schema mismatch detected when writing to the Delta table (Table ID: 419befb7-48f2-4d51-9a65-3111a7e70fcf).
To enable schema migration using DataFrameWriter or DataStreamWriter, please s`

- none (Spark stage crashed)

## Impact
Extract aborted before any claims were written. 3,745 pending rows ($6,324,000.34) are held in the source until this is resolved.

## Action
- Proposed: `open_incident`
- Decision: approved by demo-auto-approve
- Executed: `open_incident`
- Result: **succeeded**. INC-0001 opened; batch b00004 stays open and the watermark has not moved

## Fix
Upstream renamed payer_id to payer_ref. Revert it, or agree the rename and map the column in the extract, then rerun.
