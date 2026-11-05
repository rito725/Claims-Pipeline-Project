"""Model selection, and the deterministic planner used when no API key is configured.

`OfflinePlanner` is a real LangChain chat model: it emits tool calls and reads tool results
through the same graph as an LLM. Its decisions come from fixed rules, not language understanding,
so it demonstrates the agent's plumbing and tools. Set TRIAGE_MODEL for actual LLM reasoning.
"""
from __future__ import annotations

import json
import os
import uuid
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult


def get_model(failure: dict) -> BaseChatModel:
    name = os.environ.get("TRIAGE_MODEL", "").strip()
    if not name:
        return OfflinePlanner(failure=failure)
    from langchain.chat_models import init_chat_model
    return init_chat_model(name)


def money(value: Any) -> str:
    return f"${float(value or 0):,.2f}"


def files(sample: dict) -> str:
    """Render a by_source_file breakdown for a report line."""
    return ", ".join(f"{name} ({count:,})" for name, count in (sample.get("by_source_file") or {}).items()) or "unknown"


class OfflinePlanner(BaseChatModel):
    failure: dict

    @property
    def _llm_type(self) -> str:
        return "offline-rule-planner"

    def bind_tools(self, tools: Any, **_: Any) -> "OfflinePlanner":
        return self

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **_: Any) -> ChatResult:
        results: dict[str, dict] = {}
        for m in messages:
            if isinstance(m, ToolMessage):
                try:
                    results[m.name] = json.loads(m.content)
                except (TypeError, ValueError):
                    results[m.name] = {"error": str(m.content)}
        name, args = self._next_step(results)
        call = {"name": name, "args": args, "id": f"call_{uuid.uuid4().hex[:8]}", "type": "tool_call"}
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="", tool_calls=[call]))])

    def _next_step(self, seen: dict[str, dict]) -> tuple[str, dict]:
        f = self.failure
        checks = {c["name"]: c for c in f.get("checks", [])}
        signature = f"{f.get('message', '')}".upper()

        if f["kind"] == "spark_exception":
            if "SCHEMA" in signature or "METADATA_MISMATCH" in signature or "UNRESOLVED_COLUMN" in signature:
                if "compare_schema" not in seen:
                    return "compare_schema", {"source": "postgres.claims", "target": "bronze.claims"}
                if "query_postgres" not in seen:
                    return "query_postgres", {"sql": (
                        "select count(*) as pending_rows, coalesce(sum(charge_amount), 0) as charge_amount "
                        "from claims where updated_at > "
                        "(select high_watermark from etl.watermark where table_name = 'claims')")}
                return self._diagnose_schema(seen["compare_schema"], seen["query_postgres"])
            return self._unclassified("Spark stage crashed with an unrecognised error signature")

        if "new_claims_arrived" in checks:
            if "query_postgres" not in seen:
                return "query_postgres", {"sql": (
                    "select file_name, row_count, loaded_at, "
                    "round(extract(epoch from clock_timestamp() - loaded_at) / 60) as minutes_ago "
                    "from etl.ingest_log order by loaded_at desc limit 5")}
            return self._diagnose_delay(seen["query_postgres"])

        if "no_duplicate_load" in checks:
            if "sample_bad_rows" not in seen:
                return "sample_bad_rows", {"check_name": "no_duplicate_load"}
            if "query_postgres" not in seen:
                return "query_postgres", {"sql": (
                    "select file_name, sha256, count(*) as times_loaded, sum(row_count) as total_rows, "
                    "min(loaded_at) as first_loaded, max(loaded_at) as last_loaded from etl.ingest_log "
                    "group by 1, 2 having count(*) > 1 order by last_loaded desc")}
            return self._diagnose_duplicate(seen["sample_bad_rows"], seen["query_postgres"])

        null_check = next((c for c in f.get("checks", []) if "Null" in c["expectation"]), None)
        if null_check:
            if "profile_column" not in seen:
                return "profile_column", {"table": null_check["table"], "column": null_check["column"]}
            if "sample_bad_rows" not in seen:
                return "sample_bad_rows", {"check_name": null_check["name"]}
            return self._diagnose_nulls(null_check, seen["profile_column"], seen["sample_bad_rows"])

        return self._unclassified(f"no rule covers failed checks: {sorted(checks)}")

    # ---- rule conclusions ------------------------------------------------------------------

    def _unclassified(self, why: str) -> tuple[str, dict]:
        return "submit_diagnosis", {
            "root_cause": "unclassified", "confidence": 0.2, "evidence": [why, self.failure.get("message", "")],
            "rows_affected": 0, "impact": "Batch held; Gold not updated.",
            "recommended_action": "open_incident", "fix": "An engineer needs to inspect the log.",
        }

    def _diagnose_schema(self, diff: dict, pending: dict) -> tuple[str, dict]:
        held = (pending.get("rows") or [{}])[0]
        renames = diff.get("likely_renames") or []
        evidence = [f"compare_schema: only in source {diff.get('only_in_source')}, only in target {diff.get('only_in_target')}"]
        for r in renames:
            evidence.append(f"{r['shared_values']} of {r['source_distinct']} distinct values in {r['source_column']} "
                            f"also exist in {r['target_column']}, and it sits at the same ordinal "
                            f"({r['ordinal_source']} vs {r['ordinal_target']})" if r["ordinal_source"] == r["ordinal_target"]
                            else f"{r['shared_values']} of {r['source_distinct']} distinct values in {r['source_column']} also exist in {r['target_column']}")
        new = renames[0]["source_column"] if renames else "the new column"
        old = renames[0]["target_column"] if renames else "the old column"
        return "submit_diagnosis", {
            "root_cause": "schema_drift", "confidence": 0.95 if renames and renames[0]["shared_values"] else 0.7,
            "evidence": evidence, "rows_affected": held.get("pending_rows", 0),
            "impact": f"Extract aborted before any claims were written. {held.get('pending_rows', 0):,} pending rows "
                      f"({money(held.get('charge_amount'))}) are held in the source until this is resolved.",
            "recommended_action": "open_incident",
            "fix": f"Upstream renamed {old} to {new}. Revert it, or agree the rename and map the column in the extract, then rerun.",
        }

    def _diagnose_delay(self, recent: dict) -> tuple[str, dict]:
        rows = recent.get("rows") or []
        last = rows[0] if rows else {}
        evidence = ["Extract returned 0 new claims for a run that expected data.",
                    f"Last file received: {last.get('file_name')} at {last.get('loaded_at')}"]
        return "submit_diagnosis", {
            "root_cause": "upstream_delay", "confidence": 0.85, "evidence": evidence, "rows_affected": 0,
            "impact": "No new claims this run; Gold stays correct but stale.",
            "recommended_action": "rerun_partition",
            "fix": "Confirm the feed status with the sender; rerun once the file lands.",
        }

    def _diagnose_duplicate(self, sample: dict, loads: dict) -> tuple[str, dict]:
        repeats = loads.get("rows") or []
        evidence = [f"{sample.get('rows_matching', 0):,} rows match content already in Silver, from {files(sample)}"]
        for r in repeats:
            evidence.append(f"{r['file_name']} was loaded {r['times_loaded']} times with an identical sha256 "
                            f"({r['first_loaded']} and {r['last_loaded']})")
        return "submit_diagnosis", {
            "root_cause": "duplicate_load", "confidence": 0.97 if repeats else 0.75, "evidence": evidence,
            "rows_affected": sample.get("rows_matching", 0),
            "impact": f"{money(sample.get('charge_amount_affected'))} of reloaded charges stopped before Silver. Gold is unchanged.",
            "recommended_action": "quarantine_bad_rows",
            "fix": "Quarantine the repeats; add an ingest guard that rejects a sha256 already in etl.ingest_log.",
        }

    def _diagnose_nulls(self, check: dict, profile: dict, sample: dict) -> tuple[str, dict]:
        cur, base = profile.get("current_batch", {}), profile.get("baseline_prior_batches", {})
        return "submit_diagnosis", {
            "root_cause": "null_spike", "confidence": 0.93,
            "evidence": [f"{check['column']} null rate is {cur.get('null_pct')}% in {cur.get('batch_id')} against a {base.get('null_pct')}% baseline",
                         f"{sample.get('rows_matching', 0):,} of {cur.get('rows', 0):,} rows affected, from {files(sample)}"],
            "rows_affected": sample.get("rows_matching", 0),
            "impact": f"{money(sample.get('charge_amount_affected'))} of charges cannot be attributed to a payer.",
            "recommended_action": "quarantine_bad_rows",
            "fix": f"Quarantine the null rows, load the clean remainder, and ask the sender to resend the affected rows from {files(sample)}.",
        }
