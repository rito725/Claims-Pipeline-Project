"""Fast tests for the agent's safety rails and planner. No Spark; Postgres for the read-only role."""
import pytest
from langchain_core.messages import HumanMessage, ToolMessage

from claims.agent.llm import OfflinePlanner
from claims.agent.tools import Toolbox


def failure(**overrides):
    return {"failure_id": "b1-validate_bronze", "batch_id": "b1", "stage": "validate_bronze",
            "kind": "quality_check", "message": "m", "checks": [], "created_at": "t", **overrides}


def plan(fail, **tool_results):
    import json
    messages = [HumanMessage("go")] + [
        ToolMessage(json.dumps(v), tool_call_id=k, name=k) for k, v in tool_results.items()]
    call = OfflinePlanner(failure=fail).invoke(messages).tool_calls[0]
    return call["name"], call["args"]


@pytest.mark.parametrize("sql", [
    "delete from claims", "update claims set payer_id = null", "drop table claims",
    "select 1; delete from claims", "insert into claims default values",
])
def test_query_postgres_rejects_writes_before_reaching_the_database(sql):
    assert "error" in Toolbox(failure()).query_postgres(sql)


def test_read_only_role_blocks_writes_hidden_in_a_select():
    result = Toolbox(failure()).query_postgres(
        "with d as (delete from claims where row_id = -1 returning 1) select * from d")
    assert "error" in result and "read-only" in result["error"]


def test_planner_schema_crash_compares_schemas_first():
    name, args = plan(failure(kind="spark_exception", message="AnalysisException: [DELTA_METADATA_MISMATCH]"))
    assert (name, args) == ("compare_schema", {"source": "postgres.claims", "target": "bronze.claims"})


def test_planner_unknown_crash_goes_to_a_human():
    name, args = plan(failure(kind="spark_exception", message="OutOfMemoryError: Java heap"))
    assert name == "submit_diagnosis"
    assert args["root_cause"] == "unclassified" and args["recommended_action"] == "open_incident"


def test_planner_duplicate_without_hash_proof_has_lower_confidence():
    check = {"name": "no_duplicate_load", "expectation": "ExpectColumnValuesToBeInSet", "table": "bronze.claims", "column": "is_duplicate"}
    name, args = plan(failure(checks=[check]),
                      sample_bad_rows={"rows_matching": 10, "by_source_file": {"f.csv": 10}, "charge_amount_affected": 5},
                      query_postgres={"rows": []})
    assert name == "submit_diagnosis" and args["root_cause"] == "duplicate_load" and args["confidence"] < 0.9
