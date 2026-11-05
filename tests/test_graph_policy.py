"""Graph-level tests with a scripted model: the policy holds whatever the model says."""
import sqlite3
from unittest import mock

import pytest
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command

from claims.agent import graph as graph_module


class Scripted(BaseChatModel):
    args: dict

    @property
    def _llm_type(self):
        return "scripted"

    def bind_tools(self, tools, **_):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **_):
        call = {"name": "submit_diagnosis", "args": self.args, "id": "c1", "type": "tool_call"}
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="", tool_calls=[call]))])


DIAG = dict(root_cause="null_spike", confidence=0.9, evidence=["e"], rows_affected=3, impact="i",
            recommended_action="quarantine_bad_rows", fix="f")


def fail(checks=()):
    return {"failure_id": "b1-x", "batch_id": "b1", "stage": "x", "kind": "quality_check", "message": "m",
            "checks": list(checks), "created_at": "t", "log_path": None}


def start(failure, diag, tmp_path, monkeypatch):
    monkeypatch.setattr(graph_module, "get_model", lambda f: Scripted(args=diag))
    monkeypatch.setattr(graph_module.config, "REPORTS", tmp_path)
    monkeypatch.setattr("claims.agent.report.config.OUTBOX", tmp_path)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    g = graph_module.build_graph(failure, SqliteSaver(sqlite3.connect(":memory:", check_same_thread=False)))
    cfg = {"configurable": {"thread_id": "t"}}
    return g, cfg, g.invoke({"failure": failure}, cfg)


def test_nothing_executes_before_approval(tmp_path, monkeypatch):
    with mock.patch.object(graph_module.actions, "execute") as execute:
        _, _, result = start(fail(), DIAG, tmp_path, monkeypatch)
    assert result["__interrupt__"]
    execute.assert_not_called()


def test_rejection_runs_no_action(tmp_path, monkeypatch):
    with mock.patch.object(graph_module.actions, "execute") as execute:
        g, cfg, _ = start(fail(), DIAG, tmp_path, monkeypatch)
        result = g.invoke(Command(resume={"approved": False, "by": "me"}), cfg)
    execute.assert_not_called()
    assert result["action_result"]["status"] == "skipped"


def test_quarantine_downgraded_when_no_row_level_check_exists(tmp_path, monkeypatch):
    _, _, result = start(fail(), DIAG, tmp_path, monkeypatch)
    assert result["proposed_action"] == "open_incident"


def test_low_confidence_always_becomes_an_incident(tmp_path, monkeypatch):
    check = {"name": "c", "table": "bronze.claims", "bad_row_sql": "x", "observed": {}}
    _, _, result = start(fail([check]), {**DIAG, "confidence": 0.3}, tmp_path, monkeypatch)
    assert result["proposed_action"] == "open_incident"


def test_rows_affected_prefers_the_measured_count(tmp_path, monkeypatch):
    check = {"name": "c", "table": "bronze.claims", "bad_row_sql": "x", "observed": {"unexpected_count": 700}}
    _, _, result = start(fail([check]), DIAG, tmp_path, monkeypatch)
    assert result["diagnosis"]["rows_affected"] == 700


def test_invalid_root_cause_from_model_is_rejected(tmp_path, monkeypatch):
    with pytest.raises(Exception):
        start(fail(), {**DIAG, "root_cause": "gremlins"}, tmp_path, monkeypatch)
