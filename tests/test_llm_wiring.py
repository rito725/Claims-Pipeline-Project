"""Drive the graph with the real ChatOpenAI client against a local stub of the chat completions API.

This proves the wire format, meaning tool schemas are sent and tool calls and results round-trip
through LangGraph. It does not prove that a real model reasons well.
"""
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from langgraph.checkpoint.sqlite import SqliteSaver

from claims.agent import graph as graph_module


def completion(tool_name, arguments):
    return {"id": "c", "object": "chat.completion", "created": 0, "model": "stub",
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            "choices": [{"index": 0, "finish_reason": "tool_calls", "message": {
                "role": "assistant", "content": None, "tool_calls": [{
                    "id": f"call_{tool_name}", "type": "function",
                    "function": {"name": tool_name, "arguments": json.dumps(arguments)}}]}}]}


@pytest.fixture
def stub_api():
    requests = []
    replies = [
        completion("query_postgres", {"sql": "select count(*) as n from payers"}),
        completion("submit_diagnosis", {
            "root_cause": "upstream_delay", "confidence": 0.8, "evidence": ["no file"], "rows_affected": 0,
            "impact": "stale", "recommended_action": "rerun_partition", "fix": "wait"}),
    ]

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            body = json.dumps(replies.pop(0)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/v1", requests
    server.shutdown()


def test_tool_calling_round_trip_through_openai_client(stub_api, monkeypatch):
    url, requests = stub_api
    monkeypatch.setenv("TRIAGE_MODEL", "openai:stub-model")
    monkeypatch.setenv("OPENAI_API_KEY", "not-a-real-key")
    monkeypatch.setenv("OPENAI_BASE_URL", url)

    failure = {"failure_id": "b1-x", "batch_id": "b1", "stage": "x", "kind": "quality_check",
               "message": "m", "checks": [], "created_at": "t", "log_path": None}
    g = graph_module.build_graph(failure, SqliteSaver(sqlite3.connect(":memory:", check_same_thread=False)))
    result = g.invoke({"failure": failure}, {"configurable": {"thread_id": "t"}})

    assert result["__interrupt__"][0].value["root_cause"] == "upstream_delay"
    sent_tools = {t["function"]["name"] for t in requests[0]["tools"]}
    assert sent_tools == {"profile_column", "sample_bad_rows", "compare_schema", "query_postgres", "submit_diagnosis"}
    enum = requests[0]["tools"][-1]["function"]["parameters"]["properties"]["root_cause"]["enum"]
    assert "null_spike" in enum
    tool_reply = [m for m in requests[1]["messages"] if m["role"] == "tool"]
    assert '"n": 10' in tool_reply[0]["content"].replace("\n", "")
    assert requests[1]["messages"][0]["role"] == "system"
