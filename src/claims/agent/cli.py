"""Triage CLI.

  python -m claims.agent.cli start                     # diagnose latest failure, stop for approval
  python -m claims.agent.cli start --auto-approve      # demo mode
  python -m claims.agent.cli approve <failure_id> [--action open_incident] [--by NAME]
  python -m claims.agent.cli reject  <failure_id> [--note "why"]
"""
from __future__ import annotations

import argparse
import getpass
import json
import sqlite3
import sys

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command

from claims import config, failures
from claims.agent import actions
from claims.agent.graph import build_graph


def thread_config(failure: dict) -> dict:
    # created_at keeps a re-failure of the same stage from resuming the old conversation.
    return {"configurable": {"thread_id": f"{failure['failure_id']}@{failure['created_at']}"}}


def saver() -> SqliteSaver:
    config.RUNS.mkdir(parents=True, exist_ok=True)
    return SqliteSaver(sqlite3.connect(config.CHECKPOINT_DB, check_same_thread=False))


def show(result: dict) -> None:
    pending = result.get("__interrupt__")
    if pending:
        p = pending[0].value
        print(f"\nDIAGNOSIS  {p['root_cause']}  (confidence {p['confidence']:.2f}), {p['rows_affected']:,} rows")
        for e in p["evidence"]:
            print(f"  - {e}")
        print(f"\nAWAITING APPROVAL  proposed action: {p['proposed_action']}")
        print(f"  python -m claims.agent.cli approve {p['failure_id']}")
        print(f"  python -m claims.agent.cli reject  {p['failure_id']}")
    else:
        print(f"\nresult: {result['action_result']['status']}: {result['action_result'].get('detail', '')}")
        print(f"report: {result['report_path']}  (delivered to {result['delivered_to']})")


def resume(failure: dict, decision: dict) -> dict:
    graph = build_graph(failure, saver())
    return graph.invoke(Command(resume=decision), thread_config(failure))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start")
    s.add_argument("--failure-id")
    s.add_argument("--auto-approve", action="store_true")
    for name in ("approve", "reject"):
        p = sub.add_parser(name)
        p.add_argument("failure_id")
        p.add_argument("--by", default=getpass.getuser())
        p.add_argument("--note", default="")
        if name == "approve":
            p.add_argument("--action", choices=["quarantine_bad_rows", "rerun_partition", "open_incident"])
    args = ap.parse_args(argv)

    if args.cmd == "start":
        failure = failures.load(args.failure_id) if args.failure_id else failures.latest()
        if failure is None:
            print("no failures recorded")
            return 0
        if not args.failure_id and not actions.batch_is_open(failure["batch_id"]):
            print(f"latest failure {failure['failure_id']} belongs to a closed batch; nothing to triage")
            return 0
        print(f"triaging {failure['failure_id']}: {failure['message'][:160]}")
        graph = build_graph(failure, saver())
        result = graph.invoke({"failure": failure}, thread_config(failure))
        show(result)
        if args.auto_approve and result.get("__interrupt__"):
            print("\n[demo] auto-approving the proposed action")
            show(resume(failure, {"approved": True, "by": "demo-auto-approve"}))
        return 0

    failure = failures.load(args.failure_id)
    decision = {"approved": args.cmd == "approve", "by": args.by, "note": args.note}
    if args.cmd == "approve" and args.action:
        decision["action"] = args.action
    show(resume(failure, decision))
    return 0


if __name__ == "__main__":
    sys.exit(main())
