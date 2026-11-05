"""Incident report rendering and delivery."""
from __future__ import annotations

import json
import os
import urllib.request

from claims import config

TITLES = {
    "schema_drift": "Schema drift", "null_spike": "Null spike", "duplicate_load": "Duplicate load",
    "upstream_delay": "Upstream delay", "unclassified": "Unclassified failure",
}


def render(state: dict) -> str:
    f, d, a = state["failure"], state["diagnosis"], state.get("action_result") or {}
    decision = state.get("decision") or {}
    evidence = "\n".join(f"- {e}" for e in d["evidence"])
    checks = "\n".join(f"- `{c['table']}.{c['name']}`: {_observed(c)}" for c in f.get("checks", [])) or "- none (Spark stage crashed)"
    return f"""# Incident {f['failure_id']}: {TITLES.get(d['root_cause'], d['root_cause'])}

| | |
|---|---|
| Batch / stage | `{f['batch_id']}` / `{f['stage']}` |
| Detected | {f['created_at']} |
| Root cause | **{d['root_cause']}** (confidence {d['confidence']:.2f}) |
| Rows affected | {d['rows_affected']:,} |
| Diagnosed by | {state.get('model_name', 'unknown')} |

## Cause
{evidence}

## Failed signal
`{f['message'][:300]}`

{checks}

## Impact
{d['impact']}

## Action
- Proposed: `{state['proposed_action']}`
- Decision: {_decision(decision)}
- Executed: `{decision.get('action', 'none') if decision.get('approved') else 'none'}`
- Result: **{a.get('status', 'not run')}**. {a.get('detail', '')}

## Fix
{d['fix']}
"""


def _observed(check: dict) -> str:
    o = check.get("observed", {})
    if "unexpected_count" in o:
        return f"{o['unexpected_count']:,} of {o.get('element_count', 0):,} rows ({o.get('unexpected_percent', 0):.1f}%)"
    return ", ".join(f"{k}={v}" for k, v in o.items() if k != "partial_unexpected_counts") or "failed"


def _decision(d: dict) -> str:
    if not d:
        return "pending"
    verdict = "approved" if d.get("approved") else "rejected"
    return f"{verdict} by {d.get('by', 'unknown')}" + (f" ({d['note']})" if d.get("note") else "")


def notify(markdown: str, failure_id: str) -> str:
    """Post to Slack when a webhook is configured; otherwise drop the message in the local outbox."""
    url = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if url:
        body = json.dumps({"text": markdown[:3500]}).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return "slack"
    config.OUTBOX.mkdir(parents=True, exist_ok=True)
    (config.OUTBOX / f"{failure_id}.md").write_text(markdown)
    return "outbox"
