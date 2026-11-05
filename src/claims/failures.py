"""The failure artifact: the contract between the pipeline and the triage agent.

Whenever a stage fails, exactly one JSON file lands in runs/failures/. The agent gets nothing
else from the pipeline, so anything it needs to start investigating has to be in here.
"""
from __future__ import annotations

import json
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from claims import config


@dataclass
class FailedCheck:
    name: str
    table: str
    column: str | None
    expectation: str
    observed: dict
    bad_row_sql: str | None = None     # Spark SQL predicate selecting offending rows, if row-level


@dataclass
class Failure:
    batch_id: str
    stage: str
    kind: str                          # "quality_check" | "spark_exception"
    message: str
    checks: list[FailedCheck] = field(default_factory=list)
    log_path: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))

    @property
    def failure_id(self) -> str:
        return f"{self.batch_id}-{self.stage}"

    def save(self) -> Path:
        config.FAILURES.mkdir(parents=True, exist_ok=True)
        path = config.FAILURES / f"{self.failure_id}.json"
        path.write_text(json.dumps({"failure_id": self.failure_id, **asdict(self)}, indent=2, default=str))
        return path


def load(failure_id: str) -> dict:
    return json.loads((config.FAILURES / f"{failure_id}.json").read_text())


def latest() -> dict | None:
    files = sorted(config.FAILURES.glob("*.json"), key=lambda p: p.stat().st_mtime)
    return json.loads(files[-1].read_text()) if files else None


def stage_log(batch_id: str, stage: str) -> Path:
    config.LOGS.mkdir(parents=True, exist_ok=True)
    return config.LOGS / f"{batch_id}-{stage}.log"


def record_exception(batch_id: str, stage: str, exc: BaseException) -> Failure:
    """Persist a Spark/driver crash the way a real job log would hold it."""
    log = stage_log(batch_id, stage)
    with log.open("a") as fh:
        fh.write(f"{datetime.now(timezone.utc).isoformat()} ERROR stage={stage} batch={batch_id}\n")
        fh.write("".join(traceback.format_exception(exc)))
    failure = Failure(
        batch_id=batch_id, stage=stage, kind="spark_exception",
        message=f"{type(exc).__name__}: {str(exc)[:500]}", log_path=str(log),
    )
    failure.save()
    return failure
