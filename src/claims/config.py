"""Paths and connection settings. Everything derives from the environment set by scripts/env.sh."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
DATA = ROOT / "data"
RAW = DATA / "synthea" / "csv"
FEEDS = DATA / "feeds"
LAKE = DATA / "lake"
RUNS = ROOT / "runs"
FAILURES = RUNS / "failures"
INCIDENTS = RUNS / "incidents"
REPORTS = RUNS / "reports"
OUTBOX = RUNS / "outbox"
LOGS = ROOT / "logs"
CHECKPOINT_DB = RUNS / "triage_checkpoints.sqlite"

PG_HOST = os.environ.get("PGHOST", "127.0.0.1")
PG_PORT = int(os.environ.get("PGPORT", "5433"))
PG_DB = os.environ.get("PGDATABASE", "claims")
PG_USER = os.environ.get("PGUSER", "claims")
PG_RO_USER = "triage_ro"

SOURCE_TABLES = ("payers", "patients", "encounters", "claims")


def bronze(table: str) -> str:
    return str(LAKE / "bronze" / table)


def silver(table: str) -> str:
    return str(LAKE / "silver" / table)


def gold(table: str) -> str:
    return str(LAKE / "gold" / table)


def quarantine(table: str) -> str:
    return str(LAKE / "quarantine" / table)


def ensure_dirs() -> None:
    for d in (FEEDS, LAKE, FAILURES, INCIDENTS, REPORTS, OUTBOX, LOGS):
        d.mkdir(parents=True, exist_ok=True)
