"""Postgres helpers. `connect()` is the pipeline's read/write role; `connect_readonly()` is
the role handed to the triage agent, which has SELECT on a fixed set of tables and nothing else."""
from __future__ import annotations

import psycopg

from claims import config


def connect(autocommit: bool = True) -> psycopg.Connection:
    return psycopg.connect(
        host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DB,
        user=config.PG_USER, autocommit=autocommit,
    )


def connect_readonly() -> psycopg.Connection:
    conn = psycopg.connect(
        host=config.PG_HOST, port=config.PG_PORT, dbname=config.PG_DB,
        user=config.PG_RO_USER, autocommit=True,
        options="-c default_transaction_read_only=on -c statement_timeout=10000",
    )
    return conn


def jdbc_url() -> str:
    return f"jdbc:postgresql://{config.PG_HOST}:{config.PG_PORT}/{config.PG_DB}"


def jdbc_props() -> dict[str, str]:
    return {"user": config.PG_USER, "driver": "org.postgresql.Driver"}
