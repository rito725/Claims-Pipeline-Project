"""Batch lifecycle and watermarks, stored in Postgres so they survive any Spark crash."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from psycopg.types.json import Json

from claims import config, db

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def open_batch() -> str:
    """Return the open batch if one exists (a failed run being retried), else start a new one."""
    with db.connect() as conn:
        row = conn.execute("select batch_id from etl.batch where status = 'open' order by opened_at limit 1").fetchone()
        if row:
            return row[0]
        n = conn.execute("update etl.batch_seq set n = n + 1 returning n").fetchone()[0]
        batch_id = f"b{n:05d}"
        conn.execute("insert into etl.batch (batch_id) values (%s)", (batch_id,))
        return batch_id


def current_batch() -> str:
    with db.connect() as conn:
        row = conn.execute("select batch_id from etl.batch where status = 'open' order by opened_at limit 1").fetchone()
    if not row:
        raise RuntimeError("no open batch; run the extract_bronze stage first")
    return row[0]


def watermark(table: str) -> datetime:
    with db.connect() as conn:
        row = conn.execute("select high_watermark from etl.watermark where table_name = %s", (table,)).fetchone()
    return row[0] if row else EPOCH


def source_high(table: str) -> datetime | None:
    with db.connect() as conn:
        return conn.execute(f"select max(updated_at) from {table}").fetchone()[0]


def record_extract(batch_id: str, highs: dict[str, datetime], counts: dict[str, int]) -> None:
    with db.connect() as conn:
        conn.execute(
            "update etl.batch set high_watermarks = %s, row_counts = %s where batch_id = %s",
            (Json({k: v.isoformat() for k, v in highs.items()}), Json(counts), batch_id),
        )


def commit_batch(batch_id: str) -> None:
    """Advance watermarks and close the batch in one transaction. Runs only after every gate passes."""
    with db.connect(autocommit=False) as conn:
        highs = conn.execute("select high_watermarks from etl.batch where batch_id = %s", (batch_id,)).fetchone()[0]
        for table, high in highs.items():
            conn.execute(
                """insert into etl.watermark (table_name, high_watermark) values (%s, %s)
                   on conflict (table_name) do update set high_watermark = excluded.high_watermark""",
                (table, high),
            )
        conn.execute("update etl.batch set status = 'committed', closed_at = clock_timestamp() where batch_id = %s", (batch_id,))
        conn.commit()
