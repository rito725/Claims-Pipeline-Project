"""The agent's investigation tools. All are read-only; state changes happen only in actions.py,
behind human approval.

Tool output is JSON text. Column and table names are checked against real schemas, and SQL runs
under a role that holds nothing but SELECT, so a bad model decision cannot change the data.
"""
from __future__ import annotations

import json
import re
from typing import Any

from langchain_core.tools import tool
from pyspark.sql import functions as F

from claims import config, db

SPARK_LAYERS = {"bronze": config.bronze, "silver": config.silver, "gold": config.gold}
MAX_SQL_ROWS = 50
SAMPLE_COLUMNS = ["row_id", "claim_id", "patient_id", "payer_id", "source_file", "updated_at", "charge_amount"]


def to_json(obj: Any) -> str:
    return json.dumps(obj, default=str, indent=1)


def pg_columns(table: str) -> list[tuple[str, str]]:
    with db.connect_readonly() as conn:
        rows = conn.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_schema = 'public' and table_name = %s order by ordinal_position", (table,),
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


class Toolbox:
    """Holds the failure under investigation and a lazily started Spark session."""

    def __init__(self, failure: dict):
        self.failure = failure
        self._spark = None

    @property
    def spark(self):
        if self._spark is None:
            from claims.spark import get_spark
            self._spark = get_spark("claims-triage")
        return self._spark

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    def _delta(self, ref: str):
        layer, _, table = ref.partition(".")
        if layer not in SPARK_LAYERS or not table.replace("_", "").isalnum():
            raise ValueError(f"unknown table reference {ref!r}; use bronze.claims, silver.claims, gold.fact_claims")
        return self.spark.read.format("delta").load(SPARK_LAYERS[layer](table))

    def _schema(self, ref: str) -> list[tuple[str, str]]:
        layer, _, table = ref.partition(".")
        if layer == "postgres":
            cols = pg_columns(table)
            if not cols:
                raise ValueError(f"no such source table {table!r}")
            return cols
        return [(f.name, f.dataType.simpleString()) for f in self._delta(ref).schema.fields]

    # ---- tool bodies -------------------------------------------------------------------

    def profile_column(self, table: str, column: str) -> dict:
        batch_id = self.failure["batch_id"]
        layer, _, name = table.partition(".")
        if layer == "postgres":
            if column not in [c for c, _ in pg_columns(name)]:
                return {"error": f"{column} not in {table}", "columns": [c for c, _ in pg_columns(name)]}
            with db.connect_readonly() as conn:
                n, nulls, distinct = conn.execute(
                    f'select count(*), count(*) filter (where "{column}" is null), count(distinct "{column}") from {name}'
                ).fetchone()
            return {"table": table, "column": column, "rows": n, "nulls": nulls,
                    "null_pct": round(100 * nulls / max(n, 1), 2), "distinct": distinct}

        df = self._delta(table)
        if column not in df.columns:
            return {"error": f"{column} not in {table}", "columns": df.columns}

        def stats(frame) -> dict:
            row = frame.agg(F.count("*").alias("n"), F.sum(F.col(column).isNull().cast("int")).alias("nulls"),
                            F.approx_count_distinct(column).alias("distinct")).first()
            n, nulls = row["n"], row["nulls"] or 0
            return {"rows": n, "nulls": nulls, "null_pct": round(100 * nulls / max(n, 1), 2), "distinct": row["distinct"]}

        result: dict = {"table": table, "column": column}
        if "_batch_id" in df.columns:
            batch = df.where(F.col("_batch_id") == batch_id)
            result["current_batch"] = {"batch_id": batch_id, **stats(batch)}
            result["baseline_prior_batches"] = stats(df.where(F.col("_batch_id") != batch_id))
            top = batch.groupBy(column).count().orderBy(F.desc("count")).limit(5).collect()
        else:
            result["all_rows"] = stats(df)
            top = df.groupBy(column).count().orderBy(F.desc("count")).limit(5).collect()
        result["top_values"] = [{"value": r[column], "count": r["count"]} for r in top]
        return result

    def sample_bad_rows(self, check_name: str, limit: int = 5) -> dict:
        from claims.quality import suites

        check = next((c for c in self.failure["checks"] if c["name"] == check_name), None)
        if check is None:
            return {"error": f"unknown check {check_name!r}", "failed_checks": [c["name"] for c in self.failure["checks"]]}
        if not check.get("bad_row_sql"):
            return {"error": f"{check_name} is a table-level check and has no row predicate"}
        bad = suites.bronze_claims_frame(self.spark, self.failure["batch_id"]).where(check["bad_row_sql"]).cache()
        total = bad.count()
        rows = bad.select(*SAMPLE_COLUMNS).limit(min(limit, 20)).collect()
        by_file = bad.groupBy("source_file").count().orderBy(F.desc("count")).collect()
        charged = bad.agg(F.sum("charge_amount")).first()[0]
        result = {"check": check_name, "rows_matching": total, "charge_amount_affected": charged,
                  "by_source_file": {r["source_file"]: r["count"] for r in by_file},
                  "sample": [r.asDict() for r in rows]}
        bad.unpersist()
        return result

    def compare_schema(self, source: str, target: str) -> dict:
        src, tgt = self._schema(source), self._schema(target)
        ignore = lambda c: c.startswith("_") or c == "is_duplicate"
        src = [c for c in src if not ignore(c[0])]
        tgt = [c for c in tgt if not ignore(c[0])]
        src_names, tgt_names = [c for c, _ in src], [c for c, _ in tgt]
        only_src = [c for c in src_names if c not in tgt_names]
        only_tgt = [c for c in tgt_names if c not in src_names]
        result: dict = {"source": source, "target": target, "only_in_source": only_src, "only_in_target": only_tgt}

        # A column missing on one side and a new one on the other is only a *candidate* rename.
        # Overlapping values are what turn it into evidence.
        if source.startswith("postgres.") and only_src and only_tgt:
            table = source.split(".", 1)[1]
            renames = []
            target_df = self._delta(target)
            for new, old in zip(only_src, only_tgt):
                target_vals = {r[0] for r in target_df.select(old).distinct().limit(1000).collect()}
                with db.connect_readonly() as conn:
                    source_vals = {r[0] for r in conn.execute(
                        f'select distinct "{new}" from {table} where "{new}" is not null limit 1000').fetchall()}
                overlap = len(source_vals & target_vals)
                renames.append({"target_column": old, "source_column": new, "ordinal_source": src_names.index(new),
                                "ordinal_target": tgt_names.index(old),
                                "shared_values": overlap, "source_distinct": len(source_vals)})
            result["likely_renames"] = renames
        return result

    def query_postgres(self, sql: str) -> dict:
        statement = sql.strip().rstrip(";").strip()
        if ";" in statement or not re.match(r"(?is)^(select|with)\b", statement):
            return {"error": "only a single SELECT statement is allowed"}
        try:
            with db.connect_readonly() as conn:
                cur = conn.execute(statement)
                columns = [d.name for d in cur.description]
                rows = cur.fetchmany(MAX_SQL_ROWS + 1)
        except Exception as exc:  # noqa: BLE001 - return the error so the model can correct itself
            return {"error": f"{type(exc).__name__}: {str(exc)[:300]}"}
        return {"columns": columns, "rows": [dict(zip(columns, r)) for r in rows[:MAX_SQL_ROWS]],
                "truncated": len(rows) > MAX_SQL_ROWS}


def build_tools(box: Toolbox) -> list:
    @tool
    def profile_column(table: str, column: str) -> str:
        """Profile one column: rows, nulls, null %, distinct count, top values. For bronze tables
        it compares the failing batch with all prior batches. `table` is a reference such as
        bronze.claims, silver.claims, gold.fact_claims or postgres.claims."""
        return to_json(box.profile_column(table, column))

    @tool
    def sample_bad_rows(check_name: str, limit: int = 5) -> str:
        """Return rows rejected by a failed row-level check, plus the total count, the charge
        amount affected and a breakdown by source file."""
        return to_json(box.sample_bad_rows(check_name, limit))

    @tool
    def compare_schema(source: str, target: str) -> str:
        """Diff two schemas, e.g. source='postgres.claims', target='bronze.claims'. Reports columns
        only on one side, and for postgres sources tests candidate renames by value overlap."""
        return to_json(box.compare_schema(source, target))

    @tool
    def query_postgres(sql: str) -> str:
        """Run one read-only SELECT on the source database (max 50 rows). Useful tables: claims,
        patients, etl.ingest_log (file_name, sha256, row_count, loaded_at), etl.batch, etl.watermark."""
        return to_json(box.query_postgres(sql))

    return [profile_column, sample_bad_rows, compare_schema, query_postgres]
