"""Failure injection for the demo. Each command does what the real upstream mistake would do.

  python demo/inject.py feed 2026_07         clean file arrives
  python demo/inject.py patient-changes      25 patients move house (drives SCD2)
  python demo/inject.py rename-column        ALTER TABLE: payer_id -> payer_ref
  python demo/inject.py revert-column        upstream rolls that back
  python demo/inject.py null-spike 2026_09   file arrives with 20% of payer_id blanked
  python demo/inject.py reload 2026_07       the same file is loaded a second time
  python demo/inject.py resend 2026_09       corrected rows for the null spike arrive
"""
from __future__ import annotations

import csv
import random
import sys

from claims import config, db
from claims.seed import FEED_COLUMNS, load_feed


def feed(month: str) -> None:
    load_feed(config.FEEDS / f"claims_{month}.csv")


def null_spike(month: str, rate: float = 0.20) -> None:
    src = config.FEEDS / f"claims_{month}.csv"
    out_dir = config.DATA / "injected"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / src.name  # same name: the sender's corrupted file looks like any other
    rng = random.Random(42)
    with src.open(newline="") as fh, out.open("w", newline="") as w:
        reader = csv.DictReader(fh)
        writer = csv.DictWriter(w, fieldnames=FEED_COLUMNS)
        writer.writeheader()
        for row in reader:
            if rng.random() < rate:
                row["payer_id"] = ""
            writer.writerow(row)
    load_feed(out)


def resend(month: str) -> None:
    """The sender's correction: only the rows quarantined for null payer_id, with payer_id restored."""
    from claims.spark import get_spark
    spark = get_spark("resend")
    held = {r["claim_id"] for r in spark.read.format("delta").load(config.quarantine("claims"))
            .where("_quarantine_reason = 'payer_id_null_rate'").select("claim_id").collect()}
    spark.stop()
    src = config.FEEDS / f"claims_{month}.csv"
    out = config.DATA / "injected" / f"claims_{month}_resend.csv"
    with src.open(newline="") as fh, out.open("w", newline="") as w:
        reader = csv.DictReader(fh)
        writer = csv.DictWriter(w, fieldnames=FEED_COLUMNS)
        writer.writeheader()
        writer.writerows(row for row in reader if row["claim_id"] in held)
    load_feed(out)


def patient_changes(n: int = 25) -> None:
    with db.connect() as conn:
        count = conn.execute(
            """update patients set address = '100 Relocated Way', city = 'Worcester', zip = '01608'
               where patient_id in (select patient_id from patients where death_date is null
                                    order by patient_id limit %s)""", (n,),
        ).rowcount
    print(f"updated {count} patients")


def rename_column() -> None:
    with db.connect() as conn:
        conn.execute("alter table claims rename column payer_id to payer_ref")
    print("renamed claims.payer_id -> claims.payer_ref")


def revert_column() -> None:
    with db.connect() as conn:
        conn.execute("alter table claims rename column payer_ref to payer_id")
    print("renamed claims.payer_ref -> claims.payer_id")


if __name__ == "__main__":
    cmd, *args = sys.argv[1:] or ["help"]
    commands = {"feed": feed, "null-spike": null_spike, "reload": feed, "resend": resend, "patient-changes": patient_changes,
                "rename-column": rename_column, "revert-column": revert_column}
    if cmd not in commands:
        sys.exit(__doc__)
    commands[cmd](*args)
