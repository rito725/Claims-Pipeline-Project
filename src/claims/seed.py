"""Seed the OLTP source from Synthea CSVs and cut the most recent months into claim feed files.

Run:  python -m claims.seed [--feed-months 4]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import shutil
from pathlib import Path

from claims import config, db

SEED_SQL = """
truncate payers, patients, encounters, claims restart identity;
truncate etl.ingest_log, etl.watermark, etl.batch;
update etl.batch_seq set n = 0;

insert into payers (payer_id, name, ownership)
select "Id", "NAME", nullif("OWNERSHIP", '') from staging.payers;

insert into patients (patient_id, birth_date, death_date, first_name, last_name, gender, race,
                      ethnicity, address, city, state, zip, income)
select "Id", nullif("BIRTHDATE",'')::date, nullif("DEATHDATE",'')::date, "FIRST", "LAST", "GENDER",
       "RACE", "ETHNICITY", "ADDRESS", "CITY", "STATE", nullif("ZIP",''), nullif("INCOME",'')::numeric
from staging.patients;

insert into encounters (encounter_id, patient_id, payer_id, provider_id, encounter_class, code,
                        description, start_ts, stop_ts)
select "Id", "PATIENT", "PAYER", "PROVIDER", "ENCOUNTERCLASS", "CODE", "DESCRIPTION",
       "START"::timestamptz, "STOP"::timestamptz
from staging.encounters;

drop table if exists staging.claim_amounts;
create table staging.claim_amounts as
select "CLAIMID" as claim_id,
       sum(case when "TYPE" = 'CHARGE' then nullif("AMOUNT",'')::numeric else 0 end) as charge_amount,
       -- ECHECK is the insurer's remittance; CC, CASH, CHECK and COPAY are the patient paying.
       sum(case when "TYPE" = 'PAYMENT' and "METHOD" = 'ECHECK' then nullif("PAYMENTS",'')::numeric else 0 end) as payer_paid_amount,
       sum(case when "TYPE" = 'PAYMENT' and "METHOD" <> 'ECHECK' then nullif("PAYMENTS",'')::numeric else 0 end) as patient_paid_amount
from staging.claims_transactions group by 1;
create index on staging.claim_amounts (claim_id);

-- Synthea leaves PRIMARYPATIENTINSURANCEID blank for uninsured claims while the encounter carries
-- the NO_INSURANCE payer. Resolving it here gives payer_id a true 0% null baseline.
drop table if exists staging.claims_resolved;
create table staging.claims_resolved as
select c."Id" as claim_id, c."PATIENTID" as patient_id,
       coalesce(nullif(c."PRIMARYPATIENTINSURANCEID",''), e."PAYER") as payer_id,
       c."PROVIDERID" as provider_id, c."APPOINTMENTID" as encounter_id,
       c."SERVICEDATE"::timestamptz as service_date,
       nullif(c."DIAGNOSIS1",'') as diagnosis_code,
       case c."HEALTHCARECLAIMTYPEID1" when '1' then 'professional' when '2' then 'institutional' else 'other' end as claim_type,
       c."STATUS1" as status,
       coalesce(nullif(c."OUTSTANDING1",'')::numeric,0) + coalesce(nullif(c."OUTSTANDING2",'')::numeric,0)
         + coalesce(nullif(c."OUTSTANDINGP",'')::numeric,0) as outstanding_amount,
       round(coalesce(a.charge_amount,0), 2) as charge_amount,
       round(coalesce(a.payer_paid_amount,0), 2) as payer_paid_amount,
       round(coalesce(a.patient_paid_amount,0), 2) as patient_paid_amount
from staging.claims c
left join staging."encounters" e on e."Id" = c."APPOINTMENTID"
left join staging.claim_amounts a on a.claim_id = c."Id";
"""

FEED_COLUMNS = ("claim_id", "patient_id", "payer_id", "provider_id", "encounter_id", "service_date",
                "diagnosis_code", "claim_type", "status", "outstanding_amount", "charge_amount",
                "payer_paid_amount", "patient_paid_amount")


def run_schema(conn) -> None:
    conn.execute((config.ROOT / "sql" / "01_schema.sql").read_text())


def stage_csv(conn, name: str, path: Path) -> None:
    with path.open(newline="") as fh:
        header = next(csv.reader(fh))
    cols = ", ".join(f'"{c}" text' for c in header)
    conn.execute(f"drop table if exists staging.{name}")
    conn.execute(f"create unlogged table staging.{name} ({cols})")
    with conn.cursor() as cur, path.open("rb") as fh:
        with cur.copy(f"copy staging.{name} from stdin with (format csv, header true)") as copy:
            while chunk := fh.read(1 << 20):
                copy.write(chunk)
    print(f"  staged {name}")


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def load_feed(path: Path, rewrite_columns: tuple[str, ...] = FEED_COLUMNS) -> int:
    """Load one claims feed file into the source, the way an upstream SFTP drop would.

    Deliberately does not reject a file it has seen before: that is the failure the pipeline
    has to catch. The sha256 in etl.ingest_log is what lets the agent prove a double load.
    """
    with db.connect() as conn, conn.cursor() as cur:
        cols = ", ".join(rewrite_columns)
        cur.execute(f"create temp table _feed on commit preserve rows as select {cols} from claims with no data")
        with path.open("rb") as fh, cur.copy(
            f"copy _feed ({cols}) from stdin with (format csv, header true)"
        ) as copy:
            while chunk := fh.read(1 << 20):
                copy.write(chunk)
        cur.execute(
            f"insert into claims ({cols}, source_file) select {cols}, %s from _feed", (path.name,)
        )
        n = cur.rowcount
        cur.execute(
            "insert into etl.ingest_log (file_name, sha256, row_count) values (%s, %s, %s)",
            (path.name, file_sha256(path), n),
        )
    print(f"loaded {n} rows from {path.name}")
    return n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--feed-months", type=int, default=4,
                    help="most recent full months held back as feed files")
    args = ap.parse_args()
    config.ensure_dirs()

    # Seeding is a full reset: the lake and run history would otherwise point at rows that no longer exist.
    for stale in (config.LAKE, config.RUNS, config.LOGS):
        shutil.rmtree(stale, ignore_errors=True)
    config.ensure_dirs()

    with db.connect() as conn:
        conn.execute("drop table if exists claims")
        run_schema(conn)
        for name in ("payers", "patients", "encounters", "claims", "claims_transactions"):
            stage_csv(conn, name, config.RAW / f"{name}.csv")
        conn.execute(SEED_SQL)

        conn.execute("""
            drop table if exists staging.feed_months;
            create table staging.feed_months as
            select date_trunc('month', service_date at time zone 'UTC') as month, count(*) as n
            from staging.claims_resolved
            where date_trunc('month', service_date at time zone 'UTC')
                  < date_trunc('month', now() at time zone 'UTC')
            group by 1 order by 1 desc limit %d;
        """ % args.feed_months)
        months = [r[0] for r in conn.execute("select month from staging.feed_months order by month")]
        cutoff = months[0]

        cols = ", ".join(FEED_COLUMNS)
        n = conn.execute(
            f"insert into claims ({cols}, source_file) "
            f"select {cols}, 'initial_snapshot.csv' from staging.claims_resolved "
            f"where (service_date at time zone 'UTC') < %s", (cutoff,),
        ).rowcount
        print(f"seeded {n} claims before {cutoff:%Y-%m}")

        config.FEEDS.mkdir(parents=True, exist_ok=True)
        for old in config.FEEDS.glob("claims_*.csv"):
            old.unlink()
        for month in months:
            path = config.FEEDS / f"claims_{month:%Y_%m}.csv"
            with path.open("wb") as fh, conn.cursor() as cur:
                query = (
                    f"copy (select {cols} from staging.claims_resolved "
                    f"where date_trunc('month', service_date at time zone 'UTC') = "
                    f"'{month:%Y-%m-%d}' order by claim_id) to stdout with (format csv, header true)"
                )
                with cur.copy(query) as copy:
                    for chunk in copy:
                        fh.write(chunk)
            print(f"wrote feed {path.name}")
        # Staging was only needed to build the above, and it is by far the largest thing here.
        conn.execute("drop schema staging cascade")
        conn.execute("create schema staging")
        conn.execute("analyze")
        conn.execute("grant select on all tables in schema public, etl to triage_ro")


if __name__ == "__main__":
    main()
