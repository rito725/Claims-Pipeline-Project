-- Source OLTP plus ETL control tables. Idempotent.
create schema if not exists staging;
create schema if not exists etl;

create or replace function touch_updated_at() returns trigger language plpgsql as $$
begin new.updated_at := clock_timestamp(); return new; end $$;

create table if not exists payers (
  payer_id text primary key, name text not null, ownership text,
  updated_at timestamptz not null default clock_timestamp()
);
create table if not exists patients (
  patient_id text primary key, birth_date date, death_date date,
  first_name text, last_name text, gender text, race text, ethnicity text,
  address text, city text, state text, zip text, income numeric,
  updated_at timestamptz not null default clock_timestamp()
);
create table if not exists encounters (
  encounter_id text primary key, patient_id text not null, payer_id text,
  provider_id text, encounter_class text, code text, description text,
  start_ts timestamptz, stop_ts timestamptz,
  updated_at timestamptz not null default clock_timestamp()
);
-- Claims arrive as files, so this is an append-style landing table: no unique constraint on
-- claim_id. A file loaded twice really does produce duplicates here, as in production feeds.
create table if not exists claims (
  row_id bigserial primary key,
  claim_id text not null, patient_id text not null, payer_id text,
  provider_id text, encounter_id text, service_date timestamptz,
  diagnosis_code text, claim_type text, status text,
  outstanding_amount numeric(14,2), charge_amount numeric(14,2),
  payer_paid_amount numeric(14,2), patient_paid_amount numeric(14,2),
  source_file text not null,
  updated_at timestamptz not null default clock_timestamp()
);
create index if not exists claims_updated_at_idx on claims (updated_at);
create index if not exists patients_updated_at_idx on patients (updated_at);
create index if not exists encounters_updated_at_idx on encounters (updated_at);
create index if not exists payers_updated_at_idx on payers (updated_at);

do $$ begin
  create trigger patients_touch before update on patients for each row execute function touch_updated_at();
exception when duplicate_object then null; end $$;
do $$ begin
  create trigger claims_touch before update on claims for each row execute function touch_updated_at();
exception when duplicate_object then null; end $$;

create table if not exists etl.ingest_log (
  id bigserial primary key, file_name text not null, sha256 text not null,
  row_count bigint not null, loaded_at timestamptz not null default clock_timestamp()
);
create table if not exists etl.watermark (
  table_name text primary key, high_watermark timestamptz not null
);
-- One row per pipeline batch. A failed batch stays 'open' and the next run reuses its id, so
-- bronze partitions get replaced rather than duplicated and watermarks only move on success.
create table if not exists etl.batch (
  batch_id text primary key, status text not null default 'open',
  opened_at timestamptz not null default clock_timestamp(), closed_at timestamptz,
  high_watermarks jsonb not null default '{}', row_counts jsonb not null default '{}'
);
create table if not exists etl.batch_seq (n bigint not null);
insert into etl.batch_seq select 0 where not exists (select 1 from etl.batch_seq);

-- Read-only role for the triage agent's query_postgres tool.
do $$ begin
  create role triage_ro login;
exception when duplicate_object then null; end $$;
grant usage on schema public, etl to triage_ro;
grant select on all tables in schema public, etl to triage_ro;
