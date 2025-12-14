docker compose exec postgres psql -U airflow -d airflow <<'SQL'
drop table if exists public.raw_sgx_all cascade;
drop table if exists public.raw_recovered_1 cascade;
drop table if exists public.raw_recovered_2 cascade;

create table public.raw_sgx_all (
    well_id bigint,
    survey_type_id bigint,
    depth_ft double precision,
    amplitude double precision,
    quality_flag int,
    source_file text
);

create table public.raw_recovered_1 (
    well_id bigint,
    survey_type_id bigint,
    depth_ft double precision,
    amplitude double precision,
    quality_flag int,
    source_file text
);

create table public.raw_recovered_2 (
    well_id bigint,
    survey_type_id bigint,
    depth_ft double precision,
    amplitude double precision,
    quality_flag int,
    source_file text
);
SQL
