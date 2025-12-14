
  
    

  create  table "airflow"."public"."stg_readings__dbt_tmp"
  
  
    as
  
  (
    

with sgx as (
    select
        cast(well_id as bigint)            as well_id,
        cast(survey_type_id as bigint)     as survey_type_id,
        cast(depth_ft as double precision)           as depth_ft,
        cast(amplitude as double precision)          as amplitude,
        cast(quality_flag as int)          as quality_flag,
        'sgx'                               as source_format,
        cast(source_file as varchar)        as source_file
    from "airflow"."public"."raw_sgx_all"
),
p1 as (
    -- If columns differ, adjust here after you inspect schema
    select
        cast(well_id as bigint)            as well_id,
        cast(survey_type_id as bigint)     as survey_type_id,
        cast(depth_ft as double precision)           as depth_ft,
        cast(amplitude as double precision)          as amplitude,
        cast(quality_flag as int)          as quality_flag,
        'parquet_recovered'                as source_format,
        'archive_batch_seismic_readings.parquet' as source_file
    from "airflow"."public"."raw_recovered_1"
),
p2 as (
    select
        cast(well_id as bigint)            as well_id,
        cast(survey_type_id as bigint)     as survey_type_id,
        cast(depth_ft as double precision)           as depth_ft,
        cast(amplitude as double precision)          as amplitude,
        cast(quality_flag as int)          as quality_flag,
        'parquet_recovered'                as source_format,
        'archive_batch_seismic_readings_2.parquet' as source_file
    from "airflow"."public"."raw_recovered_2"
),

unioned as (
    select * from sgx
    union all
    select * from p1
    union all
    select * from p2
)

select
    *,
    current_timestamp as ingest_ts,
    -- simple deterministic checksum (DuckDB supports md5)
    md5(
      cast(well_id as varchar) || '|' ||
      cast(survey_type_id as varchar) || '|' ||
      cast(depth_ft as varchar) || '|' ||
      cast(amplitude as varchar) || '|' ||
      cast(quality_flag as varchar) || '|' ||
      source_file || '|' || source_format
    ) as row_checksum
from unioned
  );
  