
  
    

  create  table "airflow"."public"."dv_sat_readings__dbt_tmp"
  
  
    as
  
  (
    

select
    md5(cast(well_id as varchar) || '|' || cast(survey_type_id as varchar)) as hk_well_survey,
    ingest_ts                               as load_ts,
    source_file                             as record_source,
    source_format,
    row_checksum,
    depth_ft,
    amplitude,
    quality_flag,
    -- hashdiff over descriptive attributes
    md5(
      cast(depth_ft as varchar) || '|' ||
      cast(amplitude as varchar) || '|' ||
      cast(quality_flag as varchar) || '|' ||
      source_format
    ) as hashdiff
from "airflow"."public"."stg_readings"
where well_id is not null and survey_type_id is not null
  );
  