
  
    

  create  table "airflow"."public"."dv_hub_survey_type__dbt_tmp"
  
  
    as
  
  (
    

select distinct
    md5(cast(survey_type_id as varchar)) as hk_survey_type,
    survey_type_id             as survey_type_bk,
    min(ingest_ts)             as load_ts,
    min(source_file)     as record_source
from "airflow"."public"."stg_readings"
where survey_type_id is not null
group by 1,2
  );
  