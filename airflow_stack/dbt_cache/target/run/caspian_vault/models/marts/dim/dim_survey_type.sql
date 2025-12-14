
  
    

  create  table "airflow"."public"."dim_survey_type__dbt_tmp"
  
  
    as
  
  (
    

select
  hk_survey_type,
  survey_type_bk as survey_type_id,
  load_ts,
  record_source
from "airflow"."public"."dv_hub_survey_type"
  );
  