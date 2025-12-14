
  
    

  create  table "airflow"."public"."dim_source_format__dbt_tmp"
  
  
    as
  
  (
    

select distinct
  source_format,
  md5(source_format) as hk_source_format
from "airflow"."public"."dv_sat_readings"
  );
  