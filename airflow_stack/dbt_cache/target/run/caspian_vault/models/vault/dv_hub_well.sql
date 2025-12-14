
  
    

  create  table "airflow"."public"."dv_hub_well__dbt_tmp"
  
  
    as
  
  (
    

select distinct
    md5(cast(well_id as varchar)) as hk_well,
    well_id             as well_id_bk,
    min(ingest_ts)      as load_ts,
    min(source_file) as record_source
from "airflow"."public"."stg_readings"
where well_id is not null
group by 1,2
  );
  