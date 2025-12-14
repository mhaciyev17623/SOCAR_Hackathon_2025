
  
    

  create  table "airflow"."public"."mart_sensor_analysis__dbt_tmp"
  
  
    as
  
  (
    

-- treating survey_type as sensor type (only available categorical dimension)
select
  hk_survey_type as hk_sensor_type,
  count(*) as total_readings,
  avg(case when quality_flag = 1 then 1 else 0 end) as data_quality_rate,
  avg(amplitude) as avg_amplitude
from "airflow"."public"."fct_readings"
group by 1
  );
  