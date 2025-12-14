{{ config(materialized='table') }}

select
  hk_well,
  source_format,
  count(*) as total_readings,
  avg(amplitude) as avg_amplitude,
  avg(case when quality_flag = 1 then 1 else 0 end) as data_quality_rate
from {{ ref('fct_readings') }}
group by 1,2
