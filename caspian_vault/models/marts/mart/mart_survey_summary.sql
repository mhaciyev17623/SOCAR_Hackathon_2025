{{ config(materialized='table') }}

select
  hk_survey_type,
  source_format,
  count(distinct hk_well) as wells_surveyed,
  count(*) as total_readings,
  avg(amplitude) as avg_amplitude,
  min(load_ts) as first_ingest_ts,
  max(load_ts) as last_ingest_ts
from {{ ref('fct_readings') }}
group by 1,2
