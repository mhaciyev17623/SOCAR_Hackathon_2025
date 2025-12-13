{{ config(materialized='table') }}

select distinct
  source_format,
  md5(source_format) as hk_source_format
from {{ ref('dv_sat_readings') }}
