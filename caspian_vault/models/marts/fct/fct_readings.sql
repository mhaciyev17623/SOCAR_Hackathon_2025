{{ config(materialized='table') }}

with sat as (
  select
    hk_well_survey,
    load_ts,
    record_source,
    source_format,
    row_checksum,
    depth_ft,
    amplitude,
    quality_flag,
    hashdiff
  from {{ ref('dv_sat_readings') }}
),
lnk as (
  select
    hk_well_survey,
    hk_well,
    hk_survey_type
  from {{ ref('dv_link_well_survey') }}
)
select
  sat.hk_well_survey,
  lnk.hk_well,
  lnk.hk_survey_type,
  sat.load_ts,
  sat.record_source,
  sat.source_format,
  sat.row_checksum,
  sat.depth_ft,
  sat.amplitude,
  sat.quality_flag,
  sat.hashdiff
from sat
join lnk using (hk_well_survey)
