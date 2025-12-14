{{ config(materialized="view") }}

-- TEMP: recovered sources not available yet.
-- Provides correct columns, returns 0 rows, unblocks downstream.
select
  cast(null as bigint)            as well_id,
  cast(null as bigint)            as survey_type_id,
  cast(null as double precision)  as depth_ft,
  cast(null as double precision)  as amplitude,
  cast(null as int)               as quality_flag
where 1=0
