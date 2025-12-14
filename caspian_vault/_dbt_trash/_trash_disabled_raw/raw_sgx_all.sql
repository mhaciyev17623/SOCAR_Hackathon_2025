{{ config(materialized="view") }}

-- TEMP: SGX source not available yet.
-- Keeps downstream models working by providing correct schema with no rows.
select
  cast(null as bigint) as well_id,
  cast(null as bigint) as survey_type_id,
  cast(null as double precision) as depth_ft,
  cast(null as double precision) as amplitude,
  cast(null as int) as quality_flag,
  cast(null as varchar) as source_file
where 1=0
