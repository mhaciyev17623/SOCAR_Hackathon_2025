{{ config(materialized='table') }}

-- If you don't have real lat/lon in the raw data, we generate deterministic pseudo coords
-- within a Caspian-ish bounding box so the map can still work for the demo.
with base as (
  select
    hk_well,
    well_id_bk as well_id,
    load_ts,
    record_source
  from {{ ref('dv_hub_well') }}
),
geo as (
  select
    *,
    -- deterministic pseudo geo: map well_id -> lat/lon
    38.0 + (abs(abs(('x' || substr(md5(cast(well_id as text)), 1, 8))::bit(32)::int)) % 6000) / 1000.0 as lat,   -- 38..44
47.0 + (abs((('x' || substr(md5('x' || cast(well_id as text)), 1, 8))::bit(32)::int)) % 7000) / 1000.0 as lon -- 47..54
  from base
)
select * from geo
