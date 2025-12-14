{{ config(materialized='table') }}

select distinct
    {{ hk('well_id') }} as hk_well,
    well_id             as well_id_bk,
    min(ingest_ts)      as load_ts,
    min(source_file) as record_source
from {{ ref('stg_readings') }}
where well_id is not null
group by 1,2
