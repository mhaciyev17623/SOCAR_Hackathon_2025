{{ config(materialized='table') }}

select
    {{ hk2('well_id', 'survey_type_id') }} as hk_well_survey,
    ingest_ts                               as load_ts,
    source_file                             as record_source,
    source_format,
    row_checksum,
    depth_ft,
    amplitude,
    quality_flag,
    -- hashdiff over descriptive attributes
    md5(
      cast(depth_ft as varchar) || '|' ||
      cast(amplitude as varchar) || '|' ||
      cast(quality_flag as varchar) || '|' ||
      source_format
    ) as hashdiff
from {{ ref('stg_readings') }}
where well_id is not null and survey_type_id is not null
