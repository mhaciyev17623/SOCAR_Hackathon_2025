{{ config(materialized='table') }}

select distinct
    {{ hk2('well_id', 'survey_type_id') }} as hk_well_survey,
    {{ hk('well_id') }}                   as hk_well,
    {{ hk('survey_type_id') }}            as hk_survey_type,
    min(ingest_ts)                        as load_ts,
    min(source_file)                as record_source
from {{ ref('stg_readings') }}
where well_id is not null and survey_type_id is not null
group by 1,2,3
