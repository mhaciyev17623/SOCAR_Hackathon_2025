{{ config(materialized='table') }}

select distinct
    {{ hk('survey_type_id') }} as hk_survey_type,
    survey_type_id             as survey_type_bk,
    min(ingest_ts)             as load_ts,
    min(source_file)     as record_source
from {{ ref('stg_readings') }}
where survey_type_id is not null
group by 1,2
