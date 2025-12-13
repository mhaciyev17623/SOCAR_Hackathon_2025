{{ config(materialized='table') }}

select
  hk_survey_type,
  survey_type_bk as survey_type_id,
  load_ts,
  record_source
from {{ ref('dv_hub_survey_type') }}
