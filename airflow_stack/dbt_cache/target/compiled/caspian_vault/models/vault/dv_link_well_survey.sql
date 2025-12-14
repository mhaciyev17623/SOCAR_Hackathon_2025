

select distinct
    md5(cast(well_id as varchar) || '|' || cast(survey_type_id as varchar)) as hk_well_survey,
    md5(cast(well_id as varchar))                   as hk_well,
    md5(cast(survey_type_id as varchar))            as hk_survey_type,
    min(ingest_ts)                        as load_ts,
    min(source_file)                as record_source
from "airflow"."public"."stg_readings"
where well_id is not null and survey_type_id is not null
group by 1,2,3