
    
    

select
    hk_survey_type as unique_field,
    count(*) as n_records

from "airflow"."public"."dv_hub_survey_type"
where hk_survey_type is not null
group by hk_survey_type
having count(*) > 1


