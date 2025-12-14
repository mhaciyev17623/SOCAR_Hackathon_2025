
    
    

with child as (
    select hk_well_survey as from_field
    from "airflow"."public"."dv_sat_readings"
    where hk_well_survey is not null
),

parent as (
    select hk_well_survey as to_field
    from "airflow"."public"."dv_link_well_survey"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


