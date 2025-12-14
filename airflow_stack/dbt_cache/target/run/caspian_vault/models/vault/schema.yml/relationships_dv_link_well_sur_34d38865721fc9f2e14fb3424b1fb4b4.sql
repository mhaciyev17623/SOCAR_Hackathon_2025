
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select hk_well as from_field
    from "airflow"."public"."dv_link_well_survey"
    where hk_well is not null
),

parent as (
    select hk_well as to_field
    from "airflow"."public"."dv_hub_well"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test