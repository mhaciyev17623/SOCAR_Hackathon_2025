
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hk_well_survey
from "airflow"."public"."dv_link_well_survey"
where hk_well_survey is null



  
  
      
    ) dbt_internal_test