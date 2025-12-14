
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hk_survey_type
from "airflow"."public"."dv_hub_survey_type"
where hk_survey_type is null



  
  
      
    ) dbt_internal_test