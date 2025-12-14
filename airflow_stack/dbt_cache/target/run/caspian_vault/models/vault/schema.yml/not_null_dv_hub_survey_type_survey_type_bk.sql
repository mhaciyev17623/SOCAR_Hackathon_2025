
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select survey_type_bk
from "airflow"."public"."dv_hub_survey_type"
where survey_type_bk is null



  
  
      
    ) dbt_internal_test