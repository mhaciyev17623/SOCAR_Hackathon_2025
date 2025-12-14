
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hk_well
from "airflow"."public"."dv_hub_well"
where hk_well is null



  
  
      
    ) dbt_internal_test