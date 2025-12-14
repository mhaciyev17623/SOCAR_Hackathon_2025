
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select load_ts
from "airflow"."public"."dv_sat_readings"
where load_ts is null



  
  
      
    ) dbt_internal_test