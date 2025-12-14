
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select row_checksum
from "airflow"."public"."dv_sat_readings"
where row_checksum is null



  
  
      
    ) dbt_internal_test