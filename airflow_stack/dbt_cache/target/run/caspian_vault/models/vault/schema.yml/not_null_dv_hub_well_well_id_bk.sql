
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select well_id_bk
from "airflow"."public"."dv_hub_well"
where well_id_bk is null



  
  
      
    ) dbt_internal_test