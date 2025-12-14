
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "airflow"."public"."stg_readings"

where not(well_id is not null)


  
  
      
    ) dbt_internal_test