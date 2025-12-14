
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    hk_well as unique_field,
    count(*) as n_records

from "airflow"."public"."dv_hub_well"
where hk_well is not null
group by hk_well
having count(*) > 1



  
  
      
    ) dbt_internal_test