
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    hk_well_survey as unique_field,
    count(*) as n_records

from "airflow"."public"."dv_link_well_survey"
where hk_well_survey is not null
group by hk_well_survey
having count(*) > 1



  
  
      
    ) dbt_internal_test