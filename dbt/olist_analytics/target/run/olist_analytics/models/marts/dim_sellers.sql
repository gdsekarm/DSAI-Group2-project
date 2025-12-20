
  
    

    create or replace table `stellar-verve-478012-n6`.`olist_raw_analytics`.`dim_sellers`
      
    
    

    OPTIONS()
    as (
      with sellers as (
    select * from `stellar-verve-478012-n6`.`olist_raw_staging`.`stg_sellers`
)

select
    seller_id,
    zip_code,
    city,
    state
from sellers
    );
  