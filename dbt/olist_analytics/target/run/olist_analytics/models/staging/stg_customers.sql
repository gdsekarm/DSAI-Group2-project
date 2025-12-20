

  create or replace view `stellar-verve-478012-n6`.`olist_raw_staging`.`stg_customers`
  OPTIONS()
  as with source as (
    select * from `stellar-verve-478012-n6`.`olist_raw`.`customers`
),

renamed as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as zip_code,
        customer_city as city,
        customer_state as state
    from source
)

select * from renamed;

