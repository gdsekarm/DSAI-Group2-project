

  create or replace view `stellar-verve-478012-n6`.`olist_raw_staging`.`stg_sellers`
  OPTIONS()
  as with source as (
    select * from `stellar-verve-478012-n6`.`olist_raw`.`sellers`
),

renamed as (
    select
        seller_id,
        seller_zip_code_prefix as zip_code,
        seller_city as city,
        seller_state as state
    from source
)

select * from renamed;

