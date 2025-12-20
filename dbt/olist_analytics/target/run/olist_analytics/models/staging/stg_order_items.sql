

  create or replace view `stellar-verve-478012-n6`.`olist_raw_staging`.`stg_order_items`
  OPTIONS()
  as with source as (
    select * from `stellar-verve-478012-n6`.`olist_raw`.`order_items`
),

renamed as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp) as shipping_limit_date,
        price,
        freight_value
    from source
)

select * from renamed;

